package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
) // 日志
// 网络
// 内核调用
//rpc库
//http库
//互斥锁
//时间戳

// 超时阈值：任务分配后超过该时长仍未完成，视为 worker 失败，重新分配
const TaskTimeout = 10 * time.Second

// 守护 goroutine 的扫描间隔。实际超时为 TaskTimeout ~ TaskTimeout+TaskCheckInterval
const TaskCheckInterval = 1 * time.Second

// 任务状态
const (
	StatePending = iota // 在待分配链表上
	StateRunning        // 在已分配链表上
)

// 任务类型
const (
	TaskMap = iota
	TaskReduce
)

// node 是一个任务块。map 和 reduce 任务共用同一套链表，用 kind 区分。
// 一次任务分配对应一个独立的 node：任务被取出重分配时，旧 node 从已分配链表
// 摘除并回收，新 node 挂到待分配链表尾部，因此一个任务在链表中至多存在一份。
type node struct {
	prev, next *node

	kind int // TaskMap / TaskReduce
	id   int // map: 输入文件在 files 中的下标; reduce: reduceID
	// map 任务：输入文件名；reduce 任务：要归并的中间文件（上报时才填充，可能为 nil）
	fileName string
	files    []string

	nReduce    int
	startTime  time.Time // 进入已分配链表的时间戳
	state      int       // StatePending / StateRunning
	generation int       // 分配代数，用于丢弃过期 worker 的上报
}

// Coordinator 管理全部任务的分配、完成与超时回收。
type Coordinator struct {
	mu sync.Mutex

	files     []string // 全部输入文件
	nReduce   int      // reduce 分区数
	mapNum    int      // map 任务总数 == len(files)
	reduceNum int      // reduce 任务总数 == nReduce

	mapDone    map[string]bool  // 已完成的 map 任务（输入文件名集合），用于上报去重
	mapTask    map[string]*node // 输入文件名 -> 该 map 任务的当前 node
	reduceTask map[int]*node    // reduceID -> 该 reduce 任务的当前 node
	mapFiles   map[int][]string // reduceID -> 该分区已产出的全部中间文件

	// 每个任务的分配代数。node 会被回收重建，代数挂在 Coordinator 上才不会被
	// 重复执行的 worker 误认领（两个 worker 上报时读到同一个 node 会读到同一代数）。
	generation map[string]int // map: 输入文件名 -> 代数
	reduceGen  map[int]int    // reduceID -> 代数

	reducesReady bool // 是否已投放 reduce 任务
	done         bool // 全部任务是否完成

	pending *nodeList // 待分配
	running *nodeList // 已分配（进行中）

	// 复用的 node 对象池：从已分配链表摘除的 node 回到这里重建，避免反复堆分配
	free []*node
}

// nodeList 是带哨兵头尾节点的双向链表，插入恒为 O(1)。
type nodeList struct {
	head, tail *node
	size       int
}

func newNodeList() *nodeList {
	h := &node{}
	t := &node{}
	h.next = t
	t.prev = h
	return &nodeList{head: h, tail: t}
}

func (l *nodeList) pushBack(n *node) {
	n.prev = l.tail.prev
	n.next = l.tail
	l.tail.prev.next = n
	l.tail.prev = n
	l.size++
}

func (l *nodeList) remove(n *node) {
	n.prev.next = n.next
	n.next.prev = n.prev
	n.prev = nil
	n.next = nil
	l.size--
}

func (l *nodeList) empty() bool { return l.size == 0 }

// Your code here -- RPC handlers for the worker to call.

// GetTask 是 worker 申请任务时调用的 RPC：返回 map / reduce 任务，
// 或告知 worker 等待、退出。
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 阶段切换：所有 map 任务都已完成（不是"全部被领走"），才投放 reduce 任务。
	if !c.reducesReady && len(c.mapDone) == c.mapNum {
		c.startReducePhase()
	}

	// 任务全部完成：通知 worker 退出
	if c.reducesReady && c.pending.empty() && c.running.empty() {
		c.done = true
		reply.Kind = ExitTask
		return nil
	}

	if c.pending.empty() {
		reply.Kind = WaitTask
		return nil
	}

	// 取待分配链表头部，填入时间戳并挂到已分配链表尾部
	n := c.pending.head.next
	c.pending.remove(n)
	n.state = StateRunning
	n.startTime = time.Now()
	c.running.pushBack(n)

	// worker 必须全量拷贝任务数据，执行期间不再读取 coordinator 侧的任何结构
	reply.NReduce = n.nReduce
	if n.kind == TaskMap {
		reply.Kind = MapTask
		reply.MapFile = n.fileName
	} else {
		reply.Kind = ReduceTask
		reply.ReduceID = n.id
		reply.Files = n.files
	}
	return nil
}

// ReportMap 处理 map 任务完成的上报。
func (c *Coordinator) ReportMap(args *ReportMapArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 去重：已经完成过的 map 任务，丢弃迟到的重复上报
	if c.mapDone[args.MapFile] {
		return nil
	}
	n := c.mapTask[args.MapFile]
	// 过期上报：该任务已被重分配（代数不匹配）或状态不一致，丢弃
	if n == nil || n.state != StateRunning || n.generation != c.generation[args.MapFile] {
		return nil
	}

	c.running.remove(n)
	c.recycle(n)
	c.mapDone[args.MapFile] = true
	// 追加该 map 产出的中间文件。这里不能做 os.Stat 存在性检查：中间文件名是
	// worker 工作目录下的相对路径，而 coordinator 的工作目录通常不是同一个，
	// 在 coordinator 侧 stat 会全部失败，导致 reduce 拿不到任何输入。
	// 文件名由 worker 在上报前创建完毕，直接信任上报内容即可；确实为空的
	// 分区文件由 reduce 侧按大小跳过。
	for reduceID, f := range args.Files {
		if f == "" {
			continue
		}
		c.mapFiles[reduceID] = append(c.mapFiles[reduceID], f)
	}
	return nil
}

// ReportReduce 处理 reduce 任务完成的上报。
func (c *Coordinator) ReportReduce(args *ReportReduceArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	n := c.reduceTask[args.ReduceID]
	if n == nil || n.state != StateRunning || n.generation != c.reduceGen[args.ReduceID] {
		return nil
	}
	c.running.remove(n)
	c.recycle(n)
	delete(c.reduceTask, args.ReduceID)
	return nil
}

// startReducePhase 在所有 map 任务完成后，按分区快照出 reduce 任务并投放。
// 调用者必须持有 c.mu。
func (c *Coordinator) startReducePhase() {
	c.reducesReady = true
	for rid := 0; rid < c.reduceNum; rid++ {
		// 必须复制一份：mapFiles 后续不会再变动，但快照隔离避免共享底层数组
		var files []string
		if fs := c.mapFiles[rid]; len(fs) > 0 {
			files = make([]string, len(fs))
			copy(files, fs)
		}
		n := c.alloc(TaskReduce, rid, "", files)
		c.reduceTask[rid] = n
		c.pending.pushBack(n)
	}
}

// alloc 从对象池取一个 node 重新初始化，或新建一个，并推进该任务的分配代数。
// 调用者必须持有 c.mu。
func (c *Coordinator) alloc(kind, id int, fileName string, files []string) *node {
	var n *node
	if len(c.free) > 0 {
		n = c.free[len(c.free)-1]
		c.free = c.free[:len(c.free)-1]
	} else {
		n = &node{}
	}
	n.kind = kind
	n.id = id
	n.fileName = fileName
	n.files = files
	n.nReduce = c.nReduce
	n.state = StatePending
	n.prev, n.next = nil, nil

	if kind == TaskMap {
		c.generation[fileName]++
		n.generation = c.generation[fileName]
	} else {
		c.reduceGen[id]++
		n.generation = c.reduceGen[id]
	}
	return n
}

// recycle 回收已从链表摘除的 node，供后续任务复用。
// 调用者必须持有 c.mu。
func (c *Coordinator) recycle(n *node) {
	n.files = nil
	n.prev, n.next = nil, nil
	c.free = append(c.free, n)
}

// checkTimeout 周期性扫描已分配链表，把超时任务重新挂回待分配链表。
// 链表按分配时间递增排列，遇到第一个未超时元素即可停止。由守护 goroutine 调用。
func (c *Coordinator) checkTimeout() {
	c.mu.Lock()
	defer c.mu.Unlock()

	now := time.Now()
	for n := c.running.head.next; n != c.running.tail; {
		if now.Sub(n.startTime) < TaskTimeout {
			break
		}
		next := n.next
		// 超时任务：旧 node 回收，用新 node（代数 +1）重新排队
		c.running.remove(n)
		var fresh *node
		if n.kind == TaskMap {
			fresh = c.alloc(TaskMap, n.id, n.fileName, nil)
			c.mapTask[n.fileName] = fresh
		} else {
			fresh = c.alloc(TaskReduce, n.id, "", n.files)
			c.reduceTask[n.id] = fresh
		}
		c.recycle(n)
		c.pending.pushBack(fresh)
		n = next
	}
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
// Done 是纯查询接口：只加锁读状态，不做任何资源释放。
// 进程退出由 main 的 return 完成，操作系统会回收全部内存。
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:      files,
		nReduce:    nReduce,
		mapNum:     len(files),
		reduceNum:  nReduce,
		mapDone:    make(map[string]bool),
		mapTask:    make(map[string]*node),
		reduceTask: make(map[int]*node),
		mapFiles:   make(map[int][]string),
		generation: make(map[string]int),
		reduceGen:  make(map[int]int),
		pending:    newNodeList(),
		running:    newNodeList(),
	}

	// 初始化：只投放 map 任务；reduce 任务等全部 map 完成后再投放
	for i, f := range files {
		n := c.alloc(TaskMap, i, f, nil)
		c.mapTask[f] = n
		c.pending.pushBack(n)
	}

	// 守护 goroutine：周期性回收超时任务
	go func() {
		for {
			time.Sleep(TaskCheckInterval)
			c.checkTimeout()
		}
	}()

	c.server(sockname)
	return &c
}
