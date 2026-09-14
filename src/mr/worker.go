package mr

import (
	"bufio"
	"bytes"
	"container/heap"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator
// worker 内部常量
const (
	mapBufferSize = 1024 // 单个 reduce 分区缓冲区的近似上限（字节）
	readBufSize   = 1024 // 分段扫描 / 归并时的共享读缓冲大小
)

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// 用于给中间文件 / 临时输出文件取随机名。注意 randomID 必须是
	// 每次任务执行时新生成的（见 doMap / doReduce），不能每进程生成一次：
	// 同一个 worker 进程会执行多个 Map 任务，若复用同一个 randomID，
	// 后一个任务的 os.Create 会把前一个任务的中间文件截断。
	rng := rand.New(rand.NewSource(time.Now().UnixNano() ^ int64(os.Getpid())))

	for {
		reply, ok := callGetTask()
		if !ok {
			// coordinator 已退出：任务不可能再推进，worker 正常退出
			fmt.Println("worker: coordinator is gone, exiting")
			os.Exit(0)
		}

		switch reply.Kind {
		case ExitTask:
			fmt.Println("worker: all tasks finished, exiting")
			os.Exit(0)

		case WaitTask:
			// 暂时没有可分配的任务（不是"任务已完成"），稍后再问
			time.Sleep(time.Duration(100+rng.Intn(200)) * time.Millisecond)

		case MapTask:
			files, ok := doMap(reply.MapFile, reply.NReduce, mapf, rng)
			if !ok {
				// 任务执行失败，直接退出（任务将由 coordinator 超时后重派）
				log.Printf("worker: map task %v failed", reply.MapFile)
				os.Exit(1)
			}
			callReportMap(&ReportMapArgs{MapFile: reply.MapFile, Files: files})

		case ReduceTask:
			if !doReduce(reply.ReduceID, reply.Files, reducef, rng) {
				// 可能是：读文件失败（等待超时重派），或该分区已被其他 worker
				// 通过硬链接抢先完成（此时不应上报）。两种情况都继续申请任务。
				continue
			}
			callReportReduce(&ReportReduceArgs{ReduceID: reply.ReduceID})
		}
	}
}

// 执行一个 map 任务：读取输入文件，调用用户 Map 函数，把结果按
// ihash(key)%nReduce 分区，每个分区写成 map-<reduceID>-<randID>.txt。
//
// 每个分区维护一个近似 1024B 的缓冲区；缓冲区写满就把该分区内的 key
// 排序后追加到对应文件。因此每个中间文件是"若干段有序批次"的拼接：
// 段内 key 有序，段间 key 范围可以重叠。
func doMap(mapFile string, nReduce int, mapf func(string, string) []KeyValue, rng *rand.Rand) ([]string, bool) {
	// 本次执行的唯一标识：只属于这一次 Map 执行
	randID := rng.Int63()

	data, err := ioutil.ReadFile(mapFile)
	if err != nil {
		log.Printf("worker: cannot read %v: %v", mapFile, err)
		return nil, false
	}
	kva := mapf(mapFile, string(data))

	// 中间文件的名字只属于本次 map 执行（randID 唯一），因此可以直接写，
	// 不存在与其他执行写同一个文件的问题；未被上报的文件不会被 reduce 读取。
	path := make([]string, nReduce)
	files := make([]*os.File, nReduce)
	writers := make([]*bufio.Writer, nReduce)
	for rid := 0; rid < nReduce; rid++ {
		path[rid] = fmt.Sprintf("map-%d-%d.txt", rid, randID)
		f, err := os.Create(path[rid])
		if err != nil {
			log.Printf("worker: cannot create %v: %v", path[rid], err)
			return nil, false
		}
		files[rid] = f
		writers[rid] = bufio.NewWriter(f)
	}

	part := make([][]KeyValue, nReduce)
	size := make([]int, nReduce)

	flush := func(rid int) {
		ks := part[rid]
		if len(ks) == 0 {
			return
		}
		sort.Sort(ByKey(ks)) // 只排本批次：产生一个"段内有序"的段
		w := writers[rid]
		for _, kv := range ks {
			fmt.Fprintf(w, "%v %v\n", kv.Key, kv.Value)
		}
		part[rid] = ks[:0] // 复用底层数组
		size[rid] = 0
	}

	for _, kv := range kva {
		rid := ihash(kv.Key) % nReduce
		part[rid] = append(part[rid], kv)
		size[rid] += len(kv.Key) + len(kv.Value) + 2
		if size[rid] >= mapBufferSize {
			flush(rid)
		}
	}
	for rid := 0; rid < nReduce; rid++ {
		flush(rid)
		writers[rid].Flush()
		files[rid].Close()
	}
	return path, true
}

// 执行一个 reduce 任务：
//
//	阶段一：扫描该分区的全部中间文件，按"key 变小"的位置切分出有序段；
//	阶段二：用最小堆对各段做惰性 k 路归并，同一 key 的 value 收齐后调用
//	        用户 Reduce 函数，结果先写到临时文件，最后用硬链接原子落地为
//	        mr-out-<reduceID>。
func doReduce(reduceID int, files []string, reducef func(string, []string) string, rng *rand.Rand) bool {
	// 本次执行的唯一标识：只属于这一次 Reduce 执行
	randID := rng.Int63()

	a := &arena{buf: make([]byte, readBufSize)}

	// ---------- 阶段一：切分有序段 ----------
	var segs []*segment
	var handles []*os.File
	defer func() {
		for _, f := range handles {
			f.Close()
		}
	}()

	for _, p := range files {
		f, err := os.Open(p)
		if err != nil {
			continue // 中间文件可能不存在（该 map 未产出本分区数据），跳过
		}
		handles = append(handles, f)
		st, err := f.Stat()
		if err != nil {
			continue
		}
		size := st.Size()
		if size == 0 {
			continue
		}
		lr := &lineReader{f: f, off: 0, end: size, a: a}
		var prev string
		first := true
		segStart := int64(0)
		for {
			start, kv, ok := lr.next()
			if !ok {
				break
			}
			// 段内 key 非递减；新段的第一个 key 小于上一段的最后一个 key，
			// 就是段的边界
			if !first && kv.Key < prev {
				segs = append(segs, newSegment(f, segStart, start, a))
				segStart = start
			}
			prev = kv.Key
			first = false
		}
		if size > segStart {
			segs = append(segs, newSegment(f, segStart, size, a))
		}
	}

	// ---------- 阶段二：惰性 k 路归并 ----------
	// 不变量：堆中每个段恰好保留"该段下一个未读元素"，因此堆顶的 key
	// 就是所有段剩余元素中最小的 key；一旦弹出的 key 发生变化，
	// 上一个 key 的 value 必定已经收齐。
	h := &segHeap{}
	for _, s := range segs {
		if _, kv, ok := s.lr.next(); ok {
			s.item.kv = kv
			heap.Push(h, s.item)
		}
	}

	tmp := fmt.Sprintf("mr-tmp-out-%d-%d", reduceID, randID)
	out, err := os.Create(tmp)
	if err != nil {
		log.Printf("worker: cannot create %v: %v", tmp, err)
		return false
	}
	w := bufio.NewWriter(out)

	lastKey := ""
	var values []string
	started := false
	emit := func() {
		// 正确格式：每行 "%v %v\n"
		fmt.Fprintf(w, "%v %v\n", lastKey, reducef(lastKey, values))
	}

	for h.Len() > 0 {
		it := heap.Pop(h).(*heapItem)
		if !started || it.kv.Key != lastKey {
			if started {
				emit()
			}
			lastKey = it.kv.Key
			values = values[:0] // 复用底层数组
			started = true
		}
		values = append(values, it.kv.Value)
		// 从该段补读下一个元素，保持不变量
		if _, kv, ok := it.seg.lr.next(); ok {
			it.kv = kv
			heap.Push(h, it)
		}
	}
	if started {
		emit()
	}
	w.Flush()
	out.Close()

	// 原子落地：目标已存在则 link 失败（EEXIST），说明本分区已被其他 worker
	// 完成，本次不上报。这样同一个 reduceID 至多只有一个 worker 上报完成。
	final := fmt.Sprintf("mr-out-%d", reduceID)
	if err := os.Link(tmp, final); err != nil {
		os.Remove(tmp)
		return false
	}
	os.Remove(tmp)
	return true
}

// ---------- 按行读取中间文件 ----------

// arena 是所有 lineReader 共享的读缓冲。归并是单线程顺序进行的，
// 且每行读出来立刻就解析成 KeyValue，因此共享一个缓冲区是安全的，
// 内存占用为 O(1) 而不是 O(段数)。
type arena struct {
	buf     []byte
	scratch []byte
}

// lineReader 在 [off, end) 区间内按行顺序读取，end 是段的边界或文件末尾。
type lineReader struct {
	f   *os.File
	off int64
	end int64
	a   *arena
}

// next 读取下一行，返回该行的起始偏移与该行的 KeyValue。
// 到达区间末尾时返回 ok=false。
func (lr *lineReader) next() (int64, KeyValue, bool) {
	for lr.off < lr.end {
		start := lr.off
		line, ok := lr.readLine()
		if !ok {
			return 0, KeyValue{}, false
		}
		if len(line) == 0 {
			continue
		}
		if kv, ok := parseKV(line); ok {
			return start, kv, true
		}
	}
	return 0, KeyValue{}, false
}

// readLine 读取一行（不含换行符）。返回的切片可能复用共享缓冲，
// 调用者必须立即消费，不能长期持有。
func (lr *lineReader) readLine() ([]byte, bool) {
	if lr.off >= lr.end {
		return nil, false
	}
	a := lr.a
	a.scratch = a.scratch[:0]

	for lr.off < lr.end {
		n := lr.end - lr.off
		if n > int64(len(a.buf)) {
			n = int64(len(a.buf))
		}
		r, err := lr.f.ReadAt(a.buf[:n], lr.off)
		if r <= 0 {
			_ = err
			break
		}
		if i := bytes.IndexByte(a.buf[:r], '\n'); i >= 0 {
			if len(a.scratch) == 0 {
				lr.off += int64(i + 1)
				return a.buf[:i], true
			}
			a.scratch = append(a.scratch, a.buf[:i]...)
			lr.off += int64(i + 1)
			return a.scratch, true
		}
		// 缓冲区内没有换行符：整块并到 scratch，继续读
		a.scratch = append(a.scratch, a.buf[:r]...)
		lr.off += int64(r)
	}
	if len(a.scratch) == 0 {
		return nil, false
	}
	return a.scratch, true
}

// parseKV 把一行 "%v %v" 解析成 KeyValue。key 中不含空格，
// value 可能含空格（如 indexer 的文件名列表），因此按最后一个空格切分。
func parseKV(line []byte) (KeyValue, bool) {
	i := bytes.LastIndexByte(line, ' ')
	if i < 0 {
		return KeyValue{}, false
	}
	return KeyValue{string(line[:i]), string(line[i+1:])}, true
}

// ---------- 归并用最小堆 ----------

type heapItem struct {
	kv  KeyValue
	seg *segment
}

type segHeap []*heapItem

func (h segHeap) Len() int            { return len(h) }
func (h segHeap) Less(i, j int) bool  { return h[i].kv.Key < h[j].kv.Key }
func (h segHeap) Swap(i, j int)       { h[i], h[j] = h[j], h[i] }
func (h *segHeap) Push(x interface{}) { *h = append(*h, x.(*heapItem)) }
func (h *segHeap) Pop() interface{} {
	old := *h
	n := len(old)
	it := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return it
}

type segment struct {
	lr   *lineReader
	item *heapItem // 复用，避免逐个元素分配
}

func newSegment(f *os.File, start, end int64, a *arena) *segment {
	s := &segment{lr: &lineReader{f: f, off: start, end: end, a: a}}
	s.item = &heapItem{seg: s}
	return s
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func callGetTask() (*GetTaskReply, bool) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	if !call("Coordinator.GetTask", &args, &reply) {
		return nil, false
	}
	return &reply, true
}

func callReportMap(args *ReportMapArgs) bool {
	reply := ReportReply{}
	return call("Coordinator.ReportMap", args, &reply)
}

func callReportReduce(args *ReportReduceArgs) bool {
	reply := ReportReply{}
	return call("Coordinator.ReportReduce", args, &reply)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
