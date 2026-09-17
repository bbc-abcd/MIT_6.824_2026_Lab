package raft

// 该文件实现 Raft 核心。raftapi.go 定义了 Raft 对外暴露的接口。
// Make() 负责创建一个 Raft peer 实例。

import (
	"bytes"     // 3C 持久化：把状态编码成字节数组
	"math/rand" // 生成随机选举超时时间
	"sort"      // 计算 matchIndex 的多数派边界
	"sync"      // 互斥锁 / 条件变量
	"time"      // 定时器/睡眠

	"6.5840/labgob"         // lab 定制的 gob 编码器（3C 持久化）
	"6.5840/labrpc"         // 模拟的 RPC 网络层
	"6.5840/raftapi"        // Raft 接口定义（ApplyMsg 等）
	tester "6.5840/tester1" // 测试框架提供的 Persister 等
)

// Raft peer 对象。每个 Raft 节点是一个 Raft 实例。
type Raft struct {
	mu        sync.Mutex          // 保护本节点所有共享状态
	peers     []*labrpc.ClientEnd // 集群所有节点的 RPC 端点（含自己）
	persister *tester.Persister   // 持久化存储对象
	me        int                 // 自己在 peers[] 中的下标

	currentTerm     int                   // 当前任期
	votedFor        int                   //投票的id，候选者可能由于网络重放请求
	state           serverState           //记录状态
	lastContact     time.Time             //最近一次收到leader消息的时间
	electionTimeout time.Duration         //选举时间间隔，每次随机防止系统卡死
	voteCount       int                   //投票记录
	applyCh         chan raftapi.ApplyMsg //应用到上层服务的通道

	// 日志：log[0] 是 term 为 0 的哨兵条目，永远不会被提交或应用。
	// 有了哨兵之后逻辑下标与切片下标一致，第一条 AppendEntries 也可以用
	// PrevLogIndex=0 表达"从头开始"。所有访问都走下面的下标辅助函数，
	// 这样 3D 引入日志压缩时只需要改这几个函数。
	log         []LogEntry // 日志记录
	commitIndex int        // 已提交的最大下标（初始 0 = 哨兵，视为已提交）
	applyIndex  int        // 已应用到状态机的最大下标（初始 0）

	nextIndex  []int //仅 leader：下一个要发给该 follower 的下标
	matchIndex []int //仅 leader：该 follower 已确认匹配的最大下标

	// applyIndex 追赶 commitIndex 的唤醒机制
	applyCond *sync.Cond

	// 每个 peer 一个容量为 1 的通知槽位，用来唤醒对应的 replicator goroutine。
	// 容量 1 + 非阻塞发送构成"有记忆"的信号，不会因为发送时无人接收而丢失；
	// 配合"每次醒来都重新检查状态"的循环，信号被合并也无所谓。
	notify []chan struct{}

	// 快照的元数据，建立快照的时候才持久化
	lastIncludeIndex int    // 快照包含的最后一个日志下标
	lastIncludeTerm  int    // 快照包含的最后一个日志的任期
	snapshot         []byte // 快照内容；persister.Save 的第二个参数必须是它，否则会把已存的快照覆盖成 nil

	// 待投递给状态机的快照。applyCh 只能由 applier 一个 goroutine 发送，
	// 否则它和 applier 正在推送的命令会交错，上层就会看到"先快照(150)后命令(92)"。
	pendingSnapshot      []byte
	pendingSnapshotIndex int
	pendingSnapshotTerm  int
}

type serverState int

const (
	Follower serverState = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// 心跳间隔100ms（测试要求每秒不超过 10 次心跳）
const heartbeatInterval = 100 * time.Millisecond

// 单次 AppendEntries 最多携带的条目数，避免积压时单个 RPC 过大
const maxBatchSize = 64

// 随机初始化选举时间800-1200ms
func randomElectionTimeout() time.Duration {
	return time.Duration(800+rand.Intn(400)) * time.Millisecond
}

// ---- 日志下标辅助函数（唯一允许访问 log 的地方）----
//
// 日志压缩的约定：快照覆盖 [0, lastIncludeIndex]，rf.log[0] 就是逻辑下标
// lastIncludeIndex 处的条目（不是虚拟哨兵，而是那条真实条目在压缩后充当哨兵）。
// 于是有一条很值钱的不变量：
//
//	rf.log[0].Term == rf.lastIncludeTerm
//	lastIncludeIndex <= applyIndex <= commitIndex <= lastIndex()
//	lastIncludeIndex <= nextIndex[peer] <= lastIndex()+1   （只有 leader 有关）
//
// 后两条保证了 entry(next-1) / entry(n) 这类访问永远落在合法物理区间内。

// 逻辑下标 -> 物理下标（切片下标）。唯一的换算规则。
func (rf *Raft) logIdx(i int) int { return i - rf.lastIncludeIndex }

// 日志最后一条的逻辑下标；只有哨兵时等于 0。
func (rf *Raft) lastIndex() int { return len(rf.log) + rf.lastIncludeIndex - 1 }

// 日志最后一条的任期；只有哨兵时是哨兵的任期。
func (rf *Raft) lastTerm() int { return rf.log[len(rf.log)-1].Term }

// 读取逻辑下标 i 处的条目，调用者保证 lastIncludeIndex <= i <= lastIndex()。
func (rf *Raft) entry(i int) LogEntry { return rf.log[rf.logIdx(i)] }

// 返回 [from, to) 区间的副本（逻辑下标）。RPC 参数会在解锁之后继续被使用，
// 必须与共享日志解耦，所以这里显式拷贝；代价是 O(批量大小) 而不是 O(日志长度)。
func (rf *Raft) entries(from, to int) []LogEntry {
	out := make([]LogEntry, to-from)
	copy(out, rf.log[rf.logIdx(from):rf.logIdx(to)])
	return out
}

// 把 [from, lastIndex()] 拷贝成新切片并让它成为新的日志，from 成为新的哨兵。
// 必须 make+copy 而不是 rf.log = rf.log[k:]：切片头的长度变了，但底层数组仍然
// 持有被丢弃条目的指针，GC 回收不掉（Command 是 interface{}，3D 明确要求能回收）。
func (rf *Raft) truncateFront(from int) {
	newLog := make([]LogEntry, rf.lastIndex()-from+1)
	copy(newLog, rf.log[rf.logIdx(from):])
	rf.log = newLog
}

// 叫醒某个 peer 的 replicator（非阻塞：槽位已满说明已经有待处理的信号了）
func (rf *Raft) signal(peer int) {
	select {
	case rf.notify[peer] <- struct{}{}:
	default:
	}
}

// 返回当前任期以及自己是否是 Leader。
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// 把 Raft 的持久化状态写入稳定存储。调用时必须持有 rf.mu。
//
// 快照元数据（lastIncludeIndex/lastIncludeTerm）必须和 log 一起持久化：裁剪之后
// 光有 log 是解释不了下标含义的。快照内容走 Save 的第二个参数——注意每次都要带上
// rf.snapshot，传 nil 会把之前存的那份快照抹掉（TestSnapshotInit3D 就是测这个）。
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludeIndex)
	e.Encode(rf.lastIncludeTerm)
	// 编码顺序必须和 readPersist 的解码顺序完全一致。
	// Save 是原子的：raftstate 和快照一次写入，不会出现"快照是新的、状态是旧的"。
	rf.persister.Save(w.Bytes(), rf.snapshot)
}

// 从持久化数据中恢复状态（崩溃重启时调用）。
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 { // 首次启动，无历史状态
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm, votedFor, lastIncludeIndex, lastIncludeTerm int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludeIndex) != nil ||
		d.Decode(&lastIncludeTerm) != nil {
		return // 解码失败当成首次启动；不 assign，避免半截状态
	}
	if len(log) == 0 {
		log = []LogEntry{{Term: 0, Command: nil}} // 防御：日志至少要有一条哨兵
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.log = log
	rf.lastIncludeIndex = lastIncludeIndex
	rf.lastIncludeTerm = lastIncludeTerm

	// 快照隐含"到 lastIncludeIndex 为止的条目已提交且已应用"：
	// 这两个下标必须从 lastIncludeIndex 起步，否则 applier 会去读已被裁掉的条目。
	// commitIndex 从 lastIncludeIndex 起步也是安全的：即使多数派节点一起断电重启，
	// 它们恢复到同一个值，之后 leader 会把 commitIndex 推得更高。
	rf.commitIndex = lastIncludeIndex
	rf.applyIndex = lastIncludeIndex
	// 注意：这里不能往 applyCh 发快照。Make 还没返回、applyCh 的读者还没启动，
	// 同步发送会把 Make 卡死；上层本来就会自己从 persister 读快照恢复状态机。
}

// 返回当前 Raft 持久化状态占用的字节数（用于测试）。
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// 上层服务通知 Raft：index 及之前的日志已经写入快照 snapshot，可以裁剪日志了。
// 服务层在每个节点上都会调用它（不只是 leader）。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 过期/重复的快照：已经裁到 index 之后了，再来一次没有意义（重放、乱序都会走到这）。
	if index <= rf.lastIncludeIndex {
		return
	}
	// 防御：index 超过本地日志/已应用位置说明服务状态机跑在了 Raft 前面，不该发生。
	// 宁可这次不裁剪（状态大一点），也不能让 applier 之后去读物理下标为负的条目。
	if index > rf.lastIndex() || index > rf.applyIndex {
		return
	}

	rf.truncateFront(index) // index 那条留下来当哨兵，之前的条目交给 GC
	rf.lastIncludeIndex = index
	rf.lastIncludeTerm = rf.log[0].Term

	// 拷贝一份，避免上层之后复用/改写这块内存
	rf.snapshot = make([]byte, len(snapshot))
	copy(rf.snapshot, snapshot)

	// 必须和日志裁剪在同一个临界区里完成：Save 是 raftstate + 快照的原子写入，
	// 分成两次写就会留下"快照下标和 lastIncludeIndex 对不上"的窗口，一旦在这里
	// 崩溃，重启后上层从快照恢复到 150、Raft 却从 100 开始重放 → apply out of order。
	rf.persist()
}

// RequestVote RPC 的请求参数结构。
// 注意：字段名必须首字母大写（RPC 序列化要求）。
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVote RPC 的回复结构。
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	// 日志不匹配时，让 leader 一次跳过一整个任期，而不是逐条回退。
	// 字段必须导出，否则 labgob 无法序列化（会打印 lower-case field 错误）。
	XTerm  int // 冲突位置的 term；follower 日志太短时为 -1
	XIndex int // follower 中任期为 XTerm 的第一条的下标；无意义时为 -1
	XLen   int // follower 日志的长度（= 最后一条的下标 + 1）
}

// RequestVote RPC 的处理函数（服务端收到请求后执行）。
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	// 任期小于直接拒绝
	if args.Term < rf.currentTerm {
		return
	}
	// 任期更新变成follower并且持久化状态
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
	}
	reply.Term = rf.currentTerm

	// 选举限制（论文 5.4.1）：只把票投给日志至少和自己一样新的候选者
	lastLogIndex := rf.lastIndex()
	lastLogTerm := rf.lastTerm()
	// 对方的日志任期更新或者拥有相同任期的更多日志
	upToDate := args.LastLogTerm > lastLogTerm ||
		(args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && upToDate {
		rf.votedFor = args.CandidateId
		rf.state = Follower
		rf.lastContact = time.Now()
		rf.electionTimeout = randomElectionTimeout()
		rf.persist()
		reply.VoteGranted = true
	}
}

// AppendEntries RPC 的处理函数。心跳和日志复制共用这一条路径：
// Entries 为空时就是心跳（仍然要做一致性检查，让 leader 能发现并修正分歧）。
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.XTerm = -1
	reply.XIndex = -1
	reply.XLen = rf.lastIndex() + 1 // 逻辑长度 = 最后一条的下标 + 1

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.state = Follower
	rf.lastContact = time.Now()
	reply.Term = rf.currentTerm

	// ---- 日志一致性检查 ----
	if args.PrevLogIndex > rf.lastIndex() {
		// follower 的日志太短：XTerm/XIndex 保持 -1，leader 会退到 XLen
		return
	}
	// PrevLogIndex 落在快照覆盖区内时无法（也不需要）用本地日志比对：
	// 那段条目已经应用到状态机 => 已提交 => 由 leader completeness，任何 leader
	// 在相同位置都有完全相同的条目。所以跳过而不是拒绝。
	// 这个分支也顺带挡住了 entry() 的负物理下标。
	if args.PrevLogIndex >= rf.lastIncludeIndex &&
		rf.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		// 冲突：告诉 leader 冲突位置的 term，以及该 term 在 follower 里的第一条。
		// 下界停在 lastIncludeIndex：再往前是快照区，物理下标不存在，也没必要。
		reply.XTerm = rf.entry(args.PrevLogIndex).Term
		i := args.PrevLogIndex
		for i > rf.lastIncludeIndex && rf.entry(i-1).Term == reply.XTerm {
			i--
		}
		reply.XIndex = i
		return
	}
	reply.Success = true

	// ---- 追加 / 截断 ----
	// 和 leader 一样逐条比对，从"第一个矛盾处"开始抄 leader 的日志；
	// 区别只在于快照覆盖的前 k0 条不参与比对，由快照负责证明它们一致。
	if len(args.Entries) > 0 {
		k0 := max(0, rf.lastIncludeIndex-args.PrevLogIndex)
		conflict := -1
		for k := k0; k < len(args.Entries); k++ {
			i := args.PrevLogIndex + 1 + k
			if i > rf.lastIndex() {
				break // 后面都是新条目，没有冲突
			}
			if rf.entry(i).Term != args.Entries[k].Term {
				conflict = i
				break
			}
		}
		if conflict >= 0 {
			// 注意 conflict 是逻辑下标，切片要用物理下标。
			// 冲突不可能落在 <= commitIndex 的位置（那里已提交、与 leader 逐条相同），
			// 所以这条截断永远不会丢掉已知提交的条目。
			for i := rf.logIdx(conflict); i < len(rf.log); i++ {
				rf.log[i] = LogEntry{} // 清引用，否则底层数组仍然可达（GC 回收不掉）
			}
			rf.log = rf.log[:rf.logIdx(conflict)]
		}
		// 只有比 follower 现有日志更长时才需要追加剩下的部分
		if rf.lastIndex() < args.PrevLogIndex+len(args.Entries) {
			rf.log = append(rf.log, args.Entries[rf.lastIndex()-args.PrevLogIndex:]...)
		}
		rf.persist() // 3C：日志变化必须落盘
	}

	// ---- 推进 commitIndex ----
	// 必须在一致性检查通过之后才更新，并且用"最后一条新条目的下标"封顶，
	// 否则会提交自己日志里根本不存在的条目。
	//
	// 第二个 max 是必需的：PrevLogIndex+len(Entries) 描述的是"这条 AE 覆盖到哪"，
	// 它可以比 commitIndex 小得多（AE 完全落在快照区里、或者 leader 因为过期的
	// 拒绝回复把 nextIndex 退到了我们已知提交位置之下）。commitIndex 只增不减：
	// 我们自己的 commitIndex 就证明了"到那里为止我和 leader 一定一致"。
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = max(rf.commitIndex, min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries)))
		rf.applyCond.Broadcast()
	}
}

// 后台循环：把 [applyIndex+1, commitIndex] 的条目按顺序交给状态机。
// 每个节点有且只有一个 applier —— 它是 applyCh 的唯一发送者，
// 这样命令和快照的相对顺序才有保证（不会出现"先快照后命令"）。
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for rf.applyIndex >= rf.commitIndex && rf.pendingSnapshot == nil {
			rf.applyCond.Wait() // Wait 原子地解锁并挂起，被唤醒时重新加锁
		}
		// 快照优先发：它的下标一定大于正在/将要发的所有命令——InstallSnapshot
		// 只在 LastIncludeIndex > commitIndex 时才接受，而命令最大只到 commitIndex。
		if rf.pendingSnapshot != nil {
			msg := raftapi.ApplyMsg{
				SnapshotValid: true,
				Snapshot:      rf.pendingSnapshot,
				SnapshotIndex: rf.pendingSnapshotIndex,
				SnapshotTerm:  rf.pendingSnapshotTerm,
			}
			rf.pendingSnapshot = nil
			rf.mu.Unlock()
			rf.applyCh <- msg // 必须解锁之后再发，见下面那句注释
			continue
		}
		lo, hi := rf.applyIndex+1, rf.commitIndex
		batch := rf.entries(lo, hi+1)
		rf.applyIndex = hi
		rf.mu.Unlock() // 绝不能持锁向 applyCh 发送：上层可能阻塞在通道上

		for k, e := range batch {
			rf.applyCh <- raftapi.ApplyMsg{
				CommandValid: true,
				Command:      e.Command,
				CommandIndex: lo + k,
			}
		}
	}
}

// 发一轮 RPC（AppendEntries 或 InstallSnapshot），然后按心跳节奏推进这条 peer 的循环。
// send 在独立 goroutine 里执行，返回 true 表示"还有积压 / 刚回退完 nextIndex，应该立刻
// 再来一轮"，false 表示这轮正常结束。
//
// 为什么不等回复而是"最多等一个心跳周期"：labrpc.go 里写着，向已断开的 peer 发 RPC 时，
// 失败回复会被随机延迟最多 7 秒（LONGDELAY，注释原文 "let Raft tests check that leader
// doesn't send RPCs synchronously"）。在这里同步干等，这条 peer 的心跳就会停好几秒——
// 对端重连回来后收不到任何心跳，会一直保持过期的 leader 身份（TestReElection3A 会挂）。
// 而"等回复"又要保留：不然每个心跳周期都会把同一批日志重发一遍，RPC 数会失控
// （TestCount3B / TestRPCBytes3B / TestBackup3B 都会挂）。
func (rf *Raft) sendRound(peer int, send func() bool) {
	done := make(chan bool, 1) // 容量 1：超时后发送方仍能写入，不会泄漏 goroutine
	go func() { done <- send() }()

	select {
	case more := <-done:
		if more {
			return // 立刻重试，不等心跳周期
		}
	case <-time.After(heartbeatInterval):
		// 回复迟迟不来（对端慢或已断开）：不干等，按心跳节奏继续发下一轮
	}

	select {
	case <-rf.notify[peer]: // 有新日志 / 需要重试，立刻发
	case <-time.After(heartbeatInterval):
	}
}

// 每个 peer 一个常驻 goroutine，负责"向该 peer 发送 AppendEntries/InstallSnapshot"，
// 既负责日志复制也负责心跳（心跳就是 Entries 为空的 AppendEntries）。
// nextIndex 只在成功时单调推进，所以同一条日志天然不会被重复发送。
func (rf *Raft) replicator(peer int) {
	for {
		rf.mu.Lock()
		if rf.state != Leader {
			// 不是 leader 就不需要发送任何东西，阻塞等到当选（becomeLeader 会 signal）
			rf.mu.Unlock()
			<-rf.notify[peer]
			continue
		}
		next := rf.nextIndex[peer]

		// 先判快照：next-1 已经被裁掉了，用不了 PrevLogTerm，只能整份快照发过去。
		// 这个判断必须在构造 AppendEntriesArgs 之前，否则 entry(next-1) 会越界。
		if next <= rf.lastIncludeIndex {
			args := &InstallSnapshotArgs{
				Term:             rf.currentTerm,
				LeaderId:         rf.me,
				LastIncludeIndex: rf.lastIncludeIndex,
				LastIncludeTerm:  rf.lastIncludeTerm,
				Data:             rf.snapshot,
			}
			rf.mu.Unlock()

			rf.sendRound(peer, func() bool {
				var reply InstallSnapshotReply
				ok := rf.sendInstallSnapshot(peer, args, &reply)
				rf.mu.Lock()
				defer rf.mu.Unlock()
				return rf.handleInstallReply(peer, args, &reply, ok)
			})
			continue
		}

		args := &AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: next - 1,
			PrevLogTerm:  rf.entry(next - 1).Term,
			LeaderCommit: rf.commitIndex,
		}
		if rf.lastIndex() >= next {
			hi := min(rf.lastIndex()+1, next+maxBatchSize)
			args.Entries = rf.entries(next, hi)
		}
		rf.mu.Unlock()

		rf.sendRound(peer, func() bool {
			var reply AppendEntriesReply
			ok := rf.sendAppendEntries(peer, args, &reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			return rf.handleAppendReply(peer, args, &reply, ok)
		})
	}
}

// InstallSnapshot RPC 的请求/回复结构（3D）。不实现图 13 的分片 offset 机制。
type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludeIndex int
	LastIncludeTerm  int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term    int
	Success bool // follower 现在至少拥有到 LastIncludeIndex，leader 可以据此推进 matchIndex
}

// InstallSnapshot RPC 的处理函数：leader 的日志已经被裁到 follower 需要的位置之前了，
// 只能用整份快照把它带上来。
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock() // 全程只改状态、不往 applyCh 发东西，所以可以放心用 defer
	reply.Term = rf.currentTerm
	reply.Success = false

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	// 任期相等也一定要重置：一个任期只有一个 leader，所以这时我们必然是 follower，
	// 而且快照传输期间不能被选举超时打断（否则 follower 会自己发起选举）。
	rf.state = Follower
	rf.lastContact = time.Now()
	reply.Term = rf.currentTerm

	// 快照只推进、不倒退。两种情况都不需要这份快照：
	//   ① 比本地快照还旧；
	//   ② 本地日志已经有了（<= commitIndex 说明这些条目本地已提交）。
	// 仍然回 Success=true：我们确实拥有到 LastIncludeIndex 为止的条目，
	// leader 据此把 nextIndex 推到 LastIncludeIndex+1 就能继续用 AppendEntries。
	if args.LastIncludeIndex <= rf.lastIncludeIndex || args.LastIncludeIndex <= rf.commitIndex {
		reply.Success = true
		return
	}

	// 论文 §7：本地在 LastIncludeIndex 处有同任期条目 → 保留其后缀，否则整条丢弃。
	// 有的实现一律整条丢弃（让 leader 重发后缀）也正确，只是多一次往返。
	if rf.lastIndex() >= args.LastIncludeIndex &&
		rf.entry(args.LastIncludeIndex).Term == args.LastIncludeTerm {
		rf.truncateFront(args.LastIncludeIndex)
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludeTerm, Command: nil}}
	}
	rf.lastIncludeIndex = args.LastIncludeIndex
	rf.lastIncludeTerm = args.LastIncludeTerm
	rf.snapshot = args.Data

	// 裁剪之后这两个下标必须 >= lastIncludeIndex，否则 applier 会去读已经被丢掉的条目。
	// 只增不减：被裁掉的都是本地已提交的条目，服务状态机不能因此回退。
	rf.applyIndex = max(rf.applyIndex, rf.lastIncludeIndex)
	rf.commitIndex = max(rf.commitIndex, rf.lastIncludeIndex)

	// 交给 applier 投递，而不是在这里直接发 applyCh：applyCh 只能有一个发送者，
	// 否则这份快照会插到 applier 正在推送的命令中间（上层看到"先快照后命令"就乱序了）。
	// 也不能持锁发：上层收到快照消息后会回头调 Snapshot()，那个要拿 rf.mu。
	rf.pendingSnapshot = args.Data
	rf.pendingSnapshotIndex = args.LastIncludeIndex
	rf.pendingSnapshotTerm = args.LastIncludeTerm

	rf.persist()
	rf.applyCond.Broadcast()
	reply.Success = true
}

// 处理 AppendEntries 的回复。调用时必须持有 rf.mu。
// 返回值表示"是否应该立刻继续下一轮"。
func (rf *Raft) handleAppendReply(peer int, args *AppendEntriesArgs, reply *AppendEntriesReply, ok bool) bool {
	// ① 过期回复：发起时是 leader，但期间任期变了或已经不是 leader，直接丢弃
	if rf.state != Leader || rf.currentTerm != args.Term {
		return false
	}
	if !ok {
		return false // RPC 本身失败（丢包/对端宕机），等下一个周期再试
	}
	// ② 对方任期更高 → 退位
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
		return false
	}

	if reply.Success {
		// 用"发送时"的 PrevLogIndex 计算匹配位置，而不是当前的 lastIndex()：
		// 这个回复可能对应的是一份更老的 args，用当前值会让 matchIndex 虚高，
		// 从而提交实际上没被复制的条目。
		matched := args.PrevLogIndex + len(args.Entries)
		if matched > rf.matchIndex[peer] {
			rf.matchIndex[peer] = matched // 单调递增，防止乱序回复把它推回去
		}
		// 由 matchIndex 推导而不是直接用 matched+1，同样是防止乱序回复让
		// nextIndex 回退、把已经确认过的条目重发一遍。
		rf.nextIndex[peer] = rf.matchIndex[peer] + 1
		rf.advanceCommitIndex()
		// 还有积压就立刻继续，攒批发送
		return rf.nextIndex[peer] <= rf.lastIndex()
	}

	// ③ 日志不匹配：按 XTerm/XIndex/XLen 一次跨过整个任期，而不是逐条回退
	if reply.XTerm == -1 {
		// follower 日志太短：退到它日志的末尾
		rf.nextIndex[peer] = reply.XLen
	} else {
		idx := -1
		// 只在自己仍然保留的日志里找（下界 exclude 哨兵，避免负物理下标）。
		// 找不到有两种可能：leader 真的缺失该任期，或者 leader 有但已经被裁掉了；
		// 无论哪一种，回退到 XIndex 都不会出错 —— 如果确实被裁掉，下一轮就会
		// 走到"发快照"的分支（nextIndex <= lastIncludeIndex）。
		for i := rf.lastIndex(); i > rf.lastIncludeIndex; i-- {
			if rf.entry(i).Term == reply.XTerm {
				idx = i
				break
			}
		}
		if idx == -1 {
			// leader 没有这个任期：退到 follower 中该任期的第一条
			rf.nextIndex[peer] = reply.XIndex
		} else {
			// leader 有这个任期：退到该任期中最后一条的下一条
			rf.nextIndex[peer] = idx + 1
		}
	}
	// 夹取到 [max(1, lastIncludeIndex), lastIndex()+1]：这是 entry(next-1) 恒合法的充要条件。
	// 下界：nextIndex == lastIncludeIndex 表示"该发快照了"，留着它有用；但 lastIncludeIndex
	//       为 0 时没有快照可发（覆盖 0 条的快照没有意义），所以要抬到 1。
	// 上界：follower 的 XLen/XIndex 描述的是它自己的日志，而 follower 的日志可能
	//       比 leader 还长（分区期间收过一批高下标条目），直接用会让 entry(next-1)
	//       越界 panic。夹到自己的末尾，让它先当心跳发出去，由对方回报真实分歧点。
	if lo := max(1, rf.lastIncludeIndex); rf.nextIndex[peer] < lo {
		rf.nextIndex[peer] = lo
	}
	if rf.nextIndex[peer] > rf.lastIndex()+1 {
		rf.nextIndex[peer] = rf.lastIndex() + 1
	}
	return true // 立刻重试，加速追赶（TestBackup3B 靠这个）
}

// 处理 InstallSnapshot 的回复。调用时必须持有 rf.mu。
// 返回值表示"是否应该立刻继续下一轮"。
func (rf *Raft) handleInstallReply(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply, ok bool) bool {
	if rf.state != Leader || rf.currentTerm != args.Term {
		return false // 过期回复
	}
	if !ok {
		return false // 丢包/对端宕机，等下一轮
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = Follower
		rf.persist()
		return false
	}
	if !reply.Success {
		return false
	}
	// 对方现在至少拥有到 LastIncludeIndex：matchIndex 单调推进，再由它推导 nextIndex
	if args.LastIncludeIndex > rf.matchIndex[peer] {
		rf.matchIndex[peer] = args.LastIncludeIndex
	}
	rf.nextIndex[peer] = rf.matchIndex[peer] + 1
	rf.advanceCommitIndex() // 这次跳跃本身就可能凑出多数派
	return true             // 立刻继续，把快照之后的日志补上
}

// 统计多数派已经复制到的最大下标，满足条件则推进 commitIndex。
// 调用时必须持有 rf.mu。
func (rf *Raft) advanceCommitIndex() {
	if rf.state != Leader {
		return
	}
	m := make([]int, len(rf.peers))
	copy(m, rf.matchIndex)
	m[rf.me] = rf.lastIndex() // 自己当然也"匹配"了自己的日志
	sort.Ints(m)
	n := m[len(m)/2] // 升序数组里第 len/2 个 = 多数派边界（3 节点取第 2 大，5 节点取第 3 大）
	// 论文 5.4.2：leader 只能通过统计副本数提交"当前任期"的条目。
	// 之前任期的条目只能靠当前任期的条目间接提交，否则会违反 Figure 8。
	if n > rf.commitIndex && rf.entry(n).Term == rf.currentTerm {
		rf.commitIndex = n
		rf.applyCond.Broadcast()
	}
}

// 发送 RequestVote RPC 的辅助函数。
// server 是目标节点在 peers[] 中的下标。
// 由于网络可能丢包/延迟，Call 返回 false 只表示本次 RPC 失败，不一定是对方宕机。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	return rf.peers[server].Call("Raft.RequestVote", args, reply)
}

// 上层服务提交一条新命令给 Raft。
// 如果不是 Leader → 返回 false。
// 如果是 Leader → 把命令追加到本地日志，立即返回（异步复制）。
// 返回值：(命令所在的日志下标, curTerm, isLeader)
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return -1, rf.currentTerm, false
	}
	index := rf.lastIndex() + 1
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	rf.persist() // 3C：日志变化必须落盘

	// 叫醒所有 replicator，不用等下一个心跳周期
	for i := range rf.peers {
		if i != rf.me {
			rf.signal(i)
		}
	}
	return index, rf.currentTerm, true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	return rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
}

// 当选为 leader 后的初始化。调用时必须持有 rf.mu。
func (rf *Raft) becomeLeader() {
	rf.state = Leader
	n := rf.lastIndex()
	for i := range rf.peers {
		rf.nextIndex[i] = n + 1 // 乐观假设 follower 和自己一样新
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = n
	// 立刻发一轮 AppendEntries：一是宣告领导权、阻止其他节点发起选举，
	// 二是让落后的 follower 通过拒绝来修正 nextIndex
	for i := range rf.peers {
		if i != rf.me {
			rf.signal(i)
		}
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	if rf.state == Leader {
		rf.mu.Unlock()
		return
	}
	rf.state = Candidate
	rf.currentTerm++
	term := rf.currentTerm
	rf.votedFor = rf.me
	rf.voteCount = 1
	rf.lastContact = time.Now()
	rf.electionTimeout = randomElectionTimeout()
	lastLogIndex := rf.lastIndex()
	lastLogTerm := rf.lastTerm()
	rf.persist()
	rf.mu.Unlock()

	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		args := &RequestVoteArgs{
			Term: term, CandidateId: rf.me,
			LastLogIndex: lastLogIndex, LastLogTerm: lastLogTerm,
		}
		go func(server int, args *RequestVoteArgs) {
			var reply RequestVoteReply
			if !rf.sendRequestVote(server, args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = Follower
				rf.persist()
				return
			}
			// 这是一个过期或者失败的回复
			if rf.state != Candidate || rf.currentTerm != term || reply.Term != term || !reply.VoteGranted {
				return
			}
			// 票数已经过半
			if rf.voteCount > len(rf.peers)/2 {
				return
			}
			// 第一个收到过半票数的goroutine
			rf.voteCount++
			if rf.voteCount > len(rf.peers)/2 {
				rf.becomeLeader()
			}
		}(server, args)
	}
}

// 后台循环：检查是否应该发起选举。
// 心跳由各个 peer 的 replicator 负责，这里不再参与发送。
func (rf *Raft) ticker() {
	for {
		// 每10ms检查一次状态
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			continue
		}
		if time.Since(rf.lastContact) >= rf.electionTimeout {
			rf.mu.Unlock()
			rf.startElection()
			continue
		}
		rf.mu.Unlock()
	}
}

// 创建 Raft peer。测试框架会调用此函数。
// peers：所有节点的 RPC 端点；me：自己的下标；
// persister：持久化对象，初始时可能已含历史状态；
// applyCh：向服务层发送 ApplyMsg 的通道。
// 注意：Make 必须快速返回，长任务应放到 goroutine 中。
func Make(peers []*labrpc.ClientEnd, me int,
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.lastContact = time.Now()
	rf.electionTimeout = randomElectionTimeout()
	rf.applyCh = applyCh

	// 下标 0 是 term 为 0 的哨兵条目
	rf.log = []LogEntry{{Term: 0, Command: nil}}
	rf.commitIndex = 0
	rf.applyIndex = 0 // 哨兵视为已提交、已应用，不会发给 applyCh

	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.applyCond = sync.NewCond(&rf.mu)
	rf.notify = make([]chan struct{}, len(peers))
	for i := range rf.notify {
		rf.notify[i] = make(chan struct{}, 1)
	}

	// 先取一份快照：之后每次 persist() 都要把它交给 Save 的第二个参数，
	// 否则会把 persister 里已有的快照覆盖成 nil（TestSnapshotInit3D 测的就是这个）。
	// 这里不要往 applyCh 发它：Make 还没返回、读者还没起来，会卡死自己。
	rf.snapshot = persister.ReadSnapshot()

	// 从持久化状态恢复（若之前崩溃过）
	rf.readPersist(persister.ReadRaftState())

	for i := range peers {
		if i != me {
			go rf.replicator(i)
		}
	}
	go rf.applier()
	go rf.ticker()

	return rf
}
