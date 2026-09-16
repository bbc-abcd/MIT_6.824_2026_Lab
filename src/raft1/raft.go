package raft

// 该文件实现 Raft 核心。raftapi.go 定义了 Raft 对外暴露的接口。
// Make() 负责创建一个 Raft peer 实例。

import (
	//	"bytes"          // 3C 持久化时会用到（编码/解码）
	"math/rand" // 生成随机选举超时时间
	"sort"      // 计算 matchIndex 的多数派边界
	"sync"      // 互斥锁 / 条件变量
	"time"      // 定时器/睡眠

	//	"6.5840/labgob"  // 3C 持久化时会用到（lab 定制的 gob 编码器）
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

// 日志最后一条的逻辑下标；空日志时等于 0（哨兵）。
func (rf *Raft) lastIndex() int { return len(rf.log) - 1 }

// 日志最后一条的任期；空日志时等于 0（哨兵）。
func (rf *Raft) lastTerm() int { return rf.log[rf.lastIndex()].Term }

// 读取逻辑下标 i 处的条目，调用者保证 0 <= i <= lastIndex()。
func (rf *Raft) entry(i int) LogEntry { return rf.log[i] }

// 返回 [from, to) 区间的副本。RPC 参数会在解锁之后继续被使用，
// 必须与共享日志解耦，所以这里显式拷贝；代价是 O(批量大小) 而不是 O(日志长度)。
func (rf *Raft) entries(from, to int) []LogEntry {
	out := make([]LogEntry, to-from)
	copy(out, rf.log[from:to])
	return out
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

// 把 Raft 的持久化状态写入稳定存储。
// 在实现快照之前，第二个参数传 nil 即可。
func (rf *Raft) persist() {
	// ★ 3C 实现：
	// 用 labgob 编码 currentTerm、votedFor、log，然后：
	// rf.persister.Save(raftstate, nil)
	// 示例：
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.currentTerm)
	// e.Encode(rf.votedFor)
	// e.Encode(rf.log)
	// rf.persister.Save(w.Bytes(), nil)
}

// 从持久化数据中恢复状态（崩溃重启时调用）。
func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 { // 首次启动，无历史状态
		return
	}
	// ★ 3C 实现：解码 currentTerm、votedFor、log，赋值给 rf 对应字段
}

// 返回当前 Raft 持久化状态占用的字节数（用于测试）。
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// 上层服务通知 Raft：index 及之前的日志已经写入快照 snapshot，可以裁剪日志了。
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// ★ 3D 实现：裁剪 log，把 index 之前的条目丢弃，
	//   并保存 snapshot 到 persister。
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
	reply.XLen = len(rf.log)

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
	if rf.entry(args.PrevLogIndex).Term != args.PrevLogTerm {
		// 冲突：告诉 leader 冲突位置的 term，以及该 term 在 follower 里的第一条
		reply.XTerm = rf.entry(args.PrevLogIndex).Term
		i := args.PrevLogIndex
		for i > 0 && rf.entry(i-1).Term == reply.XTerm {
			i--
		}
		reply.XIndex = i
		return
	}
	reply.Success = true

	// ---- 追加 / 截断 ----
	// 图 2：如果已有条目与新条目冲突（同下标不同 term），删除该条目及其之后的所有条目。
	if len(args.Entries) > 0 {
		conflict := -1
		for k, e := range args.Entries {
			i := args.PrevLogIndex + 1 + k
			if i > rf.lastIndex() {
				break // 后面都是新条目，没有冲突
			}
			if rf.entry(i).Term != e.Term {
				conflict = i
				break
			}
		}
		if conflict >= 0 {
			rf.log = rf.log[:conflict] // 丢弃冲突条目及其之后的全部
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
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, args.PrevLogIndex+len(args.Entries))
		rf.applyCond.Broadcast()
	}
}

// 后台循环：把 [applyIndex+1, commitIndex] 的条目按顺序交给状态机。
// 每个节点有且只有一个 applier —— 多个 applier 会让 ApplyMsg 乱序或重复。
func (rf *Raft) applier() {
	for {
		rf.mu.Lock()
		for rf.applyIndex >= rf.commitIndex {
			rf.applyCond.Wait() // Wait 原子地解锁并挂起，被唤醒时重新加锁
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

// 每个 peer 一个常驻 goroutine，独占"向该 peer 发送 AppendEntries"这件事，
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

		var reply AppendEntriesReply
		ok := rf.sendAppendEntries(peer, args, &reply)

		rf.mu.Lock()
		more := rf.handleAppendReply(peer, args, &reply, ok)
		rf.mu.Unlock()

		if more {
			continue // 还有积压、或刚回退完 nextIndex，立刻重试不要等
		}
		select {
		case <-rf.notify[peer]: // 有新日志，立刻发
		case <-time.After(heartbeatInterval):
		}
	}
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
		for i := rf.lastIndex(); i >= 1; i-- {
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
	if rf.nextIndex[peer] < 1 {
		rf.nextIndex[peer] = 1 // 至少保留哨兵，prevLogIndex 不能为负
	}
	return true // 立刻重试，加速追赶（TestBackup3B 靠这个）
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
