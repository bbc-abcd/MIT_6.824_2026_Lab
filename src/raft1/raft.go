package raft

// 该文件实现 Raft 核心。raftapi.go 定义了 Raft 对外暴露的接口。
// Make() 负责创建一个 Raft peer 实例。

import (
	//	"bytes"          // 3C 持久化时会用到（编码/解码）
	"math/rand" // 生成随机选举超时时间
	"sync"      // 互斥锁
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
	lastHeartbeat   time.Time             //对leader：最近一次发送Append的时间
	voteCount       int                   //投票记录
	applyCh         chan raftapi.ApplyMsg //应用到上层服务的通道
	log             []LogEntry            // 日志记录
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

// 心跳间隔100ms
const heartbeatInterval = 100 * time.Millisecond

// 随机初始化选举时间800-1200ms
func randomElectionTimeout() time.Duration {
	return time.Duration(800+rand.Intn(400)) * time.Millisecond
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

	lastLogIndex := len(rf.log) - 1
	lastLogTerm := -1
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
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

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

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
	rf.state = Follower
	rf.lastContact = time.Now()
	reply.Term = rf.currentTerm
	reply.Success = true
}

// 发送 RequestVote RPC 的辅助函数。
// server 是目标节点在 peers[] 中的下标。
// 由于网络可能丢包/延迟，Call 返回 false 只表示本次 RPC 失败，不一定是对方宕机。
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// 上层服务提交一条新命令给 Raft。
// 如果不是 Leader → 返回 false。
// 如果是 Leader → 把命令追加到本地日志，立即返回（异步提交）。
// 返回值：(命令将被提交到的日志下标, curTerm, isLeader)
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != Leader {
		return 0, rf.currentTerm, false
	}
	return len(rf.log), rf.currentTerm, true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	return rf.peers[server].Call("Raft.AppendEntries", args, reply)
}

func (rf *Raft) sendHeartbeats(term int) {
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		args := &AppendEntriesArgs{Term: term, LeaderId: rf.me, LeaderCommit: -1}
		go func(server int, args *AppendEntriesArgs) {
			var reply AppendEntriesReply
			if !rf.sendAppendEntries(server, args, &reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = Follower
				rf.persist()
			}
		}(server, args)
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
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := -1
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
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
				rf.state = Leader
				// leader的tricker会检查心跳时间
				rf.lastHeartbeat = time.Time{}
				go rf.sendHeartbeats(term)
			}
		}(server, args)
	}
}

// 后台循环：检查是否应该发起选举。
func (rf *Raft) ticker() {
	for {
		// 每10ms检查一次状态
		time.Sleep(10 * time.Millisecond)
		rf.mu.Lock()
		now := time.Now()
		if rf.state == Leader {
			// 检查心跳是否超时
			if now.Sub(rf.lastHeartbeat) >= heartbeatInterval {
				term := rf.currentTerm
				rf.lastHeartbeat = now
				rf.lastContact = now
				rf.mu.Unlock()
				rf.sendHeartbeats(term)
				continue
			}
			rf.mu.Unlock()
			continue
		}
		if now.Sub(rf.lastContact) >= rf.electionTimeout {
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
	rf.lastHeartbeat = time.Time{}
	rf.applyCh = applyCh
	rf.log = make([]LogEntry, 0)

	// 从持久化状态恢复（若之前崩溃过）
	rf.readPersist(persister.ReadRaftState())

	// 启动后台 ticker，负责选举与心跳
	go rf.ticker()

	return rf
}
