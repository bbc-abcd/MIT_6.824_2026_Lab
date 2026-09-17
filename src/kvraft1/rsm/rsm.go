// Package rsm 定义了复制状态机（RSM）中间件的框架。
// 它位于 Raft 共识层与具体状态机（例如 KVServer）之间。
package rsm

import (
	"sync"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// checkInterval 是 Submit 等待结果期间检查自己是否还持有领导权的时间间隔。
// 前 leader 被单独分区后，它永远不会看到 index 上的命令（那些日志会被新 leader
// 覆盖，或者干脆不再有日志落到那些下标上），因此必须靠任期变化主动退出等待。
const checkInterval = 10 * time.Millisecond

// Op 表示一条将要通过 Raft 复制的命令。
// (Me, Id) 二元组唯一标识一次客户端请求：只有 Me 是不够的，因为每个 server
// 的计数器都从 0 开始，不同 server 会生成相同的 Id。
type Op struct {
	Me      int // 提交该命令的 server（即 rsm.me）
	Id      int
	Command any // 命令本身
}

// opResult 是 reader 交给等待中的 Submit 的裁定结果。
type opResult struct {
	err rpc.Err
	rep any
}

// waiter 记录一个正在等待结果的 Submit。
// 存下 Me/Id 是为了让 reader 自己判断"这个下标上的命令是不是你在等的那条"，
// Submit 拿到结果时就不必再关心匹配逻辑。
type waiter struct {
	ch chan opResult
	me int
	id int
}

// StateMachine 是具体状态机必须实现的接口。
// RSM 通过该接口调用上层服务执行操作、生成快照和恢复快照。
type StateMachine interface {
	// DoOp 执行一个操作，并返回执行结果。
	DoOp(any) any

	// Snapshot 返回状态机的快照字节。
	Snapshot() []byte

	// Restore 从快照字节恢复状态机。
	Restore([]byte)
}

// RSM 是复制状态机中间件的核心结构。
// 它持有底层 Raft 实例、apply 通道以及具体状态机，并对外提供 Submit 方法。
type RSM struct {
	mu           sync.Mutex            // 保护 RSM 内部共享状态
	me           int                   // 当前服务器在 servers[] 中的索引
	rf           raftapi.Raft          // 底层 Raft 实例
	applyCh      chan raftapi.ApplyMsg // 从 Raft 接收已提交日志的通道
	maxraftstate int                   // 当 Raft 状态超过该字节数时触发快照；-1 表示不需要快照
	sm           StateMachine          // 具体状态机，由上层服务实现

	reqNum  int             // 已提交的请求计数，用于生成唯一 Id（调用时必须持有 mu）
	pending map[int]*waiter // 日志下标 -> 等待该下标的 Submit
	down    bool            // applyCh 已关闭，不再接受新的提交
}

// reader 是唯一读取 applyCh 的 goroutine，也是唯一调用 sm.DoOp 的地方。
// 它按日志下标的顺序执行每个已提交的命令，并把结果交给等待该下标的 Submit。
//
// 注意：调用 DoOp 时不能持有 rsm.mu —— DoOp 会去拿上层服务自己的锁，而服务
// 的 RPC handler 又是先拿自己的锁再调用 Submit（Submit 需要 rsm.mu），
// 持锁调用 DoOp 会构成 ABBA 死锁。同理，向 channel 投递也放在锁外。
func (rsm *RSM) reader() {
	for {
		msg, ok := <-rsm.applyCh
		if !ok {
			// raft 关闭了 applyCh（Kill）：唤醒所有等待者，之后 Submit 直接失败。
			rsm.mu.Lock()
			rsm.down = true
			for index, w := range rsm.pending {
				w.ch <- opResult{err: rpc.ErrWrongLeader} // cap 1，不会阻塞
				delete(rsm.pending, index)
			}
			rsm.mu.Unlock()
			return
		}
		if !msg.CommandValid {
			// 4C 的 SnapshotValid 分支加在这里。
			continue
		}
		op, ok := msg.Command.(Op)
		if !ok {
			continue // 不是本 rsm 的命令，忽略
		}

		// 认领等待者：命中则连同投递一起完成，未命中说明这条命令没有 submit 在等。
		rsm.mu.Lock()
		w, waiting := rsm.pending[msg.CommandIndex]
		delete(rsm.pending, msg.CommandIndex)
		rsm.mu.Unlock()

		rep := rsm.sm.DoOp(op.Command) // 每个已提交的命令都必须执行一次

		if waiting {
			// 把状态机执行的结果发送到通道
			if op.Me == w.me && op.Id == w.id {
				w.ch <- opResult{err: rpc.OK, rep: rep}
			} else {
				// 该下标上换成了别人的命令，说明本 server 的这条请求已经丢失。
				w.ch <- opResult{err: rpc.ErrWrongLeader}
			}
		}
	}
}

// MakeRSM 创建并返回一个 RSM 实例。
//
// 参数说明：
//
//	servers[]    包含参与 Raft 的服务器端口。
//	me           当前服务器在 servers[] 中的索引。
//	persister    持久化器，用于保存 Raft 状态和快照。
//	maxraftstate 触发快照的 Raft 状态大小阈值；-1 表示不需要快照。
//	sm           具体状态机实现。
//
// 当前框架会初始化 RSM 的基础字段、创建 applyCh，
// 并在非测试模式下创建底层 Raft 实例。
// MakeRSM 必须快速返回，因此长时间运行的工作应放在 goroutine 中。
func MakeRSM(servers []*labrpc.ClientEnd, me int, persister *tester.Persister, maxraftstate int, sm StateMachine) *RSM {
	rsm := &RSM{
		me:           me,
		maxraftstate: maxraftstate,
		applyCh:      make(chan raftapi.ApplyMsg),
		sm:           sm,
		pending:      make(map[int]*waiter),
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	go rsm.reader()
	return rsm
}

// Raft 返回底层 Raft 实例的接口。
// 上层服务可以通过它访问 Raft 的状态，例如判断当前节点是否为 Leader。
func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// stale 判断以 term 任期提交的请求是否已经不可能成功：
// 自己不再是 leader，或者任期已经改变（日志可能被新 leader 覆盖）。
func (rsm *RSM) stale(term int) bool {
	rsm.mu.Lock()
	down := rsm.down
	rsm.mu.Unlock()
	if down {
		return true
	}
	cur, isLeader := rsm.rf.GetState()
	return !isLeader || cur != term
}

// discard 撤销 index 上的等待登记；只有确认那个位置还是自己的 waiter 才删，
// 避免误删后来者的登记。
func (rsm *RSM) discard(index int, ch chan opResult) {
	rsm.mu.Lock()
	defer rsm.mu.Unlock()
	if w, ok := rsm.pending[index]; ok && w.ch == ch {
		delete(rsm.pending, index)
	}
}

// Submit 将一个命令提交给 Raft，并等待其被提交。
// 如果客户端应寻找新的 Leader 并重试，则返回 ErrWrongLeader。
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// 注册必须在 Start 之前连同 Start 一起在一个临界区内完成：否则 reader 可能
	// 在这条日志被 apply 之后才看到登记，那条结果就永远送不到这里。
	rsm.mu.Lock()
	if rsm.down {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil
	}
	rsm.reqNum++
	op := Op{Me: rsm.me, Id: rsm.reqNum, Command: req}
	index, term, isLeader := rsm.rf.Start(op)
	if !isLeader {
		rsm.mu.Unlock()
		return rpc.ErrWrongLeader, nil // 本 peer 不是 leader，换个 server 重试
	}
	ch := make(chan opResult, 1) // cap 1：保证 reader 投递时永不阻塞
	rsm.pending[index] = &waiter{ch: ch, me: op.Me, id: op.Id}
	rsm.mu.Unlock()

	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()
	for {
		select {
		case r := <-ch:
			return r.err, r.rep
		case <-ticker.C:
			// 放弃之前先看一眼结果是否已经到了：日志可能其实已经提交，
			// 只是任期同时发生了变化。
			select {
			case r := <-ch:
				return r.err, r.rep
			default:
			}
			if rsm.stale(term) {
				rsm.discard(index, ch)
				return rpc.ErrWrongLeader, nil
			}
		}
	}
}
