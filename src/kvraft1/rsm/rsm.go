// Package rsm 定义了复制状态机（RSM）中间件的框架。
// 它位于 Raft 共识层与具体状态机（例如 KVServer）之间。
package rsm

import (
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	raft "6.5840/raft1"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// Op 表示一条将要通过 Raft 复制的命令。
// 当前框架中该结构体为空，具体字段尚未定义。
// 注释提醒：字段名必须大写，否则 RPC/gob 序列化会失败。
type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
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
	// Your definitions here.         // 预留位置：当前框架尚未定义其他字段
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
	}
	if !tester.UseRaftStateMachine {
		rsm.rf = raft.Make(servers, me, persister, rsm.applyCh)
	}
	return rsm
}

// Raft 返回底层 Raft 实例的接口。
// 上层服务可以通过它访问 Raft 的状态，例如判断当前节点是否为 Leader。
func (rsm *RSM) Raft() raftapi.Raft {
	return rsm.rf
}

// Submit 将一个命令提交给 Raft，并等待其被提交。
// 如果客户端应寻找新的 Leader 并重试，则返回 ErrWrongLeader。
//
// 当前框架中该方法为桩实现，直接返回 ErrWrongLeader 和 nil。
// 注释说明：Submit 内部会构造 Op 结构体，把 req 包装后送入 Raft。
func (rsm *RSM) Submit(req any) (rpc.Err, any) {

	// Submit creates an Op structure to run a command through Raft;
	// for example: op := Op{Me: rsm.me, Id: id, Req: req}, where req
	// is the argument to Submit and id is a unique id for the op.

	// your code here

	return rpc.ErrWrongLeader, nil // i'm dead, try another server.
}
