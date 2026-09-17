package kvraft

import (
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

// node 是一个键对应的 (value, version)。version 是该键被写入的次数，
// 刚创建的键 version 为 1。
type node struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	me  int
	rsm *rsm.RSM

	mu sync.Mutex // 保护 m
	m  map[string]node
}

// DoOp 在 reader goroutine 中按日志顺序执行每个已提交的命令。
// 它被 rsm 调用，因此必须自己加锁保护状态；返回值的类型与上层
// 期望一致（GetReply / PutReply），由 rsm 原样交还给 Submit。
func (kv *KVServer) DoOp(req any) any {
	switch op := req.(type) {
	case rpc.GetArgs:
		kv.mu.Lock()
		n, ok := kv.m[op.Key]
		kv.mu.Unlock()
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: n.value, Version: n.version, Err: rpc.OK}

	case rpc.PutArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		n, ok := kv.m[op.Key]
		if !ok {
			// key 不存在：只有 version == 0 时才创建。
			if op.Version != 0 {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
			kv.m[op.Key] = node{value: op.Value, version: 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		// key 存在：版本必须精确匹配。这正是"重传的 Put 至多执行一次"的
		// 保证——第一次副本把 version 加 1，之后所有副本都会在这里失败。
		if op.Version != n.version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		kv.m[op.Key] = node{value: op.Value, version: n.version + 1}
		return rpc.PutReply{Err: rpc.OK}
	}
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// B 部分不需要快照（maxraftstate = -1），C 部分再实现。
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// B 部分不需要，C 部分从快照重建 kv.m。
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	err, rep := kv.rsm.Submit(rpc.GetArgs{Key: args.Key})
	if err != rpc.OK {
		reply.Err = err // ErrWrongLeader，交给 Clerk 换 server 重试
		return
	}
	*reply = rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	err, rep := kv.rsm.Submit(rpc.PutArgs{Key: args.Key, Value: args.Value, Version: args.Version})
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(rpc.PutReply)
}

// StartKVServer() 和 MakeRSM() 必须快速返回，因此它们应该为任何
// 长时间运行的工作启动 goroutine。
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// 对希望 Go 的 RPC 库进行 marshall/unmarshall 的结构体
	// 调用 labgob.Register。
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{
		me: me,
		m:  make(map[string]node),
	}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
