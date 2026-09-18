package shardgrp

import (
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	// 你的代码放在这里
}

func (kv *KVServer) DoOp(req any) any {
	// 你的代码放在这里
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// 你的代码放在这里
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// 你的代码放在这里
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// 你的代码放在这里
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// 你的代码放在这里
}

// 冻结指定的分片（即拒绝未来针对该分片的 Get/Put 操作），
// 并返回该分片中存储的键/值。
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	// 你的代码放在这里
}

// 为指定的分片安装所提供的状态。
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// 你的代码放在这里
}

// 删除指定的分片。
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	// 你的代码放在这里
}

// StartShardServerGrp 为分片组 `gid` 启动一个服务器。
//
// StartShardServerGrp() 和 MakeRSM() 必须快速返回，因此它们应为
// 任何长时间运行的工作启动 goroutine。
func StartServerShardGrp(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// 对你希望 Go 的 RPC 库
	// 进行编组/解组的结构体调用 labgob.Register。
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})
	labgob.Register(shardrpc.FreezeShardArgs{})
	labgob.Register(shardrpc.InstallShardArgs{})
	labgob.Register(shardrpc.DeleteShardArgs{})
	labgob.Register(rsm.Op{})

	kv := &KVServer{gid: gid, me: me}
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	// 你的代码放在这里

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
