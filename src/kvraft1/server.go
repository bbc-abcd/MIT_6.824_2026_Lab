package kvraft

import (
	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

type KVServer struct {
	me  int
	rsm *rsm.RSM

	// 在这里添加你的定义。
}

// 要将 req 转换为正确的类型，可以参考下面 Go 的类型 switch 或类型断言：
//
// https://go.dev/tour/methods/16
// https://go.dev/tour/methods/15
func (kv *KVServer) DoOp(req any) any {
	// 在这里编写你的代码
	return nil
}

func (kv *KVServer) Snapshot() []byte {
	// 在这里编写你的代码
	return nil
}

func (kv *KVServer) Restore(data []byte) {
	// 在这里编写你的代码
}

func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// 在这里编写你的代码。使用 kv.rsm.Submit() 提交 args。
	// 你可以使用 Go 的类型转换把 Submit() 返回的 any 值
	// 转成 GetReply：rep.(rpc.GetReply)
}

func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// 在这里编写你的代码。使用 kv.rsm.Submit() 提交 args。
	// 你可以使用 Go 的类型转换把 Submit() 返回的 any 值
	// 转成 PutReply：rep.(rpc.PutReply)
}

// StartKVServer() 和 MakeRSM() 必须快速返回，因此它们应该为任何
// 长时间运行的工作启动 goroutine。
func StartKVServer(servers []*labrpc.ClientEnd, gid tester.Tgid, me int, persister *tester.Persister, maxraftstate int) []any {
	// 对希望 Go 的 RPC 库进行 marshall/unmarshall 的结构体
	// 调用 labgob.Register。
	labgob.Register(rsm.Op{})
	labgob.Register(rpc.PutArgs{})
	labgob.Register(rpc.GetArgs{})

	kv := &KVServer{me: me}

	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)
	// 你可能需要在这里添加初始化代码。
	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartKVServer(ends, Gid, srv, persister, tester.MaxRaftState)
}
