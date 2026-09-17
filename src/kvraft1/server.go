package kvraft

import (
	"bytes"
	"log"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

// node 是一个键对应的 (value, version)。version 是该键被写入的次数，
// 刚创建的键 version 为 1。
//
// 字段必须导出：gob 只编码导出字段，小写字段会被静默跳过（labgob 也会就此
// 报错），快照恢复出来就是一张空表。Version 必须进快照，否则重启后 CAS
// 的版本号从 0 重新开始，Put 会误判成"键不存在"而重复执行。
type node struct {
	Value   string
	Version rpc.Tversion
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
		return rpc.GetReply{Value: n.Value, Version: n.Version, Err: rpc.OK}

	case rpc.PutArgs:
		kv.mu.Lock()
		defer kv.mu.Unlock()
		n, ok := kv.m[op.Key]
		if !ok {
			// key 不存在：只有 version == 0 时才创建。
			if op.Version != 0 {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
			kv.m[op.Key] = node{Value: op.Value, Version: 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		// key 存在：版本必须精确匹配。这正是"重传的 Put 至多执行一次"的
		// 保证——第一次副本把 version 加 1，之后所有副本都会在这里失败。
		if op.Version != n.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		kv.m[op.Key] = node{Value: op.Value, Version: n.Version + 1}
		return rpc.PutReply{Err: rpc.OK}
	}
	return nil
}

// Snapshot 在 reader goroutine 中、紧跟某条命令执行之后被调用，因此
// 拍下来的必然是"执行到某个确定下标为止"的完整状态——DoOp 只有 reader
// 这一个调用者，序列化期间不会有别的命令插进来。
//
// 自己加锁而不是依赖"只有 reader 会碰 kv.m"：加锁的代价可以忽略，而它
// 让这个方法的正确性不依赖于调用者的约定。返回的字节必须非空，否则
// raft 会存下一个空快照，重启时读出来长度为 0，状态机就再也恢复不了了。
func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.m); err != nil {
		log.Fatalf("%v: encode snapshot failed: %v", kv.me, err)
	}
	return w.Bytes()
}

// Restore 从快照重建 kv.m，整表替换（不是合并）。调用时机有两个：启动时
// 从 persister 读出来的那份，以及运行期间 raft 经 applyCh 送来的那份。
// 两处都在 reader 处理命令之前/之间发生，不会和 DoOp 并发。
func (kv *KVServer) Restore(data []byte) {
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var m map[string]node
	if err := d.Decode(&m); err != nil {
		log.Fatalf("%v: decode snapshot failed: %v", kv.me, err)
	}
	if m == nil {
		m = make(map[string]node)
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.m = m
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
