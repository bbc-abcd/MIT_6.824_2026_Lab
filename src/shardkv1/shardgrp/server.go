package shardgrp

import (
	"bytes"
	"log"
	"sync"

	"6.5840/kvraft1/rsm"
	"6.5840/kvsrv1/rpc"
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

const (
	ENVKEY = "65840ENV"
)

// node 是一个键对应的 (value, version)，与 lab4 的 kvraft 相同。
// 字段必须导出，否则 labgob 会静默跳过，快照恢复出来就是一张空表。
type node struct {
	Value   string
	Version rpc.Tversion
}

// shardState 是整个分片组状态机的持久化形态。
//
// 除了每个分片的数据，还必须记住两件"元信息"：
//   - Served：本组是否正在服务该分片。客户端请求和 FreezeShard 都由它裁决，
//     而它完全由 Freeze/Install/Delete 三条命令驱动 —— 分片组从不读配置，
//     所以配置变更不需要分片组之间直接通信。
//   - Num：本组为每个分片见过的最大配置编号，用来丢弃被网络延迟、或者被
//     替换掉的旧控制器发来的过期 RPC。它必须进快照，否则组重启后归零，
//     过期请求就能重新打进来。
type shardState struct {
	Shards [shardcfg.NShards]map[string]node
	Served [shardcfg.NShards]bool
	Num    [shardcfg.NShards]shardcfg.Tnum
}

type KVServer struct {
	me  int
	rsm *rsm.RSM
	gid tester.Tgid

	mu    sync.Mutex
	state shardState
}

// init 设置状态机的初始值，只在服务器构造函数里调用一次。
//
// 第一个分片组创建时拥有全部分片；其他组从零开始，等着控制器把分片
// Install 进来。这个初始值必须设置在本组的第一次快照/日志重放之前：
// Gid1 被 leave 过之后再重启时，Freeze/Delete 会从日志里重放出来，把它
// 带回"不拥有这些分片"的正确状态。所以它只是状态机的起点，不是每轮启动
// 的兜底 —— 这里不能每次启动都无条件重置。
func (kv *KVServer) init() {
	for s := 0; s < shardcfg.NShards; s++ {
		kv.state.Shards[s] = make(map[string]node)
	}
	if kv.gid == shardcfg.Gid1 {
		for s := 0; s < shardcfg.NShards; s++ {
			kv.state.Served[s] = true
		}
	}
}

// encodeShard 序列化一个分片的数据，调用时必须持有 kv.mu。
func (kv *KVServer) encodeShard(s shardcfg.Tshid) []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(kv.state.Shards[s]); err != nil {
		log.Fatalf("%v: encode shard %v failed: %v", kv.me, s, err)
	}
	return w.Bytes()
}

// DoOp 在 reader goroutine 中按日志顺序执行每个已提交的命令。
// 它是唯一改状态的地方，也是唯一做正确性裁决的地方。
func (kv *KVServer) DoOp(req any) any {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	switch op := req.(type) {
	case rpc.GetArgs:
		s := shardcfg.Key2Shard(op.Key)
		if !kv.state.Served[s] {
			return rpc.GetReply{Err: rpc.ErrWrongGroup}
		}
		n, ok := kv.state.Shards[s][op.Key]
		if !ok {
			return rpc.GetReply{Err: rpc.ErrNoKey}
		}
		return rpc.GetReply{Value: n.Value, Version: n.Version, Err: rpc.OK}

	case rpc.PutArgs:
		s := shardcfg.Key2Shard(op.Key)
		if !kv.state.Served[s] {
			return rpc.PutReply{Err: rpc.ErrWrongGroup}
		}
		m := kv.state.Shards[s]
		n, ok := m[op.Key]
		if !ok {
			// key 不存在：只有 version == 0 时才创建。
			if op.Version != 0 {
				return rpc.PutReply{Err: rpc.ErrNoKey}
			}
			m[op.Key] = node{Value: op.Value, Version: 1}
			return rpc.PutReply{Err: rpc.OK}
		}
		// 版本精确匹配才写入，这正是重传的 Put 至多生效一次的保证。
		if op.Version != n.Version {
			return rpc.PutReply{Err: rpc.ErrVersion}
		}
		m[op.Key] = node{Value: op.Value, Version: n.Version + 1}
		return rpc.PutReply{Err: rpc.OK}

	case shardrpc.FreezeShardArgs:
		s := op.Shard
		if op.Num < kv.state.Num[s] {
			return shardrpc.FreezeShardReply{Err: shardrpc.ErrStale, Num: kv.state.Num[s]}
		}
		kv.state.Num[s] = op.Num
		kv.state.Served[s] = false
		// 数据保留到 DeleteShard 为止。同一个 Num 被重放时（B 部分接手的新
		// 控制器会这么做）重新序列化一遍即可 —— Served 已经是 false，没有
		// Put 能再改这张表，所以重放拿到的字节和第一次完全相同，天然幂等。
		return shardrpc.FreezeShardReply{
			State: kv.encodeShard(s),
			Num:   op.Num,
			Err:   rpc.OK,
		}

	case shardrpc.InstallShardArgs:
		s := op.Shard
		if op.Num < kv.state.Num[s] {
			return shardrpc.InstallShardReply{Err: shardrpc.ErrStale}
		}
		if op.Num == kv.state.Num[s] && kv.state.Served[s] {
			// 同一个配置已经装过了（重复的 InstallShard），保持现状。
			// 这条分支不能少：被重放的 Freeze 可能返回一个空快照，
			// 照单全收会把已经搬过来的数据清空。
			return shardrpc.InstallShardReply{Err: rpc.OK}
		}
		m := make(map[string]node)
		if len(op.State) > 0 {
			d := labgob.NewDecoder(bytes.NewBuffer(op.State))
			if err := d.Decode(&m); err != nil {
				log.Fatalf("%v: decode shard %v failed: %v", kv.me, s, err)
			}
			if m == nil {
				m = make(map[string]node)
			}
		}
		kv.state.Shards[s] = m
		kv.state.Served[s] = true
		kv.state.Num[s] = op.Num
		return shardrpc.InstallShardReply{Err: rpc.OK}

	case shardrpc.DeleteShardArgs:
		s := op.Shard
		if op.Num < kv.state.Num[s] {
			return shardrpc.DeleteShardReply{Err: shardrpc.ErrStale}
		}
		kv.state.Num[s] = op.Num
		kv.state.Served[s] = false
		// 真删，不只是取消标记：TestDeleteBasic5A 会拿快照大小卡这一条。
		kv.state.Shards[s] = make(map[string]node)
		return shardrpc.DeleteShardReply{Err: rpc.OK}
	}
	return nil
}

// Snapshot 在 reader goroutine 中、紧跟某条命令执行之后被调用，
// 所以拍下来的必然是"执行到某个确定下标为止"的完整状态。
func (kv *KVServer) Snapshot() []byte {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if err := e.Encode(&kv.state); err != nil {
		log.Fatalf("%v: encode snapshot failed: %v", kv.me, err)
	}
	return w.Bytes()
}

// Restore 从快照重建状态，整表替换。调用时机有两个：MakeRSM 启动时读
// persister 的那份（此时 reader 还没起来），以及运行期间 raft 经 applyCh
// 送来的那份。两处都在 reader 处理命令之前/之间发生，不会和 DoOp 并发。
func (kv *KVServer) Restore(data []byte) {
	if len(data) == 0 {
		return
	}
	var st shardState
	d := labgob.NewDecoder(bytes.NewBuffer(data))
	if err := d.Decode(&st); err != nil {
		log.Fatalf("%v: decode snapshot failed: %v", kv.me, err)
	}
	for s := 0; s < shardcfg.NShards; s++ {
		if st.Shards[s] == nil {
			st.Shards[s] = make(map[string]node)
		}
	}

	kv.mu.Lock()
	kv.state = st
	kv.mu.Unlock()
}

// Get 和 Put 都必须老老实实走 rsm.Submit，不能在处理函数里先拿内存里的
// Served 做一次"我不是这个分片的组"的提前拒绝。
//
// 那样看着省了一趟 raft 往返，实际会把自己锁死：分片组重启时 Served 的
// 真相要靠重放 raft 日志才恢复，而日志重放又需要有一条当前任期的新日志
// 把 commitIndex 推上去。提前拒绝的请求根本进不了日志，于是永远没有新
// 条目、日志永远不重放、Served 永远是 false，客户端拿着没变的配置反复
// 重试也没用。判归属只能由 DoOp 在日志序上做。
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

// 冻结指定的分片（即拒绝未来针对该分片的 Get/Put 操作），
// 并返回该分片中存储的键/值。
func (kv *KVServer) FreezeShard(args *shardrpc.FreezeShardArgs, reply *shardrpc.FreezeShardReply) {
	err, rep := kv.rsm.Submit(shardrpc.FreezeShardArgs{Shard: args.Shard, Num: args.Num})
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.FreezeShardReply)
}

// 为指定的分片安装所提供的状态。
func (kv *KVServer) InstallShard(args *shardrpc.InstallShardArgs, reply *shardrpc.InstallShardReply) {
	// State 是调用者的缓冲区，而 Submit 要等到这条日志被 apply 之后才返回，
	// 中间可能隔着很久；复制一份，免得它被并发改写。
	state := make([]byte, len(args.State))
	copy(state, args.State)

	err, rep := kv.rsm.Submit(shardrpc.InstallShardArgs{Shard: args.Shard, State: state, Num: args.Num})
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.InstallShardReply)
}

// 删除指定的分片。
func (kv *KVServer) DeleteShard(args *shardrpc.DeleteShardArgs, reply *shardrpc.DeleteShardReply) {
	err, rep := kv.rsm.Submit(shardrpc.DeleteShardArgs{Shard: args.Shard, Num: args.Num})
	if err != rpc.OK {
		reply.Err = err
		return
	}
	*reply = rep.(shardrpc.DeleteShardReply)
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
	// 必须在 MakeRSM 之前：MakeRSM 会用 persister 里的快照调 Restore，
	// 那才是重启后真正的真相，初始值只该在没有快照时生效。
	kv.init()
	kv.rsm = rsm.MakeRSM(servers, me, persister, maxraftstate, kv)

	return []any{kv, kv.rsm.Raft()}
}

func NewServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, grp tester.Tgid, srv int, persister *tester.Persister) []any {
	return StartServerShardGrp(ends, grp, srv, persister, tester.MaxRaftState)
}
