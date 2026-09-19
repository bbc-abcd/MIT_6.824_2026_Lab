package shardrpc

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
)

// ErrStale 表示请求携带的 Num 比本组为这个分片已经见过的最大 Num 还旧。
// 它只可能来自一个已经被新配置取代的旧控制器，收到它的调用者应当放弃
// 手头的配置变更（新控制器会把它做完）。
const ErrStale = rpc.Err("ErrStale")

type FreezeShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

// State 是该分片被冻结时的键/值快照，控制器把它原样转交给目标组。
// Num 回报本组见过的编号，便于调用者判断自己是否已经过期。
type FreezeShardReply struct {
	State []byte
	Num   shardcfg.Tnum
	Err   rpc.Err
}

type InstallShardArgs struct {
	Shard shardcfg.Tshid
	State []byte
	Num   shardcfg.Tnum
}

type InstallShardReply struct {
	Err rpc.Err
}

type DeleteShardArgs struct {
	Shard shardcfg.Tshid
	Num   shardcfg.Tnum
}

type DeleteShardReply struct {
	Err rpc.Err
}
