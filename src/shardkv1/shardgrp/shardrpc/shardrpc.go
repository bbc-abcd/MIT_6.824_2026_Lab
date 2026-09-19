package shardrpc

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
)

// ErrStale 表示请求携带的 Num 比本组为这个分片已经见过的最大 Num 还旧。
// 它只可能来自一个已经被新配置取代的旧控制器，收到它的调用者应当放弃
// 手头的配置变更（新控制器会把它做完）。
const ErrStale = rpc.Err("ErrStale")

// ErrUnreachable 表示整组联系不上，本次调用没拿到任何答复。
//
// 它**永远不出现在网络上**，只是组 clerk 给控制器的进程内信号。分片组从
// 不主动回这个错误，判决权在控制器手里：MoveShard 的三个 RPC 都是幂等的，
// 拿到这个信号之后到底是"再试一次"还是"这件事已经由别人做完了、收工"，
// 只有手里拿着配置的人才知道。见 ShardCtrler.moveUntilDone。
//
// 必须有这条路径：一个已经被测试器杀掉的分片组永远不会再回任何答复，
// 原地重试会把控制器永久扣住。B/C 部分里控制器会被复制着跑同一段搬运，
// 而 ExitGroup 恰好紧跟在配置发布之后 —— 重复搬运的 RPC 正好落在这个
// 窗口里，撞上一个已经消失的组。
const ErrUnreachable = rpc.Err("ErrUnreachable")

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
