package shardgrp

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

// 扫完一圈仍然没找到 leader 时的等待时间。
const retryDelay = 50 * time.Millisecond

type Clerk struct {
	*tester.Clnt
	servers []string
	leader  int // 上一次成功的 leader（servers[] 中的索引）
	// 你可以向此结构体添加内容。
}

func MakeClerk(clnt *tester.Clnt, servers []string) *Clerk {
	ck := &Clerk{Clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// ErrWrongGroup 有两层含义，上层要能同时接住：
//
//   - 组活着但说"这个分片不归我管"：请求确实被处理过，只是没动任何字节。
//     上层重读配置就能找到新组，放心转交。
//   - 整组联系不上：连答复都没拿到。测试器在发布新配置之后会立刻把离开的组
//     杀掉，所以这多半说明配置已经变了、这个组正在消失。**一个死掉的组永远
//     不会回 ErrWrongGroup**，原地重试只会把客户端永久扣住。
//
// 两种情形都回 ErrWrongGroup 让上层重读配置。若这个组其实仍是分片的归属者
// （只是暂时不可达），上层重读后拿到同样的 gid、原样再进来，等价于继续重试，
// 只是每轮多读一次配置。Get 是只读的，重试永远安全，所以这里可以放手。
//
// Put 不行：放手之前必须把"可能已经执行过"这个信息带出去，见 Put 的注释。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}

	for {
		reached := false
		for i := 0; i < len(ck.servers); i++ {
			s := (ck.leader + i) % len(ck.servers)
			var reply rpc.GetReply
			ok := ck.Call(ck.servers[s], "KVServer.Get", &args, &reply)
			if !ok {
				continue
			}
			reached = true
			if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey || reply.Err == rpc.ErrWrongGroup {
				ck.leader = s
				return reply.Value, reply.Version, reply.Err
			}
		}
		if !reached {
			return "", 0, rpc.ErrWrongGroup // 整组联系不上，交给上层重读配置
		}
		time.Sleep(retryDelay)
	}
}

// Put 仅在请求中的版本号与服务器上该键的版本号匹配时才写入。
//
// 和 Get 的关键差别：Put 一旦可能生效，"可能"这个信息就必须原样带出去，
// 不能自己吞掉。具体说，如果前面某次尝试其实已经写进去了（比如请求到了、
// 回复丢了），那次写入会把版本号 +1，于是后续收到的 ErrVersion 其实是
// "我自己写的" —— 照实上报就成了"确定没执行"，与事实相反。所以只要
// resent 为真，ErrVersion 一律升级成 ErrMaybe。
//
// ErrWrongGroup 不在这里升级：它表示分片不归本组管、一个字节都没动，
// 该由上层重读配置后换组重试，升级的事留给上层的 resent 去做。
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}

	// resent 为真表示"已经发送过一个可能已被执行、但没拿到确定结果的请求"。
	resent := false

	for {
		reached := false
		for i := 0; i < len(ck.servers); i++ {
			s := (ck.leader + i) % len(ck.servers)
			var reply rpc.PutReply
			ok := ck.Call(ck.servers[s], "KVServer.Put", &args, &reply)
			if !ok {
				// 网络失败：这次尝试可能已经被执行（请求或回复被丢弃），
				// 也可能压根没到。分不清，只能按"可能执行过"处理。
				resent = true
				continue
			}
			reached = true
			if reply.Err == rpc.ErrWrongGroup {
				// 分片不归本组管，一个字节都没动。原样交给上层重读配置。
				// 这里不要自作主张升级成 ErrMaybe —— 上层的 resent 会把
				// 换组重试后的 ErrVersion 正确地升级掉，而升级早了反而会
				// 在"版本对得上、其实没写进去"时谎报成功。
				ck.leader = s
				return rpc.ErrWrongGroup
			}
			if reply.Err == rpc.ErrVersion {
				ck.leader = s
				if resent {
					// 版本已经对不上，但上一次尝试可能就是我们自己写进去的。
					return rpc.ErrMaybe
				}
				return rpc.ErrVersion
			}
			if reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey {
				ck.leader = s
				return reply.Err
			}
			// ErrWrongLeader：请求进了日志，但它可能已经在某个下标上被
			// 提交、又可能被新 leader 覆盖。同样按"可能执行过"处理。
			resent = true
		}
		if !reached {
			// 整组联系不上：原地死等不行（这个组可能已经被测试器杀掉，一个
			// 死掉的组永远不会再回任何答案），所以放手，回到上层去重读配置。
			//
			// 这里**不能**报 ErrMaybe。线性一致性模型（models1/kv.go 的 Put
			// 分支）是按"状态版本是否等于请求版本"来选分支的：
			//
			//	if st.Version == inp.Version {
			//	    return out.Err == "OK" || out.Err == "ErrMaybe", {value, ver+1}
			//
			// 也就是说，版本对上时 ErrMaybe 会被当成"确实写进去了"，状态必须
			// 前进。而我们这里恰恰不知道写没写进去 —— 报 ErrMaybe 就是在撒谎，
			// 紧随其后的 Get 会立刻把它戳穿。
			//
			// 交回上层是对的：上层会重读配置、可能换一个组、拿同一个版本号
			// 重试。真没生效就正常写入；真生效了数据会跟着分片搬走，新组会
			// 因为版本已经 +1 而回 ErrVersion —— 上层的 resent 会把它升级成
			// ErrMaybe（版本已经不对，模型接受 ErrMaybe 且状态不变，正确）。
			return rpc.ErrWrongGroup
		}
		time.Sleep(retryDelay)
	}
}

// FreezeShard 冻结一个分片并取回它的数据。ErrStale 意味着本组已经见过更大
// 的 Num，调用者是个被取代的旧控制器，应当放弃这次配置变更。
//
// 扫完一圈没有任何服务器应答时返回 ErrUnreachable，而不是继续原地重试：
// 调用者（控制器）需要知道"没拿到答复"，才能判断是该再试一次、还是这次
// 搬运已经由别人做完了、对应的组也随之消失。这里的三个方法都是同一个形状。
func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	args := shardrpc.FreezeShardArgs{Shard: s, Num: num}

	for {
		reached := false
		for i := 0; i < len(ck.servers); i++ {
			j := (ck.leader + i) % len(ck.servers)
			var reply shardrpc.FreezeShardReply
			if !ck.Call(ck.servers[j], "KVServer.FreezeShard", &args, &reply) {
				continue
			}
			reached = true
			if reply.Err == rpc.OK || reply.Err == shardrpc.ErrStale {
				ck.leader = j
				return reply.State, reply.Err
			}
		}
		if !reached {
			return nil, shardrpc.ErrUnreachable
		}
		time.Sleep(retryDelay)
	}
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.InstallShardArgs{Shard: s, State: state, Num: num}

	for {
		reached := false
		for i := 0; i < len(ck.servers); i++ {
			j := (ck.leader + i) % len(ck.servers)
			var reply shardrpc.InstallShardReply
			if !ck.Call(ck.servers[j], "KVServer.InstallShard", &args, &reply) {
				continue
			}
			reached = true
			if reply.Err == rpc.OK || reply.Err == shardrpc.ErrStale {
				ck.leader = j
				return reply.Err
			}
		}
		if !reached {
			return shardrpc.ErrUnreachable
		}
		time.Sleep(retryDelay)
	}
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	args := shardrpc.DeleteShardArgs{Shard: s, Num: num}

	for {
		reached := false
		for i := 0; i < len(ck.servers); i++ {
			j := (ck.leader + i) % len(ck.servers)
			var reply shardrpc.DeleteShardReply
			if !ck.Call(ck.servers[j], "KVServer.DeleteShard", &args, &reply) {
				continue
			}
			reached = true
			if reply.Err == rpc.OK || reply.Err == shardrpc.ErrStale {
				ck.leader = j
				return reply.Err
			}
		}
		if !reached {
			return shardrpc.ErrUnreachable
		}
		time.Sleep(retryDelay)
	}
}
