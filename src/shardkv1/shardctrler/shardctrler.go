package shardctrler

//
// 带有 InitConfig、Query 和 ChangeConfigTo 方法的 Shardctrler
//

import (
	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

// configKey 是配置在 kvsrv 中存放的键。
// 控制器是短生命周期的、随时可能被重建，所以配置是它唯一的持久状态，
// 每次 Query 都要真的去 kvsrv 读一遍，不能缓存在这个结构体里。
const configKey = "config"

// 用于控制器和 KV clerk 的 ShardCtrler。
type ShardCtrler struct {
	clnt *tester.Clnt
	kvtest.IKVClerk

	killed int32 // 由 Kill() 设置

	// 你的数据放在这里。
}

// 创建一个 ShardCltler，它将其状态存储在 kvsrv 中。
func MakeShardCtrler(clnt *tester.Clnt) *ShardCtrler {
	sck := &ShardCtrler{clnt: clnt}
	srv := tester.ServerName(tester.GRP0, 0)
	sck.IKVClerk = kvsrv.MakeClerk(clnt, srv)
	// 你的代码放在这里。
	return sck
}

// 测试器在启动新控制器之前调用 InitController()。在 A 部分，
// 此方法不需要做任何事情。在 B 和 C 部分，此方法实现恢复。
func (sck *ShardCtrler) InitController() {
}

// 由测试器调用一次，以提供第一个配置。你可以使用
// shardcfg.String() 将 ShardConfig 序列化为字符串，然后将其以
// 版本 0 放入控制器的 kvsrv 中。你可以选择用于命名配置的键。
// 初始配置为所有分片列出 shardgrp shardcfg.Gid1。
func (sck *ShardCtrler) InitConfig(cfg *shardcfg.ShardConfig) {
	// 版本 0 表示"创建"。IKVClerk 自己会重试网络失败，所以这里
	// 除了 OK 之外只可能拿到 ErrVersion，那说明配置已经被别人写过。
	sck.Put(configKey, cfg.String(), 0)
}

// readConfig 读出当前配置以及它在 kvsrv 中的版本号。
// 版本号必须和配置一起带出来：kvsrv 的 Put 是 CAS，写回时要拿它做条件。
func (sck *ShardCtrler) readConfig() (*shardcfg.ShardConfig, rpc.Tversion, bool) {
	s, ver, err := sck.Get(configKey)
	if err != rpc.OK {
		return nil, 0, false // ErrNoKey：还没有初始化过
	}
	return shardcfg.FromString(s), ver, true
}

// 由测试器调用，要求控制器将配置从当前配置更改为 new。
// 当控制器更改配置时，它可能被另一个控制器取代。
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	old, _, ok := sck.readConfig()
	if !ok || new.Num <= old.Num {
		// 没初始化过，或者这个配置已经不比当前的新 —— 无事可做。
		return
	}

	// 按 gid 复用 clerk，保住它记着的 leader，省掉每片一次盲扫。
	clerks := make(map[tester.Tgid]*shardgrp.Clerk)
	clerkFor := func(gid tester.Tgid, srvs []string) *shardgrp.Clerk {
		ck, ok := clerks[gid]
		if !ok {
			ck = shardgrp.MakeClerk(sck.clnt, srvs)
			clerks[gid] = ck
		}
		return ck
	}

	// 搬运的单位是分片，不是组。新配置已经由测试器算好了，
	// 控制器只负责执行，不负责决定哪个分片该去哪个组。
	for s := shardcfg.Tshid(0); s < shardcfg.NShards; s++ {
		src, dst := old.Shards[s], new.Shards[s]
		if src == dst {
			continue // 不动的分片原样继续服务
		}
		// src == 0 表示这个分片在旧配置里未分配（Tgid 0 是哨兵，不在 Groups 里），
		// 此时没有数据可冻、也没有源组可清。但**不能因此跳过 Install**：分片组
		// 从不读配置，"我拥有这个分片"这件事唯一的来源就是 InstallShard。少了
		// 它，目标组的 Served[s] 会一直是 false，客户端在这个分片上无限重试。
		srcSrvs, srcOK := old.Groups[src]
		dstSrvs, dstOK := new.Groups[dst]

		// 顺序是死规矩：先冻结源组，再把数据装到目标组，最后才清源组。
		// 这样从冻结到配置发布之间，这个分片要么在源组被拒（客户端重查
		// 配置后就会找到新组），要么尚未搬完 —— 永远不会同时有两个组
		// 对外服务同一个分片。
		var state []byte
		if srcOK {
			var err rpc.Err
			state, err = clerkFor(src, srcSrvs).FreezeShard(s, new.Num)
			if err == shardrpc.ErrStale {
				return // 本组见过更新的配置，说明我们已经被取代了，放弃
			}
		}

		if dstOK {
			// state 为 nil 时 InstallShard 会装一张空表并把 Served 置 true，
			// 正是"这个分片从无到有归你了"需要的语义。
			if err := clerkFor(dst, dstSrvs).InstallShard(s, state, new.Num); err == shardrpc.ErrStale {
				return
			}
		}

		if srcOK {
			if err := clerkFor(src, srcSrvs).DeleteShard(s, new.Num); err == shardrpc.ErrStale {
				return
			}
		}
	}

	// 全部分片都搬完了，现在才发布新配置。这是整个协议的最后一环：
	// 一旦发布，客户端就会改道去新组，而新组此时已经装好了数据。
	for {
		cur, ver, ok := sck.readConfig()
		if !ok || cur.Num >= new.Num {
			return // 已经有别的控制器发布了这个（或更新的）配置
		}
		if sck.Put(configKey, new.String(), ver) == rpc.OK {
			return
		}
		// ErrVersion / ErrMaybe：期间有人动过配置，重读版本号再来一次。
	}
}

// 返回当前配置
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// 从 kvsrv 中查询当前配置。routing 完全依赖它，所以每次都真读一遍：
	// 控制器随时可能被重建，缓存一份只会让客户端拿着过期的映射走错门。
	cfg, _, ok := sck.readConfig()
	if !ok {
		return shardcfg.MakeShardConfig()
	}
	return cfg
}
