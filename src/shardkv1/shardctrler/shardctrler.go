package shardctrler

//
// 带有 InitConfig、Query 和 ChangeConfigTo 方法的 Shardctrler
//

import (
	"time"

	kvsrv "6.5840/kvsrv1"
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"
	"6.5840/shardkv1/shardgrp/shardrpc"
	tester "6.5840/tester1"
)

// moveRetryDelay 是搬运因为"整组联系不上"中断后、重来一遍之前的等待。
const moveRetryDelay = 20 * time.Millisecond

// configKey 是配置在 kvsrv 中存放的键。
// 控制器是短生命周期的、随时可能被重建，所以配置是它唯一的持久状态，
// 每次 Query 都要真的去 kvsrv 读一遍，不能缓存在这个结构体里。
const configKey = "config"

// configNextKey 存放"下一个配置"：已经被人认领、但还没搬完的那个。
//
// 它解决的是 A 部分不存在的问题。控制器可能在 ChangeConfigTo 中途死掉或
// 被分区，此时分片搬了一半、新配置也没发布；新控制器靠这个键就知道有一件
// 没做完的事，并把它做完（见 InitController）。
//
// 它**从不清空**。发布之后它和 configKey 内容相同，"next.Num > cur.Num"
// 这个判据天然把已完成的排除掉了。这样崩溃在任何位置都自洽，不需要引入
// "发布成功但清空失败"这种额外的失败窗口。
const configNextKey = "config_next"

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
//
// 恢复必须完全在这里发生：测试器启动新控制器之后只会调 InitController，
// 紧接着就 Query 断言结果（test.go 的 partitionCtrler、shardkv_test.go 的
// TestPartitionCtrler5C），不会再补一次 ChangeConfigTo。所以"把上一个控制器
// 没做完的配置搬完并发布"这件事只能由这里独立完成。
func (sck *ShardCtrler) InitController() {
	cur, _, ok := sck.readConfig()
	if !ok {
		return // 还没初始化过
	}
	nxt, _, ok := sck.readConfigNext()
	if !ok || nxt.Num <= cur.Num {
		// 没有待完成的配置：要么从来没人认领过，要么上一个已经发布完了。
		return
	}

	// 我是被指派来接手的，不需要再走 claim —— 走了也抢不到，挡在前面的
	// 正是我自己刚读到的这条 config_next。多个接手者同时跑也安全：
	// moveTo 幂等，publish 是 CAS，只有一个能发布成功。
	if sck.moveUntilDone(cur, nxt) {
		sck.publish(nxt)
	}
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

// readConfigNext 读出待完成的配置以及它的版本号。
// ok 为假表示从来没有人认领过配置变更。
func (sck *ShardCtrler) readConfigNext() (*shardcfg.ShardConfig, rpc.Tversion, bool) {
	s, ver, err := sck.Get(configNextKey)
	if err != rpc.OK {
		return nil, 0, false
	}
	return shardcfg.FromString(s), ver, true
}

// claim 抢占"由我来推进 new"的权利。返回真表示抢到了，调用者负责把它搬完
// 并发布；返回假表示这次调用应当什么都不做。
//
// 判据里的等号不能去掉，看这个不变式：只要没有变更正在进行，config_next
// 的 Num 就等于当前配置的 Num（认领时写成 cur+1，发布之后 cur 也变成
// cur+1）。所以对一个全新的变更（new.Num == cur.Num+1）来说，读到的
// next.Num == new.Num 只可能来自**另一个控制器已经为同一个 Num 认领过**。
// 此时必须放手，否则两个控制器会用同一个 Num 推进两份不同的配置 —— 正是
// C 部分要防的场景。把 >= 放松成 > 就会打开这个窗口。
func (sck *ShardCtrler) claim(new *shardcfg.ShardConfig) bool {
	nxt, ver, ok := sck.readConfigNext()
	if ok && nxt.Num >= new.Num {
		return false // 有人为这个（或更新的）Num 认领过了
	}

	var err rpc.Err
	if !ok {
		// 第一次认领：version 0 表示"创建"。
		err = sck.Put(configNextKey, new.String(), 0)
	} else {
		// CAS：期间被别的控制器抢先的话，版本号会对不上。
		err = sck.Put(configNextKey, new.String(), ver)
	}

	switch err {
	case rpc.OK:
		return true
	case rpc.ErrMaybe:
		// 这一次 Put 可能已经落地、只是回复丢了 —— 而且它**完全可能就是
		// 我们自己写进去的**。kvsrv 的 clerk 分不清这两者，所以它报
		// ErrMaybe；我们照"失败"处理就等于把自己的认领扔了，config_next
		// 从此停在那儿，这个配置再也推不动（concurrentClerk 里 join
		// 静默失效、紧接着 checkMember 为假，就是这么来的）。
		//
		// 只能回读一次来分辨。判据是**内容**而不是 Num：同一个 Num 上
		// 可能压着另一个控制器写的不同配置，那场竞争我们是真的输了，
		// 必须放手（C 部分要防的正是这个）。
		return sck.claimLanded(new)
	default:
		// ErrVersion：确定是别人抢在前面了。
		return false
	}
}

// claimLanded 在 Put 报了 ErrMaybe 之后回读 config_next，确认认领到底有没有
// 落地。直接比原始字符串而不用 Num：Num 相同、内容不同的两个配置是存在的，
// 而字符串就是权威内容本身（ShardConfig.String 是确定性的 JSON）。
func (sck *ShardCtrler) claimLanded(new *shardcfg.ShardConfig) bool {
	s, _, err := sck.Get(configNextKey)
	return err == rpc.OK && s == new.String()
}

// 由测试器调用，要求控制器将配置从当前配置更改为 new。
// 当控制器更改配置时，它可能被另一个控制器取代。
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	cur, _, ok := sck.readConfig()
	if !ok || new.Num <= cur.Num {
		// 没初始化过，或者这个配置已经不比当前的新 —— 无事可做。
		return
	}

	if !sck.claim(new) {
		// 没抢到。调用者不需要知道原因（有人做了同一个 Num，或者有人已经
		// 推进了更新的配置），它 Query 一下就能看到结果。这里放心返回还有
		// 一层保障：认领失败意味着 config_next 落在别人手上，那件事自有人
		// 负责做完 —— 要么是那个控制器，要么是下一个 InitController。
		return
	}

	// 抢到之后重读一次当前配置：认领期间别人可能已经推进过配置，拿旧的 cur
	// 当搬运起点会重复搬一段。重复本身是安全的（见 moveTo），但没必要。
	cur, _, ok = sck.readConfig()
	if !ok || new.Num <= cur.Num {
		return
	}

	// 只有搬完了才发布。中途妥协（某个组联系不上）就发布的话，配置会指向一个
	// 还没拿到数据的目标组 —— 那个分片会以"空表"对外服务，读到的数据凭空消失。
	if sck.moveUntilDone(cur, new) {
		sck.publish(new)
	}
}

// moveTo 把分片的归属从 old 的布局搬到 new 的布局。返回真表示搬完了，返回
// 假表示中途撞上联系不上的组，这一次没搬完 —— 调用者该重读配置再决定是
// 重试还是收工（见 moveUntilDone）。
//
// 它必须幂等：B/C 部分里，接手的控制器和被取代的旧控制器可能同时在搬同一
// 段（旧控制器只是被分区，进程还活着，连接恢复后会继续跑完手里那一段）。
// 分片组一侧的 Num 守卫保证重复的 RPC 要么被拒（ErrStale），要么产生完全
// 相同的结果，所以这里的每一次 RPC 都可以安全重放。
func (sck *ShardCtrler) moveTo(old, new *shardcfg.ShardConfig) bool {
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
				// 本组见过更新的配置，说明我们已经被取代了。publish 的守卫
				// 会看到当前配置已经不小于 new，因此不会写任何东西。
				return true
			}
			if err == shardrpc.ErrUnreachable {
				return false
			}
		}

		if dstOK {
			// state 为 nil 时 InstallShard 会装一张空表并把 Served 置 true，
			// 正是"这个分片从无到有归你了"需要的语义。
			err := clerkFor(dst, dstSrvs).InstallShard(s, state, new.Num)
			if err == shardrpc.ErrStale {
				return true
			}
			if err == shardrpc.ErrUnreachable {
				return false
			}
		}

		if srcOK {
			err := clerkFor(src, srcSrvs).DeleteShard(s, new.Num)
			if err == shardrpc.ErrStale {
				return true
			}
			if err == shardrpc.ErrUnreachable {
				return false
			}
		}
	}
	return true
}

// moveUntilDone 反复搬运，直到搬完。返回假表示这个变更已经被别人做完，
// 调用者不必也不该再发布。
//
// 它是"联系不上"这条路径的出口。撞上联系不上的组有两种可能，只有当前配置
// 能区分它们：
//
//   - 组只是暂时不可达（网络抖动，或者测试器稍后会把组拉起来）：当前配置
//     还停在 new 之前 —— 那就重来一遍。TestJoinLeave5B 要的正是这个行为：
//     组关着的时候搬运不许完成，组一回来必须很快完成。
//   - 这次搬运已经由另一个控制器做完、那个组随之被 ExitGroup 杀掉了：当前
//     配置已经走到 new —— 那就收工。这正是 concurrentCtrler 里会发生的
//     情况：InitController 会把别人正在推的配置也重放一遍，而它的 RPC 可能
//     落在"配置已发布、组已被杀"的那个窗口里。
func (sck *ShardCtrler) moveUntilDone(cur, new *shardcfg.ShardConfig) bool {
	for {
		if sck.moveTo(cur, new) {
			return true
		}
		cur2, _, ok := sck.readConfig()
		if ok && cur2.Num >= new.Num {
			return false
		}
		time.Sleep(moveRetryDelay)
	}
}

// publish 把 new 写进当前配置。这是整个协议的最后一环：一旦发布，客户端
// 就会改道去新组，而新组此时已经装好了数据。
//
// 它自己也带守卫（cur.Num >= new.Num 就退出），所以 moveTo 因 ErrStale 提前
// 放手之后再调它是安全的：那种情况下必然已经有更新的配置在推进，cur 不会
// 还停在 new.Num 之前。
func (sck *ShardCtrler) publish(new *shardcfg.ShardConfig) {
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
