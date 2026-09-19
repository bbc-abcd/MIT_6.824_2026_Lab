package shardkv

//
// 用于与分片键/值服务通信的客户端代码。
//
// 客户端使用 shardctrler 查询当前配置，
// 找到分片（键）到组的分配关系，
// 然后与持有该键所属分片的组通信。
//

import (
	"time"

	"6.5840/shardkv1/shardcfg"
	"6.5840/shardkv1/shardgrp"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardctrler"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt *tester.Clnt
	sck  *shardctrler.ShardCtrler
	rcks map[tester.Tgid]*shardgrp.Clerk
	// 你需要修改此结构体。
}

// 测试器调用 MakeClerk 并传入一个 shardctrler，
// 以便客户端可以调用其 Query 方法
func MakeClerk(clnt *tester.Clnt, sck *shardctrler.ShardCtrler) kvtest.IKVClerk {
	ck := &Clerk{
		clnt: clnt,
		sck:  sck,
	}
	ck.rcks = make(map[tester.Tgid]*shardgrp.Clerk)
	// 你需要在此处添加代码。
	return ck
}

func (ck *Clerk) GetClerk(gid tester.Tgid) (*shardgrp.Clerk, bool) {
	rck, ok := ck.rcks[gid]
	return rck, ok
}

// rckFor 找出 key 此刻该由哪个组服务，并返回该组的 clerk。
//
// 每一步都重新 Query：配置会变，缓存的组 clerk 又只是一组固定的服务器，
// 它自己无从知道分片是否已经搬走。先读配置再发请求，就不会拿着一个已经
// 离开集群的服务器列表干等。
//
// ck.rcks 按 gid 缓存的是"组 -> clerk"，不是"分片 -> 组"：组与它的服务器
// 列表的绑定是永久的，所以命中就能复用，连带复用 clerk 记住的 leader。
func (ck *Clerk) rckFor(key string) (*shardgrp.Clerk, bool) {
	shard := shardcfg.Key2Shard(key)
	gid, servers, ok := ck.sck.Query().GidServers(shard)
	if !ok {
		return nil, false // 这个分片还没分配出去
	}
	rck, ok := ck.rcks[gid]
	if !ok {
		rck = shardgrp.MakeClerk(ck.clnt, servers)
		ck.rcks[gid] = rck
	}
	return rck, true
}

// 从 shardgrp 获取一个键。你可以使用 shardcfg.Key2Shard(key)
// 找到负责该键的分片，并使用 ck.sck.Query() 读取
// 当前配置，查找负责该键的组中的服务器。
// 你可以通过调用 shardgrp.MakeClerk(ck.clnt, servers)
// 为该组创建一个 clerk。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	for {
		rck, ok := ck.rckFor(key)
		if !ok {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		val, ver, err := rck.Get(key)
		if err != rpc.ErrWrongGroup {
			// OK / ErrNoKey 都是最终答案；Get 是只读的，不存在
			// "可能执行了"这一说，所以这里不会有 ErrMaybe。
			return val, ver, err
		}
		// ErrWrongGroup：我们的配置是旧的，分片已经不在这个组了。
		// 回到循环开头重新 Query，下一轮就会找到新组。
		//
		// 组 clerk 在整组联系不上时也会回这个错误 —— 它无法区分"问错了组"
		// 和"这个组已经没了"。对 Get 来说两者处理方式相同：重读配置再来。
	}
}

// 向分片组放入一个键。
//
// resent 记录的是"前几轮里已经有过一次可能生效、但没拿到确定答复的尝试"。
// 组 clerk 在整组联系不上时会回 ErrWrongGroup（它无法区分"问错了组"和
// "这个组已经没了"），我们据此重读配置换组重试。但换组之后，同一个版本号
// 可能已经不再是"当前版本"了 —— 上一轮那次尝试如果其实生效了，数据会跟着
// 分片搬到新组，新组会因为版本已经 +1 而回 ErrVersion，而 ErrVersion 的含义
// 是"确定没执行"，与事实相反，会直接造成线性一致性违例。
//
// 所以 resent 之后的 ErrVersion 必须升级成 ErrMaybe：版本已经对不上，模型
// 接受 ErrMaybe 并保持状态不变，正好对应"这一步没做、但状态已经是它做的
// 那个样子"。
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	resent := false

	for {
		rck, ok := ck.rckFor(key)
		if !ok {
			time.Sleep(10 * time.Millisecond)
			continue
		}
		err := rck.Put(key, value, version)
		if err == rpc.ErrWrongGroup {
			resent = true
			continue // 重读配置，换一个组再来
		}
		if err == rpc.ErrVersion && resent {
			return rpc.ErrMaybe
		}
		// 其余都是确定答案，原样交给调用者。尤其不能拿 ErrMaybe 去重试：
		// 那会让下一次调用丢掉"上一次可能已经写入"这个信息。
		return err
	}
}
