package shardkv

//
// 用于与分片键/值服务通信的客户端代码。
//
// 客户端使用 shardctrler 查询当前配置，
// 找到分片（键）到组的分配关系，
// 然后与持有该键所属分片的组通信。
//

import (
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

// 从 shardgrp 获取一个键。你可以使用 shardcfg.Key2Shard(key)
// 找到负责该键的分片，并使用 ck.sck.Query() 读取
// 当前配置，查找负责该键的组中的服务器。
// 你可以通过调用 shardgrp.MakeClerk(ck.clnt, servers)
// 为该组创建一个 clerk。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// 你需要修改此函数。
	return "", 0, ""
}

// 向分片组放入一个键。
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// 你需要修改此函数。
	return ""
}
