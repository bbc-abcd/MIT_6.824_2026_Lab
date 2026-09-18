package shardgrp

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/shardkv1/shardcfg"
	tester "6.5840/tester1"
)

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

func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	// 在此处编写你的代码
	return "", 0, ""
}

func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// 在此处编写你的代码
	return ""
}

func (ck *Clerk) FreezeShard(s shardcfg.Tshid, num shardcfg.Tnum) ([]byte, rpc.Err) {
	// 在此处编写你的代码
	return nil, ""
}

func (ck *Clerk) InstallShard(s shardcfg.Tshid, state []byte, num shardcfg.Tnum) rpc.Err {
	// 在此处编写你的代码
	return ""
}

func (ck *Clerk) DeleteShard(s shardcfg.Tshid, num shardcfg.Tnum) rpc.Err {
	// 在此处编写你的代码
	return ""
}
