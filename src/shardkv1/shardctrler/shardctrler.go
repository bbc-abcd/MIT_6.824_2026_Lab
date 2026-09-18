package shardctrler

//
// 带有 InitConfig、Query 和 ChangeConfigTo 方法的 Shardctrler
//

import (
	kvsrv "6.5840/kvsrv1"
	kvtest "6.5840/kvtest1"
	"6.5840/shardkv1/shardcfg"
	tester "6.5840/tester1"
)

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
	// 你的代码放在这里
}

// 由测试器调用，要求控制器将配置从当前配置更改为 new。
// 当控制器更改配置时，它可能被另一个控制器取代。
func (sck *ShardCtrler) ChangeConfigTo(new *shardcfg.ShardConfig) {
	// 你的代码放在这里。
}

// 返回当前配置
func (sck *ShardCtrler) Query() *shardcfg.ShardConfig {
	// 你的代码放在这里。
	return nil
}
