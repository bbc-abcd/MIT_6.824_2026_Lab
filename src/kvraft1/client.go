package kvraft

import (
	"time"

	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

// 扫完一圈仍然没找到 leader 时的等待时间。
const retryDelay = 50 * time.Millisecond

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // 最近一次成功的 leader（servers[] 中的下标）
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get 获取某个键当前的值和版本号。如果该键不存在，返回
// ErrNoKey。面对所有其他错误时，它会一直重试。
//
// 两层循环的分工：内层从 ck.leader 出发扫一圈所有 server；外层保证
// 扫完没结果就重来，直到拿到确定答案（OK / ErrNoKey）才返回。网络
// 失败和 ErrWrongLeader 都不是答案，只是继续试下一个。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}

	for {
		for i := 0; i < len(ck.servers); i++ {
			s := (ck.leader + i) % len(ck.servers)
			// 每次调用都用全新的 reply，避免 gob 解码进非零字段。
			var reply rpc.GetReply
			ok := ck.clnt.Call(ck.servers[s], "KVServer.Get", &args, &reply)
			if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey) {
				ck.leader = s // 只有给出确定答案的 server 才是 leader
				return reply.Value, reply.Version, reply.Err
			}
			// 网络失败或 ErrWrongLeader：试下一个。
		}
		time.Sleep(retryDelay)
	}
}

// Put 仅在请求中的版本号与服务器上该键的版本号匹配时，
// 才用 value 更新 key。如果版本号不匹配，服务器应返回
// ErrVersion。如果 Put 在第一次 RPC 时就收到 ErrVersion，
// Put 应返回 ErrVersion，因为这次 Put 肯定没有在服务器上
// 执行。如果服务器在重发的 RPC 上返回 ErrVersion，那么 Put
// 必须向应用程序返回 ErrMaybe，因为它早先的 RPC 可能已经被
// 服务器成功处理，只是响应丢失了，而 Clerk 无法确定这次 Put
// 到底有没有被执行。
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}

	// resent 为真表示"已经发送过一个可能已被执行、但没拿到确定结果的
	// 请求"：网络失败的 Call，或者返回 ErrWrongLeader 的 Call（该 server
	// 可能在提交之后才失去领导权）。在这之后收到的 ErrVersion 无法判断
	// 是不是自己的那次重传撞上了已生效的第一次 Put，只能报 ErrMaybe。
	resent := false

	for {
		for i := 0; i < len(ck.servers); i++ {
			s := (ck.leader + i) % len(ck.servers)
			var reply rpc.PutReply
			ok := ck.clnt.Call(ck.servers[s], "KVServer.Put", &args, &reply)
			if ok && reply.Err == rpc.ErrVersion {
				ck.leader = s
				if resent {
					return rpc.ErrMaybe
				}
				return rpc.ErrVersion
			}
			if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey) {
				ck.leader = s
				return reply.Err
			}
			// 网络失败或 ErrWrongLeader：这次尝试可能已被执行。
			resent = true
		}
		time.Sleep(retryDelay)
	}
}
