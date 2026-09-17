package kvraft

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt    *tester.Clnt
	servers []string
	leader  int // 最近一次成功的 leader（servers[] 中的下标）
	// 你可以在这里添加字段。
}

func MakeClerk(clnt *tester.Clnt, servers []string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, servers: servers}
	// 你需要在这里添加代码。
	return ck
}

func (ck *Clerk) Leader() int {
	return ck.leader
}

// Get 获取某个键当前的值和版本号。如果该键不存在，返回
// ErrNoKey。面对所有其他错误时，它会一直重试。
//
// 你可以像下面这样向服务器 i 发送 RPC：
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Get", &args, &reply)
//
// args 和 reply 的类型（包括是否为指针）必须与 RPC 处理函数
// 声明的参数类型一致。此外，reply 必须以指针形式传入。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	// 你需要修改这个函数。
	return "", 0, ""
}

// Put 仅在请求中的版本号与服务器上该键的版本号匹配时，
// 才用 value 更新 key。如果版本号不匹配，服务器应返回
// ErrVersion。如果 Put 在第一次 RPC 时就收到 ErrVersion，
// Put 应返回 ErrVersion，因为这次 Put 肯定没有在服务器上
// 执行。如果服务器在重发的 RPC 上返回 ErrVersion，那么 Put
// 必须向应用程序返回 ErrMaybe，因为它早先的 RPC 可能已经被
// 服务器成功处理，只是响应丢失了，而 Clerk 无法确定这次 Put
// 到底有没有被执行。
//
// 你可以像下面这样向服务器 i 发送 RPC：
// ok := ck.clnt.Call(ck.servers[i], "KVServer.Put", &args, &reply)
//
// args 和 reply 的类型（包括是否为指针）必须与 RPC 处理函数
// 声明的参数类型一致。此外，reply 必须以指针形式传入。
func (ck *Clerk) Put(key string, value string, version rpc.Tversion) rpc.Err {
	// 你需要修改这个函数。
	return ""
}
