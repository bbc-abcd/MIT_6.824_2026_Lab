package kvsrv

import (
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"6.5840/tester1"
)

// How long to wait before resending an RPC whose reply hasn't arrived.
const resendDelay = 100 * time.Millisecond

type Clerk struct {
	clnt   *tester.Clnt
	server string
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	// You may add code here.
	return ck
}

// Get fetches the current value and version for a key.  It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {
	args := rpc.GetArgs{Key: key}
	reply := rpc.GetReply{}

	for {
		if ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply) {
			// A reply arrived, so the server processed the request.
			// Get doesn't modify state, so resending it is harmless,
			// and whatever Err it reports (OK or ErrNoKey) is the
			// server's answer -- don't retry it.
			return reply.Value, reply.Version, reply.Err
		}
		// No reply: either the request or the reply was dropped.
		// Resend the identical request after a short pause. Never
		// look at reply here; Call leaves it untouched on failure,
		// so it would still hold the previous iteration's contents.
		time.Sleep(resendDelay)
	}
}

// Put updates key with value only if the version in the
// request matches the version of the key at the server.  If the
// versions numbers don't match, the server should return
// ErrVersion.  If Put receives an ErrVersion on its first RPC, Put
// should return ErrVersion, since the Put was definitely not
// performed at the server. If the server returns ErrVersion on a
// resend RPC, then Put must return ErrMaybe to the application, since
// its earlier RPC might have been processed by the server successfully
// but the response was lost, and the Clerk doesn't know if
// the Put was performed or not.
//
// You can send an RPC with code like this:
// ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
//
// The types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. Additionally, reply must be passed as a pointer.
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	// args is built once and reused for every resend, so each copy of
	// the request carries the same version and is therefore executed
	// at most once by the server.
	args := rpc.PutArgs{Key: key, Value: value, Version: version}
	reply := rpc.PutReply{}

	resent := false
	for {
		if ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply) {
			if reply.Err == rpc.ErrVersion && resent {
				// An earlier copy may have been performed and only
				// its reply lost; this ErrVersion comes from a copy
				// that arrived after that one bumped the version. The
				// Clerk can't tell the two cases apart.
				return rpc.ErrMaybe
			}
			// Either OK, ErrNoKey, or ErrVersion on the very first
			// RPC -- in the last case the Put certainly wasn't
			// performed, so ErrVersion is the right answer.
			return reply.Err
		}
		resent = true
		time.Sleep(resendDelay)
	}
}
