package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// node is the value stored for one key. version is the number of
// times the key has been written; a key that has just been created
// has version 1.
type node struct {
	value   string
	version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex

	// m maps a key to its (value, version).
	m map[string]node
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		m: make(map[string]node),
	}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	n, ok := kv.m[args.Key]
	if !ok {
		// Leave reply.Value and reply.Version at their zero values;
		// the Clerk relies on Version being 0 when the key doesn't
		// exist, since 0 is also the version it must pass to create
		// the key.
		reply.Err = rpc.ErrNoKey
		return
	}
	reply.Value = n.value
	reply.Version = n.version
	reply.Err = rpc.OK
}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	n, ok := kv.m[args.Key]
	if !ok {
		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}
		kv.m[args.Key] = node{value: args.Value, version: 1}
		reply.Err = rpc.OK
		return
	}

	// The key exists, so args.Version has to match exactly. This is
	// what makes a retransmitted Put execute at most once: the first
	// copy bumps the version, so every later copy fails here.
	if args.Version != n.version {
		reply.Err = rpc.ErrVersion
		return
	}
	kv.m[args.Key] = node{value: args.Value, version: n.version + 1}
	reply.Err = rpc.OK
}

// You can ignore all arguments; they are for replicated KVservers
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
