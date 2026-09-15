package lock

import (
	"log"
	"math/rand"
	"time"

	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
)

// How long to wait before looking at the lock again. Randomized so
// that clients racing for the same lock don't stay in lockstep.
const (
	minBackoff = 10 * time.Millisecond
	maxBackoff = 60 * time.Millisecond
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk

	// name is the key holding this lock's state in the k/v server.
	name string

	// id identifies this lock client. The server stores it as the
	// key's value while the lock is held, so a client can recognize a
	// lock it already owns after losing a reply.
	id string

	// version is the version of the lock key on the server. It is
	// only valid while this client holds the lock, and always equals
	// the version the server currently has: that is what Release
	// passes to Put.
	version rpc.Tversion
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	// The id is generated once, here, and never changes: Acquire's
	// retries must keep presenting the same identity, otherwise it
	// could not tell "my earlier Put succeeded" from "someone else
	// holds the lock".
	lk := &Lock{
		ck:   ck,
		name: lockname,
		id:   kvtest.RandValue(8),
	}
	return lk
}

func sleepRandom() {
	d := minBackoff + time.Duration(rand.Int63n(int64(maxBackoff-minBackoff)))
	time.Sleep(d)
}

// The lock key's value is "" when the lock is free, and the owner's id
// when it is held. Acquire claims the lock with a conditional Put, so
// of several clients that all see the lock free, exactly one wins;
// the rest get ErrVersion and start over.
func (lk *Lock) Acquire() {
	for {
		val, ver, err := lk.ck.Get(lk.name)

		// A Get error other than ErrNoKey cannot happen (the Clerk
		// only gives up on a key that doesn't exist), but if it did,
		// starting the loop over is the safe response.
		switch err {
		case rpc.ErrNoKey:
			// The lock has never been taken. Version 0 creates the
			// key, and the server stores it with version 1.
			switch lk.ck.Put(lk.name, lk.id, 0) {
			case rpc.OK:
				lk.version = 1
				return
			case rpc.ErrMaybe:
				// Our Put may have been performed; loop so that the
				// Get above can confirm it.
			default: // ErrVersion: someone else created it first.
				sleepRandom()
			}
			continue

		case rpc.OK:
			if val == lk.id {
				// The server already records us as the owner: an
				// earlier Put of ours succeeded but its reply was
				// lost. We hold the lock; ver is the current version,
				// which is exactly what Release needs.
				lk.version = ver
				return
			}
			if val != "" {
				// Held by another client; wait and look again.
				sleepRandom()
				continue
			}
			// Free. Try to claim it at the version we just read; if
			// it is still free this succeeds.
			switch lk.ck.Put(lk.name, lk.id, ver) {
			case rpc.OK:
				// The server stored our id and bumped the version.
				lk.version = ver + 1
				return
			case rpc.ErrMaybe:
				// As above: maybe ours, maybe not. Let Get decide.
			default: // ErrVersion: another client claimed it first.
				sleepRandom()
			}
			continue

		default:
			sleepRandom()
			continue
		}
	}
}

// Release clears the lock's value. Only the holder can do this, since
// only the holder knows the current version of the key.
func (lk *Lock) Release() {
	err := lk.ck.Put(lk.name, "", lk.version)
	for err != rpc.OK {
		// While we hold the lock no other client can change the key's
		// version (they would have to pass the current version, which
		// they only use after seeing an empty value), so an ErrMaybe
		// here means our own Put did execute and the lock is already
		// released. ErrVersion shouldn't be possible at all. Verify
		// with a Get rather than assume, because returning while the
		// lock is still held would leak it and be very hard to trace.
		log.Printf("lock %v: Release of version %v got %v; verifying", lk.name, lk.version, err)

		val, ver, gerr := lk.ck.Get(lk.name)
		if gerr == rpc.ErrNoKey || val != lk.id {
			// The key is gone, or its value is no longer our id, so
			// we are not the owner and the lock is released.
			return
		}
		// Still ours: retry at the version the server just reported.
		err = lk.ck.Put(lk.name, "", ver)
	}
}
