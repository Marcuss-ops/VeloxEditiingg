// Package iopool provides a process-wide pool of copy buffers for
// streaming I/O (hashing, chunked copies, artifact transfers).
//
// Why a shared module (A4-3 follow-up): the worker (publisher manifest
// hashing, render-plan finalize) and the master (blob streaming, recovery
// tooling) all hash/copy hundreds-of-MB artifacts and each used to
// allocate a fresh 1 MiB buffer per call. One canonical pool avoids both
// the GC churn and the risk of every package growing its own
// incompatible pool.
//
// Usage contract:
//
//	buf := iopool.Get()          // *[iopool.Size]byte, exactly one word stored
//	defer iopool.Put(buf)        // MUST NOT retain a reference past Put
//	n, err := io.CopyBuffer(dst, src, buf[:])
//
// The pool stores *[Size]byte (not []byte) so each entry is a single
// pointer word. Callers slice with buf[:] for use. Buffers are reused:
// never grow, reslice with a different offset, or write past Put.
package iopool

import "sync"

// Size is the canonical copy-buffer size: 1 MiB — well above syscall
// overhead, well below the page cache's effective size. This matches the
// 1 MiB convention already used across the streaming hash call sites.
const Size = 1 << 20

var pool = sync.Pool{
	New: func() interface{} {
		return &[Size]byte{}
	},
}

// Get returns a zeroed-on-first-use buffer from the pool. The returned
// pointer's contents are unspecified (previous caller's data may remain);
// io.CopyBuffer and streaming writers overwrite before read.
func Get() *[Size]byte {
	return pool.Get().(*[Size]byte)
}

// Put returns a buffer obtained from Get to the pool. Safe to call with a
// nil pointer.
func Put(buf *[Size]byte) {
	if buf != nil {
		pool.Put(buf)
	}
}
