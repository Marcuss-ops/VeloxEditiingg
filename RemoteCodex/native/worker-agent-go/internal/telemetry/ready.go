// Package telemetry — ReadySnapshot (RW-PROD-004 §3 A3)
//
// A single atomic.Pointer[ReadySnapshot] holds the worker-side readiness
// state for /health/ready. The composition root (cmd/velox-worker-agent/
// main.go) and the worker run loop (internal/worker/worker.go) write
// transitions through UpdateReady(mutator); the HTTP handler reads the
// snapshot via Snapshot(). Read latencies on the HTTP path are
// nanosecond-scale (single atomic.Load), and cross-field consistency is
// guaranteed because every write goes through one copy-and-store.
//
// Why on-demand NotReadyReasons instead of storing reasons[] eagerly:
//
//   - Eager reasons[] needs the setter to remember to keep the bool
//     and the reason list in sync; one setter forgetting to push to
//     the list silently masks a "not ready" condition.
//   - On-demand reasons are pure functions of the current snapshot
//     so the only writer concern is the bool fields themselves.
//   - Test surface is also simpler: drive the booleans and verify the
//     reason-text directly. No "did the setter forget to add the
//     reason?" test class.
package telemetry

import (
	"sync/atomic"
	"time"
)

// ReadySnapshot is the canonical readiness state of the worker agent
// at one instant in time. All booleans default to false; the composition
// root must mark each true (or false) as the corresponding step
// succeeds.
//
// Lifecycle ordering invariant (checked by IsReady + NotReadyReasons):
//
//	Bootstrapped ─► Registered ─► Executors>0 && CacheReady && BlobReady && !DrainMode && DiskFree>=DiskThreshold
type ReadySnapshot struct {
	// Registered is true after the worker has sent Hello + received
	// an Ack (i.e. the session is live).
	Registered bool
	// DrainMode is true after the worker received MsgDrain and is
	// gracefully refusing new offers.
	DrainMode bool
	// Bootstrapped is true after pkg/bootstrap.Run completed with no
	// hard-fail. Until then the gate is closed and /health/ready
	// reports reasons including `bootstrap_not_run`.
	Bootstrapped bool
	// Executors reflects the number of executor descriptors
	// currently advertised to the master. /health/ready requires
	// >= 1 to be considered ready. Coming from the composition
	// root (registry.Len()) so it reflects the live registry table
	// rather than a stale cached value.
	Executors int
	// CacheReady is true after the cache/NewPersistedLocalCache
	// call returned a non-nil cache (composition-root wiring).
	CacheReady bool
	// BlobReady is true after the blob/NewBlobArtifacts call
	// returned a non-nil store (composition-root wiring).
	BlobReady bool
	// DiskFreeBytes is the most-recent sample worker-side disk-space
	// reading for the engine-output directory. 0 means the disk
	// watcher has not yet produced a sample.
	DiskFreeBytes int64
	// DiskThresholdBytes is the operator-tunable floor; /health/ready
	// signals `disk.critical` when DiskFreeBytes < DiskThresholdBytes.
	DiskThresholdBytes int64
	// GeneratedAt is the monotonic-clock timestamp of the latest
	// state mutation. Useful only for /health/ready callers that
	// want to spot stale snapshots (likely zero in tests that
	// don't hold the wall clock under test framework control).
	GeneratedAt time.Time
}

// ReadyState is the process-global atomic.Pointer[ReadySnapshot] holder.
// Defining it on a package-level pointer (anonymous struct, not a
// global) keeps the test surface small: tests call ResetForTest() to
// wipe prior state, then drive transitions through the public setters.
//
// One snapshot lives at a time; writers go through UpdateReady (which
// copies the pointer-target, runs the mutator, and atomically stores
// the new copy). The atomic.Pointer's genericity in Go 1.19+ means the
// API is type-safe.
type ReadyState struct {
	ptr atomic.Pointer[ReadySnapshot]
}

// NewReadyState returns a zeroed ReadySnapshot. NewReadyState SHOULD be
// invoked once at process start; tests use ResetForTest to wipe state
// between cases.
func NewReadyState() *ReadyState {
	r := &ReadyState{}
	r.ptr.Store(&ReadySnapshot{})
	return r
}

// Snapshot returns the current ReadySnapshot. The returned pointer is
// safe to dereference; the value MUST be treated as a snapshot — do
// not mutate it. Always non-nil (zero snapshot on first read).
func (r *ReadyState) Snapshot() *ReadySnapshot {
	if r == nil {
		return &ReadySnapshot{}
	}
	if p := r.ptr.Load(); p != nil {
		return p
	}
	return &ReadySnapshot{}
}

// UpdateReady applies fn to a private copy of the current snapshot,
// then stores the result. fn MUST NOT retain references to the copy
// (the copy is owned by the atomic swap; if fn blocks holding it, the
// next UpdateReady will deadlock reading it under the copy).
//
// fn is called while holding NO lock — implementations must be
// side-effect-free except for the *ReadySnapshot argument.
func (r *ReadyState) UpdateReady(fn func(*ReadySnapshot)) {
	if r == nil || fn == nil {
		return
	}
	cur := r.Snapshot()
	nxt := *cur // struct value copy
	fn(&nxt)
	if nxt.GeneratedAt.IsZero() {
		nxt.GeneratedAt = time.Now().UTC()
	}
	r.ptr.Store(&nxt)
}

// IsReady returns true iff NotReadyReasons returns an empty slice. Use
// this on the HTTP path so the bool answer and the reason list never
// drift.
func (s *ReadySnapshot) IsReady() bool {
	return len(s.NotReadyReasons()) == 0
}

// NotReadyReasons enumerates the canonical (human-and-canary-greppable)
// reasons the worker is NOT ready. The order is stable so dashboards
// can diff snapshots across deploys without spurious churn.
//
// Reasons taxonomy (canonical, do not rename without a worker+master
// release):
//
//	bootstrap_not_run        — pkg/bootstrap.Run has not yet produced Ok()=true
//	not_registered           — session is not live (Hello/Ack not yet completed)
//	drain_mode               — drain received, gracefully rejecting new offers
//	executors.empty          — registry.Len()==0
//	cache.not_initialized    — pkg/cache.NewPersistedLocalCache returned nil/err
//	blob.not_initialized     — pkg/blob.NewBlobArtifacts returned nil/err
//	disk.critical            — DiskFreeBytes < DiskThresholdBytes
//
// CanonicalReasons is the EXPORTED authoritative list. Adding a new
// reason REQUIRES a bump in BOTH worker-agent-go AND DataServer
// readiness consumers (the master fleet dashboard reads the same
// strings), plus a TestReady_StableTaxonomy addition in
// ready_test.go. Removal of a reason requires a major-version bump
// under release-channel policy (operators should not see reason
// strings silently disappear).
var CanonicalReasons = []string{
	"bootstrap_not_run",
	"not_registered",
	"drain_mode",
	"executors.empty",
	"cache.not_initialized",
	"blob.not_initialized",
	"disk.critical",
}

func (s *ReadySnapshot) NotReadyReasons() []string {
	if s == nil {
		return []string{"bootstrap_not_run"}
	}
	var reasons []string
	if !s.Bootstrapped {
		reasons = append(reasons, "bootstrap_not_run")
	}
	if !s.Registered {
		reasons = append(reasons, "not_registered")
	}
	if s.DrainMode {
		reasons = append(reasons, "drain_mode")
	}
	if s.Executors <= 0 {
		reasons = append(reasons, "executors.empty")
	}
	if !s.CacheReady {
		reasons = append(reasons, "cache.not_initialized")
	}
	if !s.BlobReady {
		reasons = append(reasons, "blob.not_initialized")
	}
	// DiskFreeBytes == 0 means the disk watcher has not yet published
	// a first sample (the composition-root goroutine always writes a
	// positive value before any traffic). Treating 0 < threshold as
	// critical would generate false positives on a fresh boot where
	// readiness probes race ahead of the goroutine. We require BOTH
	// bytes > 0 AND bytes < threshold to fire disk.critical.
	if s.DiskThresholdBytes > 0 && s.DiskFreeBytes > 0 && s.DiskFreeBytes < s.DiskThresholdBytes {
		reasons = append(reasons, "disk.critical")
	}
	return reasons
}

// DetailMap is the `detail` sub-object in the /health/ready JSON body.
// All bool fields are echoed so dashboards can graph transitions
// without parsing the string-typed reasons array.
func (s *ReadySnapshot) DetailMap() map[string]interface{} {
	d := map[string]interface{}{
		"registered":     s.Registered,
		"drain_mode":     s.DrainMode,
		"bootstrapped":   s.Bootstrapped,
		"executors_count": s.Executors,
		"cache_ready":    s.CacheReady,
		"blob_ready":     s.BlobReady,
	}
	if s.DiskFreeBytes > 0 || s.DiskThresholdBytes > 0 {
		d["disk_free_bytes"] = s.DiskFreeBytes
		d["disk_threshold_bytes"] = s.DiskThresholdBytes
	}
	return d
}

// ------------------------------------------------------------------
// Process-global ReadyState (one writer per concern, atomic swap)
// ------------------------------------------------------------------

var globalReady = NewReadyState()

// GlobalReady returns the process-global ReadyState. The HTTP handler
// (internal/telemetry/health.go) reads from this; the composition root
// and the worker run loop write to this through the helpers below.
func GlobalReady() *ReadyState { return globalReady }

// MarkRegistered flips the registered flag (called from
// worker.go on ConnReady / ConnDisconnected transitions).
func MarkRegistered(b bool) {
	GlobalReady().UpdateReady(func(s *ReadySnapshot) {
		s.Registered = b
	})
}

// MarkDrainMode flips the drain flag (called from worker.go on
// MsgDrain in receiveLoop).
func MarkDrainMode(b bool) {
	GlobalReady().UpdateReady(func(s *ReadySnapshot) {
		s.DrainMode = b
	})
}

// MarkBootstrapped flips the bootstrapped flag (called from
// main.go after pkg/bootstrap.Ok()==true).
func MarkBootstrapped(b bool) {
	GlobalReady().UpdateReady(func(s *ReadySnapshot) {
		s.Bootstrapped = b
	})
}

// MarkCacheReady flips the cache-ready flag (called from
// main.go after cache.NewPersistedLocalCache succeeded).
func MarkCacheReady(b bool) {
	GlobalReady().UpdateReady(func(s *ReadySnapshot) {
		s.CacheReady = b
	})
}

// MarkBlobReady flips the blob-ready flag (called from
// main.go after blob.NewBlobArtifacts succeeded).
func MarkBlobReady(b bool) {
	GlobalReady().UpdateReady(func(s *ReadySnapshot) {
		s.BlobReady = b
	})
}

// SetExecutorsCount records the live executor count (called from
// main.go immediately after registry.MustRegister).
//
// The setter accepts the entire count rather than incremental
// add/remove because the composition root is the single source of
// truth for "which executors are advertised" and Can do bulk read each
// time. This also avoids bugs where two setters race on
// +1/-1 arithmetic.
func SetExecutorsCount(n int) {
	GlobalReady().UpdateReady(func(s *ReadySnapshot) {
		s.Executors = n
	})
}

// SetDiskState records the latest disk free + threshold. Called
// from the diskWatcher goroutine started by main.go.
func SetDiskState(freeBytes, thresholdBytes int64) {
	GlobalReady().UpdateReady(func(s *ReadySnapshot) {
		s.DiskFreeBytes = freeBytes
		s.DiskThresholdBytes = thresholdBytes
	})
}

// ResetForTest wipes the global state. Tests in the same binary call
// this between RUN/PASS cases. Production code MUST NOT call this.
func ResetForTest() {
	globalReady = NewReadyState()
}
