// Package placement implements the placement matcher for Velox task dispatch.
// It is a pure, deterministic engine that selects the best-compatible Task
// for a given Worker snapshot without any dependency on SQL, gRPC, or protobuf.
package placement

import "time"

// ExecutorKey is the canonical (id, version) pair that the worker advertises
// and the task requires.
type ExecutorKey struct {
	ID      string
	Version int
}

// Valid returns true when both ID and version are set.
func (k ExecutorKey) Valid() bool {
	return k.ID != "" && k.Version > 0
}

// TaskCandidate is a lightweight task metadata row used by the matcher.
// It does NOT carry the full payload JSON.
type TaskCandidate struct {
	TaskID   string
	JobID    string
	Revision int
	Priority int
	CreatedAt time.Time

	Executor ExecutorKey

	RequiredCapabilities []string
}

// WorkerSnapshot captures the placement-relevant state of a connected
// worker at a single instant. It is built from the in-memory session
// state and is designed to be copied safely (no shared mutability).
type WorkerSnapshot struct {
	WorkerID  string
	SessionID string

	Ready        bool
	Draining     bool
	SessionAlive bool

	MaxParallelJobs int
	ActiveJobs      int

	Executors    map[ExecutorKey]struct{}
	Capabilities map[string]bool

	CapabilityRevision uint64
	LastHeartbeat      time.Time
}

// HasExecutor returns true when the worker snapshot carries the given key.
func (w WorkerSnapshot) HasExecutor(key ExecutorKey) bool {
	_, ok := w.Executors[key]
	return ok
}

// FreeSlots returns the number of additional tasks the worker can accept.
func (w WorkerSnapshot) FreeSlots() int {
	free := w.MaxParallelJobs - w.ActiveJobs
	if free < 0 {
		return 0
	}
	return free
}
