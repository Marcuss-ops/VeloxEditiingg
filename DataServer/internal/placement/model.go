// Package placement implements the placement matcher for Velox task dispatch.
// It is a pure, deterministic engine that selects the best-compatible Task
// for a given Worker snapshot without any dependency on SQL, gRPC, or protobuf.
package placement

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

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

// NormalizeExecutorKey converts historical "id@version" storage into the
// canonical split shape used by placement decisions: {ID: "id", Version: N}.
// If the suffix is absent or malformed, the input is returned unchanged.
func NormalizeExecutorKey(id string, version int) ExecutorKey {
	key := ExecutorKey{ID: strings.TrimSpace(id), Version: version}
	if !strings.Contains(key.ID, "@") || key.Version <= 0 {
		return key
	}
	base, suffix, ok := strings.Cut(key.ID, "@")
	if !ok || base == "" || suffix == "" {
		return key
	}
	parsed, err := strconv.Atoi(suffix)
	if err != nil || parsed != key.Version {
		return key
	}
	key.ID = base
	return key
}

// VersionedExecutorID rebuilds the legacy "id@version" storage form from the
// canonical split executor identity. Used only where the repository must CAS
// against historical rows still persisted in that shape.
func VersionedExecutorID(id string, version int) string {
	id = strings.TrimSpace(id)
	if id == "" || version <= 0 {
		return id
	}
	return fmt.Sprintf("%s@%d", id, version)
}

// TaskCandidate is a lightweight task metadata row used by the matcher.
// It does NOT carry the full payload JSON.
type TaskCandidate struct {
	TaskID    string
	JobID     string
	Revision  int
	Priority  int
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
