// Package placement implements the placement matcher for Velox task dispatch.
// It is a pure, deterministic engine that selects the best-compatible Task
// for a given Worker snapshot without any dependency on SQL, gRPC, or protobuf.
package placement

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"velox-shared/controltransport"
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
	TaskID   string
	JobID    string
	Revision int
	Priority int
	// AttemptCount is zero for the initial dispatch and positive after a
	// lease has been expired/retried.
	AttemptCount int
	CreatedAt    time.Time

	Executor ExecutorKey

	// Phase classifies the task for per-phase slot accounting.
	// Empty means "unknown" and falls back to flat FreeSlots.
	Phase TaskPhase

	RequiredCapabilities []string
	// RequiredAssetKeys are canonical input assets referenced by the task.
	// They are advisory for placement; leases remain the correctness gate.
	RequiredAssetKeys []string

	// PlacementPinWorkerID is the per-job worker pin extracted from
	// the task spec payload (_placement_pin_worker_id). When
	// non-empty, the matcher will only match this task to the named
	// worker on the initial attempt; after a lease expires, retries may move
	// to another compatible worker for recovery.
	PlacementPinWorkerID string
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

	// Flat slot accounting (legacy, fallback).
	MaxParallelJobs int
	ActiveJobs      int

	// Per-phase slot accounting from the CapacityScorecard. When non-zero,
	// the matcher uses these instead of the flat MaxParallelJobs limit.
	RenderSlots     int
	PrefetchSlots   int
	PublisherSlots  int
	ActiveRender    int
	ActivePrefetch  int
	ActivePublisher int

	// CapacityAuthoritative and DiskAuthoritative are fail-closed resource
	// facts used by warm placement. A zero/unknown value is never treated as
	// spare capacity or free disk when a preparation needs that resource.
	CapacityAuthoritative bool
	DiskAuthoritative     bool
	FreeDiskBytes         uint64
	EstimatedAvailableMS  int64
	NetworkMbps           float64
	LoadRatio             float64

	ExecutorRegistry controltransport.ExecutorRegistry
	// Capabilities contains only typed named admission features. Executor
	// metadata is exclusively represented by ExecutorRegistry.
	Capabilities controltransport.CapabilitySet
	// CachedAssetKeys is the worker's last heartbeat projection of completed
	// local assets. It is a placement hint, never a reservation.
	CachedAssetKeys map[string]struct{}

	CapabilityRevision uint64
	LastHeartbeat      time.Time
}

// HasExecutor returns true when the worker snapshot carries the given key.
func (w WorkerSnapshot) HasExecutor(key ExecutorKey) bool {
	return w.ExecutorRegistry.Has(key.ID, key.Version)
}

// FreeSlots returns the number of additional tasks the worker can accept.
// For phase-aware callers, use FreeSlotsForPhase instead.
func (w WorkerSnapshot) FreeSlots() int {
	free := w.MaxParallelJobs - w.ActiveJobs
	if free < 0 {
		return 0
	}
	return free
}

// TaskPhase classifies a task candidate for per-phase slot accounting.
type TaskPhase string

const (
	TaskPhaseRender    TaskPhase = "render"
	TaskPhasePrefetch  TaskPhase = "prefetch"
	TaskPhasePublisher TaskPhase = "publisher"
	TaskPhaseUnknown   TaskPhase = ""
)

// FreeSlotsForPhase returns the number of free slots for a specific task
// phase. When per-phase slots are configured (RenderSlots > 0), it uses
// the phase-specific limit. Otherwise it falls back to the flat FreeSlots.
func (w WorkerSnapshot) FreeSlotsForPhase(phase TaskPhase) int {
	switch phase {
	case TaskPhaseRender:
		if w.RenderSlots > 0 {
			free := w.RenderSlots - w.ActiveRender
			if free < 0 {
				return 0
			}
			return free
		}
	case TaskPhasePrefetch:
		if w.PrefetchSlots > 0 {
			free := w.PrefetchSlots - w.ActivePrefetch
			if free < 0 {
				return 0
			}
			return free
		}
	case TaskPhasePublisher:
		if w.PublisherSlots > 0 {
			free := w.PublisherSlots - w.ActivePublisher
			if free < 0 {
				return 0
			}
			return free
		}
	}
	return w.FreeSlots()
}
