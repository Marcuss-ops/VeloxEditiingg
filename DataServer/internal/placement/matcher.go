// Package placement / matcher.go
//
// Matcher is the pure, deterministic placement engine. It takes a
// WorkerSnapshot and an ordered slice of TaskCandidates and emits a
// MatchResult containing either the first compatible candidate (sorted
// by priority DESC, then created_at ASC) or a list of rejections.
//
// The Matcher has zero dependencies on SQL, gRPC, Gin, or protobuf.
// The future central scheduler will reuse this exact logic.
package placement

import (
	"fmt"
	"sort"
	"strings"
	"sync/atomic"
)

// MatchResult bundles the selected candidate (if any) with rejection
// reasons for diagnostics.
type MatchResult struct {
	Candidate  *TaskCandidate
	Rejections []Rejection
}

// Matcher is the placement engine. It is stateless with respect to
// placement decisions and safe for concurrent use. The optional
// placement-pin (SetPin) is a master-wide operator override used by
// deterministic-pick smoke harnesses (e.g.
// tests/worker-cert/smoke_one.sh) that need a single worker to be the
// only eligible target regardless of pool composition.
//
// Placement pin semantics:
//   - Pin is a worker_id string. Empty string disables the pin (the
//     matcher then behaves as the pre-pin stateless engine, matching
//     every existing call site that does not opt in).
//   - Pin mutates only via SetPin (called once at handler construction).
//   - Concurrent Select reads are lock-free via atomic.Value: zero
//     allocations on the hot dispatch path, no contention with
//     notifyTasksAvailable goroutines.
//
// The pin emits a single RejectPlacementPinExcluded rejection with
// TaskID="" (terminal worker-level gate, before candidate iteration),
// which flows naturally through the existing recordPlacementRejections
// log/sink loop so velox_placement_rejections_total{reason="placement_pin_excluded"}
// is populated without collector changes.
type Matcher struct {
	pinWorkerID atomic.Value // string; "" means "no pin"
}

// NewMatcher returns a ready-to-use Matcher with no pin installed.
func NewMatcher() *Matcher {
	m := &Matcher{}
	m.pinWorkerID.Store("")
	return m
}

// SetPin installs the worker_id that Select will allow; empty string
// disables the pin (stateless behaviour restored). Idempotent and safe
// to call concurrently with Select.
func (m *Matcher) SetPin(workerID string) {
	m.pinWorkerID.Store(strings.TrimSpace(workerID))
}

// Select evaluates candidates against the worker snapshot and returns
// the first compatible match following priority+FIFO ordering.
//
// Ordering rules:
//  1. Non-terminal worker gates (alive, ready, not draining, free slots).
//  2. Candidates sorted by priority DESC, then created_at ASC.
//  3. First candidate that matches all executor and capability gates wins.
//  4. All skipped candidates are recorded as Rejections.
//
// When no candidate matches, result.Candidate is nil and result.Rejections
// contains one entry per skipped candidate (plus any terminal worker-level
// rejection).
func (m *Matcher) Select(
	worker WorkerSnapshot,
	candidates []TaskCandidate,
) MatchResult {
	result := MatchResult{
		Rejections: make([]Rejection, 0),
	}

	// Operator-driven placement pin (VELOX_PLACEMENT_PIN_WORKER_ID):
	// the FIRST terminal gate. When the pin is set to a worker_id and
	// this worker's id is something else, the matcher emits a single
	// RejectPlacementPinExcluded and short-circuits BEFORE any
	// capability/capacity check — the pin operator intent is "only
	// this one worker, full stop". An empty pin is a no-op so the
	// pre-existing stateless call path is unchanged.
	if pin, _ := m.pinWorkerID.Load().(string); pin != "" && worker.WorkerID != pin {
		result.Rejections = append(result.Rejections, Rejection{
			Code:   RejectPlacementPinExcluded,
			Detail: fmt.Sprintf("master pinned to worker %q; worker %q excluded", pin, worker.WorkerID),
		})
		return result
	}

	// Terminal worker-level gates.
	if !worker.SessionAlive {
		result.Rejections = append(result.Rejections, Rejection{
			Code:   RejectSessionInactive,
			Detail: "worker session is not active",
		})
		return result
	}

	if !worker.Ready {
		result.Rejections = append(result.Rejections, Rejection{
			Code:   RejectWorkerNotReady,
			Detail: "worker is not ready",
		})
		return result
	}

	if worker.Draining {
		result.Rejections = append(result.Rejections, Rejection{
			Code:   RejectWorkerDraining,
			Detail: "worker is draining",
		})
		return result
	}

	if worker.FreeSlots() <= 0 {
		result.Rejections = append(result.Rejections, Rejection{
			Code:   RejectCapacityFull,
			Detail: "worker has no free task slots",
		})
		return result
	}

	// Stable-sort candidates: priority DESC, then created_at ASC (FIFO).
	ordered := append([]TaskCandidate(nil), candidates...)

	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].Priority != ordered[j].Priority {
			return ordered[i].Priority > ordered[j].Priority
		}
		// Within the same priority, prefer the task whose inputs are
		// already present on this worker. This is an advisory warm-cache
		// bonus; a cold worker remains fully eligible.
		warmI := cachedAssetCount(worker, ordered[i])
		warmJ := cachedAssetCount(worker, ordered[j])
		if warmI != warmJ {
			return warmI > warmJ
		}
		return ordered[i].CreatedAt.Before(ordered[j].CreatedAt)
	})

	for i := range ordered {
		candidate := ordered[i]

		if !candidate.Executor.Valid() {
			result.Rejections = append(result.Rejections, Rejection{
				TaskID: candidate.TaskID,
				Code:   RejectInvalidTaskRequirement,
				Detail: "task executor requirement is invalid",
			})
			continue
		}

		if !worker.HasExecutor(candidate.Executor) {
			result.Rejections = append(result.Rejections, Rejection{
				TaskID: candidate.TaskID,
				Code:   RejectUnsupportedExecutor,
				Detail: fmt.Sprintf(
					"worker does not advertise %s@%d",
					candidate.Executor.ID,
					candidate.Executor.Version,
				),
			})
			continue
		}

		// Per-job placement pin: when a task carries a
		// _placement_pin_worker_id, only the named worker may claim
		// it. All other workers receive a pin-mismatch rejection.
		// This gate runs AFTER executor/capability compatibility
		// checks so the rejection reason is "wrong worker" not
		// "unsupported executor" — the pin is an operator intent
		// overlay, not a capability constraint.
		if candidate.PlacementPinWorkerID != "" && candidate.PlacementPinWorkerID != worker.WorkerID {
			result.Rejections = append(result.Rejections, Rejection{
				TaskID: candidate.TaskID,
				Code:   RejectPlacementPinMismatch,
				Detail: fmt.Sprintf(
					"task pinned to worker %q; worker %q excluded",
					candidate.PlacementPinWorkerID, worker.WorkerID,
				),
			})
			continue
		}

		missing := ""
		for _, capability := range candidate.RequiredCapabilities {
			if !worker.Capabilities[capability] {
				missing = capability
				break
			}
		}

		if missing != "" {
			result.Rejections = append(result.Rejections, Rejection{
				TaskID: candidate.TaskID,
				Code:   RejectMissingCapability,
				Detail: "missing capability: " + missing,
			})
			continue
		}

		result.Candidate = &candidate
		return result
	}

	return result
}

func cachedAssetCount(worker WorkerSnapshot, candidate TaskCandidate) int {
	if len(candidate.RequiredAssetKeys) == 0 || len(worker.CachedAssetKeys) == 0 {
		return 0
	}
	count := 0
	for _, key := range candidate.RequiredAssetKeys {
		if _, ok := worker.CachedAssetKeys[key]; ok {
			count++
		}
	}
	return count
}
