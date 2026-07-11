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
)

// MatchResult bundles the selected candidate (if any) with rejection
// reasons for diagnostics.
type MatchResult struct {
	Candidate  *TaskCandidate
	Rejections []Rejection
}

// Matcher is the placement engine. It is stateless and safe for
// concurrent use.
type Matcher struct{}

// NewMatcher returns a ready-to-use Matcher.
func NewMatcher() *Matcher {
	return &Matcher{}
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
