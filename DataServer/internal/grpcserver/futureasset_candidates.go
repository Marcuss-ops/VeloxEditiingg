package grpcserver

// futureasset_candidates.go loads and filters READY candidates for the
// future-asset prefetch planner.  The candidate list is the first stage
// of refreshFutureAssetPlan: load → filter → reserve → build → send.

import (
	"context"

	"velox-server/internal/logging"
	"velox-server/internal/placement"
	"velox-server/internal/taskgraph"
)

const futureAssetCandidatePageSize = 256

type paginatedReadyCandidateRepository interface {
	ListReadyCandidatesPage(ctx context.Context, limit, offset int) ([]placement.TaskCandidate, error)
}

// loadCandidates fetches the next batch of READY candidates from the task
// graph.  Returns nil on error (caller logs and aborts the refresh).
func (h *Handler) loadCandidates(ctx context.Context, workerID string) ([]placement.TaskCandidate, bool) {
	var candidates []placement.TaskCandidate
	if paginated, ok := h.taskRepo.(paginatedReadyCandidateRepository); ok {
		for offset := 0; ; offset += futureAssetCandidatePageSize {
			page, err := paginated.ListReadyCandidatesPage(ctx, futureAssetCandidatePageSize, offset)
			if err != nil {
				logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPrefetchFailed,
					"[PREFETCH] future candidates worker=%s offset=%d: %v", workerID, offset, err)
				return nil, false
			}
			candidates = append(candidates, page...)
			if len(page) < futureAssetCandidatePageSize {
				return candidates, true
			}
		}
	}
	var err error
	candidates, err = h.taskRepo.ListReadyCandidates(ctx, futureAssetCandidatePageSize)
	if err != nil {
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPrefetchFailed,
			"[PREFETCH] future candidates worker=%s: %v", workerID, err)
		return nil, false
	}
	return candidates, true
}

// loadExistingReservations fetches all future reservations and partitions
// them into owned-by-this-worker and reserved-by-other maps.
func (h *Handler) loadExistingReservations(
	ctx context.Context,
	workerID string,
	store taskgraph.FutureReservationStore,
) (
	owned map[string]taskgraph.FutureReservationWithPayload,
	reservedByOther map[string]struct{},
	ok bool,
) {
	all, err := store.ListFutureReservations(ctx, "")
	if err != nil {
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPrefetchFailed,
			"[PREFETCH] future reservations worker=%s: %v", workerID, err)
		return nil, nil, false
	}
	owned = make(map[string]taskgraph.FutureReservationWithPayload)
	reservedByOther = make(map[string]struct{})
	for _, item := range all {
		if item.WorkerID == workerID {
			owned[item.TaskID] = item
		} else {
			reservedByOther[item.TaskID] = struct{}{}
		}
	}
	return owned, reservedByOther, true
}
