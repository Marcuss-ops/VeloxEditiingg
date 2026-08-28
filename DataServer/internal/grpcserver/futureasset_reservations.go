package grpcserver

// futureasset_reservations.go builds the desired reservation list from
// candidates and handles reconciliation with the store.  It also owns
// the post-send reservation state transitions (RESERVED → PLANNING).

import (
	"context"
	"fmt"
	"time"

	"velox-server/internal/logging"
	"velox-server/internal/placement"
	"velox-server/internal/taskgraph"
	"velox-shared/contract/assembly"
	"velox-shared/futureasset"
)

// buildDesiredReservations iterates candidates and produces the desired
// reservation list and job manifest list.  Existing ownership is sticky;
// new reservations are acquired via TryReserveFutureTask only within the
// prefetch horizon.
func (h *Handler) buildDesiredReservations(
	ctx context.Context,
	workerID string,
	candidates []placement.TaskCandidate,
	owned map[string]taskgraph.FutureReservationWithPayload,
	reservedByOther map[string]struct{},
	snapshot placement.WorkerSnapshot,
	warmSnapshots []assembly.WorkerPlacementSnapshot,
	store taskgraph.FutureReservationStore,
	prefetchLimit int,
	protectionLimit int,
	skipCounts *prefetchSkipCounter,
) (
	desired []taskgraph.FutureReservation,
	jobs []futureasset.Job,
) {
	desired = make([]taskgraph.FutureReservation, 0, prefetchLimit)
	jobs = make([]futureasset.Job, 0, protectionLimit)

	for _, candidate := range candidates {
		if len(jobs) >= protectionLimit {
			break
		}
		if _, blocked := reservedByOther[candidate.TaskID]; blocked {
			skipCounts.add(SkipReservedByOther)
			continue
		}
		if existing, exists := owned[candidate.TaskID]; exists {
			// Existing ownership is sticky until TTL/reconciliation.
			reservation := existing.FutureReservation
			reservation.Distance = len(jobs) + 1
			desired = append(desired, reservation)
			jobs = append(jobs, futureasset.Job{
				JobID:         existing.JobID,
				TaskID:        existing.TaskID,
				ReservationID: existing.ReservationID,
				TaskRevision:  existing.TaskRevision,
				Assets:        futureAssetManifests(existing.Payload),
			})
			continue
		}

		payload, err := store.FutureTaskPayload(ctx, candidate.TaskID)
		if err != nil {
			skipCounts.add(SkipPayloadUnavailable)
			continue
		}
		assets := futureAssetManifests(payload)
		decision, err := selectWarmPlacement(warmSnapshots, assets)
		if err != nil || decision.WorkerID != workerID {
			skipCounts.add(SkipDifferentWarmWorker)
			continue
		}
		match := h.placementMatcher.Select(snapshot, []placement.TaskCandidate{candidate})
		if match.Candidate == nil {
			skipCounts.add(SkipPlacementRejected)
			continue
		}
		reservation := taskgraph.FutureReservation{
			TaskID:        candidate.TaskID,
			JobID:         candidate.JobID,
			WorkerID:      workerID,
			ReservationID: fmt.Sprintf("future:%s:%s", workerID, candidate.TaskID),
			TaskRevision:  candidate.Revision,
			Distance:      len(jobs) + 1,
			ExpiresAt:     time.Now().UTC().Add(h.futureAssetPlanTTL()),
		}
		if len(jobs) < prefetchLimit {
			acquired, err := store.TryReserveFutureTask(ctx, reservation)
			if err != nil {
				logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPrefetchFailed,
					"[PREFETCH] reserve worker=%s task=%s: %v", workerID, candidate.TaskID, err)
				continue
			}
			if !acquired {
				skipCounts.add(SkipReservationConflict)
				continue
			}
			logGRPCf(ctx, logging.LevelInfo, logging.CodeGRPCPrefetch,
				"[PREFETCH_TIMING] event=reservation_created worker=%s task=%s at=%s",
				workerID, candidate.TaskID, time.Now().UTC().Format(time.RFC3339Nano))
			h.persistReservationCreated(ctx, candidate.JobID, workerID, candidate.TaskID, reservation.ReservationID, reservation.Distance)
		} else {
			// N+4..N+10 are retention forecasts only — no hard reservation.
			reservation.ReservationID = ""
		}
		if reservation.ReservationID != "" {
			desired = append(desired, reservation)
		}
		jobs = append(jobs, futureasset.Job{
			JobID:         candidate.JobID,
			TaskID:        candidate.TaskID,
			ReservationID: reservation.ReservationID,
			TaskRevision:  candidate.Revision,
			Assets:        futureAssetManifests(payload),
		})
	}
	return desired, jobs
}

// reconcileReservations persists the desired reservation list and returns
// the post-reconciliation timestamp.
func (h *Handler) reconcileReservations(
	ctx context.Context,
	workerID string,
	store taskgraph.FutureReservationStore,
	desired []taskgraph.FutureReservation,
) (time.Time, bool) {
	if err := store.ReconcileFutureReservations(ctx, workerID, desired); err != nil {
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPrefetchFailed,
			"[PREFETCH] reconcile worker=%s: %v", workerID, err)
		return time.Time{}, false
	}
	return time.Now().UTC(), true
}

// updateReservationStates transitions all non-empty reservations from
// RESERVED to PLANNING after the plan has been sent.
func (h *Handler) updateReservationStates(
	ctx context.Context,
	store taskgraph.FutureReservationStore,
	desired []taskgraph.FutureReservation,
) {
	for _, r := range desired {
		if r.ReservationID == "" {
			continue
		}
		_ = store.UpdateReservationState(ctx, r.ReservationID, taskgraph.ReservationPlanning)
	}
}
