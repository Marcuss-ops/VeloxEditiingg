package grpcserver

// handler_futureasset.go is the thin orchestrator for the future-asset
// prefetch refresh cycle.  It delegates to:
//   - futureasset_candidates.go  — candidate loading and filtering
//   - futureasset_reservations.go — reservation building and reconciliation
//   - futureasset_plan.go        — plan building and TTL
//   - futureasset_events.go      — event persistence
//   - futureasset_placement.go   — warm placement snapshots
//   - futureasset_manifests.go   — asset manifest extraction

import (
	"context"
	"time"

	"velox-server/internal/logging"
	"velox-server/internal/taskgraph"
	"velox-shared/futureasset"
)

// refreshFutureAssetPlan claims the next hard-reservation window for a
// worker, then sends a complete worker-scoped snapshot. Reservation ownership
// is persisted before the plan is sent, so a reconnect cannot turn a plan
// into an unowned hint.
func (h *Handler) refreshFutureAssetPlan(ctx context.Context, workerID, currentJobID string) {
	refreshStartedAt := time.Now().UTC()
	var candidatesLoadedAt, reservationsLoadedAt, reservationsReconciledAt, planBuiltAt, planSentAt time.Time
	defer func() {
		logGRPCf(ctx, logging.LevelInfo, logging.CodeGRPCPrefetch, "[PREFETCH_TIMING] worker=%s current_job=%s refresh_started_at=%s candidates_loaded_at=%s reservations_loaded_at=%s reservations_reconciled_at=%s plan_built_at=%s plan_sent_at=%s candidate_query_ms=%d reservation_query_ms=%d reservation_reconcile_ms=%d plan_build_ms=%d plan_send_ms=%d",
			workerID, currentJobID,
			refreshStartedAt.Format(time.RFC3339Nano), formatTimingTimestamp(candidatesLoadedAt), formatTimingTimestamp(reservationsLoadedAt), formatTimingTimestamp(reservationsReconciledAt), formatTimingTimestamp(planBuiltAt), formatTimingTimestamp(planSentAt),
			durationBetween(refreshStartedAt, candidatesLoadedAt), durationBetween(candidatesLoadedAt, reservationsLoadedAt), durationBetween(reservationsLoadedAt, reservationsReconciledAt), durationBetween(reservationsReconciledAt, planBuiltAt), durationBetween(planBuiltAt, planSentAt))
	}()

	store, ok := h.taskRepo.(taskgraph.FutureReservationStore)
	if !ok {
		return
	}

	// ── Stage 1: Load candidates ────────────────────────────────────────
	candidates, ok := h.loadCandidates(ctx, workerID)
	if !ok {
		return
	}
	candidatesLoadedAt = time.Now().UTC()

	// ── Stage 2: Load existing reservations ─────────────────────────────
	owned, reservedByOther, ok := h.loadExistingReservations(ctx, workerID, store)
	if !ok {
		return
	}
	reservationsLoadedAt = time.Now().UTC()

	// ── Stage 3: Build desired reservations ─────────────────────────────
	sess := h.getSession(workerID)
	if sess == nil {
		return
	}
	snapshot := sess.placementSnapshot(workerID)
	snapshot.ActiveJobs = 0 // future reservations do not consume current slots
	warmSnapshots := h.warmPlacementSnapshots()
	prefetchLimit := h.config.FutureAssetPrefetchHorizon
	if prefetchLimit <= 0 {
		prefetchLimit = futureasset.DefaultPrefetchHorizon
	}
	protectionLimit := h.config.FutureAssetProtectionLookahead
	if protectionLimit < prefetchLimit {
		protectionLimit = prefetchLimit
	}
	skipCounts := newPrefetchSkipCounter()

	desired, jobs := h.buildDesiredReservations(
		ctx, workerID, candidates, owned, reservedByOther,
		snapshot, warmSnapshots, store, prefetchLimit, protectionLimit, skipCounts,
		currentJobID,
	)

	// ── Stage 4: Reconcile reservations ─────────────────────────────────
	reconciledAt, ok := h.reconcileReservations(ctx, workerID, store, desired)
	if !ok {
		return
	}
	reservationsReconciledAt = reconciledAt

	// ── Stage 5: Build plan ─────────────────────────────────────────────
	plan, ok := h.buildFuturePlan(workerID, currentJobID, jobs)
	if !ok {
		return
	}
	planBuiltAt = time.Now().UTC()

	logGRPCf(ctx, logging.LevelInfo, logging.CodeGRPCPrefetch,
		"[PREFETCH] plan worker=%s version=%d current_job=%s hard_reservations=%d protection_jobs=%d protected_assets=%d",
		workerID, plan.Version, currentJobID, len(desired), len(jobs), len(plan.Protect))
	logGRPCf(ctx, logging.LevelInfo, logging.CodeGRPCPrefetch,
		"[PREFETCH] plan_decision worker=%s ready_candidates=%d reserved=%d skipped=%d skip_reasons=%s",
		workerID, len(candidates), len(desired), skipCounts.total(), skipCounts.summary())

	// ── Stage 6: Send plan ──────────────────────────────────────────────
	if err := h.SendFutureAssetPlan(ctx, plan); err != nil {
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPrefetchFailed,
			"[PREFETCH] send worker=%s: %v", workerID, err)
		return
	}
	planSentAt = time.Now().UTC()

	// ── Stage 7: Post-send state transitions ────────────────────────────
	h.updateReservationStates(ctx, store, desired)
	h.persistPlanSent(ctx, workerID, plan.PlanID, plan.Version, jobs)
}
