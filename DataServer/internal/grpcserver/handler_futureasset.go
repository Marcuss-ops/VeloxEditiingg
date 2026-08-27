package grpcserver

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"velox-server/internal/logging"
	"velox-server/internal/placement"
	"velox-server/internal/taskgraph"
	"velox-shared/contract"
	"velox-shared/contract/assembly"
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
	candidates, err := h.taskRepo.ListReadyCandidates(ctx, 256)
	if err != nil {
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPrefetchFailed, "[PREFETCH] future candidates worker=%s: %v", workerID, err)
		return
	}
	candidatesLoadedAt = time.Now().UTC()
	all, err := store.ListFutureReservations(ctx, "")
	if err != nil {
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPrefetchFailed, "[PREFETCH] future reservations worker=%s: %v", workerID, err)
		return
	}
	reservationsLoadedAt = time.Now().UTC()
	owned := make(map[string]taskgraph.FutureReservationWithPayload)
	reservedByOther := make(map[string]struct{})
	for _, item := range all {
		if item.WorkerID == workerID {
			owned[item.TaskID] = item
		} else {
			reservedByOther[item.TaskID] = struct{}{}
		}
	}
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
	desired := make([]taskgraph.FutureReservation, 0, prefetchLimit)
	jobs := make([]futureasset.Job, 0, protectionLimit)
	for _, candidate := range candidates {
		if len(jobs) >= protectionLimit {
			break
		}
		if _, blocked := reservedByOther[candidate.TaskID]; blocked {
			continue
		}
		if existing, exists := owned[candidate.TaskID]; exists {
			// Existing ownership is sticky until TTL/reconciliation. The
			// ranking only chooses new reservations; it must not migrate a
			// live preparation implicitly or change worker_id identity.
			reservation := existing.FutureReservation
			reservation.Distance = len(jobs) + 1
			desired = append(desired, reservation)
			jobs = append(jobs, futureasset.Job{JobID: existing.JobID, TaskID: existing.TaskID, ReservationID: existing.ReservationID, TaskRevision: existing.TaskRevision, Assets: futureAssetManifests(existing.Payload)})
			continue
		}

		payload, err := store.FutureTaskPayload(ctx, candidate.TaskID)
		if err != nil {
			continue
		}
		assets := futureAssetManifests(payload)
		decision, err := selectWarmPlacement(warmSnapshots, assets)
		if err != nil || decision.WorkerID != workerID {
			continue
		}
		match := h.placementMatcher.Select(snapshot, []placement.TaskCandidate{candidate})
		if match.Candidate == nil {
			continue
		}
		reservation := taskgraph.FutureReservation{TaskID: candidate.TaskID, JobID: candidate.JobID, WorkerID: workerID, ReservationID: fmt.Sprintf("future:%s:%s", workerID, candidate.TaskID), TaskRevision: candidate.Revision, Distance: len(jobs) + 1, ExpiresAt: time.Now().UTC().Add(h.futureAssetPlanTTL())}
		if len(jobs) < prefetchLimit {
			acquired, err := store.TryReserveFutureTask(ctx, reservation)
			if err != nil {
				logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPrefetchFailed, "[PREFETCH] reserve worker=%s task=%s: %v", workerID, candidate.TaskID, err)
				continue
			}
			if !acquired {
				continue
			}
			logGRPCf(ctx, logging.LevelInfo, logging.CodeGRPCPrefetch, "[PREFETCH_TIMING] event=reservation_created worker=%s task=%s at=%s", workerID, candidate.TaskID, time.Now().UTC().Format(time.RFC3339Nano))
			if h.dbStore != nil {
				_ = h.dbStore.LogJobEvent(candidate.JobID, "prefetch.reservation_created", map[string]interface{}{
					"worker_id":      workerID,
					"task_id":        candidate.TaskID,
					"reservation_id": reservation.ReservationID,
					"distance":       reservation.Distance,
				})
			}
		} else {
			// N+4..N+10 are retention forecasts only. They must be
			// represented in the worker snapshot, but must not acquire a
			// hard placement reservation or consume a scheduler slot.
			reservation.ReservationID = ""
		}
		if reservation.ReservationID != "" {
			desired = append(desired, reservation)
		}
		jobs = append(jobs, futureasset.Job{JobID: candidate.JobID, TaskID: candidate.TaskID, ReservationID: reservation.ReservationID, TaskRevision: candidate.Revision, Assets: futureAssetManifests(payload)})
	}
	if err := store.ReconcileFutureReservations(ctx, workerID, desired); err != nil {
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPrefetchFailed, "[PREFETCH] reconcile worker=%s: %v", workerID, err)
		return
	}
	reservationsReconciledAt = time.Now().UTC()
	limits := futureasset.Limits{PrefetchHorizon: h.config.FutureAssetPrefetchHorizon, ProtectionLookahead: h.config.FutureAssetProtectionLookahead}
	plan, err := h.futureAssetPlanner.Build(workerID, currentJobID, fmt.Sprintf("future:%s", workerID), jobs, h.futureAssetPlanTTL())
	if err != nil {
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPrefetchFailed, "[PREFETCH] build worker=%s: %v", workerID, err)
		return
	}
	planBuiltAt = time.Now().UTC()
	plan.Limits = limits
	logGRPCf(ctx, logging.LevelInfo, logging.CodeGRPCPrefetch, "[PREFETCH] plan worker=%s version=%d current_job=%s hard_reservations=%d protection_jobs=%d protected_assets=%d", workerID, plan.Version, currentJobID, len(desired), len(jobs), len(plan.Protect))
	if err := h.SendFutureAssetPlan(ctx, plan); err != nil {
		logGRPCf(ctx, logging.LevelError, logging.CodeGRPCPrefetchFailed, "[PREFETCH] send worker=%s: %v", workerID, err)
	} else {
		planSentAt = time.Now().UTC()
	}
	// Persist prefetch lifecycle events into the job journal so fleetctl
	// job inspect shows the full prefetch timeline.
	if h.dbStore != nil && planSentAt.IsZero() == false {
		for _, job := range jobs {
			if job.JobID == "" {
				continue
			}
			assetKeys := make([]string, 0, len(job.Assets))
			for _, a := range job.Assets {
				assetKeys = append(assetKeys, a.AssetKey)
			}
			_ = h.dbStore.LogJobEvent(job.JobID, "prefetch.future_plan_sent", map[string]interface{}{
				"worker_id":      workerID,
				"task_id":        job.TaskID,
				"reservation_id": job.ReservationID,
				"plan_id":        plan.PlanID,
				"plan_version":   plan.Version,
				"distance":       job.Distance,
				"asset_count":    len(job.Assets),
				"asset_keys":     assetKeys,
			})
		}
	}
}

func formatTimingTimestamp(value time.Time) string {
	if value.IsZero() {
		return ""
	}
	return value.Format(time.RFC3339Nano)
}

func durationBetween(start, end time.Time) int64 {
	if start.IsZero() || end.IsZero() || end.Before(start) {
		return 0
	}
	return end.Sub(start).Milliseconds()
}

func (h *Handler) warmPlacementSnapshots() []assembly.WorkerPlacementSnapshot {
	if h == nil {
		return nil
	}
	h.mu.RLock()
	defer h.mu.RUnlock()
	out := make([]assembly.WorkerPlacementSnapshot, 0, len(h.sessions))
	for _, sess := range h.sessions {
		if sess == nil {
			continue
		}
		snapshot := sess.placementSnapshot(sess.workerID)
		cached := make([]string, 0, len(snapshot.CachedAssetKeys))
		for key := range snapshot.CachedAssetKeys {
			cached = append(cached, key)
		}
		sort.Strings(cached)
		out = append(out, assembly.WorkerPlacementSnapshot{
			WorkerID:              snapshot.WorkerID,
			Available:             snapshot.SessionAlive && snapshot.Ready && !snapshot.Draining,
			CapacityAuthoritative: snapshot.CapacityAuthoritative,
			DiskAuthoritative:     snapshot.DiskAuthoritative,
			ActiveExecutionSlots:  snapshot.ActiveJobs,
			MaxExecutionSlots:     snapshot.MaxParallelJobs,
			FreeDiskBytes:         snapshot.FreeDiskBytes,
			EstimatedAvailableMS:  snapshot.EstimatedAvailableMS,
			NetworkMbps:           snapshot.NetworkMbps,
			LoadRatio:             snapshot.LoadRatio,
			Capabilities:          snapshot.Capabilities.All(),
			CachedSHA256:          cached,
		})
	}
	return out
}

func selectWarmPlacement(workers []assembly.WorkerPlacementSnapshot, assets []futureasset.AssetManifest) (assembly.PlacementDecision, error) {
	request := assembly.PlacementRequest{AssetSizes: make(map[string]uint64)}
	for _, asset := range assets {
		if asset.SHA256 == "" {
			continue
		}
		request.AssetSHA256 = append(request.AssetSHA256, asset.SHA256)
		if asset.SizeBytes > 0 {
			request.AssetSizes[asset.SHA256] = uint64(asset.SizeBytes)
			request.MinimumFreeDiskBytes += uint64(asset.SizeBytes)
		}
	}
	return assembly.SelectPreferredWorker(workers, request)
}

// ensureFutureReservationOwnership preserves a live preparation lease on
// its owner, but transfers it with a CAS when that owner is no longer
// eligible. This runs immediately before execution claim, so an unavailable
// preferred worker cannot strand a READY task until the original TTL.
func (h *Handler) ensureFutureReservationOwnership(ctx context.Context, workerID string, candidate *placement.TaskCandidate) (bool, error) {
	if h == nil || candidate == nil {
		return false, fmt.Errorf("future reservation fallback: missing handler or candidate")
	}
	store, ok := h.taskRepo.(taskgraph.FutureReservationStore)
	if !ok {
		// Lightweight repositories used by non-prefetch deployments have no
		// preparation reservations and retain the normal claim path.
		return true, nil
	}
	reservations, err := store.ListFutureReservations(ctx, "")
	if err != nil {
		return false, err
	}
	var current *taskgraph.FutureReservationWithPayload
	for i := range reservations {
		if reservations[i].TaskID == candidate.TaskID {
			current = &reservations[i]
			break
		}
	}
	if current == nil || current.WorkerID == workerID {
		return true, nil
	}

	assets := futureAssetManifests(current.Payload)
	request := assembly.PlacementRequest{AssetSizes: make(map[string]uint64)}
	for _, asset := range assets {
		if asset.SHA256 == "" {
			continue
		}
		request.AssetSHA256 = append(request.AssetSHA256, asset.SHA256)
		if asset.SizeBytes > 0 {
			request.AssetSizes[asset.SHA256] = uint64(asset.SizeBytes)
			request.MinimumFreeDiskBytes += uint64(asset.SizeBytes)
		}
	}

	// If the preparation owner remains eligible, it retains the soft lease;
	// another worker must not steal its execution task merely because it ran
	// a placement tick first.
	for _, snapshot := range h.warmPlacementSnapshots() {
		if snapshot.WorkerID != current.WorkerID {
			continue
		}
		if _, ownerErr := assembly.SelectPreferredWorker([]assembly.WorkerPlacementSnapshot{snapshot}, request); ownerErr == nil {
			return false, nil
		}
		break
	}

	decision, err := assembly.SelectPreferredWorker(h.warmPlacementSnapshots(), request)
	if err != nil || decision.WorkerID != workerID {
		return false, nil
	}
	transferred := current.FutureReservation
	transferred.WorkerID = workerID
	transferred.ReservationID = fmt.Sprintf("future:%s:%s:fallback:%d", workerID, candidate.TaskID, time.Now().UnixNano())
	transferred.ExpiresAt = time.Now().UTC().Add(h.futureAssetPlanTTL())
	acquired, err := store.TransferFutureTask(ctx, candidate.TaskID, current.WorkerID, transferred)
	if err != nil {
		return false, err
	}
	if acquired {
		logGRPCf(ctx, logging.LevelInfo, logging.CodeGRPCPrefetch, "[PREFETCH] preparation lease fallback task=%s from_worker=%s to_worker=%s", candidate.TaskID, current.WorkerID, workerID)
	}
	return acquired, nil
}

func (h *Handler) futureAssetPlanTTL() time.Duration {
	if h != nil && h.config != nil && h.config.FutureAssetPlanTTL > 0 {
		return h.config.FutureAssetPlanTTL
	}
	return 2 * time.Minute
}

func futureAssetManifests(payload []byte) []futureasset.AssetManifest {
	var root interface{}
	if len(payload) == 0 || json.Unmarshal(payload, &root) != nil {
		return nil
	}
	seen := make(map[string]futureasset.AssetManifest)
	var walk func(interface{})
	walk = func(value interface{}) {
		switch node := value.(type) {
		case []interface{}:
			for _, child := range node {
				walk(child)
			}
		case map[string]interface{}:
			if rawPlan, ok := node[contract.PayloadKeyCompiledRenderPlanJSON].(string); ok {
				appendCompiledPlanAssetManifests(rawPlan, seen)
			}
			key, _ := node["asset_key"].(string)
			if key == "" {
				key, _ = node["asset_id"].(string)
			}
			sha, _ := node["sha256"].(string)
			if sha == "" {
				sha, _ = node["asset_sha256"].(string)
			}
			var size int64
			switch n := node["size_bytes"].(type) {
			case float64:
				size = int64(n)
			case int64:
				size = n
			}
			if key != "" && sha != "" && size > 0 {
				role, _ := node["role"].(string)
				seen[key] = futureasset.AssetManifest{AssetKey: key, AssetID: key, SHA256: sha, SizeBytes: size, Role: role}
			}
			for _, child := range node {
				walk(child)
			}
		}
	}
	walk(root)
	out := make([]futureasset.AssetManifest, 0, len(seen))
	for _, asset := range seen {
		out = append(out, asset)
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].AssetKey < out[j].AssetKey
	})
	return out
}

func appendCompiledPlanAssetManifests(raw string, seen map[string]futureasset.AssetManifest) {
	plan, err := contract.DecodeCompiledRenderPlanV2([]byte(raw))
	if err != nil || plan == nil {
		return
	}
	for _, asset := range plan.Assets {
		key := asset.AssetKey
		if key == "" {
			key = asset.AssetID
		}
		if key == "" || asset.SHA256 == "" || asset.SizeBytes <= 0 {
			continue
		}
		seen[key] = futureasset.AssetManifest{
			AssetKey: key, AssetID: asset.AssetID, SHA256: asset.SHA256,
			SizeBytes: asset.SizeBytes, MIMEType: asset.MIME, Role: asset.Kind,
		}
	}
}
