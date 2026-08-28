package grpcserver

import (
	"context"
	"fmt"
	"sort"
	"time"

	"velox-server/internal/logging"
	"velox-server/internal/placement"
	"velox-server/internal/taskgraph"
	"velox-shared/contract/assembly"
	"velox-shared/futureasset"
)

// warmPlacementSnapshots collects a placement-eligible snapshot from every
// live worker session. The list is used by selectWarmPlacement and
// ensureFutureReservationOwnership to decide which worker should prefetch
// or own a given set of assets.
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

// selectWarmPlacement picks the best worker from the list to host a given
// set of assets, preferring the worker with the highest cache hit ratio.
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
