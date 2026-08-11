package grpcserver

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"velox-server/internal/placement"
	"velox-server/internal/taskgraph"
	"velox-shared/futureasset"
)

// refreshFutureAssetPlan claims the next hard-reservation window for a
// worker, then sends a complete worker-scoped snapshot. Reservation ownership
// is persisted before the plan is sent, so a reconnect cannot turn a plan
// into an unowned hint.
func (h *Handler) refreshFutureAssetPlan(ctx context.Context, workerID, currentJobID string) {
	store, ok := h.taskRepo.(taskgraph.FutureReservationStore)
	if !ok {
		return
	}
	candidates, err := h.taskRepo.ListReadyCandidates(ctx, 256)
	if err != nil {
		log.Printf("[PREFETCH] future candidates worker=%s: %v", workerID, err)
		return
	}
	all, err := store.ListFutureReservations(ctx, "")
	if err != nil {
		log.Printf("[PREFETCH] future reservations worker=%s: %v", workerID, err)
		return
	}
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
	limit := h.config.FutureAssetPrefetchHorizon
	if limit <= 0 {
		limit = 3
	}
	desired := make([]taskgraph.FutureReservation, 0, limit)
	jobs := make([]futureasset.Job, 0, limit)
	for _, candidate := range candidates {
		if len(desired) >= limit {
			break
		}
		if _, blocked := reservedByOther[candidate.TaskID]; blocked {
			continue
		}
		if existing, exists := owned[candidate.TaskID]; exists {
			desired = append(desired, existing.FutureReservation)
			jobs = append(jobs, futureasset.Job{JobID: existing.JobID, TaskID: existing.TaskID, ReservationID: existing.ReservationID, TaskRevision: existing.TaskRevision, Assets: futureAssetManifests(existing.Payload)})
			continue
		}
		match := h.placementMatcher.Select(snapshot, []placement.TaskCandidate{candidate})
		if match.Candidate == nil {
			continue
		}
		reservation := taskgraph.FutureReservation{TaskID: candidate.TaskID, JobID: candidate.JobID, WorkerID: workerID, ReservationID: fmt.Sprintf("future:%s:%s", workerID, candidate.TaskID), TaskRevision: candidate.Revision, Distance: len(desired) + 1, ExpiresAt: time.Now().UTC().Add(h.futureAssetPlanTTL())}
		acquired, err := store.TryReserveFutureTask(ctx, reservation)
		if err != nil {
			log.Printf("[PREFETCH] reserve worker=%s task=%s: %v", workerID, candidate.TaskID, err)
			continue
		}
		if !acquired {
			continue
		}
		payload, err := store.FutureTaskPayload(ctx, candidate.TaskID)
		if err != nil {
			continue
		}
		desired = append(desired, reservation)
		jobs = append(jobs, futureasset.Job{JobID: candidate.JobID, TaskID: candidate.TaskID, ReservationID: reservation.ReservationID, TaskRevision: candidate.Revision, Assets: futureAssetManifests(payload)})
	}
	if err := store.ReconcileFutureReservations(ctx, workerID, desired); err != nil {
		log.Printf("[PREFETCH] reconcile worker=%s: %v", workerID, err)
		return
	}
	limits := futureasset.Limits{PrefetchHorizon: h.config.FutureAssetPrefetchHorizon, ProtectionLookahead: h.config.FutureAssetProtectionLookahead}
	plan, err := h.futureAssetPlanner.Build(workerID, currentJobID, fmt.Sprintf("future:%s", workerID), jobs, h.futureAssetPlanTTL())
	if err != nil {
		log.Printf("[PREFETCH] build worker=%s: %v", workerID, err)
		return
	}
	plan.Limits = limits
	if err := h.SendFutureAssetPlan(ctx, plan); err != nil {
		log.Printf("[PREFETCH] send worker=%s: %v", workerID, err)
	}
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
	return out
}
