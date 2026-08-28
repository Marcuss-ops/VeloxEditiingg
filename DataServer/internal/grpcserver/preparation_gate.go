package grpcserver

import (
	"context"
	"fmt"
	"strings"

	"velox-server/internal/placement"
	"velox-server/internal/taskgraph"
)

// preparedAssetEvidence is the worker-signed (by the authenticated stream)
// evidence needed before a strict claim may create an Attempt.
type preparedAssetEvidence struct {
	WorkerID     string
	TaskID       string
	TaskRevision int
	AssetID      string
	SHA256       string
	SizeBytes    int64
}

// markPreparedAsset records only authenticated, reservation-scoped evidence.
// The journal remains the operator history; this small read model is the
// placement gate's fast authority.
func (h *Handler) markPreparedAsset(workerID string, eventAsset *preparedAssetEvidence, reservationID string) {
	if h == nil || eventAsset == nil || strings.TrimSpace(reservationID) == "" {
		return
	}
	eventAsset.WorkerID = workerID
	h.preparedMu.Lock()
	if h.prepared == nil {
		h.prepared = make(map[string]map[string]preparedAssetEvidence)
	}
	assets := h.prepared[reservationID]
	if assets == nil {
		assets = make(map[string]preparedAssetEvidence)
		h.prepared[reservationID] = assets
	}
	key := eventAsset.AssetID
	if key == "" {
		key = eventAsset.SHA256
	}
	if key != "" {
		assets[key] = *eventAsset
	}
	h.preparedMu.Unlock()
}

func (h *Handler) reservationPrepared(ctx context.Context, workerID string, reservation taskgraph.FutureReservationWithPayload) (bool, error) {
	assets := futureAssetManifests(reservation.Payload)
	if len(assets) == 0 {
		return true, nil
	}
	h.preparedMu.RLock()
	prepared := h.prepared[reservation.ReservationID]
	h.preparedMu.RUnlock()
	if len(prepared) == 0 {
		return false, nil
	}
	for _, asset := range assets {
		var evidence preparedAssetEvidence
		found := false
		for _, key := range []string{asset.AssetID, asset.AssetKey} {
			if key == "" {
				continue
			}
			if candidate, ok := prepared[key]; ok {
				evidence, found = candidate, true
				break
			}
		}
		if !found || evidence.WorkerID != workerID || evidence.TaskID != reservation.TaskID || evidence.TaskRevision != reservation.TaskRevision {
			return false, nil
		}
		if expected := strings.TrimPrefix(strings.ToLower(strings.TrimSpace(asset.SHA256)), "sha256:"); expected != "" && strings.TrimPrefix(strings.ToLower(strings.TrimSpace(evidence.SHA256)), "sha256:") != expected {
			return false, nil
		}
		if asset.SizeBytes > 0 && evidence.SizeBytes != asset.SizeBytes {
			return false, nil
		}
	}
	return true, nil
}

func (h *Handler) ensurePreparedBeforeClaim(ctx context.Context, workerID string, candidate *placement.TaskCandidate) (bool, error) {
	if h == nil || candidate == nil {
		return false, fmt.Errorf("preparation gate: missing handler or candidate")
	}
	store, ok := h.taskRepo.(taskgraph.FutureReservationStore)
	if !ok {
		return true, nil
	}
	reservations, err := store.ListFutureReservations(ctx, workerID)
	if err != nil {
		return false, err
	}
	for _, reservation := range reservations {
		if reservation.TaskID != candidate.TaskID || reservation.WorkerID != workerID {
			continue
		}
		return h.reservationPrepared(ctx, workerID, reservation)
	}
	return false, nil
}
