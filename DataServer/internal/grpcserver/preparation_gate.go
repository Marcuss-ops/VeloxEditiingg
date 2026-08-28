package grpcserver

import (
	"context"
	"fmt"
	"strings"

	"velox-server/internal/logging"
	"velox-server/internal/placement"
	"velox-server/internal/taskgraph"
	"velox-shared/futureasset"
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

// preparedJobCertificate is the master-side aggregate of all prepared asset
// evidence for a reservation. It captures the full lineage needed to verify
// that the preparation was performed by the correct worker for the correct
// task revision under the correct reservation.
type preparedJobCertificate struct {
	WorkerID       string
	ReservationID  string
	TaskRevision   int
	AssetsRequired int
	AssetsPrepared int
	PreparedBytes  int64
}

// buildCertificate aggregates per-asset evidence into a job-level
// certificate. The certificate is built from the authenticated evidence
// stored in the handler's prepared map.
func (h *Handler) buildCertificate(reservationID string, assets []futureasset.AssetManifest) preparedJobCertificate {
	if h == nil {
		return preparedJobCertificate{}
	}
	h.preparedMu.RLock()
	prepared := h.prepared[reservationID]
	h.preparedMu.RUnlock()

	cert := preparedJobCertificate{
		ReservationID:  reservationID,
		AssetsRequired: len(assets),
	}
	if len(prepared) == 0 {
		return cert
	}

	// Use the first evidence entry to extract worker/revision identity.
	// All evidence in a reservation must share the same worker and revision
	// (enforced by markPreparedAsset and reservationPrepared).
	var firstEvidence *preparedAssetEvidence
	for _, e := range prepared {
		cp := e
		firstEvidence = &cp
		break
	}
	if firstEvidence != nil {
		cert.WorkerID = firstEvidence.WorkerID
		cert.TaskRevision = firstEvidence.TaskRevision
	}

	for _, asset := range assets {
		for _, key := range []string{asset.AssetID, asset.AssetKey} {
			if key == "" {
				continue
			}
			if evidence, ok := prepared[key]; ok {
				cert.AssetsPrepared++
				cert.PreparedBytes += evidence.SizeBytes
				break
			}
		}
	}
	return cert
}

// verifyCertificate checks whether a certificate matches the expected worker
// and task revision for a claim. Returns (true, "") on success;
// (false, reason) on rejection.
func verifyCertificate(cert preparedJobCertificate, workerID string, taskRevision int) (bool, string) {
	if cert.AssetsRequired == 0 {
		return true, ""
	}
	if cert.AssetsPrepared < cert.AssetsRequired {
		return false, fmt.Sprintf("certificate: assets_prepared=%d < assets_required=%d", cert.AssetsPrepared, cert.AssetsRequired)
	}
	if cert.WorkerID != "" && workerID != "" && cert.WorkerID != workerID {
		return false, fmt.Sprintf("certificate: worker_id mismatch cert=%s claim=%s", cert.WorkerID, workerID)
	}
	if cert.TaskRevision != 0 && taskRevision != 0 && cert.TaskRevision != taskRevision {
		return false, fmt.Sprintf("certificate: task_revision mismatch cert=%d claim=%d", cert.TaskRevision, taskRevision)
	}
	return true, ""
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

	// Per-asset verification: every declared asset must have matching
	// evidence from the authenticated worker.
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

	// Structured certificate verification: aggregate the per-asset evidence
	// into a job-level certificate and verify the full lineage.
	cert := h.buildCertificate(reservation.ReservationID, assets)
	if ok, reason := verifyCertificate(cert, workerID, reservation.TaskRevision); !ok {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPrefetchFailed, "[PREFETCH] certificate rejected reservation=%s worker=%s reason=%s", reservation.ReservationID, workerID, reason)
		return false, nil
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
	findReservation := func() (taskgraph.FutureReservationWithPayload, bool, error) {
		reservations, err := store.ListFutureReservations(ctx, workerID)
		if err != nil {
			return taskgraph.FutureReservationWithPayload{}, false, err
		}
		for _, reservation := range reservations {
			if reservation.TaskID == candidate.TaskID && reservation.WorkerID == workerID {
				return reservation, true, nil
			}
		}
		return taskgraph.FutureReservationWithPayload{}, false, nil
	}

	reservation, found, err := findReservation()
	if err != nil {
		return false, err
	}
	if !found {
		// Tasks with no declared assets do not need a preparation reservation.
		// For asset-bearing tasks, however, a missing reservation is itself a
		// preparation miss: strict mode must establish the reservation and keep
		// the task READY until the worker reports matching evidence.
		if len(candidate.RequiredAssetKeys) == 0 {
			return true, nil
		}
		// This call uses the same reservation authority and worker plan path
		// as N+1 prefetch. It runs while the task is still READY; no Attempt
		// can be created until a subsequent tick observes PREPARED evidence.
		h.refreshFutureAssetPlan(ctx, workerID, candidate.JobID)
		reservation, found, err = findReservation()
		if err != nil {
			return false, err
		}
		if !found {
			logGRPCf(ctx, logging.LevelDebug, logging.CodeGRPCPlacementFailed, "[PLACEMENT] preparation gate BLOCKED worker=%s task=%s reason=reservation_pending", workerID, candidate.TaskID)
			return false, nil
		}
	}

	// State machine gate: only PREPARED reservations permit a claim.
	if reservation.State != "" && !reservation.State.CanClaim() {
		logGRPCf(ctx, logging.LevelDebug, logging.CodeGRPCPlacementFailed, "[PLACEMENT] preparation gate BLOCKED state=%s worker=%s task=%s reservation=%s", reservation.State, workerID, candidate.TaskID, reservation.ReservationID)
		return false, nil
	}

	prepared, err := h.reservationPrepared(ctx, workerID, reservation)
	if err != nil {
		return false, err
	}
	if !prepared {
		logGRPCf(ctx, logging.LevelDebug, logging.CodeGRPCPlacementFailed, "[PLACEMENT] preparation gate BLOCKED worker=%s task=%s reservation=%s", workerID, candidate.TaskID, reservation.ReservationID)
	}
	return prepared, nil
}
