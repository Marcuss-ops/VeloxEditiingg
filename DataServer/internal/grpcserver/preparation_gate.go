package grpcserver

import (
	"context"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/logging"
	"velox-server/internal/placement"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
	"velox-shared/futureasset"
)

// PreparationDecision describes the outcome of the preparation gate
// evaluation. The placement pipeline uses this to decide whether to
// claim the task, wait, or skip.
type PreparationDecision int

const (
	// PreparationNotRequired means the task has no declared assets or
	// the repository does not support future reservations. The claim
	// may proceed immediately.
	PreparationNotRequired PreparationDecision = iota
	// PreparationWaiting means a reservation exists but the worker has
	// not yet reported matching PREPARED evidence for all required
	// assets. The task stays READY; the next placement tick retries.
	PreparationWaiting
	// PreparationReady means every required asset has verified evidence
	// from the correct worker under the correct reservation, and the
	// certificate passes attempt identity verification. The claim may
	// proceed.
	PreparationReady
	// PreparationExpired means the reservation TTL elapsed without
	// reaching PREPARED. The reservation is stale; the claim must not
	// proceed.
	PreparationExpired
)

// String returns the human-readable label for a PreparationDecision.
func (d PreparationDecision) String() string {
	switch d {
	case PreparationNotRequired:
		return "NOT_REQUIRED"
	case PreparationWaiting:
		return "WAITING"
	case PreparationReady:
		return "READY"
	case PreparationExpired:
		return "EXPIRED"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", int(d))
	}
}

// PreparationGate is the lifecycle state machine that governs the boundary
// between placement selection and ClaimTaskForWorkerAtomic. It enforces the
// rule: NO REQUIRED ASSETS PREPARED → NO ATTEMPT.
//
// The gate evaluates a candidate against the future reservation store and
// the in-memory evidence map. When a reservation exists but is not yet
// PREPARED, the gate blocks the claim and the task stays READY for the
// next placement tick.
type PreparationGate struct {
	// store is the persistent future reservation store (SQLite in prod,
	// mock in tests).
	store taskgraph.FutureReservationStore
	// handler is needed to call refreshFutureAssetPlan when the gate
	// discovers a missing reservation for an asset-bearing task.
	handler *Handler
	// dbStore is used to persist reservation state transitions.
	dbStore *store.SQLiteStore
}

// NewPreparationGate creates a gate wired to the given store. The handler
// reference is required for the first-job path (refreshFutureAssetPlan)
// and may be nil in isolated unit tests.
func NewPreparationGate(s taskgraph.FutureReservationStore, h *Handler, db *store.SQLiteStore) *PreparationGate {
	if s == nil {
		return nil
	}
	return &PreparationGate{store: s, handler: h, dbStore: db}
}

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
func (pg *PreparationGate) buildCertificate(reservationID string, assets []futureasset.AssetManifest) preparedJobCertificate {
	if pg == nil || pg.handler == nil {
		return preparedJobCertificate{}
	}
	h := pg.handler
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

// verifyAttempt checks whether a prepared job certificate is consistent
// with the execution claim that is about to be made. The certificate
// proves preparation lineage; the claim fields prove execution identity.
// A mismatch means the preparation was for a different worker, a
// different task revision, or a stale reservation — all of which must
// block the claim.
//
// Returns (true, "") on success; (false, reason) on rejection.
func verifyAttempt(cert preparedJobCertificate, workerID string, taskRevision int, reservationID string) (bool, string) {
	// Zero certificate with no reservation means no preparation was ever
	// requested — this is the non-prefetch path and must not block.
	if cert.AssetsRequired == 0 && reservationID == "" {
		return true, ""
	}
	// If a reservation exists but the certificate has no evidence, the
	// worker never completed preparation.
	if cert.AssetsRequired > 0 && cert.AssetsPrepared == 0 {
		return false, "attempt: certificate has assets_required but zero assets_prepared"
	}
	// Worker identity must match.
	if cert.WorkerID != "" && workerID != "" && cert.WorkerID != workerID {
		return false, fmt.Sprintf("attempt: worker_id mismatch cert=%s claim=%s", cert.WorkerID, workerID)
	}
	// Task revision must match — prevents claiming with stale preparation.
	if cert.TaskRevision != 0 && taskRevision != 0 && cert.TaskRevision != taskRevision {
		return false, fmt.Sprintf("attempt: task_revision mismatch cert=%d claim=%d", cert.TaskRevision, taskRevision)
	}
	// All declared assets must be prepared.
	if cert.AssetsRequired > 0 && cert.AssetsPrepared < cert.AssetsRequired {
		return false, fmt.Sprintf("attempt: assets_prepared=%d < assets_required=%d", cert.AssetsPrepared, cert.AssetsRequired)
	}
	return true, ""
}// MarkPreparedAsset records authenticated, reservation-scoped evidence
// in the gate's read model. The journal remains the operator history;
// this map is the placement gate's fast authority.
//
// When the first asset evidence arrives for a reservation, this method
// drives the state transition to PREPARING via UpdateReservationState.
func (pg *PreparationGate) MarkPreparedAsset(workerID string, eventAsset *preparedAssetEvidence, reservationID string) {
	if pg == nil || pg.handler == nil || eventAsset == nil || strings.TrimSpace(reservationID) == "" {
		return
	}
	eventAsset.WorkerID = workerID
	h := pg.handler
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

// markPreparedAsset delegates to PreparationGate.MarkPreparedAsset.
// Kept for backward compatibility with existing call sites.
func (h *Handler) markPreparedAsset(workerID string, eventAsset *preparedAssetEvidence, reservationID string) {
	if h == nil {
		return
	}
	gate := h.getPrepGate()
	if gate == nil {
		return
	}
	gate.MarkPreparedAsset(workerID, eventAsset, reservationID)
}

// reservationPrepared checks whether every declared asset in the reservation
// has matching evidence from the authenticated worker AND the aggregate
// certificate passes lineage verification.
func (pg *PreparationGate) reservationPrepared(ctx context.Context, workerID string, reservation taskgraph.FutureReservationWithPayload) (bool, error) {
	if pg == nil || pg.handler == nil {
		return false, nil
	}
	assets := futureAssetManifests(reservation.Payload)
	if len(assets) == 0 {
		return true, nil
	}
	h := pg.handler

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
	cert := pg.buildCertificate(reservation.ReservationID, assets)
	if ok, reason := verifyCertificate(cert, workerID, reservation.TaskRevision); !ok {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPrefetchFailed, "[PREFETCH] certificate rejected reservation=%s worker=%s reason=%s", reservation.ReservationID, workerID, reason)
		return false, nil
	}

	return true, nil
}

// reservationPrepared delegates to PreparationGate.reservationPrepared.
// Kept for backward compatibility with existing call sites.
func (h *Handler) reservationPrepared(ctx context.Context, workerID string, reservation taskgraph.FutureReservationWithPayload) (bool, error) {
	if h == nil {
		return false, nil
	}
	gate := h.getPrepGate()
	if gate == nil {
		return false, nil
	}
	return gate.reservationPrepared(ctx, workerID, reservation)
}

func (h *Handler) getPrepGate() *PreparationGate {
	if h == nil {
		return nil
	}
	if h.prepGate != nil {
		return h.prepGate
	}
	if h.taskRepo == nil {
		return nil
	}
	if frs, ok := h.taskRepo.(taskgraph.FutureReservationStore); ok {
		h.prepGate = NewPreparationGate(frs, h, h.dbStore)
	}
	return h.prepGate
}

// EnsurePrepared evaluates the preparation state for a candidate task and
// returns a PreparationDecision describing whether the claim may proceed.
// This is the sole entry point for the preparation gate in the placement
// pipeline.
func (pg *PreparationGate) EnsurePrepared(ctx context.Context, workerID string, candidate *placement.TaskCandidate) (PreparationDecision, error) {
	if pg == nil || pg.handler == nil || candidate == nil {
		return PreparationWaiting, fmt.Errorf("preparation gate: missing gate, handler, or candidate")
	}
	findReservation := func() (taskgraph.FutureReservationWithPayload, bool, error) {
		reservations, err := pg.store.ListFutureReservations(ctx, workerID)
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
		return PreparationWaiting, err
	}
	if !found {
		// Tasks with no declared assets do not need a preparation reservation.
		if len(candidate.RequiredAssetKeys) == 0 {
			return PreparationNotRequired, nil
		}
		// First-job path: create the reservation while the task is still READY.
		if pg.handler != nil {
			pg.handler.refreshFutureAssetPlan(ctx, workerID, candidate.JobID)
		}
		reservation, found, err = findReservation()
		if err != nil {
			return PreparationWaiting, err
		}
		if !found {
			logGRPCf(ctx, logging.LevelDebug, logging.CodeGRPCPlacementFailed, "[PLACEMENT] preparation gate BLOCKED worker=%s task=%s reason=reservation_pending", workerID, candidate.TaskID)
			return PreparationWaiting, nil
		}
	}

	// TTL expiry gate.
	if !reservation.ExpiresAt.IsZero() && time.Now().UTC().After(reservation.ExpiresAt) {
		logGRPCf(ctx, logging.LevelDebug, logging.CodeGRPCPlacementFailed, "[PLACEMENT] preparation gate BLOCKED expired worker=%s task=%s reservation=%s expired_at=%s", workerID, candidate.TaskID, reservation.ReservationID, reservation.ExpiresAt.Format(time.RFC3339))
		return PreparationExpired, nil
	}

	// State machine gate: only PREPARED reservations permit a claim.
	if reservation.State != "" && !reservation.State.CanClaim() {
		logGRPCf(ctx, logging.LevelDebug, logging.CodeGRPCPlacementFailed, "[PLACEMENT] preparation gate BLOCKED state=%s worker=%s task=%s reservation=%s", reservation.State, workerID, candidate.TaskID, reservation.ReservationID)
		return PreparationWaiting, nil
	}

	// Drive state transitions when the reservation state is empty (legacy).
	// Once evidence exists, the reservation should be at least PREPARING.
	if reservation.State == "" {
		assets := futureAssetManifests(reservation.Payload)
		if len(assets) > 0 {
			pg.handler.preparedMu.RLock()
			preparedAssets := pg.handler.prepared[reservation.ReservationID]
			pg.handler.preparedMu.RUnlock()
			if len(preparedAssets) > 0 {
				_ = pg.store.UpdateReservationState(ctx, reservation.ReservationID, taskgraph.ReservationPreparing)
			}
		}
	}

	prepared, err := pg.reservationPrepared(ctx, workerID, reservation)
	if err != nil {
		return PreparationWaiting, err
	}
	if !prepared {
		logGRPCf(ctx, logging.LevelDebug, logging.CodeGRPCPlacementFailed, "[PLACEMENT] preparation gate BLOCKED worker=%s task=%s reservation=%s", workerID, candidate.TaskID, reservation.ReservationID)
		return PreparationWaiting, nil
	}

	// Attempt identity verification.
	assets := futureAssetManifests(reservation.Payload)
	cert := pg.buildCertificate(reservation.ReservationID, assets)
	if ok, reason := verifyAttempt(cert, workerID, candidate.Revision, reservation.ReservationID); !ok {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCPlacementFailed, "[PLACEMENT] attempt verification FAILED worker=%s task=%s reservation=%s reason=%s", workerID, candidate.TaskID, reservation.ReservationID, reason)
		return PreparationWaiting, nil
	}

	// Drive the reservation to PREPARED state on successful gate pass.
	if reservation.State != taskgraph.ReservationPrepared {
		_ = pg.store.UpdateReservationState(ctx, reservation.ReservationID, taskgraph.ReservationPrepared)
	}

	return PreparationReady, nil
}

// ensurePreparedBeforeClaim delegates to PreparationGate.EnsurePrepared
// and translates the PreparationDecision to a boolean for backward
// compatibility with existing callers.
func (h *Handler) ensurePreparedBeforeClaim(ctx context.Context, workerID string, candidate *placement.TaskCandidate) (bool, error) {
	if h == nil {
		return true, nil
	}
	gate := h.getPrepGate()
	if gate == nil {
		return true, nil
	}
	decision, err := gate.EnsurePrepared(ctx, workerID, candidate)
	if err != nil {
		return false, err
	}
	return decision == PreparationReady || decision == PreparationNotRequired, nil
}
