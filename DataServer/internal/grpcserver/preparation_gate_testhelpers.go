// Package grpcserver — shared mocks for preparation-gate tests.
package grpcserver

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"velox-server/internal/placement"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
)

// ── Expiry-aware mock store ────────────────────────────────────────────────

// expiryMockStore extends mockFutureReservationStore with time-based expiry.
// When now() returns a time after ExpiresAt, ListFutureReservations returns
// empty — simulating the SQLite store's DELETE WHERE expires_at <= now.
type expiryMockStore struct {
	mu          sync.Mutex
	reservation *taskgraph.FutureReservationWithPayload
	payload     []byte
	reserved    bool
	now         func() time.Time
}

func (m *expiryMockStore) TryReserveFutureTask(_ context.Context, r taskgraph.FutureReservation) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reserved {
		return false, nil
	}
	m.reservation = &taskgraph.FutureReservationWithPayload{FutureReservation: r, Payload: m.payload}
	m.reserved = true
	return true, nil
}

func (m *expiryMockStore) ReconcileFutureReservations(_ context.Context, _ string, _ []taskgraph.FutureReservation) error {
	return nil
}

func (m *expiryMockStore) ListFutureReservations(_ context.Context, workerID string) ([]taskgraph.FutureReservationWithPayload, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reservation == nil {
		return nil, nil
	}
	// Simulate expiry: if now() is after ExpiresAt, return empty.
	if m.now != nil && !m.reservation.ExpiresAt.IsZero() && m.now().After(m.reservation.ExpiresAt) {
		return nil, nil
	}
	if workerID != "" && m.reservation.WorkerID != workerID {
		return nil, nil
	}
	return []taskgraph.FutureReservationWithPayload{*m.reservation}, nil
}

func (m *expiryMockStore) FutureTaskPayload(_ context.Context, _ string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.payload, nil
}

func (m *expiryMockStore) TransferFutureTask(_ context.Context, _, _ string, _ taskgraph.FutureReservation) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return true, nil
}

var _ taskgraph.FutureReservationStore = (*expiryMockStore)(nil)

// SetState advances the reservation to the given state for testing
// lifecycle transitions.
func (m *expiryMockStore) SetState(state taskgraph.ReservationState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reservation != nil {
		m.reservation.State = state
	}
}

func (m *expiryMockStore) UpdateReservationState(_ context.Context, reservationID string, state taskgraph.ReservationState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reservation != nil && m.reservation.ReservationID == reservationID {
		m.reservation.State = state
	}
	return nil
}

// expiryRepo wraps expiryMockStore for the handler's taskRepo.
type expiryRepo struct {
	taskgraph.Repository
	frs *expiryMockStore
}

func (c *expiryRepo) TryReserveFutureTask(ctx context.Context, r taskgraph.FutureReservation) (bool, error) {
	return c.frs.TryReserveFutureTask(ctx, r)
}
func (c *expiryRepo) ReconcileFutureReservations(ctx context.Context, w string, rs []taskgraph.FutureReservation) error {
	return c.frs.ReconcileFutureReservations(ctx, w, rs)
}
func (c *expiryRepo) ListFutureReservations(ctx context.Context, w string) ([]taskgraph.FutureReservationWithPayload, error) {
	return c.frs.ListFutureReservations(ctx, w)
}
func (c *expiryRepo) FutureTaskPayload(ctx context.Context, id string) ([]byte, error) {
	return c.frs.FutureTaskPayload(ctx, id)
}
func (c *expiryRepo) TransferFutureTask(ctx context.Context, id, from string, r taskgraph.FutureReservation) (bool, error) {
	return c.frs.TransferFutureTask(ctx, id, from, r)
}
func (c *expiryRepo) UpdateReservationState(ctx context.Context, reservationID string, state taskgraph.ReservationState) error {
	return c.frs.UpdateReservationState(ctx, reservationID, state)
}

// noopProgress is a minimal asset progress sink for tests.
type noopProgress struct{}

func (n *noopProgress) IngestAssetDownloadProgress(_ context.Context, _ store.AssetDownloadProgressRecord) error {
	return nil
}

// buildExpiryHandler creates a handler wired with an expiryMockStore.
func buildExpiryHandler(t *testing.T, frs *expiryMockStore) *Handler {
	t.Helper()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, &HandlerConfig{
		PushMode:            true,
		StrictPrefetchClaim: true,
		FutureAssetPlanTTL:  2 * time.Minute,
	})
	h.taskRepo = &expiryRepo{frs: frs}
	h.SetAssetDownloadProgressSink(&noopProgress{})
	return h
}

// expiryCandidate builds a TaskCandidate for expiry tests.
func expiryCandidate(taskID, jobID string, revision int) *placement.TaskCandidate {
	return &placement.TaskCandidate{
		TaskID:    taskID,
		JobID:     jobID,
		Revision:  revision,
		Executor:  placement.ExecutorKey{ID: "video.assemble.copy.v1", Version: 1},
		Priority:  1,
		CreatedAt: time.Now().UTC(),
	}
}

// ──────────────────────────────────────────────────────────────────────────
// INVARIANT 1: Gate blocks claim when no PREPARED evidence
// (Reinforced with structured assertion)
// ──────────────────────────────────────────────────────────────────────────

// ── Future reservation store stub ──────────────────────────────────────────

// mockFutureReservationStore implements taskgraph.FutureReservationStore with
// in-memory maps so the preparation gate can be exercised in isolation without
// SQLite. All methods are safe for concurrent use by the handler's goroutines.
type mockFutureReservationStore struct {
	mu          sync.Mutex
	reservation *taskgraph.FutureReservationWithPayload
	payload     []byte
	reserved    bool
	transferred bool
}

func (m *mockFutureReservationStore) TryReserveFutureTask(_ context.Context, r taskgraph.FutureReservation) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reserved {
		return false, nil
	}
	m.reservation = &taskgraph.FutureReservationWithPayload{FutureReservation: r, Payload: m.payload}
	m.reserved = true
	return true, nil
}

func (m *mockFutureReservationStore) ReconcileFutureReservations(_ context.Context, _ string, _ []taskgraph.FutureReservation) error {
	return nil
}

func (m *mockFutureReservationStore) ListFutureReservations(_ context.Context, workerID string) ([]taskgraph.FutureReservationWithPayload, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reservation == nil {
		return nil, nil
	}
	if workerID != "" && m.reservation.WorkerID != workerID {
		return nil, nil
	}
	return []taskgraph.FutureReservationWithPayload{*m.reservation}, nil
}

func (m *mockFutureReservationStore) FutureTaskPayload(_ context.Context, _ string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.payload, nil
}

func (m *mockFutureReservationStore) TransferFutureTask(_ context.Context, _, _ string, _ taskgraph.FutureReservation) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.transferred = true
	return true, nil
}

func (m *mockFutureReservationStore) UpdateReservationState(_ context.Context, reservationID string, state taskgraph.ReservationState) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reservation != nil && m.reservation.ReservationID == reservationID {
		m.reservation.State = state
	}
	return nil
}

var _ taskgraph.FutureReservationStore = (*mockFutureReservationStore)(nil)

// SetState advances the reservation to the given state for testing
// lifecycle transitions.
func (m *mockFutureReservationStore) SetState(state taskgraph.ReservationState) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.reservation != nil {
		m.reservation.State = state
	}
}

// ── Minimal asset progress sink ────────────────────────────────────────────

type noopAssetProgressSink struct{}

func (n *noopAssetProgressSink) IngestAssetDownloadProgress(_ context.Context, _ store.AssetDownloadProgressRecord) error {
	return nil
}

// ── Test helpers ───────────────────────────────────────────────────────────

// taskCandidate builds a minimal TaskCandidate for the preparation gate.
func taskCandidate(taskID, jobID string, revision int) *placement.TaskCandidate {
	return &placement.TaskCandidate{
		TaskID:    taskID,
		JobID:     jobID,
		Revision:  revision,
		Executor:  placement.ExecutorKey{ID: "video.assemble.copy.v1", Version: 1},
		Priority:  1,
		CreatedAt: time.Now().UTC(),
	}
}

// reservationPayload builds a JSON payload with one asset manifest.
// The SHA256 must match the prepared evidence for the gate to pass.
func reservationPayload(sha256 string, sizeBytes int64) []byte {
	return []byte(fmt.Sprintf(`{"assets":[{"asset_key":"video-fragment","asset_id":"video-fragment","sha256":"%s","size_bytes":%d}]}`, sha256, sizeBytes))
}

// buildHandler creates a Handler wired with a FutureReservationStore and the
// given StrictPrefetchClaim setting. The taskRepo must implement
// FutureReservationStore for the gate to activate.
func buildHandler(t *testing.T, strict bool, store *mockFutureReservationStore) *Handler {
	t.Helper()
	h := NewHandler(nil, nil, nil, nil, nil, nil, nil, &HandlerConfig{
		PushMode:            true,
		StrictPrefetchClaim: strict,
		FutureAssetPlanTTL:  2 * time.Minute,
	})
	// Wire a composite repo: nil taskRepo + FutureReservationStore.
	// The gate only uses the FutureReservationStore interface assertion,
	// but sendPushTaskOffer also needs ListReadyCandidates. For the
	// preparation gate unit tests, we only call ensurePreparedBeforeClaim
	// directly — sendPushTaskOffer is not exercised.
	//
	// We set a noop progress sink to avoid nil-pointer in handler paths.
	h.SetAssetDownloadProgressSink(&noopAssetProgressSink{})
	return h
}

// compositeRepo wraps a FutureReservationStore so handler.taskRepo
// satisfies the FutureReservationStore interface assertion in the gate.
type compositeRepo struct {
	taskgraph.Repository
	frs taskgraph.FutureReservationStore
}

func (c *compositeRepo) TryReserveFutureTask(ctx context.Context, r taskgraph.FutureReservation) (bool, error) {
	return c.frs.TryReserveFutureTask(ctx, r)
}
func (c *compositeRepo) ReconcileFutureReservations(ctx context.Context, w string, rs []taskgraph.FutureReservation) error {
	return c.frs.ReconcileFutureReservations(ctx, w, rs)
}
func (c *compositeRepo) ListFutureReservations(ctx context.Context, w string) ([]taskgraph.FutureReservationWithPayload, error) {
	return c.frs.ListFutureReservations(ctx, w)
}
func (c *compositeRepo) FutureTaskPayload(ctx context.Context, id string) ([]byte, error) {
	return c.frs.FutureTaskPayload(ctx, id)
}
func (c *compositeRepo) TransferFutureTask(ctx context.Context, id, from string, r taskgraph.FutureReservation) (bool, error) {
	return c.frs.TransferFutureTask(ctx, id, from, r)
}
func (c *compositeRepo) UpdateReservationState(ctx context.Context, reservationID string, state taskgraph.ReservationState) error {
	return c.frs.UpdateReservationState(ctx, reservationID, state)
}

// ──────────────────────────────────────────────────────────────────────────
// TEST 1: Gate blocks claim when reservation exists but no PREPARED evidence
// ──────────────────────────────────────────────────────────────────────────
