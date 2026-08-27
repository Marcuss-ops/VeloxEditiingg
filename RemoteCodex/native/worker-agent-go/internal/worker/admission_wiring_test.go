package worker

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"velox-worker-agent/internal/prefetch"
)

// mockAdmissionController is a controllable admission controller for testing.
type mockAdmissionController struct {
	throttled  atomic.Bool
	rejections atomic.Int64
}

func (m *mockAdmissionController) IsThrottled(kind ResourceKind) bool {
	return m.throttled.Load()
}

// ── PublisherPool: Acquire blocks when publish is throttled ──────────────────
func TestPublisherPool_AdmissionBlocksWhenThrottled(t *testing.T) {
	ctrl := &mockAdmissionController{}
	pool := NewPublisherPool(2)
	pool.SetAdmissionController(ctrl)

	// With no throttling, Acquire should succeed immediately.
	ctx := context.Background()
	if err := pool.Acquire(ctx); err != nil {
		t.Fatalf("Acquire should succeed when not throttled: %v", err)
	}
	pool.Release()

	// Throttle publish — Acquire should block.
	ctrl.throttled.Store(true)

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	err := pool.Acquire(ctx)
	if err == nil {
		pool.Release()
		t.Fatal("Acquire should fail when throttled and context times out")
	}
	if err != context.DeadlineExceeded {
		t.Fatalf("expected DeadlineExceeded, got %v", err)
	}

	// Unthrottle — Acquire should now succeed.
	ctrl.throttled.Store(false)
	ctx2 := context.Background()
	if err := pool.Acquire(ctx2); err != nil {
		t.Fatalf("Acquire should succeed after unthrottle: %v", err)
	}
	pool.Release()
}

// ── PublisherPool: nil adapter is safe ──────────────────────────────────────
func TestPublisherPool_NilAdmissionControllerIsSafe(t *testing.T) {
	pool := NewPublisherPool(2)
	pool.SetAdmissionController(nil)

	ctx := context.Background()
	if err := pool.Acquire(ctx); err != nil {
		t.Fatalf("Acquire should succeed with nil admission controller: %v", err)
	}
	pool.Release()
}

// ── AdmissionAdapter: round-trip CanAdmit/RecordAdmissionResult ─────────────
func TestAdmissionAdapter_AdmitBelowThreshold(t *testing.T) {
	// 40/70 = 57.1% — below all thresholds → admit.
	rac := NewResourceAdmissionController(fixedSampler(40<<30), fixedTotalRAM(70<<30))
	adapter := newAdmissionAdapter(rac)

	if d := adapter.CanAdmit(prefetch.AdmissionPrefetch); d != prefetch.AdmissionAdmit {
		t.Fatalf("CanAdmit(prefetch) at 57%% RSS = %v, want Admit", d)
	}
	adapter.RecordAdmissionResult(prefetch.AdmissionPrefetch, true)
}

func TestAdmissionAdapter_RejectPrefetchAboveThreshold(t *testing.T) {
	// 60/70 = 85.7% > 80% prefetch threshold → reject.
	rac := NewResourceAdmissionController(fixedSampler(60<<30), fixedTotalRAM(70<<30))
	adapter := newAdmissionAdapter(rac)

	d := adapter.CanAdmit(prefetch.AdmissionPrefetch)
	if d == prefetch.AdmissionAdmit {
		t.Fatal("CanAdmit(prefetch) at 85.7%% RSS should reject, got Admit")
	}
	t.Logf("CanAdmit(prefetch) = %v at 85.7%% RSS (correct: rejected)", d)
}

func TestAdmissionAdapter_NilControllerReturnsNil(t *testing.T) {
	adapter := newAdmissionAdapter(nil)
	if adapter != nil {
		t.Fatalf("newAdmissionAdapter(nil) should return nil, got %v", adapter)
	}
}
