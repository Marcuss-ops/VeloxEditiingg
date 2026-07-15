package outbox_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"velox-server/internal/outbox"
)

// ── 7. Handler transient: retry until MaxAttempts then FAILED ─────────────

func TestOutbox_HandlerTransient_RetriesUntilMaxAttemptsMarkFailed(t *testing.T) {
	store := newTestStore(t)
	reg := outbox.NewRegistry()
	var invocations atomic.Int32
	_ = reg.Register(&stubHandler{
		eventType: "EVT_FAIL_ALWAYS",
		fn: func(ctx context.Context, e outbox.Event) error {
			invocations.Add(1)
			return outbox.Transient(fmt.Errorf("attempt %d always fails", invocations.Load()))
		},
	})

	insertOne(t, store, "EVT_FAIL_ALWAYS", "fail", "f-1", []byte(`{}`))

	// LockDuration=10ms so each retry is recoverable within a tight wait.
	d := outbox.NewDispatcher(store, reg, outbox.Config{
		PollInterval: 10 * time.Millisecond, BatchSize: 5,
		LockDuration: 10 * time.Millisecond, MaxAttempts: 3,
	})
	ctx := startDispatcher(t, d)

	// Wait until status flips to FAILED.
	if err := waitFor(t, 3*time.Second, func() bool {
		n, _ := store.CountByStatus(ctx, outbox.StatusFailed)
		return n >= 1
	}); err != nil {
		t.Fatalf("event did not reach FAILED after MaxAttempts: %v", err)
	}
	// Per the dispatcher contract: Claim atomically increments attempt_count
	// BEFORE invoking Handle; on transient error the row is released back to
	// the ready queue. With MaxAttempts=3 the handler runs exactly 3 times
	// (attempt_count=1, 2, 3) — the third fires the >= MaxAttempts check
	// and the row flips to FAILED.
	if got := invocations.Load(); got != 3 {
		t.Fatalf("invocations = %d, want exactly 3 (MaxAttempts boundary)", got)
	}
}

func TestOutbox_HandlerPermanent_FailsImmediately(t *testing.T) {
	store := newTestStore(t)
	reg := outbox.NewRegistry()
	var invocations atomic.Int32
	_ = reg.Register(&stubHandler{
		eventType: "EVT_PERM",
		fn: func(ctx context.Context, e outbox.Event) error {
			invocations.Add(1)
			return outbox.Permanent(fmt.Errorf("nope"))
		},
	})

	insertOne(t, store, "EVT_PERM", "perm", "p-1", []byte(`{}`))

	d := outbox.NewDispatcher(store, reg, outbox.Config{
		PollInterval: 5 * time.Millisecond, BatchSize: 5,
		LockDuration: 5 * time.Second, MaxAttempts: 5,
	})
	ctx := startDispatcher(t, d)

	if err := waitFor(t, 2*time.Second, func() bool {
		n, _ := store.CountByStatus(ctx, outbox.StatusFailed)
		return n >= 1
	}); err != nil {
		t.Fatalf("event did not reach FAILED: %v", err)
	}
	if got := invocations.Load(); got != 1 {
		t.Fatalf("invocations = %d, want 1 (permanent should not retry)", got)
	}
}

// ── 8. Duplicate event: Insert twice → two rows, two dispatches ───────────

func TestOutbox_DuplicateEvent_InsertsTwoRows(t *testing.T) {
	store := newTestStore(t)
	reg := outbox.NewRegistry()
	var invocations atomic.Int32
	_ = reg.Register(&stubHandler{
		eventType: "EVT_DUP_PROD",
		fn: func(ctx context.Context, e outbox.Event) error {
			invocations.Add(1)
			return nil
		},
	})

	// Same logical event written twice. Each Insert produces a distinct
	// event_id (no UNIQUE collision on (aggregate_*, event_type, payload)).
	id1, err := store.Insert(context.Background(), nil, outbox.InsertParams{
		AggregateType: "demo", AggregateID: "d-1",
		EventType: "EVT_DUP_PROD", Payload: []byte(`{"x":1}`),
	})
	if err != nil {
		t.Fatalf("Insert 1: %v", err)
	}
	id2, err := store.Insert(context.Background(), nil, outbox.InsertParams{
		AggregateType: "demo", AggregateID: "d-1",
		EventType: "EVT_DUP_PROD", Payload: []byte(`{"x":1}`),
	})
	if err != nil {
		t.Fatalf("Insert 2: %v", err)
	}
	if id1 == id2 {
		t.Fatalf("Insert returned the same event_id (%q) twice", id1)
	}

	d := outbox.NewDispatcher(store, reg, outbox.Config{
		PollInterval: 5 * time.Millisecond, BatchSize: 5,
		LockDuration: 5 * time.Second, MaxAttempts: 3,
	})
	ctx := startDispatcher(t, d)
	if err := waitFor(t, 2*time.Second, func() bool {
		n, _ := store.CountByStatus(ctx, outbox.StatusProcessed)
		return n >= 2
	}); err != nil {
		t.Fatalf("not both rows processed: %v", err)
	}
	if got := invocations.Load(); got != 2 {
		t.Fatalf("invocations = %d, want 2", got)
	}
}

// ── 9. Idempotent handler: MarkProcessed no-ops on PROCESSED row ───────────

func TestOutbox_IdempotentHandler_MarkProcessedIsIdempotent(t *testing.T) {
	store := newTestStore(t)

	id := insertOne(t, store, "EVT_IDEMP", "idemp", "i-1", []byte(`{}`))

	// Manually claim once → row goes PROCESSING.
	events, err := store.Claim(context.Background(), "test", time.Now().Add(time.Second), 1)
	if err != nil || len(events) != 1 {
		t.Fatalf("Claim: events=%v err=%v", events, err)
	}
	if events[0].EventID != id {
		t.Fatalf("Claim got event_id %q, want %q", events[0].EventID, id)
	}

	// MarkProcessed first time.
	if err := store.MarkProcessed(context.Background(), id); err != nil {
		t.Fatalf("MarkProcessed 1: %v", err)
	}
	_, status1, _ := store.GetByID(context.Background(), id)
	if status1 != string(outbox.StatusProcessed) {
		t.Fatalf("after first MarkProcessed status = %q, want PROCESSED", status1)
	}

	// MarkProcessed second time: WHERE clause includes `status='PROCESSING'`
	// so it should affect zero rows (only PROCESSING → PROCESSED).
	if err := store.MarkProcessed(context.Background(), id); err != nil {
		t.Fatalf("MarkProcessed 2: %v", err)
	}
	_, status2, _ := store.GetByID(context.Background(), id)
	if status2 != string(outbox.StatusProcessed) {
		t.Fatalf("after second MarkProcessed status = %q, want PROCESSED", status2)
	}
}

// ── 10. PROCESSED-only-after-success ───────────────────────────────────────

func TestOutbox_ProcessedOnlyAfterHandlerSuccess(t *testing.T) {
	// Case A: handler returns nil → row becomes PROCESSED.
	storeA := newTestStore(t)
	regA := outbox.NewRegistry()
	_ = regA.Register(&stubHandler{eventType: "EVT_OK"}) // returns nil
	idA := insertOne(t, storeA, "EVT_OK", "ok", "ok-1", []byte(`{}`))
	dA := outbox.NewDispatcher(storeA, regA, outbox.Config{
		PollInterval: 5 * time.Millisecond, BatchSize: 5,
		LockDuration: 5 * time.Second, MaxAttempts: 3,
	})
	ctxA := startDispatcher(t, dA)
	if err := waitFor(t, 2*time.Second, func() bool {
		_, st, _ := storeA.GetByID(ctxA, idA)
		return st == string(outbox.StatusProcessed)
	}); err != nil {
		t.Fatalf("A: not processed: %v", err)
	}

	// Stop Case A's dispatcher BEFORE Case B starts: without this, A's
	// goroutine keeps ticking on the shared SQLite memory DB and claims
	// Case B's freshly-inserted row before B's dispatcher can race for
	// it. A's registry only knows EVT_OK, so the row ends up FAILED
	// instead of being held for B's EVT_ERR_NOFLIP handler — and B's
	// select on firedB then times out. The t.Cleanup registered by
	// startDispatcher only fires at end of test, so we tear down A now
	// to give the SQLite memory DB a quiet period before B starts.
	dA.Stop()

	// Case B: handler returns error → row stays at PROCESSING (transient
	// that did not exceed MaxAttempts). We use a blocking handler that we
	// then cancel — same crash semantics, but the relevant invariant is
	// "never PROCESSED unless Handle returned nil".
	storeB := newTestStore(t)
	regB := outbox.NewRegistry()
	firedB := make(chan struct{})
	_ = regB.Register(&stubHandler{
		eventType: "EVT_ERR_NOFLIP",
		fn: func(ctx context.Context, e outbox.Event) error {
			close(firedB)
			<-ctx.Done()
			return ctx.Err()
		},
	})
	idB := insertOne(t, storeB, "EVT_ERR_NOFLIP", "err", "err-1", []byte(`{}`))

	ctxB, cancelB := context.WithCancel(context.Background())
	doneB := make(chan struct{})
	go func() {
		defer close(doneB)
		_ = outbox.NewDispatcher(storeB, regB, outbox.Config{
			PollInterval: 5 * time.Millisecond, BatchSize: 5,
			LockDuration: 5 * time.Second, MaxAttempts: 3,
		}).Run(ctxB)
	}()
	select {
	case <-firedB:
	case <-time.After(2 * time.Second):
		t.Fatalf("B: handler did not fire")
	}
	cancelB()
	<-doneB

	_, stB, _ := storeB.GetByID(context.Background(), idB)
	if stB == string(outbox.StatusProcessed) {
		t.Fatalf("B: status flipped to PROCESSED despite handler error: %q", stB)
	}
	if stB != string(outbox.StatusProcessing) {
		// Acceptable: if lock expired and we ran another poll cycle which
		// marked it FAILED. But the ProcessED status must NEVER appear here.
		t.Logf("B: status = %q (acceptable, but PROCESSED forbidden)", stB)
	}
}

// ── 11. ParsePayload ────────────────────────────────────────────────────────

func TestParsePayload(t *testing.T) {
	var s struct {
		X string `json:"x"`
	}

	// Valid JSON: should succeed and populate fields.
	if err := outbox.ParsePayload(outbox.Event{Payload: []byte(`{"x":"hi"}`)}, &s); err != nil {
		t.Fatalf("valid JSON should succeed: %v", err)
	}
	if s.X != "hi" {
		t.Fatalf("got %q, want hi", s.X)
	}

	// Invalid JSON: should return permanent error.
	s.X = ""
	if err := outbox.ParsePayload(outbox.Event{Payload: []byte(`{oops`)}, &s); err == nil {
		t.Fatal("invalid JSON should error")
	}
	if s.X != "" {
		t.Fatalf("invalid JSON should not mutate target, got %q", s.X)
	}
}
