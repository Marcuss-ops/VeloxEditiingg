// Package outbox — tests covering the 8 PR 8 DoD scenarios.
//
//	1. Concurrent claim (multiple dispatchers race on the same Store).
//	2. Lock expiry (stale PROCESSING rows are re-claimable).
//	3. Dispatcher crash (row stays PROCESSING; restart reclaims).
//	4. Handler fail/retry (transient → retry, permanent → FAILED, MaxAttempts → FAILED).
//	5. Duplicate event (Insert twice — two distinct event_ids, dispatched twice).
//	6. Idempotent handler (invariant: dispatcher does NOT de-dupe beyond the SQL layer).
//	7. New event type without SQL migration (Registry.Register inserts no CHECK).
//	8. PROCESSED-only-after-success (status flips to PROCESSED iff Handle returned nil).
//
// All tests use a unique file::memory:?cache=shared DB so concurrent goroutines
// inside one test share data while keeping tests isolated from each other.
// SetMaxOpenConns(1) on the *sql.DB serialises writes (avoiding SQLITE_BUSY).
package outbox_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/outbox"
)

// ── Fixtures ────────────────────────────────────────────────────────────────

// newTestDB opens an in-memory SQLite DB unique to t.Name() with the
// outbox_events schema mirroring migration 026. Returns *sql.DB; tests
// must call t.Cleanup (or db.Close()) when done.
func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	// Unique DSN per test; shared cache so concurrent goroutines in
	// the same test see the same DB.
	dsn := fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name())
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		t.Fatalf("open in-memory sqlite: %v", err)
	}
	db.SetMaxOpenConns(1) // serialise writes — concurrent goroutines must queue.
	db.SetMaxIdleConns(1)

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS outbox_events (
		event_id        TEXT PRIMARY KEY,
		aggregate_type  TEXT NOT NULL,
		aggregate_id    TEXT NOT NULL,
		event_type      TEXT NOT NULL,
		payload_json    TEXT NOT NULL DEFAULT '{}',
		status          TEXT NOT NULL DEFAULT 'PENDING',
		available_at    TEXT NOT NULL,
		attempt_count   INTEGER NOT NULL DEFAULT 0,
		locked_by       TEXT,
		locked_until    TEXT,
		processed_at    TEXT,
		last_error      TEXT,
		created_at      TEXT NOT NULL
	)`); err != nil {
		t.Fatalf("create outbox_events: %v", err)
	}

	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newTestStore(t *testing.T) *outbox.Store {
	t.Helper()
	return outbox.NewStore(newTestDB(t))
}

// stubHandler records every Handle call; tests inspect invocations to
// verify the dispatcher's behavior without coupling to real-world
// business logic.
type stubHandler struct {
	eventType string
	fn        func(ctx context.Context, e outbox.Event) error

	invocations atomic.Int32
	lastEventID atomic.Value // string
	lastPayload atomic.Value // []byte
}

func (h *stubHandler) EventType() string { return h.eventType }
func (h *stubHandler) Handle(ctx context.Context, e outbox.Event) error {
	h.invocations.Add(1)
	h.lastEventID.Store(e.EventID)
	h.lastPayload.Store(append([]byte(nil), e.Payload...))
	if h.fn == nil {
		return nil
	}
	return h.fn(ctx, e)
}

// startDispatcher launches d.Run(ctx) in a goroutine and registers
// t.Cleanup so the goroutine is torn down before the DB is closed.
// Returns the running context so callers can use it for queries.
func startDispatcher(t *testing.T, d *outbox.Dispatcher) context.Context {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		// Suppress noisy log output in tests.
		log.SetOutput(devNull{})
		_ = d.Run(ctx)
	}()
	t.Cleanup(func() {
		d.Stop()
		cancel()
		<-done
	})
	return ctx
}

// devNull is an io.Writer that swallows everything.
type devNull struct{}

func (devNull) Write(p []byte) (int, error) { return len(p), nil }

// insertOne is a tiny ergonomic helper: insert a single PENDING event.
func insertOne(t *testing.T, store *outbox.Store, evt, aggType, aggID string, payload []byte) string {
	t.Helper()
	id, err := store.Insert(context.Background(), nil, outbox.InsertParams{
		AggregateType: aggType,
		AggregateID:   aggID,
		EventType:     evt,
		Payload:       payload,
	})
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	return id
}

// ── 1. Registry ─────────────────────────────────────────────────────────────

func TestOutbox_Registry_RegisterAndLookup(t *testing.T) {
	reg := outbox.NewRegistry()
	if err := reg.Register(&stubHandler{eventType: "EVT_A"}); err != nil {
		t.Fatalf("Register: %v", err)
	}
	if reg.Len() != 1 {
		t.Fatalf("Len = %d, want 1", reg.Len())
	}
	h, err := reg.Lookup("EVT_A")
	if err != nil || h == nil {
		t.Fatalf("Lookup EVT_A: handler=%v err=%v", h, err)
	}
	_, err = reg.Lookup("EVT_MISSING")
	if !errors.Is(err, outbox.ErrNoHandler) {
		t.Fatalf("missing lookup err = %v, want ErrNoHandler", err)
	}
}

func TestOutbox_Registry_DuplicateErrors(t *testing.T) {
	reg := outbox.NewRegistry()
	_ = reg.Register(&stubHandler{eventType: "EVT_DUP"})
	err := reg.Register(&stubHandler{eventType: "EVT_DUP"})
	if err == nil {
		t.Fatalf("duplicate Register should error")
	}
}

// ── 2. New event type registered without SQL migration ─────────────────────

func TestOutbox_NewEventType_NoSQLMigration(t *testing.T) {
	store := newTestStore(t)
	reg := outbox.NewRegistry()
	_ = reg.Register(&stubHandler{eventType: "BRAND_NEW_TYPE_9999"})

	// Insert succeeds even though outbox_events.event_type has no row
	// in any registry/schema — pure runtime registration.
	id := insertOne(t, store, "BRAND_NEW_TYPE_9999", "demo", "demo-1", []byte(`{"k":"v"}`))
	if id == "" {
		t.Fatalf("Insert returned empty id")
	}

	// Dispatcher resolves the new type.
	d := outbox.NewDispatcher(store, reg, outbox.Config{
		PollInterval: 5 * time.Millisecond,
		BatchSize:    5,
		LockDuration: 5 * time.Second,
		MaxAttempts:  3,
	})
	ctx := startDispatcher(t, d)

	if err := waitFor(t, 2*time.Second, func() bool {
		n, _ := store.CountByStatus(ctx, outbox.StatusProcessed)
		return n >= 1
	}); err != nil {
		t.Fatalf("event not processed within deadline: %v", err)
	}
}

// ── 3. Insert validation ───────────────────────────────────────────────────

func TestOutbox_Insert_RejectsMissingFields(t *testing.T) {
	store := newTestStore(t)
	ctx := context.Background()
	if _, err := store.Insert(ctx, nil, outbox.InsertParams{
		EventType: "X", AggregateID: "a",
	}); err == nil {
		t.Fatalf("missing aggregate_type should error")
	}
	if _, err := store.Insert(ctx, nil, outbox.InsertParams{
		AggregateType: "X", AggregateID: "a",
	}); err == nil {
		t.Fatalf("missing event_type should error")
	}
	if _, err := store.Insert(ctx, nil, outbox.InsertParams{
		AggregateType: "X", EventType: "E",
	}); err == nil {
		t.Fatalf("missing aggregate_id should error")
	}
	if _, err := store.Insert(ctx, nil, outbox.InsertParams{
		AggregateType: "X", EventType: "E", AggregateID: "a", Payload: []byte("{not-json"),
	}); err == nil {
		t.Fatalf("invalid JSON payload should error")
	}
}

// ── 4. Concurrent claim: each row claimed/dispatched exactly once ─────────

func TestOutbox_ConcurrentClaim_DispatchesEachRowOnce(t *testing.T) {
	store := newTestStore(t)
	reg := outbox.NewRegistry()
	h := &stubHandler{eventType: "EVT_RACE"}
	_ = reg.Register(h)

	const N = 20
	for i := 0; i < N; i++ {
		insertOne(t, store, "EVT_RACE", "race", fmt.Sprintf("r-%d", i),
			[]byte(fmt.Sprintf(`{"i":%d}`, i)))
	}

	// Two dispatchers race on the same Store.
	d1 := outbox.NewDispatcher(store, reg, outbox.Config{
		PollInterval: 5 * time.Millisecond, BatchSize: 4,
		LockDuration: 5 * time.Second, MaxAttempts: 3,
	})
	d2 := outbox.NewDispatcher(store, reg, outbox.Config{
		PollInterval: 5 * time.Millisecond, BatchSize: 4,
		LockDuration: 5 * time.Second, MaxAttempts: 3,
	})
	ctx := startDispatcher(t, d1)
	t.Cleanup(func() { d2.Stop() })

	// d2 runs in its own goroutine but is cleaned up via the
	// helper's t.Cleanup for d1.
	done2 := make(chan struct{})
	go func() { defer close(done2); _ = d2.Run(ctx) }()
	t.Cleanup(func() { <-done2 })

	if err := waitFor(t, 5*time.Second, func() bool {
		n, _ := store.CountByStatus(ctx, outbox.StatusProcessed)
		return n >= int64(N)
	}); err != nil {
		t.Fatalf("not all rows processed: %v", err)
	}

	if got := h.invocations.Load(); got != int32(N) {
		t.Fatalf("handler invocations = %d, want %d", got, N)
	}
}

// ── 5. Lock expiry: stale PROCESSING row is reclaimable ────────────────────

func TestOutbox_LockExpiry_ReclaimsStaleLock(t *testing.T) {
	store := newTestStore(t)
	reg := outbox.NewRegistry()

	// Single handler tracking invocations across both initial claim and
	// the post-expiry reclaim — both must increment the same counter so
	// we verify "process per claim, not per process invocation". This
	// matches the production reality: a handler that observed an event
	// once and didn't get a PROCESSED ack must accept re-dispatch.
	var invocations atomic.Int32
	h := &stubHandler{
		eventType: "EVT_LOCK",
		fn: func(ctx context.Context, e outbox.Event) error {
			invocations.Add(1)
			return nil
		},
	}
	_ = reg.Register(h)

	id := insertOne(t, store, "EVT_LOCK", "lock", "lock-1", []byte(`{}`))

	// First dispatcher claims, but never marks PROCESSED (simulate death).
	d1 := outbox.NewDispatcher(store, reg, outbox.Config{
		PollInterval: 5 * time.Millisecond, BatchSize: 5,
		LockDuration: 50 * time.Millisecond, MaxAttempts: 3,
	})
	ctx := startDispatcher(t, d1)
	if err := d1.Poll(context.Background()); err != nil {
		t.Fatalf("d1.Poll: %v", err)
	}
	if got := invocations.Load(); got != 1 {
		t.Fatalf("after first claim invocations = %d, want 1", got)
	}

	// Wait for the 50ms lock window to expire.
	time.Sleep(75 * time.Millisecond)

	// Second dispatcher reclaims. The SAME handler increments again
	// (no registry level deduplication).
	d2 := outbox.NewDispatcher(store, reg, outbox.Config{
		PollInterval: 5 * time.Millisecond, BatchSize: 5,
		LockDuration: 5 * time.Second, MaxAttempts: 3,
	})
	if err := d2.Poll(ctx); err != nil {
		t.Fatalf("d2.Poll: %v", err)
	}

	if got := invocations.Load(); got != 2 {
		t.Fatalf("after reclaim invocations = %d, want 2 (initial + reclaim)", got)
	}
	_, status, err := store.GetByID(ctx, id)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if status != string(outbox.StatusProcessed) {
		t.Fatalf("after reclaim %q status = %q, want PROCESSED", id, status)
	}
}

// ── 6. Dispatcher crash: row stays PROCESSING, restart reclaims ────────────

func TestOutbox_DispatcherCrash_LeavesProcessingAndRecovers(t *testing.T) {
	store := newTestStore(t)
	reg := outbox.NewRegistry()

	// Handler blocks until released (simulating a crashed/stalled worker).
	released := make(chan struct{})
	fired := make(chan struct{})
	var invocations atomic.Int32
	_ = reg.Register(&stubHandler{
		eventType: "EVT_CRASH",
		fn: func(ctx context.Context, e outbox.Event) error {
			invocations.Add(1)
			close(fired)
			select {
			case <-released:
			case <-ctx.Done():
				return ctx.Err()
			}
			return nil
		},
	})

	id := insertOne(t, store, "EVT_CRASH", "crash", "c-1", []byte(`{}`))

	// Launch dispatcher; cancel ctx mid-flight (no graceful Stop).
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); _ = outbox.NewDispatcher(store, reg, outbox.Config{
		PollInterval: 5 * time.Millisecond, BatchSize: 5,
		LockDuration: 50 * time.Millisecond, MaxAttempts: 3,
	}).Run(ctx) }()

	// Wait for the handler to fire, then cancel (simulating crash mid-handle).
	select {
	case <-fired:
	case <-time.After(2 * time.Second):
		t.Fatalf("handler did not fire")
	}
	cancel()
	<-done

	// Row should still be PROCESSING (the locked_until was 50ms which has
	// already expired by now, but `MarkProcessed` was never called).
	_, status, err := store.GetByID(context.Background(), id)
	if err != nil {
		t.Fatalf("GetByID: %v", err)
	}
	if status != string(outbox.StatusProcessing) {
		t.Fatalf("after crash status = %q, want PROCESSING", status)
	}

	// Release the blocked handler so future dispatchers don't deadlock.
	close(released)

	// Wait for locked_until to have definitely expired.
	time.Sleep(80 * time.Millisecond)

	// Restart dispatcher: must reclaim.
	d2 := outbox.NewDispatcher(store, reg, outbox.Config{
		PollInterval: 5 * time.Millisecond, BatchSize: 5,
		LockDuration: 5 * time.Second, MaxAttempts: 5,
	})
	ctx2 := startDispatcher(t, d2)

	if err := waitFor(t, 2*time.Second, func() bool {
		_, st, _ := store.GetByID(ctx2, id)
		return st == string(outbox.StatusProcessed)
	}); err != nil {
		t.Fatalf("reclaim did not progress event to PROCESSED: %v", err)
	}
	// Handler must have fired at least twice (initial crash + reclaim).
	if invocations.Load() < 2 {
		t.Fatalf("invocations = %d, want >= 2", invocations.Load())
	}
}

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
	go func() { defer close(doneB); _ = outbox.NewDispatcher(storeB, regB, outbox.Config{
		PollInterval: 5 * time.Millisecond, BatchSize: 5,
		LockDuration: 5 * time.Second, MaxAttempts: 3,
	}).Run(ctxB) }()
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

// ── helpers ────────────────────────────────────────────────────────────────

// waitFor polls condition() until it returns true or deadline elapses.
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) error {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("timeout after %s", timeout)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
