// Package outbox_test exercises the outbox contract across focused test files.
//
// This file owns shared fixtures plus registry, insert-validation and concurrent
// claim coverage. Recovery and terminal-semantics scenarios live in
// outbox_recovery_test.go and outbox_semantics_test.go.
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

// dsnCounter increments every time newTestDB opens a connection so each
// caller gets a freshly-keyed in-memory SQLite DB. Without it, calling
// newTestStore twice from the SAME test (e.g. a test that exercises two
// independent workflows via separate stores) opens two *sql.DB handles
// on the SAME `file:t.Name()?mode=memory&cache=shared` DSN — and with
// cache=shared, both hits resolve to the SAME shared-memory database.
// A lingering dispatcher goroutine from case A could then claim a row
// freshly inserted by case B (with the wrong registry, marking it
// FAILED before case B's dispatcher observes it). The counter forces a
// fresh cache key per call, keeping stores rigorously isolated.
var dsnCounter atomic.Int64

// newTestDB opens an in-memory SQLite DB unique to (t.Name(), counter)
// with the outbox_events schema mirroring migration 026. Returns
// *sql.DB; tests must call t.Cleanup (or db.Close()) when done.
func newTestDB(t *testing.T) *sql.DB {
	t.Helper()
	// Unique DSN per (test, call); shared cache so concurrent goroutines
	// in the same call see one DB, but different calls get DIFFERENT
	// databases — see dsnCounter comment above. The counter is embedded
	// in the filename portion (not as a URI fragment, which is technically
	// not part of URI identity under RFC 3986 §3.5 — a future driver
	// upgrade could drop it from the cache key).
	n := dsnCounter.Add(1)
	dsn := fmt.Sprintf("file:%s-inst%d?mode=memory&cache=shared", t.Name(), n)
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
//
// Important cleanup note: t.Cleanup runs registrations in LIFO order, so
// if we registered `<-done2` BEFORE `d2.Stop()`, the wait would run first
// and deadlock against the never-stopped d2. We register them in a single
// block to make the contract explicit and avoid LIFO ordering traps.

func TestOutbox_ConcurrentClaim_DispatchesEachRowOnce(t *testing.T) {
	store := newTestStore(t)
	reg := outbox.NewRegistry()
	h := &stubHandler{eventType: "EVT_RACE"}
	_ = reg.Register(h)

	const N = 8 // small enough to drain under SetMaxOpenConns(1) within the 2s budget
	for i := 0; i < N; i++ {
		insertOne(t, store, "EVT_RACE", "race", fmt.Sprintf("r-%d", i),
			[]byte(fmt.Sprintf(`{"i":%d}`, i)))
	}

	// Two dispatchers race on the same Store. PollInterval=10ms keeps
	// ticker pressure low under SQLite single-conn contention; batch=4
	// drains N=8 in 2 cycles per dispatcher.
	d1 := outbox.NewDispatcher(store, reg, outbox.Config{
		PollInterval: 10 * time.Millisecond, BatchSize: 4,
		LockDuration: 5 * time.Second, MaxAttempts: 3,
	})
	d2 := outbox.NewDispatcher(store, reg, outbox.Config{
		PollInterval: 10 * time.Millisecond, BatchSize: 4,
		LockDuration: 5 * time.Second, MaxAttempts: 3,
	})
	ctx := startDispatcher(t, d1)

	// d2 in its own goroutine. Register stop+wait in a SINGLE cleanup
	// block so LIFO runs them together — see the doc comment above.
	done2 := make(chan struct{})
	go func() { defer close(done2); _ = d2.Run(ctx) }()
	t.Cleanup(func() {
		d2.Stop()
		<-done2
	})

	if err := waitFor(t, 2*time.Second, func() bool {
		n, _ := store.CountByStatus(ctx, outbox.StatusProcessed)
		return n >= int64(N)
	}); err != nil {
		t.Fatalf("not all rows processed: %v", err)
	}

	if got := h.invocations.Load(); got != int32(N) {
		t.Fatalf("handler invocations = %d, want %d", got, N)
	}
}
