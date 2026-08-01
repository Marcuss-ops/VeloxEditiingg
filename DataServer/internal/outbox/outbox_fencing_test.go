package outbox_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"velox-server/internal/outbox"
)

func claimForTakeover(t *testing.T, store *outbox.Store, eventID string) (outbox.Event, outbox.Event) {
	t.Helper()
	first, err := store.Claim(context.Background(), "dispatcher-a", time.Now().Add(10*time.Millisecond), 1)
	if err != nil || len(first) != 1 || first[0].EventID != eventID {
		t.Fatalf("initial Claim = events=%v err=%v", first, err)
	}
	if first[0].FenceToken == "" {
		t.Fatal("initial claim returned an empty fencing token")
	}

	time.Sleep(25 * time.Millisecond)
	second, err := store.Claim(context.Background(), "dispatcher-b", time.Now().Add(time.Minute), 1)
	if err != nil || len(second) != 1 || second[0].EventID != eventID {
		t.Fatalf("takeover Claim = events=%v err=%v", second, err)
	}
	if second[0].FenceToken == "" || second[0].FenceToken == first[0].FenceToken {
		t.Fatalf("takeover token = %q, initial token = %q; want a fresh token", second[0].FenceToken, first[0].FenceToken)
	}
	return first[0], second[0]
}

func TestOutbox_Fencing_StaleHandlerCannotMarkProcessedAfterTakeover(t *testing.T) {
	store := newTestStore(t)
	id := insertOne(t, store, "EVT_FENCE_PROCESSED", "fence", "processed", []byte(`{}`))
	stale, current := claimForTakeover(t, store, id)

	if err := store.MarkProcessed(context.Background(), id, stale.FenceToken); !errors.Is(err, outbox.ErrLeaseLost) {
		t.Fatalf("stale MarkProcessed error = %v, want ErrLeaseLost", err)
	}
	_, status, err := store.GetByID(context.Background(), id)
	if err != nil {
		t.Fatalf("GetByID after stale MarkProcessed: %v", err)
	}
	if status != string(outbox.StatusProcessing) {
		t.Fatalf("status after stale MarkProcessed = %q, want PROCESSING", status)
	}
	if err := store.MarkProcessed(context.Background(), id, current.FenceToken); err != nil {
		t.Fatalf("current MarkProcessed: %v", err)
	}
}

func TestOutbox_Fencing_StaleHandlerCannotMarkFailedAfterTakeover(t *testing.T) {
	store := newTestStore(t)
	id := insertOne(t, store, "EVT_FENCE_FAILED", "fence", "failed", []byte(`{}`))
	stale, current := claimForTakeover(t, store, id)

	if err := store.MarkFailed(context.Background(), id, stale.FenceToken, "stale failure"); !errors.Is(err, outbox.ErrLeaseLost) {
		t.Fatalf("stale MarkFailed error = %v, want ErrLeaseLost", err)
	}
	_, status, err := store.GetByID(context.Background(), id)
	if err != nil {
		t.Fatalf("GetByID after stale MarkFailed: %v", err)
	}
	if status != string(outbox.StatusProcessing) {
		t.Fatalf("status after stale MarkFailed = %q, want PROCESSING", status)
	}
	if err := store.MarkFailed(context.Background(), id, current.FenceToken, "current failure"); err != nil {
		t.Fatalf("current MarkFailed: %v", err)
	}
}

func TestOutbox_Fencing_StaleHandlerCannotExtendLockAfterTakeover(t *testing.T) {
	store := newTestStore(t)
	id := insertOne(t, store, "EVT_FENCE_EXTEND", "fence", "extend", []byte(`{}`))
	stale, current := claimForTakeover(t, store, id)

	if err := store.ExtendLock(context.Background(), id, stale.FenceToken, time.Now().Add(time.Hour), "stale extension"); !errors.Is(err, outbox.ErrLeaseLost) {
		t.Fatalf("stale ExtendLock error = %v, want ErrLeaseLost", err)
	}
	_, status, err := store.GetByID(context.Background(), id)
	if err != nil {
		t.Fatalf("GetByID after stale ExtendLock: %v", err)
	}
	if status != string(outbox.StatusProcessing) {
		t.Fatalf("status after stale ExtendLock = %q, want PROCESSING", status)
	}
	if err := store.MarkProcessed(context.Background(), id, current.FenceToken); err != nil {
		t.Fatalf("current MarkProcessed after stale ExtendLock: %v", err)
	}
}

func TestOutbox_Fencing_ExpiredCurrentTokenCannotWriteBeforeTakeover(t *testing.T) {
	store := newTestStore(t)
	id := insertOne(t, store, "EVT_FENCE_EXPIRED", "fence", "expired", []byte(`{}`))
	claimed, err := store.Claim(context.Background(), "dispatcher-a", time.Now().Add(10*time.Millisecond), 1)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("Claim = events=%v err=%v", claimed, err)
	}
	time.Sleep(25 * time.Millisecond)

	if err := store.MarkProcessed(context.Background(), id, claimed[0].FenceToken); !errors.Is(err, outbox.ErrLeaseLost) {
		t.Fatalf("expired MarkProcessed error = %v, want ErrLeaseLost", err)
	}
	_, status, err := store.GetByID(context.Background(), id)
	if err != nil {
		t.Fatalf("GetByID after expired write: %v", err)
	}
	if status != string(outbox.StatusProcessing) {
		t.Fatalf("status after expired write = %q, want PROCESSING", status)
	}
}
