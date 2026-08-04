package spool

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestTaskResultOutbox_UpsertIsIdempotentAndPersistsAcrossReopen(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spool.db")
	store, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	ctx := context.Background()
	payload := []byte("task-result-payload")
	if err := store.UpsertTaskResult(ctx, "task-1", "attempt-1", "hash-1", payload); err != nil {
		t.Fatalf("first upsert: %v", err)
	}
	if err := store.MarkTaskResultAttempt(ctx, "task-1", "attempt-1", "hash-1", time.Now().Add(-time.Second)); err != nil {
		t.Fatalf("mark attempt: %v", err)
	}
	if err := store.UpsertTaskResult(ctx, "task-1", "attempt-1", "hash-1", []byte("different")); err != nil {
		t.Fatalf("duplicate upsert: %v", err)
	}
	count, err := store.PendingTaskResultCount(ctx)
	if err != nil || count != 1 {
		t.Fatalf("count after duplicate = %d, err=%v; want 1", count, err)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	store, err = Open(path)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer store.Close()
	entries, err := store.ListDueTaskResults(ctx, time.Now(), 10)
	if err != nil {
		t.Fatalf("list due: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("due entries = %d, want 1", len(entries))
	}
	if string(entries[0].Payload) != string(payload) || entries[0].AttemptCount != 1 {
		t.Fatalf("reopened entry = payload %q attempt_count %d", entries[0].Payload, entries[0].AttemptCount)
	}
}

func TestTaskResultOutbox_ClaimIsCompareAndSet(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.UpsertTaskResult(ctx, "task-claim", "attempt-claim", "hash-claim", []byte("payload")); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	next := time.Now().Add(time.Hour)
	claimed, err := store.ClaimTaskResultAttempt(ctx, "task-claim", "attempt-claim", "hash-claim", 0, time.Now(), next)
	if err != nil || !claimed {
		t.Fatalf("first claim = %v, err=%v; want true", claimed, err)
	}
	claimed, err = store.ClaimTaskResultAttempt(ctx, "task-claim", "attempt-claim", "hash-claim", 0, time.Now(), next)
	if err != nil {
		t.Fatalf("second claim: %v", err)
	}
	if claimed {
		t.Fatal("second claim succeeded with stale expected attempt count")
	}
	entries, err := store.ListDueTaskResults(ctx, time.Now(), 10)
	if err != nil {
		t.Fatalf("list due: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("claimed entry unexpectedly due with attempt_count=%d", entries[0].AttemptCount)
	}
	all, err := store.ListTaskResultsForAttempt(ctx, "task-claim", "attempt-claim")
	if err != nil || len(all) != 1 || all[0].AttemptCount != 1 {
		t.Fatalf("claimed entry = %#v, err=%v; want attempt_count=1", all, err)
	}
}

func TestTaskResultOutbox_ConcurrentClaimOnlyOneWins(t *testing.T) {
	store, err := Open(filepath.Join(t.TempDir(), "concurrent.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.UpsertTaskResult(ctx, "task-concurrent", "attempt-concurrent", "hash-concurrent", []byte("payload")); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	results := make(chan bool, 2)
	for i := 0; i < 2; i++ {
		go func() {
			claimed, claimErr := store.ClaimTaskResultAttempt(ctx, "task-concurrent", "attempt-concurrent", "hash-concurrent", 0, time.Now(), time.Now().Add(time.Hour))
			if claimErr != nil {
				t.Errorf("claim: %v", claimErr)
			}
			results <- claimed
		}()
	}
	wins := 0
	for i := 0; i < 2; i++ {
		if <-results {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("concurrent claim wins = %d, want 1", wins)
	}
}

func TestTaskResultOutbox_DueSchedulingAndAckDeletion(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	if err := store.UpsertTaskResult(ctx, "task-2", "attempt-2", "hash-2", []byte("payload")); err != nil {
		t.Fatalf("upsert: %v", err)
	}
	entries, err := store.ListDueTaskResults(ctx, time.Now(), 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("initial due entries = %d, err=%v; want 1", len(entries), err)
	}
	future := time.Now().Add(time.Hour)
	if err := store.MarkTaskResultAttempt(ctx, "task-2", "attempt-2", "hash-2", future); err != nil {
		t.Fatalf("mark future attempt: %v", err)
	}
	entries, err = store.ListDueTaskResults(ctx, time.Now(), 10)
	if err != nil {
		t.Fatalf("list before retry: %v", err)
	}
	if len(entries) != 0 {
		t.Fatalf("entries before retry = %d, want 0", len(entries))
	}
	deleted, err := store.DeleteTaskResultsForAttempt(ctx, "task-2", "attempt-2")
	if err != nil || !deleted {
		t.Fatalf("ACK delete: deleted=%v err=%v; want true", deleted, err)
	}
	count, err := store.PendingTaskResultCount(ctx)
	if err != nil || count != 0 {
		t.Fatalf("count after ACK = %d, err=%v; want 0", count, err)
	}
}

func TestTaskResultOutbox_DistinguishesReportHashes(t *testing.T) {
	store, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer store.Close()
	ctx := context.Background()
	for _, hash := range []string{"hash-a", "hash-b"} {
		if err := store.UpsertTaskResult(ctx, "task-3", "attempt-3", hash, []byte(hash)); err != nil {
			t.Fatalf("upsert %s: %v", hash, err)
		}
	}
	count, err := store.PendingTaskResultCount(ctx)
	if err != nil || count != 2 {
		t.Fatalf("count with two hashes = %d, err=%v; want 2", count, err)
	}
}
