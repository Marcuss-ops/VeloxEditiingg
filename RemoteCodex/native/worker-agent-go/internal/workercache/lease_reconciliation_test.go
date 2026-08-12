package workercache

import (
	"context"
	"path/filepath"
	"testing"
	"time"
)

func TestLeaseReleaseReconciliation_PersistsAndIsIdempotent(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "cache.db")
	first, err := Open(path)
	if err != nil {
		t.Fatalf("open first cache: %v", err)
	}

	now := time.Date(2026, 8, 12, 10, 0, 0, 0, time.UTC)
	if err := first.EnqueueLeaseRelease(ctx, "audio-master", "job-1", now); err != nil {
		t.Fatalf("enqueue first: %v", err)
	}
	if err := first.EnqueueLeaseRelease(ctx, "audio-master", "job-1", now.Add(time.Minute)); err != nil {
		t.Fatalf("enqueue duplicate: %v", err)
	}
	count, err := first.PendingLeaseReleaseCount(ctx)
	if err != nil || count != 1 {
		t.Fatalf("pending count = %d err=%v, want 1", count, err)
	}

	entries, err := first.ListDueLeaseReleases(ctx, now, 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("due entries = %d err=%v, want 1", len(entries), err)
	}
	if entries[0].AttemptCount != 0 || entries[0].AssetKey != "audio-master" || entries[0].JobID != "job-1" {
		t.Fatalf("initial queue entry = %+v", entries[0])
	}

	next := now.Add(2 * time.Minute)
	if err := first.MarkLeaseReleaseRetry(ctx, "audio-master", "job-1", "database is locked", 1, next, now); err != nil {
		t.Fatalf("mark retry: %v", err)
	}
	if due, err := first.ListDueLeaseReleases(ctx, now.Add(time.Second), 10); err != nil || len(due) != 0 {
		t.Fatalf("retry became due too early: entries=%v err=%v", due, err)
	}
	if err := first.Close(); err != nil {
		t.Fatalf("close first cache: %v", err)
	}

	second, err := Open(path)
	if err != nil {
		t.Fatalf("reopen cache: %v", err)
	}
	defer second.Close()
	entries, err = second.ListDueLeaseReleases(ctx, next, 10)
	if err != nil || len(entries) != 1 {
		t.Fatalf("persisted due entries = %d err=%v, want 1", len(entries), err)
	}
	if entries[0].AttemptCount != 1 || entries[0].LastError != "database is locked" {
		t.Fatalf("persisted retry metadata = %+v", entries[0])
	}

	if err := second.DeleteLeaseRelease(ctx, "audio-master", "job-1"); err != nil {
		t.Fatalf("delete completed queue item: %v", err)
	}
	count, err = second.PendingLeaseReleaseCount(ctx)
	if err != nil || count != 0 {
		t.Fatalf("pending count after delete = %d err=%v, want 0", count, err)
	}
}

func TestLeaseReleaseReconciliation_MarkMissingFailsClosed(t *testing.T) {
	cache, err := Open(":memory:")
	if err != nil {
		t.Fatalf("open cache: %v", err)
	}
	defer cache.Close()
	if err := cache.MarkLeaseReleaseRetry(context.Background(), "missing", "job", "failure", 1, time.Now(), time.Now()); err == nil {
		t.Fatal("MarkLeaseReleaseRetry(missing) returned nil, want failure")
	}
}
