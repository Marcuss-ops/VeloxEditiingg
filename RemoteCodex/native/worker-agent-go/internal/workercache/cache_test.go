package workercache

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// newTestCache returns a Cache backed by a fresh temp-dir DB; the
// DB is closed via t.Cleanup. Each test gets isolated state.
func newTestCache(t *testing.T) *Cache {
	t.Helper()
	c, err := Open(filepath.Join(t.TempDir(), "cache.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func TestCache_Find_MissingReturnsFalseNoError(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	_, ok, err := c.Find(context.Background(), "missing-id")
	if err != nil {
		t.Fatalf("Find unexpected error: %v", err)
	}
	if ok {
		t.Fatal("ok = true, want false for unknown drive_file_id")
	}
}

func TestCache_Find_EmptyIDReturnsErrEmptyID(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	_, _, err := c.Find(context.Background(), "")
	if !errors.Is(err, ErrEmptyID) {
		t.Fatalf("Find(\"\") err = %v, want ErrEmptyID", err)
	}
}

func TestCache_StoreAndFind_Roundtrip(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	ctx := context.Background()

	now := time.Now().UTC().Truncate(time.Second)
	in := Entry{
		DriveFileID:      "ABC123",
		LocalPath:        "/var/lib/velox-worker/assets/ABC123.mp4",
		SizeBytes:        48392741,
		ActiveJobID:      "",
		DownloadComplete: true,
		CreatedAt:        now,
		LastUsedAt:       now,
	}
	if err := c.Store(ctx, in); err != nil {
		t.Fatalf("Store: %v", err)
	}

	got, ok, err := c.Find(ctx, "ABC123")
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if !ok {
		t.Fatal("ok = false, want true after Store")
	}
	if got.DriveFileID != in.DriveFileID ||
		got.LocalPath != in.LocalPath ||
		got.SizeBytes != in.SizeBytes ||
		got.ActiveJobID != in.ActiveJobID ||
		got.DownloadComplete != in.DownloadComplete {
		t.Fatalf("Find = %+v, want drive_file_id/local_path/size/active_job_id/download_complete to match Store inputs", got)
	}
	if !got.CreatedAt.Equal(in.CreatedAt) {
		t.Errorf("CreatedAt = %v, want %v", got.CreatedAt, in.CreatedAt)
	}
	if !got.LastUsedAt.Equal(in.LastUsedAt) {
		t.Errorf("LastUsedAt = %v, want %v", got.LastUsedAt, in.LastUsedAt)
	}
}

func TestCache_Store_DuplicateReturnsErrDuplicate(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	ctx := context.Background()
	e := Entry{DriveFileID: "DUPL", LocalPath: "/tmp/d.mp4"}
	if err := c.Store(ctx, e); err != nil {
		t.Fatalf("first Store: %v", err)
	}
	err := c.Store(ctx, e)
	if !errors.Is(err, ErrDuplicate) {
		t.Fatalf("second Store err = %v, want ErrDuplicate", err)
	}
	if !strings.Contains(err.Error(), "DUPL") {
		t.Errorf("error message should include drive_file_id, got %q", err.Error())
	}
}

func TestCache_Store_MissingLocalPathRejected(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	err := c.Store(context.Background(), Entry{DriveFileID: "NOPATH"})
	if err == nil || !strings.Contains(err.Error(), "local_path") {
		t.Fatalf("Store without local_path err = %v, want non-nil mentioning local_path", err)
	}
}

func TestCache_Store_DefaultsCreatedAndLastUsed(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	ctx := context.Background()

	before := time.Now().UTC().Add(-time.Second)
	if err := c.Store(ctx, Entry{DriveFileID: "DEFAULTS", LocalPath: "/tmp/d.mp4"}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	after := time.Now().UTC().Add(time.Second)

	got, ok, err := c.Find(ctx, "DEFAULTS")
	if err != nil || !ok {
		t.Fatalf("Find: ok=%v err=%v", ok, err)
	}
	if got.CreatedAt.Before(before) || got.CreatedAt.After(after) {
		t.Errorf("CreatedAt = %v, want within [%v..%v]", got.CreatedAt, before, after)
	}
	if got.LastUsedAt.Before(before) || got.LastUsedAt.After(after) {
		t.Errorf("LastUsedAt = %v, want within [%v..%v]", got.LastUsedAt, before, after)
	}
	if got.CreatedAt != got.LastUsedAt {
		t.Errorf("CreatedAt != LastUsedAt on fresh insert: %v vs %v", got.CreatedAt, got.LastUsedAt)
	}
}

func TestCache_MarkUsed_BumpsAndFailsOnMissing(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	ctx := context.Background()

	if err := c.Store(ctx, Entry{DriveFileID: "USE", LocalPath: "/tmp/u.mp4"}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	// Force an obsolete last_used_at so the bump is observable.
	if _, err := c.db.ExecContext(ctx,
		`UPDATE cached_assets SET last_used_at = ? WHERE drive_file_id = ?`,
		"2000-01-01T00:00:00Z", "USE"); err != nil {
		t.Fatalf("seed: %v", err)
	}

	beforeBump := time.Now().UTC().Add(-time.Second)
	if err := c.MarkUsed(ctx, "USE"); err != nil {
		t.Fatalf("MarkUsed: %v", err)
	}
	afterBump := time.Now().UTC().Add(time.Second)

	got, _, _ := c.Find(ctx, "USE")
	if got.LastUsedAt.Before(beforeBump) || got.LastUsedAt.After(afterBump) {
		t.Errorf("LastUsedAt = %v, want within [%v..%v]", got.LastUsedAt, beforeBump, afterBump)
	}
	if got.DownloadComplete != false {
		// untouched by MarkUsed
		t.Errorf("MarkUsed must not flip DownloadComplete, got %v", got.DownloadComplete)
	}

	if err := c.MarkUsed(ctx, "DOES-NOT-EXIST"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("MarkUsed on missing err = %v, want ErrNotFound", err)
	}
}

func TestCache_MarkDownloadComplete_TransitionsAndFailsOnMissing(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	ctx := context.Background()

	if err := c.Store(ctx, Entry{DriveFileID: "DL", LocalPath: "/tmp/dl.mp4.part"}); err != nil {
		t.Fatalf("Store: %v", err)
	}

	beforeBump := time.Now().UTC().Add(-time.Second)
	if err := c.MarkDownloadComplete(ctx, "DL", "/tmp/dl.mp4", 4096); err != nil {
		t.Fatalf("MarkDownloadComplete: %v", err)
	}
	afterBump := time.Now().UTC().Add(time.Second)

	got, ok, _ := c.Find(ctx, "DL")
	if !ok {
		t.Fatal("Find: missing row after MarkDownloadComplete")
	}
	if !got.DownloadComplete {
		t.Errorf("DownloadComplete = false, want true after MarkDownloadComplete")
	}
	if got.LocalPath != "/tmp/dl.mp4" {
		t.Errorf("LocalPath = %q, want /tmp/dl.mp4", got.LocalPath)
	}
	if got.SizeBytes != 4096 {
		t.Errorf("SizeBytes = %d, want 4096", got.SizeBytes)
	}
	if got.LastUsedAt.Before(beforeBump) || got.LastUsedAt.After(afterBump) {
		t.Errorf("LastUsedAt = %v, want within [%v..%v]", got.LastUsedAt, beforeBump, afterBump)
	}

	if err := c.MarkDownloadComplete(ctx, "GHOST", "/tmp/x.mp4", 1); !errors.Is(err, ErrNotFound) {
		t.Fatalf("MarkDownloadComplete on missing err = %v, want ErrNotFound", err)
	}
	if err := c.MarkDownloadComplete(ctx, "DL", "", 0); err == nil {
		t.Fatal("MarkDownloadComplete with empty localPath should error, got nil")
	}
}

func TestCache_AcquireRelease_HappyPath(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	ctx := context.Background()

	if err := c.Store(ctx, Entry{DriveFileID: "JOB", LocalPath: "/tmp/job.mp4"}); err != nil {
		t.Fatalf("Store: %v", err)
	}

	if err := c.Acquire(ctx, "JOB", "job-101"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	got, _, _ := c.Find(ctx, "JOB")
	if got.ActiveJobID != "job-101" {
		t.Fatalf("ActiveJobID = %q, want job-101", got.ActiveJobID)
	}

	if err := c.Release(ctx, "JOB", "job-101"); err != nil {
		t.Fatalf("Release: %v", err)
	}
	got, _, _ = c.Find(ctx, "JOB")
	if got.ActiveJobID != "" {
		t.Fatalf("ActiveJobID after Release = %q, want empty", got.ActiveJobID)
	}
}

func TestCache_Release_WrongJobIsNoop(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	ctx := context.Background()

	if err := c.Store(ctx, Entry{DriveFileID: "OWN", LocalPath: "/tmp/o.mp4"}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := c.Acquire(ctx, "OWN", "job-A"); err != nil {
		t.Fatalf("Acquire: %v", err)
	}

	// Release from a non-owning job should be a benign no-op:
	// the row exists, the lease was set, but the lease is still
	// owned by job-A. This protects against the JOB A acquires ->
	// JOB B acquires -> JOB A releases race.
	if err := c.Release(ctx, "OWN", "job-B"); err != nil {
		t.Fatalf("Release with mismatched jobID should NOT error, got %v", err)
	}
	got, _, _ := c.Find(ctx, "OWN")
	if got.ActiveJobID != "job-A" {
		t.Fatalf("ActiveJobID = %q, want job-A (B should NOT have cleared it)", got.ActiveJobID)
	}
}

func TestCache_MultipleLeasesProtectUntilLastRelease(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	ctx := context.Background()
	if err := c.Store(ctx, Entry{DriveFileID: "SHARED", LocalPath: "/tmp/shared.mp4"}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := c.Acquire(ctx, "SHARED", "job-A"); err != nil {
		t.Fatal(err)
	}
	if err := c.Acquire(ctx, "SHARED", "job-B"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteIfUnleased(ctx, "SHARED"); err == nil {
		t.Fatal("DeleteIfUnleased removed an asset with active leases")
	} else if !errors.Is(err, ErrNotFound) {
		t.Fatalf("DeleteIfUnleased while leased: %v", err)
	}
	if err := c.Release(ctx, "SHARED", "job-A"); err != nil {
		t.Fatal(err)
	}
	if _, ok, err := c.Find(ctx, "SHARED"); err != nil || !ok {
		t.Fatalf("asset disappeared after first release: ok=%v err=%v", ok, err)
	}
	if err := c.Release(ctx, "SHARED", "job-B"); err != nil {
		t.Fatal(err)
	}
	if err := c.DeleteIfUnleased(ctx, "SHARED"); err != nil {
		t.Fatalf("DeleteIfUnleased after last release: %v", err)
	}
}

func TestCache_Release_OnMissingRowReturnsErrNotFound(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	err := c.Release(context.Background(), "GHOST", "job-x")
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Release on missing row err = %v, want ErrNotFound", err)
	}
}

func TestCache_Acquire_EmptyIDOrEmptyJobRejected(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	ctx := context.Background()
	if err := c.Store(ctx, Entry{DriveFileID: "X", LocalPath: "/tmp/x.mp4"}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := c.Acquire(ctx, "", "job-x"); !errors.Is(err, ErrEmptyID) {
		t.Errorf("Acquire empty id err = %v, want ErrEmptyID", err)
	}
	if err := c.Acquire(ctx, "X", ""); err == nil {
		t.Error("Acquire empty jobID should error, got nil")
	}
}

func TestCache_Delete_HappyAndMissing(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	ctx := context.Background()

	if err := c.Store(ctx, Entry{DriveFileID: "RM", LocalPath: "/tmp/rm.mp4"}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := c.Delete(ctx, "RM"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	_, ok, _ := c.Find(ctx, "RM")
	if ok {
		t.Fatal("row still present after Delete")
	}
	if err := c.Delete(ctx, "RM"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("second Delete err = %v, want ErrNotFound", err)
	}
}

func TestCache_List_OrdersByIDAndHandlesEmpty(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	ctx := context.Background()

	// Empty cache → empty slice.
	list, err := c.List(ctx)
	if err != nil {
		t.Fatalf("List on empty: %v", err)
	}
	if len(list) != 0 {
		t.Errorf("List on empty = %d entries, want 0", len(list))
	}

	// Store out of order; List must return sorted by drive_file_id.
	in := []Entry{
		{DriveFileID: "ZZZ", LocalPath: "/tmp/z.mp4"},
		{DriveFileID: "aaa", LocalPath: "/tmp/a.mp4"},
		{DriveFileID: "MMM", LocalPath: "/tmp/m.mp4"},
	}
	for _, e := range in {
		if err := c.Store(ctx, e); err != nil {
			t.Fatalf("Store: %v", err)
		}
	}
	list, err = c.List(ctx)
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	wantOrder := []string{"MMM", "ZZZ", "aaa"} // Sort: 'M'(0x4D) < 'Z'(0x5A) < 'a'(0x61)
	if len(list) != len(wantOrder) {
		t.Fatalf("List length = %d, want %d", len(list), len(wantOrder))
	}
	for i, w := range wantOrder {
		if list[i].DriveFileID != w {
			t.Errorf("list[%d].DriveFileID = %q, want %q", i, list[i].DriveFileID, w)
		}
	}
}

func TestCache_Store_RejectsPreSetActiveJobID(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	err := c.Store(context.Background(), Entry{
		DriveFileID: "LEASE",
		LocalPath:   "/tmp/lease.mp4",
		ActiveJobID: "unexpected-job",
	})
	if err == nil {
		t.Fatal("Store with pre-set ActiveJobID should error, got nil")
	}
	if !strings.Contains(err.Error(), "ActiveJobID") {
		t.Errorf("error message should mention ActiveJobID, got %q", err.Error())
	}
	// And the row must not have been inserted despite the failure.
	_, ok, _ := c.Find(context.Background(), "LEASE")
	if ok {
		t.Error("Store with pre-set ActiveJobID must not leave a row behind")
	}
}

func TestCache_ConcurrentStores_SerialisedBySQLite(t *testing.T) {
	t.Parallel()
	c := newTestCache(t)
	ctx := context.Background()

	// Two goroutines racing to insert the SAME key; one must
	// succeed, one must fall through to ErrDuplicate (or both
	// succeed if a SELECT-after-INSERT retry happens — but our
	// Store is strictly INSERT, so duplicate is the only outcome).
	// We don't branch on success/failure outcome: just verify no
	// panic and at least one Store returned nil.
	gotResults := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			gotResults <- c.Store(ctx, Entry{
				DriveFileID: "RACE",
				LocalPath:   "/tmp/race.mp4",
			})
		}()
	}
	saw := map[bool]int{}
	for i := 0; i < 2; i++ {
		err := <-gotResults
		if err == nil {
			saw[true]++
		} else if errors.Is(err, ErrDuplicate) {
			saw[false]++
		} else {
			t.Errorf("unexpected error: %v", err)
		}
	}
	if saw[true] != 1 || saw[false] != 1 {
		t.Errorf("results = %v, want exactly 1 success + 1 ErrDuplicate", saw)
	}
}
