// Tests for Store.Ensure — the single idempotent registration surface
// for the worker_output_spool publisher.
//
// The UNIQUE(task_id, attempt_id, worker_spool_key) tuple means a worker
// that re-registers the same logical output must converge on the existing
// row instead of failing the whole publication with ErrDuplicateSpool.
// Ensure owns that logic ONCE; callers must not catch ErrDuplicateSpool
// and branch on it.
//
// Coverage targets (the 6 mandatory cases):
//   - new output          → INSERT, created=true
//   - duplicate RENDERING → existing row, created=false
//   - duplicate OUTPUT_READY → existing row, created=false
//   - duplicate UPLOADING → existing row, created=false
//   - after restart       → existing row survives reopen, created=false
//   - incompatible identity → *ErrIncompatibleSpool
package spool

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"
)

// ensureEntry builds a content-carrying registration for the identity
// tuple. sha must be 64 hex chars (MarkReady enforces this); tests use
// strings.Repeat to build it.
func ensureEntry(taskID, attemptID, key, sha string, size int64) SpoolEntry {
	return SpoolEntry{
		TaskID:         taskID,
		AttemptID:      attemptID,
		WorkerSpoolKey: key,
		LocalPath:      "/var/scratch/" + key + ".mp4",
		SHA256:         sha,
		SizeBytes:      size,
		Status:         StatusRendering,
	}
}

func sha64(seed rune) string { return strings.Repeat(string(seed), 64) }

// TestEnsure_NewOutputCreatesRow: a fresh identity inserts a row and
// reports created=true.
func TestEnsure_NewOutputCreatesRow(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()

	entry, created, err := s.Ensure(ctx, ensureEntry("T-new", "A-new", "K-new", sha64('a'), 100))
	if err != nil {
		t.Fatalf("Ensure(new) → %v", err)
	}
	if !created {
		t.Fatal("Ensure(new): want created=true")
	}
	if entry == nil || entry.SpoolID == "" {
		t.Fatal("Ensure(new): want non-empty SpoolID")
	}
	if entry.Status != StatusRendering {
		t.Errorf("Ensure(new): status = %q; want RENDERING", entry.Status)
	}
	// Exactly one row persists.
	rows, err := s.ListByAttempt(ctx, "T-new", "A-new")
	if err != nil {
		t.Fatalf("ListByAttempt → %v", err)
	}
	if len(rows) != 1 {
		t.Fatalf("Ensure(new): want 1 row, got %d", len(rows))
	}
}

// TestEnsure_DuplicateRenderingReturnsExisting: a second registration of
// the same identity while the row is still RENDERING returns the existing
// row with created=false. Two subcases: the row already carries content
// (the publisher passes sha/size at registration) and the row was created
// without content (bare Insert, MarkReady never ran — the caller's
// MarkReady completes it).
func TestEnsure_DuplicateRenderingReturnsExisting(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()

	t.Run("rendering with content", func(t *testing.T) {
		first, created, err := s.Ensure(ctx, ensureEntry("T-rend", "A-rend", "K-rend", sha64('b'), 200))
		if err != nil {
			t.Fatalf("Ensure(first) → %v", err)
		}
		if !created {
			t.Fatal("Ensure(first): want created=true")
		}

		again, created, err := s.Ensure(ctx, ensureEntry("T-rend", "A-rend", "K-rend", sha64('b'), 200))
		if err != nil {
			t.Fatalf("Ensure(second) → %v", err)
		}
		if created {
			t.Fatal("Ensure(second): want created=false")
		}
		if again == nil || again.SpoolID != first.SpoolID {
			t.Fatalf("Ensure(second): want existing SpoolID %s, got %v", first.SpoolID, again)
		}
		if again.Status != StatusRendering {
			t.Errorf("Ensure(second): status = %q; want RENDERING", again.Status)
		}
	})

	t.Run("rendering never finalized", func(t *testing.T) {
		// Bare Insert leaves sha256/size empty (MarkReady never ran).
		bare, err := s.Insert(ctx, SpoolEntry{
			TaskID: "T-bare", AttemptID: "A-bare", WorkerSpoolKey: "K-bare",
		})
		if err != nil {
			t.Fatalf("Insert(bare) → %v", err)
		}
		if bare.SHA256 != "" || bare.SizeBytes != 0 {
			t.Fatalf("bare row should have empty content, got sha=%q size=%d", bare.SHA256, bare.SizeBytes)
		}

		again, created, err := s.Ensure(ctx, ensureEntry("T-bare", "A-bare", "K-bare", sha64('c'), 300))
		if err != nil {
			t.Fatalf("Ensure(after bare insert) → %v", err)
		}
		if created {
			t.Fatal("Ensure(after bare insert): want created=false (row already exists)")
		}
		if again == nil || again.SpoolID != bare.SpoolID {
			t.Fatalf("Ensure(after bare insert): want existing SpoolID %s, got %v", bare.SpoolID, again)
		}
		// The caller's MarkReady will stamp content on the returned row.
		if err := s.MarkReady(ctx, again.SpoolID, sha64('c'), 300); err != nil {
			t.Fatalf("MarkReady on returned row → %v", err)
		}
	})
}

// TestEnsure_DuplicateOutputReadyReturnsExisting: a second registration
// while the row is already OUTPUT_READY returns the existing row with
// created=false (the resume case: the artifact was already finalized).
func TestEnsure_DuplicateOutputReadyReturnsExisting(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()

	entry, created, err := s.Ensure(ctx, ensureEntry("T-ready", "A-ready", "K-ready", sha64('d'), 400))
	if err != nil {
		t.Fatalf("Ensure(first) → %v", err)
	}
	if err := s.MarkReady(ctx, entry.SpoolID, sha64('d'), 400); err != nil {
		t.Fatalf("MarkReady → %v", err)
	}

	again, created, err := s.Ensure(ctx, ensureEntry("T-ready", "A-ready", "K-ready", sha64('d'), 400))
	if err != nil {
		t.Fatalf("Ensure(second) → %v", err)
	}
	if created {
		t.Fatal("Ensure(second): want created=false")
	}
	if again == nil || again.SpoolID != entry.SpoolID {
		t.Fatalf("Ensure(second): want existing SpoolID %s, got %v", entry.SpoolID, again)
	}
	if again.Status != StatusOutputReady {
		t.Errorf("Ensure(second): status = %q; want OUTPUT_READY", again.Status)
	}
	if again.SHA256 != sha64('d') || again.SizeBytes != 400 {
		t.Errorf("Ensure(second): content not preserved, sha=%q size=%d", again.SHA256, again.SizeBytes)
	}
}

// TestEnsure_DuplicateUploadingReturnsExisting: a second registration
// while the row is mid-upload (UPLOADING) returns the existing row with
// created=false and leaves the row's progress untouched.
func TestEnsure_DuplicateUploadingReturnsExisting(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()

	entry, created, err := s.Ensure(ctx, ensureEntry("T-up", "A-up", "K-up", sha64('e'), 500))
	if err != nil {
		t.Fatalf("Ensure(first) → %v", err)
	}
	if err := s.MarkReady(ctx, entry.SpoolID, sha64('e'), 500); err != nil {
		t.Fatalf("MarkReady → %v", err)
	}
	if err := s.MarkUploadPending(ctx, entry.SpoolID, "up-ensure"); err != nil {
		t.Fatalf("MarkUploadPending → %v", err)
	}
	if err := s.MarkUploading(ctx, entry.SpoolID, 123); err != nil {
		t.Fatalf("MarkUploading → %v", err)
	}

	again, created, err := s.Ensure(ctx, ensureEntry("T-up", "A-up", "K-up", sha64('e'), 500))
	if err != nil {
		t.Fatalf("Ensure(second) → %v", err)
	}
	if created {
		t.Fatal("Ensure(second): want created=false")
	}
	if again == nil || again.SpoolID != entry.SpoolID {
		t.Fatalf("Ensure(second): want existing SpoolID %s, got %v", entry.SpoolID, again)
	}
	if again.Status != StatusUploading {
		t.Errorf("Ensure(second): status = %q; want UPLOADING", again.Status)
	}
	if again.UploadedBytes != 123 {
		t.Errorf("Ensure(second): uploaded_bytes = %d; want 123 (progress untouched)", again.UploadedBytes)
	}
}

// TestEnsure_AfterRestartReturnsExisting: the idempotent registration
// survives a store reopen — a row persisted before restart is returned
// (created=false) instead of being duplicated.
func TestEnsure_AfterRestartReturnsExisting(t *testing.T) {
	path := filepath.Join(t.TempDir(), "spool-ensure.db")
	ctx := context.Background()

	s1, err := Open(path)
	if err != nil {
		t.Fatalf("Open(first) → %v", err)
	}
	entry, created, err := s1.Ensure(ctx, ensureEntry("T-restart", "A-restart", "K-restart", sha64('f'), 600))
	if err != nil {
		t.Fatalf("Ensure(first) → %v", err)
	}
	if !created {
		t.Fatal("Ensure(first): want created=true")
	}
	if err := s1.MarkReady(ctx, entry.SpoolID, sha64('f'), 600); err != nil {
		t.Fatalf("MarkReady → %v", err)
	}
	spoolIDBefore := entry.SpoolID
	if err := s1.Close(); err != nil {
		t.Fatalf("Close(first) → %v", err)
	}

	s2, err := Open(path)
	if err != nil {
		t.Fatalf("Open(second) → %v", err)
	}
	defer s2.Close()
	again, created, err := s2.Ensure(ctx, ensureEntry("T-restart", "A-restart", "K-restart", sha64('f'), 600))
	if err != nil {
		t.Fatalf("Ensure(after restart) → %v", err)
	}
	if created {
		t.Fatal("Ensure(after restart): want created=false (row persisted)")
	}
	if again == nil || again.SpoolID != spoolIDBefore {
		t.Fatalf("Ensure(after restart): want persisted SpoolID %s, got %v", spoolIDBefore, again)
	}
	if again.Status != StatusOutputReady {
		t.Errorf("Ensure(after restart): status = %q; want OUTPUT_READY", again.Status)
	}
}

// TestEnsure_IncompatibleIdentityFails: the same identity tuple with
// DIFFERENT content (different sha256 / size) is a real conflict, not a
// benign retry — Ensure must reject it with ErrIncompatibleSpool.
func TestEnsure_IncompatibleIdentityFails(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()

	entry, created, err := s.Ensure(ctx, ensureEntry("T-conf", "A-conf", "K-conf", sha64('g'), 700))
	if err != nil {
		t.Fatalf("Ensure(first) → %v", err)
	}
	if !created {
		t.Fatal("Ensure(first): want created=true")
	}
	if err := s.MarkReady(ctx, entry.SpoolID, sha64('g'), 700); err != nil {
		t.Fatalf("MarkReady → %v", err)
	}

	// Same identity, different bytes → incompatible.
	_, _, err = s.Ensure(ctx, ensureEntry("T-conf", "A-conf", "K-conf", sha64('h'), 800))
	if err == nil {
		t.Fatal("Ensure(incompatible): want error, got nil")
	}
	if !errors.Is(err, ErrIncompatibleSpool) {
		t.Fatalf("Ensure(incompatible): err = %v; want ErrIncompatibleSpool", err)
	}
	// Same identity, same sha but different size → also incompatible.
	_, _, err = s.Ensure(ctx, ensureEntry("T-conf", "A-conf", "K-conf", sha64('g'), 999))
	if err == nil || !errors.Is(err, ErrIncompatibleSpool) {
		t.Fatalf("Ensure(size mismatch): err = %v; want ErrIncompatibleSpool", err)
	}
	// The original row is untouched.
	got, err := s.Get(ctx, entry.SpoolID)
	if err != nil {
		t.Fatalf("Get → %v", err)
	}
	if got.SHA256 != sha64('g') || got.SizeBytes != 700 {
		t.Errorf("original row content changed: sha=%q size=%d", got.SHA256, got.SizeBytes)
	}
}
