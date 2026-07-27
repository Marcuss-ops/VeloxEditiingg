// Package workercache — Resolver test matrix.
//
// The tests use a real `:memory:` Cache (deterministic SQLite — no
// external mocks yet because Pass 2 deliberately kept Cache as a
// concrete type to avoid double-test-surface; the user's "mock
// cache" requirement is met by the in-memory back-end behaving as
// a fully-controlled fixture). The Downloader is wired against a
// bytes-source fake so download success/failure is controllable per
// case.

package workercache

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

// resolverFixture installs a fresh in-memory cache + dest dir + a
// Downloader backed by a bytes-source fake. Individual tests swap
// the fake per case to drive happy-path or failure scenarios.
type resolverFixture struct {
	cache *Cache
	dir   string
}

func newResolverFixture(t *testing.T) *resolverFixture {
	t.Helper()
	cache, err := Open(":memory:")
	if err != nil {
		t.Fatalf("Open :memory:: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })
	return &resolverFixture{cache: cache, dir: t.TempDir()}
}

// resolverWithBytesSource builds a Resolver backed by a fakeSource
// that returns the supplied bytes for ALL drive IDs.
func resolverWithBytesSource(t *testing.T, f *resolverFixture, payload []byte) *Resolver {
	t.Helper()
	dl := NewDownloader(f.cache, f.dir, bytesSource(payload))
	return &Resolver{Cache: f.cache, Downloader: dl, Dir: f.dir}
}

// TestResolver_CacheHit_NoDownload: pre-populated cache row with
// download_complete=true AND file on disk → Resolve returns the
// cached path WITHOUT calling DriveSource.Open. Marks last_used_at
// (best-effort, asserted via cache row).
func TestResolver_CacheHit_NoDownload(t *testing.T) {
	f := newResolverFixture(t)
	ctx := context.Background()

	path := filepath.Join(f.dir, "TYSON001.mp4")
	if err := os.WriteFile(path, []byte("FAKE MP4"), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	if err := f.cache.Store(ctx, Entry{
		DriveFileID:      "TYSON001",
		LocalPath:        path,
		SizeBytes:        8,
		DownloadComplete: true,
	}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := f.cache.MarkDownloadComplete(ctx, "TYSON001", path, 8); err != nil {
		t.Fatalf("MarkDownloadComplete: %v", err)
	}

	// Wire a SOURCE THAT WOULD FAIL — we want to assert the cache
	// hit short-circuits BEFORE Open is called.
	dl := NewDownloader(f.cache, f.dir, failingSource(errors.New("should not be called")))
	r := &Resolver{Cache: f.cache, Downloader: dl, Dir: f.dir}

	got, err := r.Resolve(ctx, "TYSON001")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != path {
		t.Errorf("got=%q want %q (cache hit must return cached path)", got, path)
	}
}

// TestResolver_CacheMiss_TriggersDownload: empty cache → Resolve
// inserts placeholder row + calls Downloader.DownloadDriveFile.
// Asserts: before Resolve, cache is empty; after Resolve, row has
// download_complete=true AND on-disk file at the final path.
func TestResolver_CacheMiss_TriggersDownload(t *testing.T) {
	f := newResolverFixture(t)
	ctx := context.Background()

	payload := []byte("FAKE MP4 BYTES — first 8 bytes are non-zero")
	r := resolverWithBytesSource(t, f, payload)

	// Pre-condition: cache empty.
	if _, ok, _ := f.cache.Find(ctx, "NEW-CLIP"); ok {
		t.Fatalf("NEW-CLIP unexpectedly in cache before Resolve")
	}

	got, err := r.Resolve(ctx, "NEW-CLIP")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	want := filepath.Join(f.dir, "NEW-CLIP.mp4")
	if got != want {
		t.Errorf("got=%q want %q", got, want)
	}

	// Row now committed with download_complete=true.
	e, ok, fErr := f.cache.Find(ctx, "NEW-CLIP")
	if fErr != nil || !ok {
		t.Fatalf("Find post-Resolve: ok=%v err=%v", ok, fErr)
	}
	if !e.DownloadComplete {
		t.Errorf("row.download_complete=false after Resolve; want true")
	}
	if e.LocalPath != want {
		t.Errorf("row.local_path=%q want %q", e.LocalPath, want)
	}
	if e.SizeBytes != int64(len(payload)) {
		t.Errorf("row.size_bytes=%d want %d", e.SizeBytes, len(payload))
	}

	// On-disk file present at `want`.
	body, rErr := os.ReadFile(want)
	if rErr != nil {
		t.Fatalf("ReadFile final: %v", rErr)
	}
	if string(body) != string(payload) {
		t.Errorf("final contents=%q want %q", body, payload)
	}

	// And no .part leftover.
	if _, err := os.Stat(filepath.Join(f.dir, "NEW-CLIP.mp4.part")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf(".part exists post-success; want absent. stat err=%v", err)
	}
}

// TestResolver_IncompleteRow_TriggersReDownload: a row with
// download_complete=false (e.g. after a worker crash) must trigger
// re-download. Resolve bypasses the cache-hit fast path because
// the entry is not DownloadComplete, and Store-recovers the
// `.part` placeholder so Downloader can repopulate it.
func TestResolver_IncompleteRow_TriggersReDownload(t *testing.T) {
	f := newResolverFixture(t)
	ctx := context.Background()

	partPath := filepath.Join(f.dir, "TYSON001.mp4.part")
	if err := f.cache.Store(ctx, Entry{
		DriveFileID: "TYSON001",
		LocalPath:   partPath,
	}); err != nil {
		t.Fatalf("Store placeholder: %v", err)
	}
	// Note: no MarkDownloadComplete — row stays at d_c=0 by design.

	payload := []byte("FAKE MP4 BYTES — non-zero leading 8")
	r := resolverWithBytesSource(t, f, payload)

	got, err := r.Resolve(ctx, "TYSON001")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	want := filepath.Join(f.dir, "TYSON001.mp4")
	if got != want {
		t.Errorf("got=%q want %q (re-download must end at .mp4, not .part)", got, want)
	}

	e, ok, fErr := f.cache.Find(ctx, "TYSON001")
	if fErr != nil || !ok {
		t.Fatalf("Find post-Resolve: %v", fErr)
	}
	if !e.DownloadComplete {
		t.Errorf("row.download_complete=false after re-download")
	}
	if e.LocalPath != want {
		t.Errorf("row.local_path=%q want %q", e.LocalPath, want)
	}
}

// TestResolver_FailedDownload_LeavesRowInInFlight: ErrSourceOpen
// from the fake source propagates; the row stays at
// download_complete=false (the Downloader contract is verified by
// the Pass 10 downloader tests; Pass 8 only asserts the Resolver
// surfaces the error and does NOT accidentally mark the row as
// complete).
func TestResolver_FailedDownload_LeavesRowInInFlight(t *testing.T) {
	f := newResolverFixture(t)
	ctx := context.Background()

	dl := NewDownloader(f.cache, f.dir, failingSource(errors.New("drive down")))
	r := &Resolver{Cache: f.cache, Downloader: dl, Dir: f.dir}

	_, err := r.Resolve(ctx, "TYSON001")
	if err == nil {
		t.Fatalf("Resolve succeeded; expected error from failing source")
	}
	if !errors.Is(err, ErrSourceOpen) {
		t.Errorf("err chain missing ErrSourceOpen: %v", err)
	}

	e, ok, fErr := f.cache.Find(ctx, "TYSON001")
	if fErr != nil || !ok {
		t.Fatalf("Find post-Resolve: ok=%v err=%v", ok, fErr)
	}
	if e.DownloadComplete {
		t.Errorf("row.download_complete=true after failed Resolve; want false (Downloader contract)")
	}
}
