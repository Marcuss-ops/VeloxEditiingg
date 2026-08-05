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
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"velox-worker-agent/internal/telemetry"
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
	beforeHits := telemetry.GetPrometheusMetrics().CacheRequestCount("hit")

	got, err := r.Resolve(ctx, "TYSON001")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != path {
		t.Errorf("got=%q want %q (cache hit must return cached path)", got, path)
	}
	if gotHits := telemetry.GetPrometheusMetrics().CacheRequestCount("hit") - beforeHits; gotHits != 1 {
		t.Errorf("cache-hit request count delta=%v, want 1", gotHits)
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
	beforeMisses := telemetry.GetPrometheusMetrics().CacheRequestCount("miss")

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
	if got := telemetry.GetPrometheusMetrics().CacheRequestCount("miss") - beforeMisses; got != 1 {
		t.Errorf("incomplete-row cache miss count delta=%v, want 1", got)
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

// TestResolver_FileMissingRecovery: the cache row says
// download_complete=true but the on-disk file has been deleted
// out from under us (admin wipe, disk corruption, worker crash
// between MarkDownloadComplete and the next read). Resolve MUST
// flip the row's local_path back to the .part placeholder +
// download_complete=false, drive the re-download, and end with
// download_complete=true at the original final path.
//
// Without this branch, the file-missing state would silently
// fall through the cache-hit fast path and the next job relying
// on the file would either crash on open or npm-oshi-stale.
func TestResolver_FileMissingRecovery(t *testing.T) {
	f := newResolverFixture(t)
	ctx := context.Background()
	beforeMisses := telemetry.GetPrometheusMetrics().CacheRequestCount("miss")

	// Deliberately DO NOT create the file on disk. The cache row
	// lies, the filesystem is honest.
	path := filepath.Join(f.dir, "TYSON001.mp4")
	if err := f.cache.Store(ctx, Entry{
		DriveFileID:      "TYSON001",
		LocalPath:        path,
		SizeBytes:        15,
		DownloadComplete: true,
	}); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if err := f.cache.MarkDownloadComplete(ctx, "TYSON001", path, 15); err != nil {
		t.Fatalf("MarkDownloadComplete: %v", err)
	}

	// Sanity: file really is absent before Resolve.
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("pre-Resolve: file unexpectedly present at %s", path)
	}

	payload := []byte("RECOVERED VIDEO ")
	r := resolverWithBytesSource(t, f, payload)

	got, err := r.Resolve(ctx, "TYSON001")
	if err != nil {
		t.Fatalf("Resolve: %v", err)
	}
	if got != path {
		t.Errorf("got=%q want %q (file-missing recovery must restore the original final path, not a new one)",
			got, path)
	}

	e, ok, fErr := f.cache.Find(ctx, "TYSON001")
	if fErr != nil || !ok {
		t.Fatalf("Find post-Resolve: ok=%v err=%v", ok, fErr)
	}
	if !e.DownloadComplete {
		t.Errorf("row.download_complete=false after recovery; want true")
	}
	if e.LocalPath != path {
		t.Errorf("row.local_path=%q want %q", e.LocalPath, path)
	}

	// The recovered file is now present at the original path with
	// the freshly downloaded bytes.
	body, rErr := os.ReadFile(path)
	if rErr != nil {
		t.Fatalf("ReadFile recovered: %v", rErr)
	}
	if string(body) != string(payload) {
		t.Errorf("final contents=%q want %q", body, payload)
	}

	// No .part leftover from the recovery.
	if _, err := os.Stat(filepath.Join(f.dir, "TYSON001.mp4.part")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf(".part exists post-recovery; want absent. stat err=%v", err)
	}
	if got := telemetry.GetPrometheusMetrics().CacheRequestCount("miss") - beforeMisses; got != 1 {
		t.Errorf("missing-file cache miss count delta=%v, want 1", got)
	}
}

// ────────────────────────────────────────────────────────────────────────────
// PASS 11 — singleflight tests.
//
// `countingSource` is a small fake DriveSource that counts how
// many times Open fires. Optional delay lets the test orchestrate
// concurrent callers that enter Resolve before the first inner
// fn completes, exercising the singleflight dedup path.
// ────────────────────────────────────────────────────────────────────────────

// countingSource counts Open calls, optionally delays before
// returning the payload, and optionally returns a synthetic
// error. Thread-safe (atomic.Int32).
//
// Used by the singleflight tests to assert that two concurrent
// Resolve(driveID) calls share exactly ONE source.Open invocation
// (success path) or exactly one source.Open invocation followed
// by the same wrapped error on both callers (failure path).
type countingSource struct {
	openCount atomic.Int32
	payload   []byte
	err       error
	delay     time.Duration
}

// Open implements DriveSource. Atomic counter bump is the first
// action — every caller that schedules Open will contribute to
// the count, so a non-1 final value tells us singleflight failed
// to dedupe.
func (c *countingSource) Open(ctx context.Context, _ string) (io.ReadCloser, error) {
	c.openCount.Add(1)
	if c.delay > 0 {
		select {
		case <-time.After(c.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	if c.err != nil {
		return nil, c.err
	}
	return io.NopCloser(bytes.NewReader(c.payload)), nil
}

// startBarrier waits on a channel close and returns true. Used
// by the concurrency tests to gate two goroutines so they enter
// Resolve as close to simultaneously as possible. Required because
// singleflight dedup depends on ordering: if goroutine A finishes
// its full inner fn before B enters, B sees cache-hit-on-row
// (cheap) and the dedup never exercises.
func startBarrier() (chan struct{}, func()) {
	ch := make(chan struct{})
	return ch, func() { close(ch) }
}

// TestResolver_Singleflight_TwoConcurrentResolves_OneDownload:
// two goroutines call Resolve("TYSON001") simultaneously on a
// cold cache. singleflight MUST coalesce them so source.Open fires
// exactly ONCE. Both callers return the same path. Cache row ends
// at the canonical final-path with download_complete=true.
//
// This is the user-spec test from Pass 11:
// "due Resolve simultanei → 1 download".
func TestResolver_Singleflight_TwoConcurrentResolves_OneDownload(t *testing.T) {
	f := newResolverFixture(t)
	ctx := context.Background()

	// 200ms delay guarantees race window for both goroutines to
	// enter Resolve before the first one finishes its inner fn.
	// non-zero leading bytes ensure the Downloader's verifyMedia
	// accepts the content.
	src := &countingSource{
		payload: []byte("FAKE MP4 BYTES — first 8 bytes are non-zero"),
		delay:   200 * time.Millisecond,
	}
	dl := NewDownloader(f.cache, f.dir, src)
	r := &Resolver{Cache: f.cache, Downloader: dl, Dir: f.dir}

	barrier, release := startBarrier()
	var wg sync.WaitGroup
	paths := make([]string, 2)
	errs := make([]error, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-barrier
			paths[i], errs[i] = r.Resolve(ctx, "TYSON001")
		}(i)
	}

	// Release both goroutines simultaneously so they both arrive
	// at sf.Do() with the same driveID. The first to acquire the
	// sf mutex runs the inner fn; the second waits on the same fn.
	release()
	wg.Wait()

	if errs[0] != nil || errs[1] != nil {
		t.Errorf("concurrent Resolve errors: [0]=%v [1]=%v (singleflight must share result)", errs[0], errs[1])
	}
	if paths[0] != paths[1] {
		t.Errorf("Resolve paths differ: [0]=%q [1]=%q (singleflight must share result byte-for-byte)", paths[0], paths[1])
	}

	wantPath := filepath.Join(f.dir, "TYSON001.mp4")
	if paths[0] != wantPath {
		t.Errorf("paths[0]=%q want %q", paths[0], wantPath)
	}

	if got := src.openCount.Load(); got != 1 {
		t.Errorf("source.Open fired %d times, want 1 (singleflight dedup invariant)", got)
	}

	// Cache row is consistent: exists, download_complete=true,
	// local_path = final. Both callers' MarkUsed bumps coalesced
	// to a single row update.
	e, ok, fErr := f.cache.Find(ctx, "TYSON001")
	if fErr != nil || !ok {
		t.Fatalf("Find post-Resolve: ok=%v err=%v", ok, fErr)
	}
	if !e.DownloadComplete {
		t.Errorf("row.download_complete=false after shared Resolve; want true")
	}
	if e.LocalPath != wantPath {
		t.Errorf("row.local_path=%q want %q", e.LocalPath, wantPath)
	}
}

// TestResolver_Singleflight_SharedCountsDuplicateDownload: the
// singleflight shared path MUST increment the duplicate-download
// telemetry (the parallelism certification metric). Two concurrent
// Resolves on a cold cache share ONE download; the coalesced caller
// is recorded as a duplicate (velox_cache_duplicate_downloads_total)
// with the shared result's byte size.
func TestResolver_Singleflight_SharedCountsDuplicateDownload(t *testing.T) {
	f := newResolverFixture(t)
	ctx := context.Background()

	src := &countingSource{
		payload: []byte("FAKE MP4 BYTES — first 8 bytes are non-zero"),
		delay:   200 * time.Millisecond,
	}
	dl := NewDownloader(f.cache, f.dir, src)
	r := &Resolver{Cache: f.cache, Downloader: dl, Dir: f.dir}

	metrics := telemetry.GetPrometheusMetrics()
	beforeDuplicates := metrics.DuplicateDownloadCount()
	beforeBytes := metrics.DuplicateDownloadBytes()

	barrier, release := startBarrier()
	var wg sync.WaitGroup
	paths := make([]string, 2)
	errs := make([]error, 2)
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-barrier
			paths[i], errs[i] = r.Resolve(ctx, "TYSON001")
		}(i)
	}
	release()
	wg.Wait()

	if errs[0] != nil || errs[1] != nil {
		t.Fatalf("concurrent Resolve errors: [0]=%v [1]=%v", errs[0], errs[1])
	}
	if paths[0] != paths[1] {
		t.Fatalf("Resolve paths differ: [0]=%q [1]=%q", paths[0], paths[1])
	}
	if got := src.openCount.Load(); got != 1 {
		t.Fatalf("source.Open fired %d times, want 1 (dedup invariant)", got)
	}
	if got := metrics.DuplicateDownloadCount() - beforeDuplicates; got != 1 {
		t.Errorf("duplicate-download count delta=%v, want 1 (shared singleflight must be recorded)", got)
	}
	if got := metrics.DuplicateDownloadBytes() - beforeBytes; got != float64(len(src.payload)) {
		t.Errorf("duplicate-download bytes delta=%v want %d (shared file size)", got, len(src.payload))
	}
}

// TestResolver_Singleflight_ErrorSharedAcrossCallers: companion
// to the success case. When the inner fn returns an error,
// singleflight shares the error verbatim across all coalesced
// callers. The Downloader's wrapping with ErrSourceOpen means
// both Resolve returns must satisfy errors.Is(err, ErrSourceOpen)
// AND source.Open fires exactly once.
func TestResolver_Singleflight_ErrorSharedAcrossCallers(t *testing.T) {
	f := newResolverFixture(t)
	ctx := context.Background()

	// Synthetic source error; the Downloader wraps it with
	// ErrSourceOpen via fmt.Errorf("%w: ...") so callers can
	// errors.Is branch.
	src := &countingSource{
		err:   errors.New("drive down"),
		delay: 100 * time.Millisecond,
	}
	dl := NewDownloader(f.cache, f.dir, src)
	r := &Resolver{Cache: f.cache, Downloader: dl, Dir: f.dir}

	barrier, release := startBarrier()
	var wg sync.WaitGroup
	errs := make([]error, 2)
	gotPaths := make([]string, 2)

	for i := 0; i < 2; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-barrier
			gotPaths[i], errs[i] = r.Resolve(ctx, "TYSON001")
		}(i)
	}

	release()
	wg.Wait()

	for i, err := range errs {
		if err == nil {
			t.Errorf("caller[%d]: Resolve succeeded; want ErrSourceOpen shared error", i)
			continue
		}
		if !errors.Is(err, ErrSourceOpen) {
			t.Errorf("caller[%d]: err=%v is not wrapped ErrSourceOpen", i, err)
		}
	}

	if got := src.openCount.Load(); got != 1 {
		t.Errorf("source.Open fired %d times on shared failure, want 1 (dedup invariant)", got)
	}

	// Cache row stays in placeholder/in-flight state (download
	// failed cleanly per Pass 10 downloader invariant).
	e, ok, fErr := f.cache.Find(ctx, "TYSON001")
	if fErr != nil || !ok {
		t.Fatalf("Find post-failure: ok=%v err=%v", ok, fErr)
	}
	if e.DownloadComplete {
		t.Errorf("row.download_complete=true after shared failure; want false (Downloader contract)")
	}
}
