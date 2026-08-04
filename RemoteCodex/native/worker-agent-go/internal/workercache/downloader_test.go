// Package workercache — Downloader test matrix.
//
// Each fixture pre-populates the cache row with a .part placeholder
// (download_complete=false) so the test exercises the FULL Resolve
// pipeline: stream → verify → rename → mark-complete. This mirrors
// what the dispatch path will do in Pass 10.5 (worker integration).

package workercache

import (
	"bytes"
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"testing"
)

// ────────────────────────────────────────────────────────────────────────
// fake DriveSource — bytes in-memory, one ErrSourceOpen variant.
// ────────────────────────────────────────────────────────────────────────

// driveSourceFunc turns a plain function into a DriveSource.
type driveSourceFunc func(ctx context.Context, driveID string) (io.ReadCloser, error)

func (f driveSourceFunc) Open(ctx context.Context, driveID string) (io.ReadCloser, error) {
	return f(ctx, driveID)
}

func bytesSource(b []byte) DriveSource {
	return driveSourceFunc(func(_ context.Context, _ string) (io.ReadCloser, error) {
		return io.NopCloser(bytes.NewReader(b)), nil
	})
}

func failingSource(err error) DriveSource {
	return driveSourceFunc(func(_ context.Context, _ string) (io.ReadCloser, error) {
		return nil, err
	})
}

// ────────────────────────────────────────────────────────────────────────
// fixture: cache + dir + pre-populated .part placeholder row.
// ────────────────────────────────────────────────────────────────────────

type downloaderFixture struct {
	cache     *Cache
	dir       string
	driveID   string
	partPath  string
	finalPath string
}

func newDownloaderFixture(t *testing.T) *downloaderFixture {
	t.Helper()
	dir := t.TempDir()
	cache, err := Open(filepath.Join(dir, "cache.db"))
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = cache.Close() })

	driveID := "TYSON001"
	partPath := filepath.Join(dir, driveID+".mp4.part")
	// Insert placeholder row per the cache.go contract.
	if err := cache.Store(context.Background(), Entry{
		DriveFileID: driveID,
		LocalPath:   partPath,
	}); err != nil {
		t.Fatalf("Store placeholder: %v", err)
	}
	return &downloaderFixture{
		cache:     cache,
		dir:       dir,
		driveID:   driveID,
		partPath:  partPath,
		finalPath: filepath.Join(dir, driveID+".mp4"),
	}
}

// ────────────────────────────────────────────────────────────────────────
// tests
// ────────────────────────────────────────────────────────────────────────

// TestDownloader_HappyPath: source OK → verify passes → rename OK →
// cache row flips to download_complete=true with the right size +
// final path. .part absent, final present.
func TestDownloader_HappyPath(t *testing.T) {
	f := newDownloaderFixture(t)
	payload := []byte("FAKE MP4 BYTES — first 8 bytes are non-zero")
	dl := NewDownloader(f.cache, f.dir, bytesSource(payload))

	finalPath, err := dl.DownloadDriveFile(context.Background(), f.driveID)
	if err != nil {
		t.Fatalf("DownloadDriveFile: %v", err)
	}
	if finalPath != f.finalPath {
		t.Errorf("finalPath=%q want %q", finalPath, f.finalPath)
	}
	secondID := "TYSON002"
	secondPart := filepath.Join(f.dir, secondID+".mp4.part")
	if err := f.cache.Store(context.Background(), Entry{DriveFileID: secondID, LocalPath: secondPart}); err != nil {
		t.Fatalf("Store metadata placeholder: %v", err)
	}
	metadata, metadataErr := dl.DownloadDriveFileWithMetadata(context.Background(), secondID)
	if metadataErr != nil {
		t.Fatalf("DownloadDriveFileWithMetadata: %v", metadataErr)
	}
	if metadata.Path == "" || metadata.Bytes == 0 || len(metadata.SHA256) != 64 || metadata.HashDuration <= 0 {
		t.Errorf("download metadata = %+v, want path/bytes/SHA-256/hash duration", metadata)
	}

	// On-disk: .part gone, final present with the bytes we wrote.
	if _, err := os.Stat(f.partPath); !errors.Is(err, os.ErrNotExist) {
		t.Errorf(".part exists after success; want it removed. stat err=%v", err)
	}
	got, rErr := os.ReadFile(f.finalPath)
	if rErr != nil {
		t.Fatalf("ReadFile(final): %v", rErr)
	}
	if !bytes.Equal(got, payload) {
		t.Errorf("final bytes=%q want %q", got, payload)
	}

	// Cache row: download_complete=1, size_bytes correct.
	e, ok, cErr := f.cache.Find(context.Background(), f.driveID)
	if cErr != nil || !ok {
		t.Fatalf("Find after Download: ok=%v err=%v", ok, cErr)
	}
	if !e.DownloadComplete {
		t.Errorf("row.download_complete=false after success; want true")
	}
	if e.LocalPath != f.finalPath {
		t.Errorf("row.local_path=%q want %q", e.LocalPath, f.finalPath)
	}
	if e.SizeBytes != int64(len(payload)) {
		t.Errorf("row.size_bytes=%d want %d", e.SizeBytes, len(payload))
	}
}

// TestDownloader_VerifyMediaFails: source returns bytes that fail
// the default verifier (all-zero first 8 bytes) → .part removed,
// no rename, no MarkDownloadComplete, row.download_complete=false.
// This is the canonical "verify Media fallisce (nessun rename)" test.
func TestDownloader_VerifyMediaFails(t *testing.T) {
	f := newDownloaderFixture(t)
	// Verifier-rejecting content: an HTML-looking page that opens
	// with non-zero bytes (so defaultVerifyMedia's all-zero guard
	// is not the rejection path) — instead we install a custom
	// failing verifier that always rejects. This isolates the
	// "verify failure" branch from the "default-rejects" branch.
	dl := NewDownloader(f.cache, f.dir, bytesSource([]byte("X")))
	dl.WithVerify(func(_ io.Reader) error { return errors.New("synthetic reject") })

	finalPath, err := dl.DownloadDriveFile(context.Background(), f.driveID)
	if err == nil {
		t.Fatalf("DownloadDriveFile returned nil err; want verify error")
	}
	if !errors.Is(err, ErrVerifyFailed) {
		t.Errorf("err=%v; want errors.Is(err, ErrVerifyFailed)=true", err)
	}
	if finalPath != "" {
		t.Errorf("finalPath=%q want empty on failure", finalPath)
	}

	// On-disk: .part absent, final absent.
	if _, statErr := os.Stat(f.partPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf(".part exists after verify-fail; want it removed. stat err=%v", statErr)
	}
	if _, statErr := os.Stat(f.finalPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("final exists after verify-fail (rename was attempted); want absent. stat err=%v", statErr)
	}

	// Cache row untouched: placeholder path, download_complete=false.
	e, ok, cErr := f.cache.Find(context.Background(), f.driveID)
	if cErr != nil || !ok {
		t.Fatalf("Find after verify-fail: ok=%v err=%v", ok, cErr)
	}
	if e.DownloadComplete {
		t.Errorf("row.download_complete=true after verify-fail; want false (no rename happened)")
	}
	if e.LocalPath != f.partPath {
		t.Errorf("row.local_path=%q want %q (placeholder preserved)", e.LocalPath, f.partPath)
	}
}

// TestDownloader_DefaultVerifyRejectsEmptyBody: the default verifier
// must reject an empty body (zero-byte source) without going further
// than the verify step.
func TestDownloader_DefaultVerifyRejectsEmptyBody(t *testing.T) {
	f := newDownloaderFixture(t)
	dl := NewDownloader(f.cache, f.dir, bytesSource([]byte{}))

	_, err := dl.DownloadDriveFile(context.Background(), f.driveID)
	if !errors.Is(err, ErrVerifyFailed) {
		t.Errorf("err=%v; want errors.Is(err, ErrVerifyFailed)=true on empty body", err)
	}
	if _, statErr := os.Stat(f.partPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf(".part exists after empty-body rejection; want absent. stat err=%v", statErr)
	}
}

// TestDownloader_DefaultVerifyRejectsAllZeroBytes: the default verifier
// catches all-zero-byte pages (e.g., empty 200 OK, padding accidents).
func TestDownloader_DefaultVerifyRejectsAllZeroBytes(t *testing.T) {
	f := newDownloaderFixture(t)
	dl := NewDownloader(f.cache, f.dir, bytesSource([]byte{0, 0, 0, 0, 0, 0, 0, 0}))

	_, err := dl.DownloadDriveFile(context.Background(), f.driveID)
	if !errors.Is(err, ErrVerifyFailed) {
		t.Errorf("err=%v; want errors.Is(err, ErrVerifyFailed)=true on all-zero body", err)
	}
}

// TestDownloader_SourceOpenFails: source.Open returns an error
// before any .part exists. .part and final absent, row preserved.
func TestDownloader_SourceOpenFails(t *testing.T) {
	f := newDownloaderFixture(t)
	dl := NewDownloader(f.cache, f.dir, failingSource(errors.New("network down")))

	_, err := dl.DownloadDriveFile(context.Background(), f.driveID)
	if !errors.Is(err, ErrSourceOpen) {
		t.Errorf("err=%v; want errors.Is(err, ErrSourceOpen)=true", err)
	}
	if _, statErr := os.Stat(f.partPath); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf(".part exists after source-open fail; want absent")
	}
	e, ok, _ := f.cache.Find(context.Background(), f.driveID)
	if !ok {
		t.Fatalf("placeholder row disappeared")
	}
	if e.DownloadComplete {
		t.Errorf("download_complete=true after source-open fail")
	}
}

// TestDownloader_EmptyDriveID_ErrEmptyID: input validation.
func TestDownloader_EmptyDriveID_ErrEmptyID(t *testing.T) {
	f := newDownloaderFixture(t)
	dl := NewDownloader(f.cache, f.dir, bytesSource([]byte("X")))

	_, err := dl.DownloadDriveFile(context.Background(), "")
	if !errors.Is(err, ErrEmptyID) {
		t.Errorf("err=%v; want errors.Is(err, ErrEmptyID)=true", err)
	}
}

// TestDownloader_NilCacheOrSource_Panics: constructor contract.
func TestDownloader_NilCacheOrSource_Panics(t *testing.T) {
	if !panicked(func() { _ = NewDownloader(nil, "/tmp", bytesSource([]byte("X"))) }) {
		t.Errorf("NewDownloader(nil cache) did not panic")
	}
	if !panicked(func() { _ = NewDownloader(&Cache{}, "/tmp", nil) }) {
		t.Errorf("NewDownloader(nil source) did not panic")
	}
}

// TestDownloader_CleanupAfterDownloadRespectsInFlight: Pairs the
// downloader with the Cleanup predicate — after a failed download
// the row is at download_complete=false, and Cleanup MUST skip it
// even when no lease is held and the protected set is empty.
func TestDownloader_CleanupAfterDownloadRespectsInFlight(t *testing.T) {
	f := newDownloaderFixture(t)
	// Inject verify-fail: row ends at download_complete=false.
	dl := NewDownloader(f.cache, f.dir, bytesSource([]byte("X")))
	dl.WithVerify(func(_ io.Reader) error { return errors.New("synthetic") })
	if _, err := dl.DownloadDriveFile(context.Background(), f.driveID); err == nil {
		t.Fatalf("verify should fail")
	}

	// Cleanup pass: must NOT delete the in-flight row.
	stats, err := Cleanup(context.Background(), f.cache, nil)
	if err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if stats.SkippedInFlight != 1 {
		t.Errorf("SkippedInFlight=%d want 1 (the failed-download row)", stats.SkippedInFlight)
	}
	if stats.Removed != 0 {
		t.Errorf("Removed=%d want 0 (failed-download row must survive cleanup)", stats.Removed)
	}
	if _, ok, _ := f.cache.Find(context.Background(), f.driveID); !ok {
		t.Errorf("failed-download row disappeared after Cleanup")
	}
}

// ────────────────────────────────────────────────────────────────────────
// helpers
// ────────────────────────────────────────────────────────────────────────

func panicked(fn func()) (panicked bool) {
	defer func() {
		if r := recover(); r != nil {
			panicked = true
		}
	}()
	fn()
	return false
}
