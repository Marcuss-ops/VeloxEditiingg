// Package workercache — Downloader is the worker-side byte-stream +
// atomic-rename pipeline that promotes a placeholder cache row from
// `download_complete=false` to `download_complete=true`.
//
// Contract (Pass 10):
//
//  1. Resolver inserts a placeholder row via Store with
//     local_path = "<dir>/<driveID>.mp4.part" and download_complete=false.
//     The cleaner never deletes such rows (download is in flight).
//  2. Downloader streams the bytes from DriveSource to the .part path.
//  3. Re-opens the .part and runs verifyMedia.
//  4. On verify success, atomically renames .part → final filename.
//  5. Calls cache.MarkDownloadComplete(final, size).
//
// Failure semantics:
//
//   - Source.Open fails before .part exists → wrapped ErrSourceOpen.
//     No on-disk side effect, cache row unchanged.
//   - io.Copy fails mid-stream → .part removed, returns wrapped err.
//   - verifyMedia fails → .part removed, returns wrapped ErrVerifyFailed.
//   - os.Rename fails (e.g., EXDEV cross-filesystem) → .part removed,
//     returns wrapped ErrRename. There is NO fallback to copy+remove:
//     operator decides mount topology rather than silently transferring
//     a few GB between filesystems.
//   - cache.MarkDownloadComplete fails after a successful rename →
//     final on-disk file removed, returns wrapped err. The cache row
//     stays at download_complete=0 so the next retry rebuilds from a
//     fresh .part.
//
// The cleaner's existing invariant (skip download_complete=false)
// composes correctly with this contract: any row whose download
// failed stays on disk under the .part name until either a fresh
// Resolve tries it (and overwrites) or an explicit operator cleanup
// removes the .part dir.
package workercache

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"velox-worker-agent/internal/telemetry"
)

// Sentinel errors so callers (and tests) can branch via errors.Is.
// Each wraps an underlying cause with %w so the original network or
// syscall detail remains visible in the error chain.
var (
	ErrVerifyFailed = errors.New("workercache: verifyMedia rejected bytes")
	ErrSourceOpen   = errors.New("workercache: drive source open failed")
	ErrRename       = errors.New("workercache: atomic rename failed")
)

// DriveSource abstracts the byte stream of a Drive file. The
// downloader reads from the returned io.ReadCloser until EOF and
// always closes it. Streaming (NOT buffer-all) is mandatory: a single
// Drive clip can approach 1 GB; buffering the entire body in memory
// would blow up the worker's RSS.
type DriveSource interface {
	Open(ctx context.Context, driveID string) (io.ReadCloser, error)
}

// VerifyMedia reads the downloaded file and decides whether the
// bytes are usable. Return nil to accept; return any non-nil error
// to abort the download (the .part is removed; the cache row stays
// at download_complete=0).
//
// The verifier is called on a freshly opened io.ReadCloser so it
// sees the exact on-disk bytes, not whatever Copy buffered. This
// also lets future passes verify hash + magic-byte checksums against
// the file rather than against an in-memory copy.
type VerifyMedia func(r io.Reader) error

// Downloader composes a Cache, a destination directory, and a
// DriveSource. verify is initialised to defaultVerifyMedia and
// overridable via WithVerify.
type Downloader struct {
	cache  *Cache
	dir    string
	source DriveSource
	verify VerifyMedia
}

// DownloadMetadata describes a completed Drive-cache download. SHA256 is
// computed while streaming, so callers can build content-addressed logs
// without rereading the entire file.
type DownloadMetadata struct {
	Path           string
	Bytes          int64
	SHA256         string
	HashDuration   time.Duration
	VerifyDuration time.Duration
}

// NewDownloader wires the canonical dependencies. Panics on a nil
// cache or source — the fields are required for any meaningful
// download, and silent fall-backs would mask operator config bugs.
// Passing an empty dir falls back to the current working directory
// (".").
func NewDownloader(cache *Cache, dir string, source DriveSource) *Downloader {
	if cache == nil {
		panic("workercache.NewDownloader: cache is required (nil cache)")
	}
	if source == nil {
		panic("workercache.NewDownloader: source is required (nil source)")
	}
	if dir == "" {
		dir = "."
	}
	return &Downloader{
		cache:  cache,
		dir:    dir,
		source: source,
		verify: defaultVerifyMedia,
	}
}

// WithVerify replaces the default verifier. Nil is ignored (the
// existing verifier is preserved) so an unset option is harmless.
// Returns the receiver for chainable construction.
func (d *Downloader) WithVerify(v VerifyMedia) *Downloader {
	if v != nil {
		d.verify = v
	}
	return d
}

// DownloadDriveFile streams the Drive content for `driveID`, runs
// verifyMedia, renames .part → final, and marks the cache row
// complete. Returns the final filesystem path on success.
//
// Caller MUST pre-populate the cache row with `cache.Store(...,
// download_complete=false, local_path = .part)` so the cleaner
// observes a stable row during the download. The downloader
// OVERWRITES download_complete + final local_path + size via
// cache.MarkDownloadComplete on success, which keeps the row schema
// invariant (drive_file_id is the only PK, local_path is mutable).
func (d *Downloader) DownloadDriveFile(ctx context.Context, driveID string) (string, error) {
	result, err := d.DownloadDriveFileWithMetadata(ctx, driveID)
	if err != nil {
		return "", err
	}
	return result.Path, nil
}

// DownloadDriveFileWithMetadata is the metadata-returning form of
// DownloadDriveFile. It preserves the same atomic and cache-row semantics.
func (d *Downloader) DownloadDriveFileWithMetadata(ctx context.Context, driveID string) (DownloadMetadata, error) {
	started := time.Now()
	if driveID == "" {
		return DownloadMetadata{}, ErrEmptyID
	}

	partPath := filepath.Join(d.dir, driveID+".mp4.part")
	finalPath := filepath.Join(d.dir, driveID+".mp4")

	// 1. Open the source stream.
	src, err := d.source.Open(ctx, driveID)
	if err != nil {
		return DownloadMetadata{}, fmt.Errorf("%w: drive_id=%s: %v", ErrSourceOpen, driveID, err)
	}
	defer src.Close()

	// 2. Stream into .part.
	partFile, err := os.Create(partPath)
	if err != nil {
		return DownloadMetadata{}, fmt.Errorf("workercache.Downloader: create %s: %w", partPath, err)
	}
	hasher := sha256.New()
	hashStarted := time.Now()
	written, copyErr := io.Copy(partFile, io.TeeReader(src, hasher))
	hashDuration := time.Since(hashStarted)
	closeErr := partFile.Close()
	if copyErr != nil {
		_ = os.Remove(partPath)
		return DownloadMetadata{}, fmt.Errorf("workercache.Downloader: copy to .part: %w", copyErr)
	}
	if closeErr != nil {
		_ = os.Remove(partPath)
		return DownloadMetadata{}, fmt.Errorf("workercache.Downloader: close .part: %w", closeErr)
	}

	// 3. Verify — re-open so the verifier inspects the on-disk file.
	verifyDuration := time.Duration(0)
	if d.verify != nil {
		reop, reopenErr := os.Open(partPath)
		if reopenErr != nil {
			_ = os.Remove(partPath)
			return DownloadMetadata{}, fmt.Errorf("workercache.Downloader: re-open .part for verify: %w", reopenErr)
		}
		verifyStarted := time.Now()
		verifyErr := d.verify(reop)
		telemetry.GetPrometheusMetrics().RecordCacheVerify(time.Since(verifyStarted))
		_ = reop.Close()
		if verifyErr != nil {
			_ = os.Remove(partPath)
			return DownloadMetadata{}, fmt.Errorf("%w: drive_id=%s: %v", ErrVerifyFailed, driveID, verifyErr)
		}
	}

	// 4. Atomic rename within the same filesystem. This is the
	// lifecycle pivot: the on-disk file is now committed, and the
	// cleaner can lawfully delete this row when its lease drops.
	if renameErr := os.Rename(partPath, finalPath); renameErr != nil {
		_ = os.Remove(partPath)
		return DownloadMetadata{}, fmt.Errorf("%w: drive_id=%s: %v", ErrRename, driveID, renameErr)
	}

	// 5. Flip the cache row to download_complete=1. Without this
	// the row keeps download_complete=false and the cleaner
	// considers it "in-flight", which is a correct fallback but
	// also blocks legitimate eviction. The MarkDownloadComplete
	// call also bumps last_used_at so the 3-minute grace window
	// (Pass 11) treats this row as fresh.
	if mErr := d.cache.MarkDownloadComplete(ctx, driveID, finalPath, written); mErr != nil {
		// Roll back the rename so a subsequent retry doesn't see a
		// stale final file from this attempt. The cache row stays
		// at download_complete=false (placeholder) so the next
		// retry can rebuild from a fresh .part.
		_ = os.Remove(finalPath)
		return DownloadMetadata{}, fmt.Errorf("workercache.Downloader: mark complete: %w", mErr)
	}
	telemetry.GetPrometheusMetrics().RecordCacheDownload(written, time.Since(started))

	return DownloadMetadata{
		Path: finalPath, Bytes: written,
		SHA256:       hex.EncodeToString(hasher.Sum(nil)),
		HashDuration: hashDuration, VerifyDuration: verifyDuration,
	}, nil
}

// defaultVerifyMedia returns nil iff the bytes look like real
// media: at least 1 byte AND at least one non-zero byte in the first
// 8 bytes. This is tight enough to reject a misconfigured HTTP
// source that returns an HTML error page or an empty body, while
// remaining format-agnostic (Pass 10 does not commit to MP4-only).
//
// Real-format verifiers (ffprobe, magic bytes, hash against Drive's
// MD5) are deferred to a later pass and injected via WithVerify.
func defaultVerifyMedia(r io.Reader) error {
	if r == nil {
		return errors.New("verifyMedia: nil reader")
	}
	buf := make([]byte, 8)
	n, err := io.ReadFull(r, buf)
	if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) && !errors.Is(err, io.EOF) {
		return fmt.Errorf("verifyMedia: read: %w", err)
	}
	if n == 0 {
		return errors.New("verifyMedia: empty body")
	}
	nonZero := false
	for _, b := range buf[:n] {
		if b != 0 {
			nonZero = true
			break
		}
	}
	if !nonZero {
		return errors.New("verifyMedia: all-zero bytes in first 8B (likely HTML error page)")
	}
	return nil
}
