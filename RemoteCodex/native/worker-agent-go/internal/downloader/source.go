package downloader

// source.go — the pluggable byte pipeline a Transfer delegates to.
//
// The manager never opens sockets or touches the cache layout itself. A
// Transferer owns the two halves of the canonical download pipeline:
//
//   - Check  — the cache probe. Returns the already-verified local path when
//     the asset is present (CACHE_HIT), otherwise reports a miss. A hit is
//     valid only after the integrity metadata supplied in the request has
//     been verified against the on-disk file.
//   - Transfer — the byte fetch + atomic promotion. Downloads to a partial
//     path, verifies size and SHA-256, renames atomically, and only then
//     returns a path the caller may open. A returned error means the asset
//     is NOT on disk at a verified location.
//
// In production the worker wires its master-bridge transferer (HTTP GET on
// /api/v1/worker-assets/{id} with Bearer auth, atomic write via the worker's
// cache helpers). Tests wire byte fakes, which is why the package stays free
// of net/http and worker internals.

import (
	"context"
	"fmt"
	"io"

	"velox-shared/assetref"
)

// SourceMetadata describes the response represented by an AssetSource.Open
// call. Size is the total asset size when known; it is not the number of bytes
// remaining after the requested offset. SHA256 and MIMEType are optional
// source hints and do not replace the manager's final integrity verification.
type SourceMetadata struct {
	SizeBytes int64
	SHA256    assetref.ContentHash
	MIMEType  string
}

// AssetSource is the canonical byte source contract for resumable downloads.
// Open must return a stream beginning at offset. An offset of zero requests
// the complete asset; a non-zero offset requests a ranged suffix when
// SupportsRange reports true. Implementations that do not support ranges must
// return an error for non-zero offsets rather than silently returning bytes
// from the beginning, which could corrupt a resumed partial file.
type AssetSource interface {
	Open(ctx context.Context, offset int64) (io.ReadCloser, SourceMetadata, error)
	SupportsRange() bool
}

// ValidateSourceOffset centralizes the contract's invalid-offset rule for
// implementations and tests. It is intentionally small so HTTP/file sources
// can reject invalid input consistently before opening a connection.
func ValidateSourceOffset(offset int64) error {
	if offset < 0 {
		return fmt.Errorf("asset source: negative offset %d", offset)
	}
	return nil
}

// CacheCheckResult is the outcome of a Transferer.Check.
type CacheCheckResult struct {
	// CacheHit is true only when a verified on-disk file exists.
	CacheHit bool
	// LocalPath is the verified local path when CacheHit is true.
	LocalPath string
	// SHA256 is the verified content identity of a cache hit. It prevents a
	// read-model sync from erasing the digest already persisted by the cache.
	SHA256 assetref.ContentHash
}

// TransferResult is the outcome of a Transferer.Transfer.
type TransferResult struct {
	LocalPath string
	Bytes     int64
	SHA256    assetref.ContentHash
}

// Transferer is the byte pipeline used by one Transfer. The manager calls
// Check once per transfer (in the transfer's own goroutine) and Transfer at
// most once per scheduling slot.
//
// Context contract: ctx is the TRANSFER context — owned by the transfer,
// independent of any single job, cancelled when the last waiter leaves or the
// manager closes. All cancellation-sensitive work (HTTP requests, io.Copy,
// cache writes) MUST bind to ctx. reportCtx is the context of the FIRST job
// that requested the asset, captured purely for caller-scoped telemetry
// (structured logs, per-attempt event recorders, operation trackers). It
// MUST be used only for value reads and non-blocking reporting — never for
// I/O that would abort when that first job is cancelled.
type Transferer interface {
	Check(ctx context.Context, reportCtx context.Context, req DownloadRequest) (CacheCheckResult, error)
	// Transfer fetches the asset bytes, invoking onProgress as bytes land on
	// disk so the manager can refresh byte counters, throughput and ETA and
	// publish throttled snapshots. onProgress is never nil in production; the
	// callback is cheap and must be safe to call once per read chunk.
	Transfer(ctx context.Context, reportCtx context.Context, req DownloadRequest, onProgress func(downloadedBytes int64)) (TransferResult, error)
}

// TransfererFunc adapts a plain function to the Transferer interface. Both
// halves share the same function; implementers may branch on req to keep the
// cache-probe and byte-fetch behaviours distinct. Used by tests.
type TransfererFunc func(ctx context.Context, reportCtx context.Context, req DownloadRequest, check bool, onProgress func(downloadedBytes int64)) (CacheCheckResult, TransferResult, error)

// Check implements Transferer.
func (f TransfererFunc) Check(ctx context.Context, reportCtx context.Context, req DownloadRequest) (CacheCheckResult, error) {
	hit, _, err := f(ctx, reportCtx, req, true, nil)
	return hit, err
}

// Transfer implements Transferer.
func (f TransfererFunc) Transfer(ctx context.Context, reportCtx context.Context, req DownloadRequest, onProgress func(downloadedBytes int64)) (TransferResult, error) {
	_, result, err := f(ctx, reportCtx, req, false, onProgress)
	return result, err
}
