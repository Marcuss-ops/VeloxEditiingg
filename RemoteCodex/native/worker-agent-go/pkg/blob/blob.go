// Package blob provides content-addressed blob storage with publish-time
// hash verification and an in-memory upload queue for master-side
// publication (consumed via the master transport).
//
// Invariants:
//   - Put verifies hash matches data; returns ErrHashMismatch on mismatch
//     WITHOUT persisting. This protects against silent upstream corruption.
//   - Get hashverifies on read; corruption bumps the corruption counter and
//     detaches the file (async).
//   - Get returns ErrBlobNotFound on physical miss — recoverable.
//   - Upload queue is bounded; full queue drops pending uploads and bumps
//     publish_failed so operators see backpressure.
//   - Close() drains the upload queue and stops the processor goroutine.
//     Wire to worker shutdown so the stub processor does not leak.
package blob

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

// ErrHashMismatch is returned by Put when data's hash disagrees with the
// caller's expected hash. Protects against silent corruption upstream.
var ErrHashMismatch = errors.New("blob: hash mismatch")

// ErrBlobNotFound is returned by Get when no blob matches hash.
// Recoverable — caller can refetch or compose from upstream.
var ErrBlobNotFound = errors.New("blob: not found")

// ErrBlobCorruption is returned by Get when the on-disk content does
// not match the requested hash.
var ErrBlobCorruption = errors.New("blob: on-disk content hash mismatch")

// ErrClosed is returned by storage paths after Close.
var ErrClosed = errors.New("blob: closed")

// BlobStats is a snapshot of BlobArtifacts counters and gauges.
type BlobStats struct {
	Publish         int64 `json:"publish"`
	PublishFailed   int64 `json:"publish_failed"`
	Fetch           int64 `json:"fetch"`
	FetchMiss       int64 `json:"fetch_miss"`
	FetchCorruption int64 `json:"fetch_corruption"`
	Bytes           int64 `json:"bytes"`
	Entries         int   `json:"entries"`
	QueueDepth      int   `json:"queue_depth"`
} // UploadJob is one pending master-upload job. The transport
// implementation will pull these off the channel.
type UploadJob struct {
	Hash       string    `json:"hash"`
	SizeBytes  int64     `json:"size_bytes"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}

// BlobOptions configures BlobArtifacts.
type BlobOptions struct {
	// Root is the on-disk directory. Required.
	Root string
	// UploadQueueSize bounds the in-memory upload channel. Default 1024.
	UploadQueueSize int
}

// BlobArtifacts is a thread-safe, content-addressed blob store with an
// upload queue wired for master-side publication.
type BlobArtifacts struct {
	root     string
	uploadCh chan UploadJob
	closed   atomic.Bool
	stopOnce sync.Once
	uploadWG sync.WaitGroup

	blobs atomic.Int64
	bytes atomic.Int64

	publish         atomic.Int64
	publishFailed   atomic.Int64
	fetch           atomic.Int64
	fetchMiss       atomic.Int64
	fetchCorruption atomic.Int64
}

// NewBlobArtifacts constructs the store and starts one background
// processor goroutine that drains uploadCh. It will replace the
// noop processor body with a master-transport publish call.
func NewBlobArtifacts(opts BlobOptions) (*BlobArtifacts, error) {
	if opts.Root == "" {
		return nil, errors.New("blob: Root is required")
	}
	if opts.UploadQueueSize <= 0 {
		opts.UploadQueueSize = 1024
	}
	if err := os.MkdirAll(opts.Root, 0o755); err != nil {
		return nil, fmt.Errorf("blob: create root: %w", err)
	}
	b := &BlobArtifacts{
		root:     opts.Root,
		uploadCh: make(chan UploadJob, opts.UploadQueueSize),
	}
	b.uploadWG.Add(1)
	go b.processUploads()
	return b, nil
}

// Stats returns a snapshot.
func (b *BlobArtifacts) Stats() BlobStats {
	return BlobStats{
		Publish:         b.publish.Load(),
		PublishFailed:   b.publishFailed.Load(),
		Fetch:           b.fetch.Load(),
		FetchMiss:       b.fetchMiss.Load(),
		FetchCorruption: b.fetchCorruption.Load(),
		Bytes:           b.bytes.Load(),
		Entries:         int(b.blobs.Load()),
		QueueDepth:      len(b.uploadCh),
	}
}

// Put writes blob under the given hash. On hash mismatch it returns
// ErrHashMismatch WITHOUT persisting (hash-mismatch bumps publish_failed
// exactly once). On filesystem-error paths the deferred single-bump
// keeps publish_failed in sync with attempts (no double-counting on
// compound failures). On successful write the upload-queue drop
// path bumps publish_failed at most once.
func (b *BlobArtifacts) Put(_ context.Context, hash string, data []byte) (err error) {
	if b.closed.Load() {
		return ErrClosed
	}
	if !isValidHash(hash) {
		return errors.New("blob: invalid hash format")
	}

	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != hash {
		b.publishFailed.Add(1)
		return fmt.Errorf("%w: want %s got %s", ErrHashMismatch, hash, got)
	}

	path := b.entryPath(hash)
	// Filesystem-error paths share a single publish_failed bump
	// via deferred increment. Hash-mismatch and queue-full take
	// their own paths (hash-mismatch already incremented; queue-full
	// increments below).
	defer func() {
		if err != nil {
			b.publishFailed.Add(1)
		}
	}()

	if err = os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return fmt.Errorf("blob: mkdir prefix: %w", err)
	}
	tmpPath := path + ".tmp"
	if err = os.WriteFile(tmpPath, data, 0o644); err != nil {
		return fmt.Errorf("blob: write tmp: %w", err)
	}
	if err = os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return fmt.Errorf("blob: rename: %w", err)
	}

	b.publish.Add(1)
	b.blobs.Add(1)
	b.bytes.Add(int64(len(data)))

	job := UploadJob{
		Hash:       hash,
		SizeBytes:  int64(len(data)),
		EnqueuedAt: time.Now(),
	}
	select {
	case b.uploadCh <- job:
	default:
		// Queue full → drop. Operators see backpressure via
		// Stats().QueueDepth and PublishFailed.
		b.publishFailed.Add(1)
	}
	return nil
}

// Get returns the bytes stored under hash, hash-verifying on read.
func (b *BlobArtifacts) Get(_ context.Context, hash string) ([]byte, error) {
	if b.closed.Load() {
		return nil, ErrClosed
	}
	if !isValidHash(hash) {
		return nil, errors.New("blob: invalid hash format")
	}
	path := b.entryPath(hash)
	data, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			b.fetchMiss.Add(1)
			return nil, ErrBlobNotFound
		}
		return nil, fmt.Errorf("blob: read %s: %w", hash, err)
	}

	sum := sha256.Sum256(data)
	if got := hex.EncodeToString(sum[:]); got != hash {
		b.fetchCorruption.Add(1)
		// Detach corrupt file async (never the hot path's mutex).
		go func(p string) { _ = os.Remove(p) }(path)
		return nil, ErrBlobCorruption
	}

	b.fetch.Add(1)
	return data, nil
}

// Close drains the upload queue and stops the processor goroutine.
// Idempotent. Wire to worker shutdown.
func (b *BlobArtifacts) Close() error {
	b.stopOnce.Do(func() {
		b.closed.Store(true)
		close(b.uploadCh)
	})
	b.uploadWG.Wait()
	return nil
} // processUploads drains uploadCh. It will replace the noop sink
// with a master-transport publish call (see UploadJob).
func (b *BlobArtifacts) processUploads() {
	defer b.uploadWG.Done()
	for range b.uploadCh {
		// Stub: count received, do nothing.
	}
}

func (b *BlobArtifacts) entryPath(hash string) string {
	return filepath.Join(b.root, hash[:2], hash)
}

func isValidHash(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f')) {
			return false
		}
	}
	return true
}
