// Package blob provides content-addressed blob storage with hash verification.
// Master publication is owned by the worker artifact lifecycle, not by this
// local storage adapter.
//
// Invariants:
//   - Put verifies hash matches data; returns ErrHashMismatch on mismatch
//     WITHOUT persisting. This protects against silent upstream corruption.
//   - Get hashverifies on read; corruption bumps the corruption counter and
//     detaches the file (async).
//   - Get returns ErrBlobNotFound on physical miss — recoverable.
//   - Close() prevents new reads and writes after worker shutdown.
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
}

// BlobOptions configures BlobArtifacts.
type BlobOptions struct {
	// Root is the on-disk directory. Required.
	Root string
}

// BlobArtifacts is a thread-safe, content-addressed blob store.
type BlobArtifacts struct {
	root   string
	closed atomic.Bool
	mu     sync.RWMutex

	blobs atomic.Int64
	bytes atomic.Int64

	publish         atomic.Int64
	publishFailed   atomic.Int64
	fetch           atomic.Int64
	fetchMiss       atomic.Int64
	fetchCorruption atomic.Int64
}

// NewBlobArtifacts constructs the local content-addressed store.
func NewBlobArtifacts(opts BlobOptions) (*BlobArtifacts, error) {
	if opts.Root == "" {
		return nil, errors.New("blob: Root is required")
	}
	if err := os.MkdirAll(opts.Root, 0o755); err != nil {
		return nil, fmt.Errorf("blob: create root: %w", err)
	}
	b := &BlobArtifacts{root: opts.Root}
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
	}
}

// Put writes blob under the given hash. On hash mismatch it returns
// ErrHashMismatch WITHOUT persisting (hash-mismatch bumps publish_failed
// exactly once). On filesystem-error paths the deferred single-bump
// keeps publish_failed in sync with attempts (no double-counting on
// compound failures). A successful write only persists the local blob;
// publication is a separate lifecycle owned by the worker artifact protocol.
func (b *BlobArtifacts) Put(_ context.Context, hash string, data []byte) (err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
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
	// via deferred increment. Hash-mismatch is handled before this defer.
	defer func() {
		if err != nil {
			b.publishFailed.Add(1)
		}
	}()
	existingSize := int64(-1)
	if existing, readErr := os.ReadFile(path); readErr == nil {
		existingSum := sha256.Sum256(existing)
		if hex.EncodeToString(existingSum[:]) == hash {
			// Content-addressed Put is idempotent. Avoid rewriting the same
			// bytes and do not inflate the unique-entry/byte gauges.
			return nil
		}
		// A corrupt object is replaced by the already-verified input below.
		existingSize = int64(len(existing))
	} else if !errors.Is(readErr, os.ErrNotExist) {
		return fmt.Errorf("blob: inspect existing entry: %w", readErr)
	}

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
	if existingSize < 0 {
		b.blobs.Add(1)
		b.bytes.Add(int64(len(data)))
	} else {
		b.bytes.Add(int64(len(data)) - existingSize)
	}

	return nil
}

// Get returns the bytes stored under hash, hash-verifying on read.
func (b *BlobArtifacts) Get(_ context.Context, hash string) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
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

// Close prevents future reads and writes. Idempotent. Wire to worker shutdown.
func (b *BlobArtifacts) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closed.Store(true)
	return nil
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
