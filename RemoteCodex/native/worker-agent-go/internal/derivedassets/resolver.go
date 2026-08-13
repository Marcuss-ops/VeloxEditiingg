// Package derivedassets resolves reusable normalized media assets over the
// worker's canonical cache.
//
// A "derived asset" is the deterministic output of normalizing one verified
// source (identified by its content hash) into one complete media profile
// (workercache.NormalizationProfile). The identity of such an output is the
// canonical derived key already produced by workercache.DerivedAssetKey, so
// this package owns no second keying scheme and no second cache/index: it
// delegates every lookup and store to the existing workercache.DerivedAssetStore.
//
// The Resolver is the MISS-side orchestrator that glues production to
// promotion. On a cache hit it returns the cached entry untouched; on a miss
// it runs an injected Producer (the native FramePipeline in production), then
// re-verifies the produced bytes' size and SHA-256 before registering them
// atomically in the cache. Verification is fail-closed: a produced file that
// is empty, missing, or whose hash disagrees with the producer's claim is
// removed and never promoted.
package derivedassets

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"

	"velox-shared/assetref"
	"velox-worker-agent/internal/workercache"
)

// Sentinel errors so callers can branch with errors.Is, not string match.
var (
	// ErrInvalidSource: the source content hash is not a valid SHA-256, or
	// the source path is missing when production is required.
	ErrInvalidSource = errors.New("derivedassets: invalid source")

	// ErrInvalidProfile: the normalization profile is incomplete and cannot
	// produce a safe cache key.
	ErrInvalidProfile = errors.New("derivedassets: invalid normalization profile")

	// ErrProduceFailed: the producer could not normalize the source. The
	// wrapped error carries the producer's own failure.
	ErrProduceFailed = errors.New("derivedassets: normalization failed")

	// ErrVerification: the produced file failed byte-level verification
	// (missing, empty, wrong size, or SHA-256 mismatch). It is never promoted.
	ErrVerification = errors.New("derivedassets: produced asset failed verification")

	// ErrStoreFailed: the verified asset could not be registered in the
	// canonical cache.
	ErrStoreFailed = errors.New("derivedassets: cache store failed")
)

// Produced is the materialized result of one Producer run. The producer owns
// placing the file at its final, atomically-promoted Path; it may also supply
// the SHA-256 it computed over those exact bytes. The Resolver never trusts a
// claimed hash: it re-computes SHA-256 over Path and fails closed on any
// disagreement. A nil/empty Hash means "not precomputed" and the Resolver
// computes it from the bytes.
type Produced struct {
	Path string
	Size int64
	Hash assetref.ContentHash
}

// Producer normalizes one source file into a reusable derived asset matching
// profile. Implementations run the native FramePipeline (or a test double).
// The returned file MUST be complete and flushed before return; the Resolver
// verifies it and registers it in the cache. Profile-level verification
// (codec, resolution, frame rate, pixel format) is the producer's contract:
// it must never return a file that does not match the requested profile.
type Producer func(ctx context.Context, sourcePath string, profile workercache.NormalizationProfile) (Produced, error)

// Result is the outcome of one Resolve call. Hit reports whether the entry
// came from the cache (true) or was produced and promoted in this call
// (false). Entry is always the complete, usable cache entry.
type Result struct {
	Key   assetref.AssetKey
	Entry workercache.Entry
	Hit   bool
}

// Resolver resolves derived assets against the canonical worker cache.
// It is a thin orchestration layer: storage, leases and eviction remain owned
// by the underlying workercache.DerivedAssetStore.
type Resolver struct {
	store   workercache.DerivedAssetStore
	produce Producer
}

// NewResolver builds a Resolver over store with the given Producer. Both
// dependencies are required: a resolver with a nil store or producer is a
// fail-open wiring error, so it is rejected here.
func NewResolver(store workercache.DerivedAssetStore, produce Producer) (*Resolver, error) {
	if store == nil {
		return nil, errors.New("derivedassets: store must not be nil")
	}
	if produce == nil {
		return nil, errors.New("derivedassets: producer must not be nil")
	}
	return &Resolver{store: store, produce: produce}, nil
}

// Resolve returns the cached derived asset for (sourceHash, profile), or
// produces, verifies and atomically promotes it on a miss. It is safe for
// concurrent callers: a duplicate promotion is a benign race that re-reads
// the winning entry and reports it as a hit.
func (r *Resolver) Resolve(ctx context.Context, sourceHash assetref.ContentHash, sourcePath string, profile workercache.NormalizationProfile) (Result, error) {
	if r == nil {
		return Result{}, errors.New("derivedassets: nil resolver")
	}
	if _, err := assetref.ParseContentHash(string(sourceHash)); err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrInvalidSource, err)
	}
	if err := profile.Validate(); err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrInvalidProfile, err)
	}
	key, err := workercache.DerivedAssetKey(sourceHash, profile)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %v", ErrInvalidProfile, err)
	}

	// HIT: a complete derived asset already exists; return it without
	// invoking the producer. FindDerived already skips incomplete rows so a
	// half-promoted crash can never be served to a render.
	if entry, found, err := r.store.FindDerived(ctx, sourceHash, profile); err != nil {
		return Result{}, err
	} else if found {
		return Result{Key: key, Entry: entry, Hit: true}, nil
	}

	// MISS: production requires a real source file to read.
	if sourcePath == "" {
		return Result{}, fmt.Errorf("%w: source path is empty", ErrInvalidSource)
	}

	produced, err := r.produce(ctx, sourcePath, profile)
	if err != nil {
		return Result{}, fmt.Errorf("%w: %w", ErrProduceFailed, err)
	}

	verified, err := verifyProduced(produced)
	if err != nil {
		removeProduced(produced)
		return Result{}, err
	}

	entry := workercache.Entry{
		AssetKey:         key,
		ContentHash:      verified.hash,
		LocalPath:        produced.Path,
		SizeBytes:        verified.size,
		DownloadComplete: true,
	}
	if _, err := r.store.StoreDerived(ctx, sourceHash, profile, entry); err != nil {
		removeProduced(produced)
		if errors.Is(err, workercache.ErrDuplicate) {
			// A concurrent producer won the race. Reuse its entry rather
			// than discarding an already-verified asset.
			if concurrent, found, findErr := r.store.FindDerived(ctx, sourceHash, profile); findErr != nil {
				return Result{}, findErr
			} else if found {
				return Result{Key: key, Entry: concurrent, Hit: true}, nil
			}
		}
		return Result{}, fmt.Errorf("%w: %v", ErrStoreFailed, err)
	}

	return Result{Key: key, Entry: entry, Hit: false}, nil
}

type verifiedProduced struct {
	hash assetref.ContentHash
	size int64
}

// verifyProduced re-checks the produced bytes at the Resolver boundary. It
// never trusts Produced.Size or Produced.Hash: the file on disk is the only
// source of truth.
func verifyProduced(produced Produced) (verifiedProduced, error) {
	if produced.Path == "" {
		return verifiedProduced{}, fmt.Errorf("%w: produced path is empty", ErrVerification)
	}
	info, err := os.Stat(produced.Path)
	if err != nil {
		return verifiedProduced{}, fmt.Errorf("%w: stat produced asset: %v", ErrVerification, err)
	}
	if !info.Mode().IsRegular() {
		return verifiedProduced{}, fmt.Errorf("%w: produced path is not a regular file", ErrVerification)
	}
	if info.Size() <= 0 {
		return verifiedProduced{}, fmt.Errorf("%w: produced asset is empty", ErrVerification)
	}
	if produced.Size > 0 && produced.Size != info.Size() {
		return verifiedProduced{}, fmt.Errorf("%w: produced size mismatch (got %d, want %d)", ErrVerification, info.Size(), produced.Size)
	}

	hash, err := hashFile(produced.Path)
	if err != nil {
		return verifiedProduced{}, fmt.Errorf("%w: hash produced asset: %v", ErrVerification, err)
	}
	if produced.Hash != "" {
		claimed, err := assetref.ParseContentHash(string(produced.Hash))
		if err != nil {
			return verifiedProduced{}, fmt.Errorf("%w: producer returned invalid hash: %v", ErrVerification, err)
		}
		if hash != claimed {
			return verifiedProduced{}, fmt.Errorf("%w: produced SHA-256 mismatch", ErrVerification)
		}
	}
	return verifiedProduced{hash: hash, size: info.Size()}, nil
}

// removeProduced is best-effort fail-closed hygiene: a produced file that is
// not registered must not linger on disk as an orphan.
func removeProduced(produced Produced) {
	if produced.Path != "" {
		_ = os.Remove(produced.Path)
	}
}

// hashFile computes the lowercase hex SHA-256 of the file at path.
func hashFile(path string) (assetref.ContentHash, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return assetref.ContentHash(hex.EncodeToString(h.Sum(nil))), nil
}
