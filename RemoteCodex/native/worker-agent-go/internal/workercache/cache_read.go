package workercache

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"velox-shared/assetref"
)

func (c *Cache) Find(ctx context.Context, assetKey string) (Entry, bool, error) {
	if assetKey == "" {
		return Entry{}, false, ErrEmptyID
	}
	row := c.db.QueryRowContext(ctx,
		`SELECT `+selectCols+selectFrom+` WHERE a.asset_key = ?`,
		assetKey)
	e, err := scanEntry(row)
	if errors.Is(err, ErrNotFound) {
		return Entry{}, false, nil
	}
	if err != nil {
		return Entry{}, false, fmt.Errorf("workercache.Find(%q): %w", assetKey, err)
	}
	if e.DownloadComplete {
		if !validStoredContentHash(e.storedContentHash) {
			if invalidateErr := c.invalidateStoredBlob(ctx, e.storedContentHash); invalidateErr != nil && !errors.Is(invalidateErr, ErrNotFound) && !errors.Is(invalidateErr, ErrBlobInFlight) && !errors.Is(invalidateErr, ErrBlobProtected) {
				return Entry{}, false, fmt.Errorf("workercache.Find(%q): invalidate invalid hash: %w", assetKey, invalidateErr)
			}
			e.LocalPath = ""
			e.SizeBytes = 0
			e.DownloadComplete = false
			return *e, true, nil
		}
		valid, validationErr := c.validatePhysicalPath(e.LocalPath, e.SizeBytes)
		if validationErr != nil {
			return Entry{}, false, fmt.Errorf("workercache.Find(%q): validate blob: %w", assetKey, validationErr)
		}
		if !valid {
			if invalidateErr := c.invalidateStoredBlob(ctx, e.storedContentHash); invalidateErr != nil && !errors.Is(invalidateErr, ErrNotFound) && !errors.Is(invalidateErr, ErrBlobInFlight) && !errors.Is(invalidateErr, ErrBlobProtected) {
				return Entry{}, false, fmt.Errorf("workercache.Find(%q): invalidate blob: %w", assetKey, invalidateErr)
			}
			// Keep asset_key → content_hash metadata, but turn this lookup into
			// a miss so the resolver downloads and promotes a fresh blob.
			e.LocalPath = ""
			e.SizeBytes = 0
			e.DownloadComplete = false
		}
	}
	return *e, true, nil
}

// FindBlob returns the physical blob for a verified content hash. It is the
// content-addressed lookup a resolver uses when the asset_key mapping is
// unknown: a known SHA probes cached_blobs directly, so an asset whose bytes
// are already cached under another asset ID is still found. The boolean is
// false (with a zero Blob) when no blob exists, mirroring Find.
func (c *Cache) FindBlob(ctx context.Context, contentHash assetref.ContentHash) (Blob, bool, error) {
	if contentHash == "" {
		return Blob{}, false, nil
	}
	var (
		b         Blob
		dlInt     int
		createdS  string
		usedS     string
		verifiedS string
	)
	err := c.db.QueryRowContext(ctx,
		`SELECT content_hash, local_path, size_bytes, download_complete, created_at, last_used_at, verified_at
		   FROM cached_blobs WHERE content_hash = ?`,
		string(contentHash),
	).Scan(&b.ContentHash, &b.LocalPath, &b.SizeBytes, &dlInt, &createdS, &usedS, &verifiedS)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return Blob{}, false, nil
		}
		return Blob{}, false, fmt.Errorf("workercache.FindBlob(%q): %w", contentHash, err)
	}
	b.DownloadComplete = dlInt != 0
	if b.CreatedAt, err = parseRFC3339Nano(createdS); err != nil {
		return Blob{}, false, fmt.Errorf("workercache.FindBlob(%q): created_at: %w", contentHash, err)
	}
	if b.LastUsedAt, err = parseRFC3339Nano(usedS); err != nil {
		return Blob{}, false, fmt.Errorf("workercache.FindBlob(%q): last_used_at: %w", contentHash, err)
	}
	if b.VerifiedAt, err = parseRFC3339Nano(verifiedS); err != nil {
		return Blob{}, false, fmt.Errorf("workercache.FindBlob(%q): verified_at: %w", contentHash, err)
	}
	valid, validationErr := c.ValidateBlobForRead(ctx, b)
	if validationErr != nil {
		return Blob{}, false, fmt.Errorf("workercache.FindBlob(%q): validate blob: %w", contentHash, validationErr)
	}
	if !b.DownloadComplete || !valid {
		if invalidateErr := c.InvalidateCorruptBlob(ctx, contentHash); invalidateErr != nil && !errors.Is(invalidateErr, ErrNotFound) && !errors.Is(invalidateErr, ErrBlobInFlight) && !errors.Is(invalidateErr, ErrBlobProtected) {
			return Blob{}, false, fmt.Errorf("workercache.FindBlob(%q): invalidate blob: %w", contentHash, invalidateErr)
		}
		return Blob{}, false, nil
	}
	return b, true, nil
}

// Store inserts a new entry. Returns ErrDuplicate if asset_key is
// already present; callers should treat this as a benign race (a
// concurrent Resolve already wrote the entry) and reload it via
// Find. CreatedAt and LastUsedAt default to time.Now().UTC() if
// zero. LocalPath is required.
