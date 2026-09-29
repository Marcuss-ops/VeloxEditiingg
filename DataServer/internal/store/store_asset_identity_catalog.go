package store

import (
	"context"
	"database/sql"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// AssetIdentity is a worker-verified content identity indexed by its stable
// source asset ID. The locator remains the source; SHA-256 is the identity.
type AssetIdentity struct {
	SHA256    string
	SizeBytes int64
}

// RecordAssetIdentity stores the identity reported after the worker has
// downloaded and verified a deferred source asset.
func (s *SQLiteStore) RecordAssetIdentity(ctx context.Context, assetID, workerID, sha256 string, sizeBytes int64) error {
	if s == nil || s.db == nil {
		return fmt.Errorf("asset identity catalog: store is not initialized")
	}
	assetID, workerID, sha256 = strings.TrimSpace(assetID), strings.TrimSpace(workerID), strings.ToLower(strings.TrimSpace(sha256))
	if assetID == "" || workerID == "" || sizeBytes <= 0 || len(sha256) != 64 {
		return fmt.Errorf("asset identity catalog: incomplete identity")
	}
	if _, err := hex.DecodeString(sha256); err != nil {
		return fmt.Errorf("asset identity catalog: malformed SHA-256")
	}
	if _, err := s.db.ExecContext(ctx, `INSERT INTO asset_identity_catalog(asset_id,sha256,size_bytes,verified_at,worker_id)
VALUES(?,?,?,?,?) ON CONFLICT(asset_id) DO UPDATE SET sha256=excluded.sha256,size_bytes=excluded.size_bytes,verified_at=excluded.verified_at,worker_id=excluded.worker_id`, assetID, sha256, sizeBytes, time.Now().UTC().Format(time.RFC3339Nano), workerID); err != nil {
		return fmt.Errorf("asset identity catalog: record %q: %w", assetID, err)
	}
	return nil
}

// LookupAssetIdentity returns the latest worker-verified identity for a
// stable source asset ID. A missing catalog entry is not an error.
func (s *SQLiteStore) LookupAssetIdentity(ctx context.Context, assetID string) (AssetIdentity, bool, error) {
	if s == nil || s.db == nil {
		return AssetIdentity{}, false, fmt.Errorf("asset identity catalog: store is not initialized")
	}
	var identity AssetIdentity
	// Drive file IDs can be overwritten. Treat an identity as reusable only
	// for a short freshness window; older locators return to deferred
	// resolution instead of making stale integrity metadata authoritative.
	freshAfter := time.Now().UTC().Add(-24 * time.Hour).Format(time.RFC3339Nano)
	err := s.db.QueryRowContext(ctx, `SELECT sha256,size_bytes FROM asset_identity_catalog WHERE asset_id=? AND julianday(verified_at) >= julianday(?)`, strings.TrimSpace(assetID), freshAfter).Scan(&identity.SHA256, &identity.SizeBytes)
	if err == sql.ErrNoRows {
		return AssetIdentity{}, false, nil
	}
	if err != nil {
		return AssetIdentity{}, false, err
	}
	return identity, true, nil
}
