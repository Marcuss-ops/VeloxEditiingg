// Package api — protected-asset snapshot (Pass 7).
//
// The master exposes GET /api/v1/workers/cache/protected-assets
// (delivered in Pass 6 of the master plan). This file mirrors the
// JSON shape into the worker's HTTP client so the worker poller
// (internal/worker/protected_assets_poller.go) can fetch snapshots
// over the existing `*Client.doRequest` retry + circuit-breaker
// path without any ad-hoc transport setup.
//
// SECURITY posture:
//   - The DTO is a SEPARATE mirror of
//     DataServer/internal/protectedasset.ProtectedAssetSnapshot.
//     The two Go modules cannot import each other directly, so
//     the wire shape is pinned by the happy-path test
//     (protected_assets_test.go) — any drift there MUST be
//     reflected here in the same atomic commit (same pattern as
//     pkg/api/workers_read.go).
//   - Authentication reuses *Client.authToken via the standard
//     Bearer header (SetAuthToken after worker registration).
//   - No new query params, no undocumented headers, no transport
//     concerns beyond what doRequest already provides.

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

// ProtectedAssetSnapshot mirrors the master JSON shape returned
// by GET /api/v1/workers/cache/protected-assets.
//
// JSON tags pin the wire shape byte-for-byte. Field order is
// chosen to mirror the master so a snapshot printed by the master
// reads naturally in worker logs.
type ProtectedAssetSnapshot struct {
	// Version is a master-monotonic counter. Workers should treat
	// strictly-decreasing versions (across consecutive successful
	// polls) as a clock-skew signal — the master restarted its
	// snapshot service. The poller accepts both equal and
	// strictly-increasing versions; replaying an older snapshot
	// is harmless because the protected set is a superset check.
	Version uint64 `json:"version"`

	// GeneratedAt is the RFC3339 instant the master stamped the
	// snapshot. Stored as string (not time.Time) so the DTO has
	// zero dependency on the master's time internals and the
	// parser at the call site can choose tolerant / strict
	// semantics. The poller parses this to time.Time ONLY on the
	// read side; the wire value stays a string.
	GeneratedAt string `json:"generated_at"`

	// LookaheadJobs is the number of jobs the master actually
	// inspected while building the snapshot (NOT the configured
	// VELOX_CACHE_LOOKAHEAD_JOBS limit). A value smaller than
	// the configured limit signals a low-traffic moment when the
	// queue was short enough to read every job.
	LookaheadJobs int `json:"lookahead_jobs"`

	// DriveFileIDs is the deduplicated, canonical (assetref.DriveFileID)
	// set of file IDs the master considers protected for the
	// next ~30s. Empty slice is a legitimate "no jobs in queue"
	// response; the worker cleanup loop must NOT interpret an
	// empty slice as "evict everything not actively leased"
	// (see Pass 12 CleanupPolicy / RecentUseGrace).
	DriveFileIDs       []string `json:"drive_file_ids"`
	ProtectedAssetKeys []string `json:"protected_asset_keys"`
}

// ProtectedAssetsAPI is the stable interface for the snapshot GET.
// Consumers depend on this interface rather than the *Client
// concrete type so the implementation can be swapped (real client,
// httptest mock, fake) without breaking callers.
type ProtectedAssetsAPI interface {
	GetProtectedAssets(ctx context.Context) (*ProtectedAssetSnapshot, error)
}

// Compile-time guard: *Client must satisfy ProtectedAssetsAPI.
// Catches future refactors at build time, not via runtime type
// assertion deep in a worker goroutine.
var _ ProtectedAssetsAPI = (*Client)(nil)

// ProtectedAssetsPath is the canonical path the master publishes.
// Centralised so a future v2 rename only touches one place.
const ProtectedAssetsPath = "/api/v1/workers/cache/protected-assets"

// GetProtectedAssets fetches the most recent protected-asset
// snapshot from the master.
//
// Honors *Client's:
//   - authToken via SetAuthToken (Bearer header)
//   - retry policy via WithRetry (default: no retry)
//   - circuit breaker via WithCircuitBreaker (default: enabled)
//
// Returns:
//   - (*ProtectedAssetSnapshot, nil) on 2xx + valid JSON.
//   - (nil, error) otherwise. The error wraps the underlying
//     *Client.doRequest error so callers can use errors.Is to
//     classify (network / 5xx / 4xx / JSON decode).
func (c *Client) GetProtectedAssets(ctx context.Context) (*ProtectedAssetSnapshot, error) {
	body, err := c.doRequest(ctx, http.MethodGet, ProtectedAssetsPath, nil)
	if err != nil {
		return nil, err
	}
	var snap ProtectedAssetSnapshot
	if err := json.Unmarshal(body, &snap); err != nil {
		return nil, fmt.Errorf("decode protected assets snapshot: %w", err)
	}
	return &snap, nil
}
