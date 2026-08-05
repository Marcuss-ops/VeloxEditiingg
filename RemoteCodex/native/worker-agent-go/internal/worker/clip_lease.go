// Package worker — ClipLease is the per-job acquisition/release
// helper that owns the cached-asset lease lifecycle around asset-using
// jobs (Chronon render + clip-driven executions).
//
// This is Pass 9 of the Velox Asset Cache & Protected-Asset Snapshot
// feature (see the design spec in the user's master plan):
//
//	1. Before render starts: call cache.Acquire(jobID, driveID) for
//	   every Drive clip the job will read.
//	2. After render completes — SUCCESS OR FAILURE — call
//	   cache.Release(jobID, driveID) for every acquired clip.
//	3. defer is the idiomatic shape, so a panic or returned error
//	   in step 2 still releases.
//
// Acquire vs Release contract (per workercache.Cache):
//
//   - Acquire inserts the (asset, job) relation in the authoritative
//     many-to-many lease table. Multiple jobs may hold the same asset
//     concurrently; duplicate acquisition by one job is idempotent.
//   - Release removes only the caller's relation. Releasing another
//     job's lease never clears that job's protection. The conditional
//     WHERE clause is enforced in workercache.Cache.Release.
//
// AcquireJobClips is the single entry point: it acquires all rows in
// the supplied drive-id slice in order, returning a Lease the caller
// defers ReleaseAll() on. On any mid-loop failure, the partial set is
// released before the error surfaces so a partial acquire never
// leaks (a future run could observe rows leased by an aborted job).

package worker

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"velox-shared/assetref"
	"velox-worker-agent/internal/workercache"
)

// ClipLease holds the set of (asset key, jobID) pairs acquired against
// a workercache.Cache for one job. ReleaseAll releases every entry.
// Safe to call ReleaseAll more than once — subsequent calls become
// no-ops because the per-asset Release conditional predicate matches
// zero rows after the first release, which workercache releases treats
// as benign by design.
type ClipLease struct {
	cache    *workercache.Cache
	jobID    string
	driveIDs []string
}

// DriveIDs returns a copy of the driveID slice in the order they
// were acquired. Useful for log lines and metrics labels; callers
// must NOT mutate the returned slice (defensive copy).
func (l *ClipLease) DriveIDs() []string {
	if l == nil {
		return nil
	}
	out := make([]string, len(l.driveIDs))
	copy(out, l.driveIDs)
	return out
}

// ReleaseAll releases every acquired asset. Idempotent under
// repeat-invocation. Returns the FIRST error encountered, joined
// with subsequent ones only if the caller chooses to log them; the
// loop continues past errors so a single short-lived row failure
// (e.g. ErrNotFound because someone else deleted the row between
// Acquire and Release) does not leak the rest of the lease.
//
// A nil receiver is a no-op (defensive: lets callers defer
// ReleaseAll() unconditionally on a Lease that may have failed
// during AcquireJobClips return).
func (l *ClipLease) ReleaseAll(ctx context.Context) error {
	if l == nil || l.cache == nil {
		return nil
	}
	var firstErr error
	for _, id := range l.driveIDs {
		if err := l.cache.Release(ctx, id, l.jobID); err != nil {
			if firstErr == nil {
				firstErr = fmt.Errorf("worker.ClipLease.ReleaseAll(%s): %w", id, err)
			}
			// Continue: surface the first error, but release the
			// remaining rows so the lease has a chance to complete.
		}
	}
	return firstErr
}

// AcquireJobClips takes a workercache.Cache, a jobID, and a list of
// canonical drive_file_ids, and acquires the lease on each in order.
// On any mid-loop failure, all rows acquired SO FAR are released
// before the error surfaces, so an aborted Acquire never leaks a
// partial lease. The returned error is wrapped with the failing
// driveID for triage.
//
// Preconditions:
//
//   - cache MUST be non-nil (ErrEmptyID / ErrNotFound propagate as-is).
//   - jobID MUST be non-empty (validated by workercache.Acquire).
//   - Each driveID MUST already be present in the cache AND
//     DownloadComplete=true. The resolver is responsible for
//     Store+MarkDownloadComplete before AcquireJobClips is invoked.
//     Calling Acquire on a missing row returns ErrNotFound
//     (acquire-loop rolls back partial state and surfaces this to
//     the caller).
//
// DriveID order matters for the rollback path: Acquire failures
// trigger reverse-order Release of the rows that were already
// acquired (workercache.Release is conditional on the per-row lease
// still being owned by jobID, so the rollback is guaranteed safe
// even when two AcquireJobClips calls for the same jobID overlap,
// which is impossible in practice because AcquireJobClips is called
// once per job).
func AcquireJobClips(ctx context.Context, cache *workercache.Cache, jobID string, driveIDs []string) (*ClipLease, error) {
	if cache == nil {
		return nil, fmt.Errorf("worker.AcquireJobClips: nil cache")
	}
	if jobID == "" {
		return nil, fmt.Errorf("worker.AcquireJobClips: jobID is required")
	}

	lease := &ClipLease{
		cache:    cache,
		jobID:    jobID,
		driveIDs: append([]string(nil), driveIDs...),
	}

	for _, id := range driveIDs {
		if err := cache.Acquire(ctx, id, jobID); err != nil {
			// Roll back the rows already acquired in REVERSE order.
			// Reverse order keeps the latest acquire (whose context
			// is freshest) at the back, which is the conventional
			// release-stack idiom.
			for j := len(lease.driveIDs) - 1; j >= 0; j-- {
				_ = lease.cache.Release(ctx, lease.driveIDs[j], jobID)
			}
			return nil, fmt.Errorf("worker.AcquireJobClips(%s) for %s: %w", id, jobID, err)
		}
	}
	return lease, nil
}

// extractDriveIDsFromJSON re-marshals `payload` (a map-shaped decoded
// TaskSpec) into JSON and runs Pass 4's canonical assetref extractor
// on it. The extracted IDs are returned in sorted-deterministic order
// so the lease rollback path is stable across retry attempts.
//
// Re-marshaling cost is O(|payload|) per dispatch; for typical job
// payloads (≤ a few kB) this is negligible relative to the
// render-time work. The clean alternative — adding a RawPayload
// json.RawMessage field to executor.TaskSpec — would leak a
// Velox-specific field into the generic executor package and is
// rejected for Pass 9.
//
// Returns nil when the payload has no resolvable Drive IDs (a job
// with no clip references is a legitimate input; the caller MUST
// treat nil as "no lease to acquire, skip the defer").
func extractDriveIDsFromJSON(payload map[string]interface{}) []string {
	if payload == nil {
		return nil
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	idSet := assetref.ExtractAssetKeys(raw)
	if len(idSet) == 0 {
		return nil
	}
	out := make([]string, 0, len(idSet))
	for id := range idSet {
		out = append(out, id)
	}
	sort.Strings(out)
	return out
}
