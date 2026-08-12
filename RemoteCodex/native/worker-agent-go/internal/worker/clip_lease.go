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
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"velox-shared/assetref"
	"velox-shared/contract"
	"velox-worker-agent/internal/workercache"
)

// ClipLease holds the set of (asset key, jobID) pairs acquired against
// a workercache.Cache for one job. ReleaseAll releases every entry.
// Safe to call ReleaseAll more than once — subsequent calls become
// no-ops because the per-asset Release conditional predicate matches
// zero rows after the first release, which workercache releases treats
// as benign by design.
type ClipLease struct {
	cache     *workercache.Cache
	jobID     string
	assetKeys []string
}

// AssetKeys returns a copy of the asset-key slice in the order they
// were acquired. Useful for log lines and metrics labels; callers
// must NOT mutate the returned slice (defensive copy).
func (l *ClipLease) AssetKeys() []string {
	if l == nil {
		return nil
	}
	out := make([]string, len(l.assetKeys))
	copy(out, l.assetKeys)
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
	cleanupCtx := leaseCleanupContext(ctx)
	var firstErr error
	for _, id := range l.assetKeys {
		var err error
		for attempt := 0; attempt < 3; attempt++ {
			err = l.cache.Release(cleanupCtx, id, l.jobID)
			if err == nil || errors.Is(err, workercache.ErrNotFound) || attempt == 2 {
				break
			}
			time.Sleep(time.Duration(attempt+1) * 10 * time.Millisecond)
		}
		if err != nil {
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
// canonical asset keys, and acquires the lease on each in order.
// On any mid-loop failure, all rows acquired SO FAR are released
// before the error surfaces, so an aborted Acquire never leaks a
// partial lease. The returned error is wrapped with the failing
// key for triage.
//
// Preconditions:
//
//   - cache MUST be non-nil (ErrEmptyID / ErrNotFound propagate as-is).
//   - jobID MUST be non-empty (validated by workercache.Acquire).
//   - Each key MUST already be present in the cache AND
//     DownloadComplete=true. The resolver is responsible for
//     Store+MarkDownloadComplete before AcquireJobClips is invoked.
//     Calling Acquire on a missing row returns ErrNotFound
//     (acquire-loop rolls back partial state and surfaces this to
//     the caller).
//
// Key order matters for the rollback path: Acquire failures
// trigger reverse-order Release of the rows that were already
// acquired (workercache.Release is conditional on the per-row lease
// still being owned by jobID, so the rollback is guaranteed safe
// even when two AcquireJobClips calls for the same jobID overlap,
// which is impossible in practice because AcquireJobClips is called
// once per job).
func AcquireJobClips(ctx context.Context, cache *workercache.Cache, jobID string, assetKeys []string) (*ClipLease, error) {
	if cache == nil {
		return nil, fmt.Errorf("worker.AcquireJobClips: nil cache")
	}
	if jobID == "" {
		return nil, fmt.Errorf("worker.AcquireJobClips: jobID is required")
	}

	lease := &ClipLease{
		cache: cache,
		jobID: jobID,
	}

	for _, id := range assetKeys {
		if err := cache.Acquire(ctx, id, jobID); err != nil {
			// Roll back the rows already acquired in REVERSE order.
			// Reverse order keeps the latest acquire (whose context
			// is freshest) at the back, which is the conventional
			// release-stack idiom.
			cleanupCtx := leaseCleanupContext(ctx)
			for j := len(lease.assetKeys) - 1; j >= 0; j-- {
				_ = lease.cache.Release(cleanupCtx, lease.assetKeys[j], jobID)
			}
			return nil, fmt.Errorf("worker.AcquireJobClips(%s) for %s: %w", id, jobID, err)
		}
		// Track only successful acquisitions so rollback never attempts
		// to release rows this call did not own.
		lease.assetKeys = append(lease.assetKeys, id)
	}
	return lease, nil
}

// leaseCleanupContext deliberately detaches lease cleanup from the task
// lifetime. A timed-out/canceled render must still release its durable
// protection rows; passing the canceled task context to SQLite would make
// cleanup fail before the DELETE executes.
func leaseCleanupContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return context.WithoutCancel(ctx)
}

// extractAssetKeysFromJSON re-marshals `payload` (a map-shaped decoded
// TaskSpec) into JSON and runs the canonical assetref extractor on it. It
// also inspects the stringified compiled_render_plan_json envelope: V2 asset
// identities are bare asset_id fields, so they are not visible to the legacy
// URL/velox-reference walker. The two sets are unioned before sorting.
//
// Re-marshaling cost is O(|payload|) per dispatch; for typical job
// payloads (≤ a few kB) this is negligible relative to render-time work.
// The lease path deliberately reads the canonical plan but never mutates it
// or adds local paths to it.
//
// Returns nil when the payload has no resolvable asset keys (a job
// with no clip references is a legitimate input; the caller MUST
// treat nil as "no lease to acquire, skip the defer").
func extractAssetKeysFromJSON(payload map[string]interface{}) []string {
	if payload == nil {
		return nil
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return nil
	}
	idSet := assetref.ExtractAssetKeys(raw)

	// V2 is transported as a canonical JSON string inside the task payload.
	// Walk only that document and collect exact asset_id fields; this covers
	// assets[], final_audio.asset_id and every video segment without coupling
	// the lease layer to a particular track nesting shape.
	if compiledRaw, ok := payload[contract.PayloadKeyCompiledRenderPlanJSON].(string); ok {
		var compiledPlan interface{}
		if json.Unmarshal([]byte(compiledRaw), &compiledPlan) == nil {
			collectCompiledPlanAssetIDs(compiledPlan, idSet)
		}
	}

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

// collectCompiledPlanAssetIDs adds V2's canonical asset identities to the
// lease set. A wire URI is normalized to its cache identity when encountered
// defensively; V2 normally carries bare asset IDs. Invalid/empty values are
// ignored here and rejected by the V2 validator before dispatch.
func collectCompiledPlanAssetIDs(value interface{}, ids map[string]struct{}) {
	switch typed := value.(type) {
	case map[string]interface{}:
		// AssetKey is the canonical cache/lease identity when present. The
		// surrounding plan may still refer to the asset by AssetID, but the
		// cache row and the resolver use one identity and must not diverge.
		identity := ""
		if rawKey, ok := typed["asset_key"].(string); ok {
			identity = strings.TrimSpace(rawKey)
		}
		if identity == "" {
			if rawID, ok := typed["asset_id"].(string); ok {
				identity = strings.TrimSpace(rawID)
			}
		}
		if wireID, isWire := assetref.WireAssetID(identity); isWire {
			identity = wireID
		}
		if identity != "" {
			ids[identity] = struct{}{}
		}
		for _, child := range typed {
			collectCompiledPlanAssetIDs(child, ids)
		}
	case []interface{}:
		for _, child := range typed {
			collectCompiledPlanAssetIDs(child, ids)
		}
	}
}
