// Package workercache — Resolver is the worker-side high-level
// orchestrator for "give me the local path of this Drive clip":
//
//	1. If the cache has the row AND DownloadComplete=true AND the
//	   on-disk file is present, return the local path (zero bytes
//	   downloaded; MarkUsed bumps last_used_at for the Pass 12 grace
//	   rule to keep the row alive).
//	2. If the row is missing OR DownloadComplete=false OR the file
//	   is missing on disk, insert a placeholder row + drive the
//	   canonical Downloader.DownloadDriveFile pipeline (atomic
//	   `.part` → verifyMedia → os.Rename → MarkDownloadComplete).
//
// The Resolver is intentionally THIN: it composes Cache + Downloader
// already shipped in Pass 2/10. The unique logic here is the
// "cache hit fast path" and the placeholder recovery for rows
// that survived a worker crash with download_complete=false.
//
// PASS 11 — concurrent-Resolve dedup. The public Resolve method
// is wrapped in golang.org/x/sync/singleflight.Group.Do(driveID, fn):
//
//   - Two goroutines racing a cold cache for the same driveID
//     share ONE inner invocation (the slower one's fn).
//   - All callers receive THE SAME (path, err) result. If the
//     first caller's fn returns ErrSourceOpen, every caller
//     sees ErrSourceOpen — singleflight's documented semantic.
//   - The inner fn runs with the FIRST caller's ctx. If that
//     caller cancels before subsequent callers enter, fn sees
//     ctx.Err and returns; the subsequent callers wait but
//     inherit the same error.
//   - The dedupe key is the canonical Drive ID; callers are
//     expected to have already normalised URLs (see
//     DataServer/internal/assetref.DriveFileID).
//
// PASS 11 — 3-min grace + 2-min stale-snapshot guard already
// shipped in cleanup_policy.go (was Pass 12 in earlier numbering);
// the user-spec commit message groups all three but this file's
// delta is the singleflight integration only.

package workercache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"golang.org/x/sync/singleflight"
)

// Resolver composes a Cache, a Downloader, and the destination
// directory. The downloader's source is whatever the bootstrap
// wires in; for tests a bytes-source fake is sufficient.
//
// The embedded `sf` deduplicates concurrent Resolve(driveID)
// calls via the standard singleflight.Group API. Two concurrent
// goroutines calling Resolve on the same driveID race a single
// resolveInner invocation; subsequent callers wait on the same
// (path, err) return.
type Resolver struct {
	Cache      *Cache
	Downloader *Downloader
	Dir        string

	// sf is the singleflight dedup primitive. Zero-value
	// singleflight.Group is safe; NewResolver leaves it at the
	// zero value. Field is unexported because callers have no
	// reason to interact with it directly.
	sf singleflight.Group
}

// NewResolver wires the canonical dependencies. The Downloader
// already owns DriveSource + verifyMedia; the Resolver adds the
// cache-lookup fast path + singleflight dedup on top. Panics on
// nil dependencies so silent fall-backs do not mask operator
// config bugs (mirrors the NewDownloader convention).
func NewResolver(cache *Cache, dl *Downloader) *Resolver {
	if cache == nil {
		panic("workercache.NewResolver: cache is required (nil cache)")
	}
	if dl == nil {
		panic("workercache.NewResolver: downloader is required (nil downloader)")
	}
	return &Resolver{Cache: cache, Downloader: dl, Dir: dl.dir}
}

// Resolve returns the local filesystem path for `driveID`. The
// entire body is wrapped in singleflight so two concurrent
// callers race a single inner invocation.
//
// Resolution states (preserved from Pass 8):
//
//   - Cache hit (row present AND DownloadComplete=true AND file
//     exists on disk): zero-download fast path. MarkUsed bumps
//     last_used_at so the Pass 11/12 grace rule preserves this row
//     during subsequent Cleanup passes.
//
//   - Cache miss (no row OR file missing on disk): insert a
//     placeholder row with the `.part` local_path, then drive
//     Downloader.DownloadDriveFile.
//
//   - Cache incomplete (DownloadComplete=false; e.g. a previous
//     worker crashed mid-download): treat as a cache miss.
//
//   - Download failure: the Downloader reverts the row's on-disk
//     state to download_complete=false. The Resolver returns the
//     wrapped error verbatim; callers (the worker's dispatch path)
//     decide whether to abort the job or retry on the next attempt.
func (r *Resolver) Resolve(ctx context.Context, driveID string) (string, error) {
	if driveID == "" {
		return "", ErrEmptyID
	}

	// Dedupe key is the canonical Drive ID. We use the worker's
	// pre-normalised form (assetref.DriveFileID) so two callers
	// with semantically-equal URLs hit the same dedupe key.
	raw, err, _ := r.sf.Do(driveID, func() (interface{}, error) {
		return r.resolveInner(ctx, driveID)
	})
	if err != nil {
		return "", err
	}
	return raw.(string), nil
}

// resolveInner is the original Resolve body from Pass 8. It runs
// AT MOST ONCE per driveID per coalescing window when N concurrent
// goroutines call Resolve(driveID) — singleflight handles the
// sharing.
//
// Kept private so the singleflight wrapper is the only public
// entry point. Tests drive this method's branches via the public
// Resolve; the singleflight tests verify the dedup behaviour at
// the public surface.
//
// Pass 12 's grace / snapshot-stale guards live in
// CleanupWithPolicy (cleanup_policy.go) and read this Resolver's
// output via the CleanupLoop → SnapshotSource adapter — they are
// NOT reimplemented here.
func (r *Resolver) resolveInner(ctx context.Context, driveID string) (string, error) {
	entry, ok, err := r.Cache.Find(ctx, driveID)
	if err != nil {
		return "", fmt.Errorf("workercache.Resolve(%s): cache.Find: %w", driveID, err)
	}

	if ok && entry.DownloadComplete && fileExists(entry.LocalPath) {
		// Cache hit fast path. MarkUsed is best-effort; a SQLite
		// error here must not mask the resolved path (the cleanup
		// loop can rebuild last_used_at via MarkDownloadComplete
		// on the next refresh).
		if mErr := r.Cache.MarkUsed(ctx, driveID); mErr != nil {
			// Fall through with a slightly verbose logging
			// surface — the worker logs the err from the wider
			// dispatch, not here (workercache stays log-free).
			_ = mErr
		}
		return entry.LocalPath, nil
	}

	// Cache miss OR incomplete OR on-disk missing: drive the
	// Downloader. Ensure a placeholder row exists so the
	// Downloader's MarkDownloadComplete call (Pass 10) lands on a
	// valid row.
	if !ok {
		partPath := r.partPathFor(driveID)
		if sErr := r.Cache.Store(ctx, Entry{
			DriveFileID: driveID,
			LocalPath:   partPath,
		}); sErr != nil && !errors.Is(sErr, ErrDuplicate) {
			return "", fmt.Errorf("workercache.Resolve(%s): cache.Store: %w", driveID, sErr)
		}
	} else if !entry.DownloadComplete {
		// Row exists but download_complete=false — the placeholder
		// local_path was either the .part or a partially renamed
		// file. Re-Store is benign thanks to ErrDuplicate handling,
		// but the Downloader expects local_path to be the .part
		// path; we update only when it's stale to avoid clobbering
		// recovery state.
		if !strings.HasSuffix(entry.LocalPath, ".part") {
			partPath := r.partPathFor(driveID)
			if sErr := r.Cache.Store(ctx, Entry{
				DriveFileID:      driveID,
				LocalPath:        partPath,
				DownloadComplete: false,
				CreatedAt:        entry.CreatedAt,
				LastUsedAt:       entry.LastUsedAt,
			}); sErr != nil && !errors.Is(sErr, ErrDuplicate) {
				return "", fmt.Errorf("workercache.Resolve(%s): recover-placeholder: %w", driveID, sErr)
			}
		}
	} else {
		// DownloadComplete=true but file is missing on disk. Mark
		// for re-acquisition by flipping the row back to .part
		// placeholder + download_complete=false via UPDATEs.
		if _, mErr := r.Cache.DB().ExecContext(ctx,
			`UPDATE cached_assets SET local_path = ?, download_complete = 0
			 WHERE drive_file_id = ?`,
			r.partPathFor(driveID), driveID); mErr != nil {
			return "", fmt.Errorf("workercache.Resolve(%s): recover-file-missing: %w", driveID, mErr)
		}
	}

	finalPath, dlErr := r.Downloader.DownloadDriveFile(ctx, driveID)
	if dlErr != nil {
		return "", fmt.Errorf("workercache.Resolve(%s): downloader: %w", driveID, dlErr)
	}
	return finalPath, nil
}

// partPathFor is the canonical `<dir>/<driveID>.mp4.part` path the
// Downloader expects on its placeholder rows. Mirrors the
// convention documented in cache.go's MarkDownloadComplete godoc.
func (r *Resolver) partPathFor(driveID string) string {
	return filepath.Join(r.Dir, driveID+".mp4.part")
}

// fileExists is a thin wrapper around os.Stat so the Resolve hot
// path is a single syscall. Errors other than ErrNotExist are
// tolerated as "file may be unavailable"; the row will fall through
// to the cache-miss branch and re-download.
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
