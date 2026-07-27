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
// Pass 8 ships the user-spec signature: Resolve(ctx, driveID)
// (string, error). The companion test matrix exercises all four
// state transitions: hit, miss, incomplete, download-fail.

package workercache

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// Resolver composes a Cache, a Downloader, and the destination
// directory. The downloader's source is whatever the bootstrap
// wires in; for tests a bytes.NewReader-backed fake is sufficient.
type Resolver struct {
	Cache      *Cache
	Downloader *Downloader
	Dir        string
}

// NewResolver wires the canonical dependencies. The Downloader
// already owns DriveSource + verifyMedia; the Resolver adds the
// cache-lookup fast path on top. Panics on nil dependencies so
// silent fall-backs do not mask operator config bugs (mirrors
// the NewDownloader convention).
func NewResolver(cache *Cache, dl *Downloader) *Resolver {
	if cache == nil {
		panic("workercache.NewResolver: cache is required (nil cache)")
	}
	if dl == nil {
		panic("workercache.NewResolver: downloader is required (nil downloader)")
	}
	return &Resolver{Cache: cache, Downloader: dl, Dir: dl.dir}
}

// Resolve returns the local filesystem path for `driveID`.
//
// Resolution states:
//
//   - Cache hit (row present AND DownloadComplete=true AND file
//     exists on disk): zero-download fast path. MarkUsed bumps
//     last_used_at so the Pass 12 grace rule preserves this row
//     during subsequent Cleanup passes.
//
//   - Cache miss (no row OR file missing on disk): insert a
//     placeholder row with the `.part` local_path, then drive
//     Downloader.DownloadDriveFile. The Downloader's contract
//     (Pass 10 — atomic .part → verify → rename → MarkDownloadComplete)
//     handles the rest.
//
//   - Cache incomplete (DownloadComplete=false; e.g. a previous
//     worker crashed mid-download): treat as a cache miss. The
//     placeholder row is re-Store'd (a duplicate Store returns
//     ErrDuplicate which we ignore — concurrent resolvers).
//
//   - Download failure: the Downloader reverts the row's on-disk
//     state to download_complete=false. The Resolver returns the
//     wrapped error verbatim; callers (the worker's dispatch path)
//     decide whether to abort the job or retry on the next attempt.
//
// `now` for any internal clock use is taken from the Downloader's
// instantiated DriveSource open path; the Resolver itself does NOT
// stamp rows with time — that's the Cache's job.
func (r *Resolver) Resolve(ctx context.Context, driveID string) (string, error) {
	if driveID == "" {
		return "", ErrEmptyID
	}

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
