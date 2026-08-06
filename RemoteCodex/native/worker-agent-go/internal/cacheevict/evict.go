// Package cacheevict plans and executes narrowly-scoped worker asset-cache
// removals. It never removes a directory and never follows symlinks.
package cacheevict

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"velox-worker-agent/internal/workercache"
)

var (
	ErrNoAssetIDs        = errors.New("cacheevict: at least one asset ID is required")
	ErrInvalidID         = errors.New("cacheevict: invalid asset ID")
	ErrUnsafeRoot        = errors.New("cacheevict: cache root must be an absolute directory other than filesystem root")
	ErrActiveLease       = errors.New("cacheevict: cached asset has an active lease")
	ErrActiveReservation = errors.New("cacheevict: cached asset has an active reservation")
	ErrExecutionNoIndex  = errors.New("cacheevict: --execute requires a cache index")
)

var assetIDPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]{0,127}$`)
var shaPrefixPattern = regexp.MustCompile(`^[0-9a-f]{12}$`)

// Options configures a selective eviction operation.
type Options struct {
	// Root is the explicit worker asset-cache root. The tool only scans below
	// this directory and never derives a deletion root from an asset ID.
	Root string
	// AssetIDs is the explicit allow-list. IDs are de-duplicated before work.
	AssetIDs []string
	// Index is optional for dry-runs. When supplied, it is the durable
	// workercache SQLite index and active leases are checked before deletion.
	Index *workercache.Cache
	// Execute must be true to remove anything. False produces a plan only.
	// Execute requires Index so deletion can be atomically fenced against leases.
	Execute bool
}

// Item is the machine-readable result for one requested asset ID.
type Item struct {
	AssetID      string   `json:"asset_id"`
	Paths        []string `json:"paths"`
	Bytes        int64    `json:"bytes"`
	IndexPresent bool     `json:"index_present"`
	ActiveJobID  string   `json:"active_job_id,omitempty"`
	IndexPath    string   `json:"-"`
	CacheStatus  string   `json:"cache_status"`
	Action       string   `json:"action"`
	Reason       string   `json:"reason,omitempty"`
}

// Run validates the request, plans exact cache files, and optionally removes
// them. It is safe by default: Execute=false cannot mutate the filesystem or
// index. A requested asset with an active durable lease blocks the entire
// operation before any deletion starts.
func Run(ctx context.Context, opts Options) ([]Item, error) {
	if opts.Execute && opts.Index == nil {
		return nil, ErrExecutionNoIndex
	}
	ids, err := normalizeIDs(opts.AssetIDs)
	if err != nil {
		return nil, err
	}
	root, err := validateRoot(opts.Root)
	if err != nil {
		return nil, err
	}

	items := make([]Item, 0, len(ids))
	for _, id := range ids {
		item := Item{AssetID: id, Action: "none"}
		paths, bytes, err := discover(root, id)
		if err != nil {
			return nil, err
		}
		item.Paths = paths
		item.Bytes = bytes
		item.CacheStatus = "miss"
		if len(paths) > 0 {
			item.CacheStatus = "present"
		}
		if opts.Index != nil {
			entry, found, err := opts.Index.Find(ctx, id)
			if err != nil {
				return nil, fmt.Errorf("inspect cache index %q: %w", id, err)
			}
			item.IndexPresent = found
			if found {
				item.ActiveJobID = entry.ActiveJobID
				if entry.LocalPath != "" {
					item.IndexPath, err = filepath.Abs(entry.LocalPath)
					if err != nil {
						return nil, fmt.Errorf("asset %q index path %q: %w", id, entry.LocalPath, err)
					}
				} else {
					item.IndexPath = ""
				}
				if entry.ActiveLeaseCount > 0 {
					item.Action = "blocked"
					item.Reason = "active cache lease"
					items = append(items, item)
					continue
				}
				if entry.ActiveReservationCount > 0 {
					item.Action = "blocked"
					item.Reason = "active cache reservation"
					items = append(items, item)
					continue
				}
				if entry.LocalPath != "" && !isWithin(root, entry.LocalPath) {
					return nil, fmt.Errorf("asset %q index path %q is outside cache root", id, entry.LocalPath)
				}
			}
		}
		if !opts.Execute {
			item.Action = "dry_run"
			item.Reason = "no files or index rows will be changed"
		} else if len(paths) == 0 && !item.IndexPresent {
			item.Action = "missing"
			item.Reason = "no exact cache entry found"
		} else {
			item.Action = "deleted"
		}
		items = append(items, item)
	}

	if !opts.Execute {
		return items, nil
	}
	for i := range items {
		if items[i].Action != "blocked" {
			continue
		}
		if items[i].Reason == "active cache reservation" {
			return items, fmt.Errorf("%w: asset %q has an active cache reservation", ErrActiveReservation, items[i].AssetID)
		}
		return items, fmt.Errorf("%w: asset %q has active cache lease %q", ErrActiveLease, items[i].AssetID, items[i].ActiveJobID)
	}
	for i := range items {
		item := &items[i]
		// Evict the durable index and its indexed file under the same fenced
		// lease/reservation predicate used by the canonical cleanup policy.
		// Additional exact matches are removed only after the index fence.
		if item.IndexPresent {
			if item.IndexPath == "" {
				item.Action = "blocked"
				item.Reason = "indexed cache entry has no local path"
				return items, fmt.Errorf("asset %q index entry has no local path", item.AssetID)
			}
			if err := opts.Index.EvictIfUnleased(ctx, item.AssetID, item.IndexPath); err != nil {
				if errors.Is(err, workercache.ErrNotFound) {
					// ErrNotFound also represents a protection acquired after
					// planning. Never remove discovered files while the row is
					// still present and protected.
					entry, found, findErr := opts.Index.Find(ctx, item.AssetID)
					if findErr != nil {
						return items, findErr
					}
					if found {
						if entry.ActiveReservationCount > 0 {
							return items, fmt.Errorf("%w: asset %q became reserved during eviction", ErrActiveReservation, item.AssetID)
						}
						if entry.ActiveLeaseCount > 0 {
							return items, fmt.Errorf("%w: asset %q became leased during eviction", ErrActiveLease, item.AssetID)
						}
						return items, err
					}
					item.IndexPresent = false
				} else {
					item.Action = "blocked"
					item.Reason = err.Error()
					return items, err
				}
			}
		}
		for _, path := range item.Paths {
			if path == item.IndexPath {
				continue
			}
			if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return items, fmt.Errorf("remove cache entry %q: %w", path, err)
			}
		}
		item.Action = "deleted"
	}
	return items, nil
}

func normalizeIDs(raw []string) ([]string, error) {
	if len(raw) == 0 {
		return nil, ErrNoAssetIDs
	}
	seen := make(map[string]struct{}, len(raw))
	ids := make([]string, 0, len(raw))
	for _, rawID := range raw {
		id := strings.TrimSpace(rawID)
		if !assetIDPattern.MatchString(id) {
			return nil, fmt.Errorf("%w: %q", ErrInvalidID, rawID)
		}
		if _, ok := seen[id]; !ok {
			seen[id] = struct{}{}
			ids = append(ids, id)
		}
	}
	return ids, nil
}

func validateRoot(root string) (string, error) {
	if strings.TrimSpace(root) == "" {
		return "", ErrUnsafeRoot
	}
	abs, err := filepath.Abs(root)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrUnsafeRoot, err)
	}
	clean := filepath.Clean(abs)
	if clean == string(filepath.Separator) {
		return "", ErrUnsafeRoot
	}
	rootInfo, err := os.Lstat(clean)
	if err != nil {
		return "", fmt.Errorf("cache root %q: %w", clean, err)
	}
	if rootInfo.Mode()&os.ModeSymlink != 0 {
		return "", fmt.Errorf("%w: cache root must not be a symlink", ErrUnsafeRoot)
	}
	info, err := os.Stat(clean)
	if err != nil {
		return "", fmt.Errorf("cache root %q: %w", clean, err)
	}
	if !info.IsDir() {
		return "", fmt.Errorf("cache root %q is not a directory", clean)
	}
	return clean, nil
}

func discover(root, assetID string) ([]string, int64, error) {
	var paths []string
	var bytes int64
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if path == root {
			return nil
		}
		if entry.Type()&os.ModeSymlink != 0 {
			if entry.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if entry.IsDir() {
			return nil
		}
		if !entry.Type().IsRegular() || !matchesAssetFile(entry.Name(), assetID) {
			return nil
		}
		info, err := entry.Info()
		if err != nil {
			return err
		}
		paths = append(paths, path)
		bytes += info.Size()
		return nil
	})
	if err != nil {
		return nil, 0, fmt.Errorf("scan cache root: %w", err)
	}
	sort.Strings(paths)
	return paths, bytes, nil
}

func matchesAssetFile(name, assetID string) bool {
	if strings.HasPrefix(name, assetID+".") {
		return allowedCacheExtension("." + strings.TrimPrefix(name, assetID+"."))
	}
	prefix := assetID + "_"
	if !strings.HasPrefix(name, prefix) {
		return false
	}
	rest := strings.TrimPrefix(name, prefix)
	parts := strings.SplitN(rest, ".", 2)
	return len(parts) == 2 && shaPrefixPattern.MatchString(parts[0]) && allowedCacheExtension("."+parts[1])
}

func allowedCacheExtension(name string) bool {
	ext := strings.ToLower(filepath.Ext(name))
	switch ext {
	case ".aac", ".ass", ".audio", ".flac", ".jpeg", ".jpg", ".m4a", ".mp3", ".mp4", ".oga", ".ogg", ".png", ".srt", ".wav", ".webm", ".webp":
		return true
	default:
		return false
	}
}

func isWithin(root, path string) bool {
	rootAbs, err := filepath.Abs(root)
	if err != nil {
		return false
	}
	pathAbs, err := filepath.Abs(path)
	if err != nil {
		return false
	}
	rel, err := filepath.Rel(filepath.Clean(rootAbs), filepath.Clean(pathAbs))
	if err != nil || rel == "." || rel == ".." || strings.HasPrefix(rel, ".."+string(filepath.Separator)) || filepath.IsAbs(rel) {
		return false
	}
	current := rootAbs
	for _, part := range strings.Split(rel, string(filepath.Separator)) {
		current = filepath.Join(current, part)
		info, statErr := os.Lstat(current)
		if statErr != nil {
			if errors.Is(statErr, os.ErrNotExist) {
				return true
			}
			return false
		}
		if info.Mode()&os.ModeSymlink != 0 {
			return false
		}
	}
	return true
}
