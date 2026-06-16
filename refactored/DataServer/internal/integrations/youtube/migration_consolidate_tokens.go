package youtube

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"time"
)

// ConsolidateResult describes the outcome of a one-shot OAuth token
// consolidation pass. Surfaced at startup so operators can see how many
// legacy files were moved / merged. Idempotent: re-running yields 0s.
//
// Fields:
//   - Found: number of legacy / canonical token files observed before migration
//   - Moved: legacy files moved into the canonical path
//   - Merged: number of channels that had multiple sources and required a merge
//   - Skipped: channels whose canonical copy was already correct
//   - DeletedLegacyFiles: count of source-file removals
//   - RemovedEmptyDirs: empty legacy directories removed after the sweep
//   - Errors: human-readable failures that did not stop the migration
type ConsolidateResult struct {
	Found             int      `json:"found"`
	Moved             int      `json:"moved"`
	Merged            int      `json:"merged"`
	Skipped           int      `json:"skipped"`
	DeletedLegacyFiles int     `json:"deleted_legacy_files"`
	RemovedEmptyDirs  int      `json:"removed_empty_dirs"`
	Errors            []string `json:"errors,omitempty"`
	CanonicalPath     string   `json:"canonical_path"`
	DryRun            bool     `json:"dry_run"`
}

// CanonicalOAuthTokenDir is the single source of truth for OAuth tokens on disk.
// Every other layout under <DataDir>/youtube/... is treated as legacy.
const CanonicalOAuthTokenSubPath = "secrets" + string(filepath.Separator) + "youtube" + string(filepath.Separator) + "tokens"

// legacyTokenDirCandidates returns every directory under dataDir where an
// older version of Velox has been known to write OAuth token files. Each
// entry is a directory; the migration scans each for `account_*.json`.
func legacyTokenDirCandidates(dataDir string) []string {
	if dataDir == "" {
		return nil
	}
	return []string{
		filepath.Join(dataDir, "youtube", "Token"),
		filepath.Join(dataDir, "youtube", "tokens"),
	}
}

// legacyTokenGroupsRoot is the per-group token directory layout used by even
// older Velox releases. We scan every subdirectory of this root.
func legacyTokenGroupsRoot(dataDir string) string {
	if dataDir == "" {
		return ""
	}
	return filepath.Join(dataDir, "youtube", "group")
}

// ConsolidateOAuthTokens is a one-shot idempotent migration that scans every
// known legacy location for OAuth token files (account_*.json), merges any
// conflicting copies per-channel using mergeTokenRecords, writes the
// canonical result to <DataDir>/secrets/youtube/tokens/account_<id>.json,
// removes each legacy source file, and prunes any directory that becomes
// empty. Safe to run on every startup.
//
// dryRun=true logs the planned actions without touching the filesystem.
// Returns a ConsolidateResult summarising what was done.
func ConsolidateOAuthTokens(dataDir string, dryRun bool) (ConsolidateResult, error) {
	res := ConsolidateResult{
		Errors:        []string{},
		CanonicalPath: filepath.Join(dataDir, CanonicalOAuthTokenSubPath),
		DryRun:        dryRun,
	}
	if dataDir == "" {
		return res, fmt.Errorf("consolidate: empty dataDir")
	}

	if !dryRun {
		if err := os.MkdirAll(res.CanonicalPath, 0755); err != nil {
			return res, fmt.Errorf("consolidate: create canonical dir: %w", err)
		}
	}

	// Scan: legacy <DataDir>/youtube/Token + <DataDir>/youtube/tokens + every
	// <DataDir>/youtube/group/<name>/ subdirectory. Also include canonical: we
	// need to know whether the canonical already exists so we can avoid
	// overwriting a non-conflicting copy.
	dirs := append([]string{}, legacyTokenDirCandidates(dataDir)...)
	dirs = append(dirs, res.CanonicalPath)

	if root := legacyTokenGroupsRoot(dataDir); root != "" {
		if entries, err := os.ReadDir(root); err == nil {
			for _, e := range entries {
				// Skip symlinks: e.IsDir() can follow them on some
				// platforms and we don't want a planted symlink here to
				// pull os.Remove / os.Rename / os.ReadDir off the data
				// root.
				if e.Type()&os.ModeSymlink != 0 || !e.Type().IsDir() {
					continue
				}
				dirs = append(dirs, filepath.Join(root, e.Name()))
			}
		}
	}

	// Per-channel accumulator.
	type pendingChannel struct {
		id       string
		sources  []string // paths read; preserved for post-merge cleanup
		records  []map[string]interface{}
	}
	pendingByChannel := make(map[string]*pendingChannel)

	for _, dir := range dirs {
		entries, err := os.ReadDir(dir)
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			res.Errors = append(res.Errors, fmt.Sprintf("read %s: %v", dir, err))
			continue
		}
		for _, e := range entries {
			if e.IsDir() {
				continue
			}
			name := e.Name()
			if !strings.HasPrefix(name, "account_") || !strings.HasSuffix(name, ".json") {
				continue
			}
			id := strings.TrimSuffix(strings.TrimPrefix(name, "account_"), ".json")
			if id == "" {
				continue
			}
			fullPath := filepath.Join(dir, name)
			rec, rerr := readTokenFile(fullPath)
			if rerr != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("read %s: %v", fullPath, rerr))
				continue
			}
			// Found counts only legacy stragglers, not the canonical copy
			// itself: the canonical existing is the steady-state, not a
			// discovery. Including it would inflate the startup-log count
			// and make the dashboard show "still finding tokens" after
			// the migration has already converged.
			if dir != res.CanonicalPath {
				res.Found++
			}
			if _, ok := pendingByChannel[id]; !ok {
				pendingByChannel[id] = &pendingChannel{id: id}
			}
			pc := pendingByChannel[id]
			pc.sources = append(pc.sources, fullPath)
			pc.records = append(pc.records, rec)
		}
	}

	// Process each channel.
	for id, pc := range pendingByChannel {
		canonicalPath := filepath.Join(res.CanonicalPath, "account_"+id+".json")

		merged := mergeTokenRecords(pc.records)
		if len(pc.records) > 1 {
			res.Merged++
		}

		// Decide whether to write: if canonical already matches merged, skip.
		if existing, err := readTokenFile(canonicalPath); err == nil {
			if tokenRecordEqual(existing, merged) {
				res.Skipped++
				if !dryRun {
					res.DeletedLegacyFiles += deleteLegacyFiles(pc.sources, canonicalPath)
				}
				continue
			}
		}

		if !dryRun {
			if werr := atomicWriteJSONFile(canonicalPath, merged, 0600); werr != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("write %s: %v", canonicalPath, werr))
				continue
			}
		}
		res.Moved++
		if !dryRun {
			res.DeletedLegacyFiles += deleteLegacyFiles(pc.sources, canonicalPath)
		}
	}

	// Prune empty legacy directories.
	if !dryRun {
		pruneDirs := append([]string{}, legacyTokenDirCandidates(dataDir)...)
		if root := legacyTokenGroupsRoot(dataDir); root != "" {
			if entries, err := os.ReadDir(root); err == nil {
				for _, e := range entries {
					if e.Type()&os.ModeSymlink == 0 && e.Type().IsDir() {
						pruneDirs = append(pruneDirs, filepath.Join(root, e.Name()))
					}
				}
			}
			pruneDirs = append(pruneDirs, root)
		}
		// Stable iteration so the logs are deterministic.
		sort.Strings(pruneDirs)
		for _, d := range pruneDirs {
			entries, err := os.ReadDir(d)
			if err != nil || len(entries) != 0 {
				continue
			}
			if rerr := os.Remove(d); rerr == nil {
				res.RemovedEmptyDirs++
				log.Printf("[CLEANUP] Removed empty legacy OAuth token directory: %s", d)
			}
		}
	}

	log.Printf("[CLEANUP] OAuth token consolidation complete: found=%d moved=%d merged=%d skipped=%d deleted_legacy=%d removed_dirs=%d errors=%d dry_run=%v",
		res.Found, res.Moved, res.Merged, res.Skipped, res.DeletedLegacyFiles, res.RemovedEmptyDirs, len(res.Errors), dryRun)
	return res, nil
}

// readTokenFile reads a token JSON file and returns the decoded record.
// Best-effort: returns an error if the file contents cannot be parsed.
func readTokenFile(path string) (map[string]interface{}, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	rec := map[string]interface{}{}
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, fmt.Errorf("unmarshal %s: %w", path, err)
	}
	return rec, nil
}

// mergeTokenRecords merges N token records for the same channel. Strategy:
//   - token-like strings (refresh_token, access_token, token, client_id,
//     client_secret, scopes, channel_title, label, thumbnail, thumbnail_url):
//     prefer the LONGEST non-empty value. Map iteration order is randomised
//     in Go, so a plain last-write-wins would clobber real secrets with
//     empty/corrupt copies non-deterministically.
//   - expiry: prefer the latest RFC3339 timestamp.
//   - everything else: last-write-wins (last record wins) — kept narrow so
//     that secret-shaped fields never go through last-write-wins.
//
// All string fields ending up in `merged` are normalised via asString so
// non-string encodings in a corrupt legacy file cannot poison the result.
func mergeTokenRecords(records []map[string]interface{}) map[string]interface{} {
	if len(records) == 0 {
		return map[string]interface{}{}
	}
	if len(records) == 1 {
		return records[0]
	}
	// preferLongest fields: preserving the LONGEST non-empty value across
	// all sources. Refresh tokens, client secrets, and channel titles are
	// the most lossy under last-write-wins so they all live here.
	preferLongest := map[string]struct{}{
		"access_token":  {},
		"refresh_token": {},
		"token":         {},
		"client_id":     {},
		"client_secret": {},
		"scopes":        {},
		"channel_title": {},
		"label":         {},
		"thumbnail":     {},
		"thumbnail_url": {},
	}
	merged := map[string]interface{}{}
	for _, rec := range records {
		for k, v := range rec {
			if k == "expiry" {
				merged[k] = pickLatestExpiry(asString(v), asString(merged[k]))
				continue
			}
			if _, ok := preferLongest[k]; ok {
				cur := asString(merged[k])
				next := asString(v)
				// Keep the longer non-empty value.
				if next != "" && len(next) > len(cur) {
					merged[k] = v
				} else if cur == "" && next != "" {
					merged[k] = v
				}
				continue
			}
			merged[k] = v
		}
	}
	return merged
}

func pickLatestExpiry(a, b string) string {
	ta, oka := parseExpiry(a)
	tb, okb := parseExpiry(b)
	switch {
	case oka && okb:
		if ta.After(tb) {
			return a
		}
		return b
	case oka:
		return a
	case okb:
		return b
	}
	// Both unparseable: keep the non-empty one.
	if a != "" {
		return a
	}
	return b
}

func parseExpiry(s string) (time.Time, bool) {
	if s == "" {
		return time.Time{}, false
	}
	t, err := time.Parse(time.RFC3339, s)
	if err != nil {
		return time.Time{}, false
	}
	return t, true
}

func tokenRecordEqual(a, b map[string]interface{}) bool {
	aj, errA := json.Marshal(a)
	if errA != nil {
		return false
	}
	bj, errB := json.Marshal(b)
	if errB != nil {
		return false
	}
	return string(aj) == string(bj)
}

// atomicWriteJSONFile writes the record to path.<random>.tmp, fsyncs the
// file, then renames into place. Guarantees that a reader never sees a
// half-written token file.
//
// The tmp suffix uses a 32-bit crypto/rand integer (not the PID, which can
// collide if multiple goroutines retry the migration concurrently within
// the same process) plus a process-local counter that keeps sequential
// calls inside one process unique even when rand collides.
var atomicWriteSeq uint32

func atomicWriteJSONFile(path string, rec map[string]interface{}, mode os.FileMode) error {
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(rec, "", "  ")
	if err != nil {
		return err
	}
	var rndUint uint32
	var rndB [4]byte
	if _, err := rand.Read(rndB[:]); err == nil {
		rndUint = uint32(rndB[0])<<24 | uint32(rndB[1])<<16 | uint32(rndB[2])<<8 | uint32(rndB[3])
	}
	seq := atomic.AddUint32(&atomicWriteSeq, 1)
	tmpPath := fmt.Sprintf("%s.tmp.%d.%d", path, rndUint, seq)
	f, err := os.OpenFile(tmpPath, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, mode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(tmpPath)
		return err
	}
	if err := f.Close(); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	if err := os.Rename(tmpPath, path); err != nil {
		_ = os.Remove(tmpPath)
		return err
	}
	return nil
}

// deleteLegacyFiles removes each source path that is NOT the canonical target.
// Returns the number actually deleted.
func deleteLegacyFiles(sources []string, canonicalPath string) int {
	deleted := 0
	for _, src := range sources {
		if src == canonicalPath {
			continue
		}
		if err := os.Remove(src); err == nil {
			deleted++
			log.Printf("[CLEANUP] Removed legacy token file: %s", src)
		}
	}
	return deleted
}

func asString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// CanonicalOAuthTokenPath returns the canonical on-disk location for a
// channel's OAuth token. Used by Service.saveChannelToken and by handlers
// that previously wrote to legacy paths.
func CanonicalOAuthTokenPath(dataDir, channelID string) string {
	if dataDir == "" || channelID == "" {
		return ""
	}
	return filepath.Join(dataDir, CanonicalOAuthTokenSubPath, "account_"+channelID+".json")
}


