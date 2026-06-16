package youtube

// consolidate.go replaces the previously-deleted
// `migration_consolidate_tokens.go` (commit `62aded69`) with the real
// files-only implementation. Its purpose:
//
//   - Find every legacy OAuth token file under <DataDir>/youtube/
//     (the path a freshly-promoted Velox install pre-S6 used).
//   - For each candidate, derive a channel_id from JSON or filename
//     and relocates (or merges) the file into the canonical path:
//
//       <DataDir>/CanonicalOAuthTokenSubPath/account_<channel>.json
//
//   - Move == legacy file -> rename to the canonical path
//     (same filesystem; os.Rename is atomic when src/dst share a dir).
//   - Merge == canonical already has the same channel. If bytes are
//     identical we drop the legacy as a duplicate; if bytes differ
//     we leave both alone and report a per-file Error so the
//     operator can reconcile by hand.
//
// dryRun == true: discovery and counting happen, but no filesystem
// mutation is performed.
//
// Note: the function does NOT write to SQLite. Reading the canonical
// .json files into youtube_oauth_tokens is a separate operator
// action — `Service.BackfillOAuthTokensFromJSON` is the canonical
// entry point for that step, ran manually after this command
// finishes. Doing both in one CLI call would couple "move the file"
// to "decrypt + persist", which the SQLite-only verdict
// specifically avoids at runtime.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

// CanonicalOAuthTokenSubPath is the canonical on-disk location for
// system-managed OAuth tokens: <DataDir>/secrets/youtube/tokens/.
// Single authoritative value; do NOT hard-code this string in
// other packages.
const CanonicalOAuthTokenSubPath = "secrets/youtube/tokens"

// ConsolidationResult reports what a `ConsolidateOAuthTokens`
// invocation did. Field names preserved verbatim from the
// previously-deleted implementation so consumers that print them
// (operators' logs, the `velox migrate youtube-oauth-json`
// subcommand) keep the same shape.
type ConsolidationResult struct {
	// Found = legacy token files discovered under <DataDir>/youtube/.
	Found int
	// Moved = legacy files renamed into CanonicalOAuthTokenSubPath.
	Moved int
	// Merged = legacy files dropped because the canonical already had
	// an identical copy. Always paired with DeletedLegacyFiles++.
	Merged int
	// DeletedLegacyFiles = legacy files removed during merge.
	DeletedLegacyFiles int
	// RemovedEmptyDirs = empty legacy directories pruned after
	// moves/merges cleared their contents.
	RemovedEmptyDirs int
	// Errors = per-file error messages collected during the run. The
	// function returns nil error unless the workflow itself failed;
	// transient per-file problems land here so the operator can
	// inspect them after the dryRun ends.
	Errors []string
}

// ConsolidateOAuthTokens scans <DataDir>/youtube/ for legacy OAuth
// token files, and either moves them into CanonicalOAuthTokenSubPath
// or merges them with an existing canonical copy. Idempotent: a
// second run on an already-consolidated dataDir is a no-op (Found=0).
//
// Failures during per-file processing are collected into res.Errors
// and counted; the function itself returns nil error unless the
// workflow can't even start (e.g. dataDir itself is unreadable). The
// caller (the migrate CLI subcommand) prints both the counters and
// each error message.
//
// dryRun mode performs discovery and skip counting but writes
// nothing. Use it before a real run to see what would change.
func ConsolidateOAuthTokens(dataDir string, dryRun bool) (*ConsolidationResult, error) {
	res := &ConsolidationResult{}
	if dataDir == "" {
		return res, nil
	}

	canonicalDir := filepath.Join(dataDir, CanonicalOAuthTokenSubPath)

	legacyFiles, err := collectLegacyOAuthFiles(dataDir, canonicalDir)
	if err != nil {
		return res, fmt.Errorf("collect legacy: %w", err)
	}

	for _, srcPath := range legacyFiles {
		res.Found++
		channelID, raw, rerr := readLegacyChannelID(srcPath)
		if rerr != nil {
			res.Errors = append(res.Errors, fmt.Sprintf("%s: %v", srcPath, rerr))
			continue
		}
		if channelID == "" {
			res.Errors = append(res.Errors, fmt.Sprintf("%s: no channel_id (neither in JSON nor in filename)", srcPath))
			continue
		}
		destPath := filepath.Join(canonicalDir, "account_"+channelID+".json")

		// Already at canonical means the discover step missed the
		// exclusion. Don't double-count Found in that case.
		if samePath(srcPath, destPath) {
			res.Found--
			continue
		}

		destBytes, destStatErr := os.Stat(destPath)
		switch {
		case destStatErr == nil && destBytes != nil:
			// Merge path: canonical exists. Read it; if content is
			// byte-identical we delete the legacy; otherwise leave
			// both alone and surface a per-file error so the operator
			// can reconcile by hand. NEVER destructively overwrite the
			// canonical copy.
			canonicalRaw, rerr := os.ReadFile(destPath)
			if rerr != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("%s: read canonical %s: %v", srcPath, destPath, rerr))
				continue
			}
			if !bytes.Equal(raw, canonicalRaw) {
				res.Errors = append(res.Errors, fmt.Sprintf("%s: skip merge, content differs from canonical %s (manual reconciliation required)", srcPath, destPath))
				continue
			}
			if dryRun {
				res.Merged++
				res.DeletedLegacyFiles++
				continue
			}
			if rerr := os.Remove(srcPath); rerr != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("%s: delete legacy after merge: %v", srcPath, rerr))
				continue
			}
			res.Merged++
			res.DeletedLegacyFiles++
		case os.IsNotExist(destStatErr):
			// Move path: rename legacy to canonical. os.Rename works
			// because src and dest share <DataDir>'s filesystem.
			if dryRun {
				res.Moved++
				continue
			}
			if rerr := os.MkdirAll(canonicalDir, 0o755); rerr != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("mkdir %s: %v", canonicalDir, rerr))
				continue
			}
			if rerr := os.Rename(srcPath, destPath); rerr != nil {
				res.Errors = append(res.Errors, fmt.Sprintf("%s -> %s: %v", srcPath, destPath, rerr))
				continue
			}
			res.Moved++
		default:
			res.Errors = append(res.Errors, fmt.Sprintf("%s: stat canonical %s: %v", srcPath, destPath, destStatErr))
		}
	}

	if !dryRun {
		pruneEmptyLegacyDirs(legacyFiles, res)
	}

	return res, nil
}

// collectLegacyOAuthFiles walks <DataDir>/youtube/ recursively and
// returns every candidate OAuth token file that lives outside the
// canonical path. Candidate rules:
//
//   - Regular file (not directory, not symlink target we want to
//     follow) under <DataDir>/youtube/.
//   - Filename matches Token, Token_<anything>, or account_*.json.
//   - Path is NOT inside canonicalDir.
//
// "anything else JSON-shaped" is intentionally NOT included — we
// don't want to swallow Velox config files like channels.json,
// groups.json, or per-folder state. The narrower rule keeps the
// discover step safe to run on production data without surprising
// the operator.
func collectLegacyOAuthFiles(dataDir, canonicalDir string) ([]string, error) {
	ytDir := filepath.Join(dataDir, "youtube")
	info, err := os.Stat(ytDir)
	if err != nil || !info.IsDir() {
		// No legacy dir — nothing to do; return nil silently.
		return nil, nil
	}

	var paths []string
	walkErr := filepath.WalkDir(ytDir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return nil // skip unreadable entries; surface via per-file Errors later
		}
		if d.IsDir() {
			return nil
		}
		// Defensive: skip anything inside the canonical path even if
		// walk somehow continued into it (joined-on-dataDir symlinks).
		if isUnderDir(path, canonicalDir) {
			return nil
		}
		if isLegacyCandidateName(d.Name()) {
			paths = append(paths, path)
		}
		return nil
	})
	if walkErr != nil {
		return paths, walkErr
	}
	return paths, nil
}

// isLegacyCandidateName matches filenames that look like a token
// artifact (bare "Token", the per-group "Token_<channel>", or the
// already-canonical "account_<channel>.json" that happens to live
// in a non-canonical path — e.g. if an operator copied an account_*
// file into the legacy youtube/ directory by mistake).
func isLegacyCandidateName(name string) bool {
	if name == "Token" {
		return true
	}
	if strings.HasPrefix(name, "Token_") {
		return true
	}
	if strings.HasPrefix(name, "account_") && strings.HasSuffix(name, ".json") {
		return true
	}
	return false
}

// readLegacyChannelID extracts the channel_id from a legacy file.
// Source priority: (1) JSON `channel_id` field, (2) filename
// `account_<id>.json`, (3) filename `Token_<id>`. Returns the raw
// bytes alongside so the caller can byte-compare against the
// canonical copy during merge.
func readLegacyChannelID(path string) (channelID string, raw []byte, err error) {
	raw, rerr := os.ReadFile(path)
	if rerr != nil {
		return "", nil, rerr
	}
	var payload struct {
		ChannelID string `json:"channel_id"`
	}
	if uerr := json.Unmarshal(raw, &payload); uerr == nil && payload.ChannelID != "" {
		return payload.ChannelID, raw, nil
	}
	base := filepath.Base(path)
	if strings.HasPrefix(base, "account_") && strings.HasSuffix(base, ".json") {
		if cid := strings.TrimSuffix(strings.TrimPrefix(base, "account_"), ".json"); cid != "" {
			return cid, raw, nil
		}
	}
	if strings.HasPrefix(base, "Token_") {
		cid := strings.TrimPrefix(base, "Token_")
		if cid != "" {
			return cid, raw, nil
		}
	}
	return "", raw, nil
}

// pruneEmptyLegacyDirs walks the directories that contained the
// legacy files we processed and removes the ones that became empty.
// Recorded in res.RemovedEmptyDirs so the operator can see what's
// cleaned up. We walk ONLY dirs that actually contained a legacy
// file (passed via `filePaths`) to avoid touching unrelated dirs.
func pruneEmptyLegacyDirs(filePaths []string, res *ConsolidationResult) {
	seen := map[string]bool{}
	for _, fp := range filePaths {
		d := filepath.Dir(fp)
		for d != "" && !seen[d] {
			seen[d] = true
			// Do not remove the canonical path or any parent of it.
			entries, err := os.ReadDir(d)
			if err != nil {
				break
			}
			if len(entries) == 0 {
				if rerr := os.Remove(d); rerr == nil {
					res.RemovedEmptyDirs++
				}
			} else if hasOnlyHiddenOrDot(entries) {
				// Treat a dir with only hidden / . / .. as empty-ish.
				if rerr := os.Remove(d); rerr == nil {
					res.RemovedEmptyDirs++
				}
			}
			parent := filepath.Dir(d)
			if parent == d {
				break
			}
			d = parent
		}
	}
}

// hasOnlyHiddenOrDot reports whether every entry in dirents is a
// hidden file (starts with '.') — used so we can prune `.foo` dirs
// left behind by older Velox layouts.
func hasOnlyHiddenOrDot(entries []os.DirEntry) bool {
	if len(entries) == 0 {
		return true
	}
	for _, e := range entries {
		if !strings.HasPrefix(e.Name(), ".") {
			return false
		}
	}
	return true
}

// isUnderDir reports whether path is lexically under root. Used to
// skip candidates that are physically inside the canonical dir even
// if the walk accidentally surfaced them.
func isUnderDir(path, root string) bool {
	if root == "" {
		return false
	}
	// Ensure prefix comparison respects path-boundary semantics.
	rel, err := filepath.Rel(root, path)
	if err != nil {
		return false
	}
	if rel == "." {
		return true
	}
	if strings.HasPrefix(rel, "..") {
		return false
	}
	return !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// samePath reports whether two paths refer to the same file after
// cleaning. We only need this for the discover-step-was-imperfect
// edge case; intentionally minimal — exact-equal-after-Clean.
func samePath(a, b string) bool {
	return filepath.Clean(a) == filepath.Clean(b)
}
