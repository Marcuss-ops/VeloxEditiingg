package audit

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"time"
)

// DualDatabaseCandidate describes a single on-disk velox.db file that
// could be the runtime path or an alternate "source" copy.
type DualDatabaseCandidate struct {
	Path   string
	Exists bool
	MTime  time.Time
	Size   int64
}

// DualDatabaseReport summarises the relationship between the runtime
// velox.db and any other velox.db file found beside it on disk. Operators
// use this to detect the "two DB copies" race that historically caused
// YouTube groups to disappear after a deploy (runtime DBDSN pointed at a
// shared/stale file).
type DualDatabaseReport struct {
	RuntimePath               string
	RuntimeCanonical          string
	Sources                   []DualDatabaseCandidate
	Warnings                  []string
	SamePathHit               bool   // canonical-path equality
	SameInodeHit              bool   // os.SameFile (catches symlinks)
	SourceNewerThanRuntime    bool
	LagHours                  float64
}

// DualDatabaseSourceCandidates returns the well-known on-disk locations
// where a velox.db "source" copy could exist alongside the runtime one.
// Mirrors the path enumeration already used by
// internal/handlers/server/audit/persistence.go::dualDBStatus and
// internal/audit/data_layer.go::checkDatabase so all three sites agree.
func DualDatabaseSourceCandidates(dataDir string) []string {
	if dataDir == "" {
		return nil
	}
	return []string{
		filepath.Join(dataDir, "..", "data", "velox.db"),
		filepath.Join(dataDir, "data", "velox.db"),
		filepath.Join(dataDir, ".velox", "data", "velox.db"),
		filepath.Join(dataDir, "worker_runtime", "velox.db"),
		filepath.Join(dataDir, "source", "velox.db"),
	}
}

// CheckDualDatabase compares the runtime velox.db against every
// well-known source candidate and returns warnings without failing the
// caller. Always runs the same-file check (canonical path equality AND
// os.SameFile inode equality, to catch symlink-aliased aliases). Runs
// the staleness check only when threshold > 0; setting threshold
// explicitly to 0 disables it while preserving same-file detection
// (useful for hot dev loops where the runtime DB is supposed to be the
// bundled source).
//
// Limitation: this check cannot detect the inverse failure mode where
// a snapshotter silently OVERWRITES the runtime path with an older
// copy and deletes any source copy — there is then no candidate left
// to compare against. Pair with a bootstrap-time mtime assertion
// against manifest_v2.json or similar authoritative artifact for
// paranoid detection.
func CheckDualDatabase(dataDir, runtimePath string, threshold time.Duration) DualDatabaseReport {
	report := DualDatabaseReport{RuntimePath: runtimePath}
	if runtimePath == "" {
		return report
	}
	report.RuntimeCanonical = filepath.Clean(runtimePath)
	runtimeInfo, runtimeStatErr := os.Stat(runtimePath)
	if runtimeStatErr != nil && !errors.Is(runtimeStatErr, fs.ErrNotExist) {
		report.Warnings = append(report.Warnings, fmt.Sprintf("stat runtime velox.db: %v", runtimeStatErr))
	}

	for _, p := range DualDatabaseSourceCandidates(dataDir) {
		cand := DualDatabaseCandidate{Path: p}
		info, err := os.Stat(p)
		if err != nil {
			report.Sources = append(report.Sources, cand)
			continue
		}
		cand.Exists = true
		cand.MTime = info.ModTime()
		cand.Size = info.Size()
		report.Sources = append(report.Sources, cand)

		if filepath.Clean(p) == report.RuntimeCanonical {
			report.SamePathHit = true
			report.Warnings = append(report.Warnings,
				"runtime velox.db matches a candidate location (same on-disk file): "+p)
		}
		// Inode-equality catches symlink aliases: e.g. runtime =
		// /opt/velox/data/velox.db (symlink) and candidate =
		// /mnt/source/velox.db (target). filepath.Clean alone misses this.
		if runtimeStatErr == nil && runtimeInfo != nil {
			// os.SameFile(Fi1, Fi2) bool is the current stdlib signature.
			// Discard `err` was removed in a Go release; older code used a
			// (bool, error) return. The function panics on type mismatch.
			if same := os.SameFile(info, runtimeInfo); same {
				if !report.SamePathHit { // avoid duplicate warning when both flags fight
					report.SameInodeHit = true
					report.Warnings = append(report.Warnings,
						fmt.Sprintf("runtime velox.db is a symlink alias of source %q (same inode)", p))
				}
			}
		}
	}

	if runtimeStatErr != nil {
		// Missing or unreadable runtime: skip staleness comparison.
		return report
	}
	if threshold <= 0 {
		return report
	}

	var newest time.Time
	var newestPath string
	for _, c := range report.Sources {
		if !c.Exists {
			continue
		}
		if filepath.Clean(c.Path) == report.RuntimeCanonical {
			continue // counting self against itself would always read delta=0
		}
		if c.MTime.After(newest) {
			newest = c.MTime
			newestPath = c.Path
		}
	}
	if newest.IsZero() {
		return report
	}
	delta := newest.Sub(runtimeInfo.ModTime())
	if delta > threshold {
		report.SourceNewerThanRuntime = true
		report.LagHours = delta.Hours()
		report.Warnings = append(report.Warnings, fmt.Sprintf(
			"runtime velox.db is older than source %q by %.1fh (threshold %s); runtime=%s source_mtime=%s runtime_mtime=%s",
			newestPath, delta.Hours(), threshold, runtimePath,
			newest.Format(time.RFC3339), runtimeInfo.ModTime().Format(time.RFC3339)))
	}
	return report
}
