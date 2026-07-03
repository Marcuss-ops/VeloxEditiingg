// Package doctor — Step 6/8 StateDirValidator.
//
// Why this exists: the Velox worker's mutable state (cache, blob
// store, executor spool, scratch assets) used to live under
// /app/RemoteCodex/assets_cache (legacy bind mount). The new canonical
// root is cfg.StateDir (env VELOX_STATE_DIR, default
// /var/lib/velox/worker). Without a fail-fast check, a worker launched
// on a host where the canonical root is not writable would silently
// fall back to permissive defaults or fail mid-task with a confusing
// EACCES. This validator surfaces the failure — with the actual UID
// and a chown recipe — before any task claim, before the disk watcher
// spins up, before the cache/blob wiring runs.
//
// It also emits a one-shot DEPRECATION warning when the legacy
// /app/RemoteCodex/assets_cache dir is populated AND the canonical
// ${StateDir}/assets_cache is empty. The warning is non-fatal — the
// worker still starts — but operators should know data is stranded
// in the old bind mount.
package doctor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"velox-worker-agent/pkg/config"
)

// StateDirValidator is the startup writability probe for the
// canonical worker state root. Wired into the doctor.Run chain by
// cmd/velox-worker-agent/main.go BEFORE any worker subsystem is
// constructed.
type StateDirValidator struct{}

// ID returns the validator's identifier, used in the doctor report.
func (v *StateDirValidator) ID() string { return "state_dir" }

// Run executes the writability probe + deprecation check.
//
// Probe mechanics: write a 2-byte file named
// .velox_state_doctor_probe under cfg.StateDir and remove it. If the
// write fails the validator returns fail() with the effective UID/GID
// and a precise chown recipe. If the write succeeds the probe file is
// removed and the validator returns pass() (with an optional
// deprecation note — informational only).
func (v *StateDirValidator) Run(_ context.Context, cfg *config.WorkerConfig) Result {
	stateDir := cfg.StateDir
	if stateDir == "" {
		// Defaulting here (vs failing) keeps the validator safe-by-default.
		// applyDefaults() in pkg/config should have resolved this but a
		// caller that bypassed LoadConfig still gets a sensible root.
		stateDir = "/var/lib/velox/worker"
	}
	stateDir = filepath.Clean(stateDir)

	// mkdir -p the root (best-effort). If this fails the writability
	// probe below fails with a precise error, so we don't treat mkdir
	// failure as fatal here.
	_ = os.MkdirAll(stateDir, 0o755)

	probePath := filepath.Join(stateDir, ".velox_state_doctor_probe")
	if err := os.WriteFile(probePath, []byte("ok"), 0o644); err != nil {
		euid := os.Geteuid()
		egid := os.Getegid()
		return fail(
			"state_dir",
			fmt.Sprintf(
				"VELOX_STATE_DIR=%s is not writable by effective UID=%d GID=%d: %v",
				stateDir, euid, egid, err),
			fmt.Sprintf(
				"mkdir -p %s && chown -R %d:%d %s  # then restart velox-worker-agent",
				stateDir, euid, egid, stateDir),
		)
	}
	_ = os.Remove(probePath)

	// Deprecation warning (non-fatal).
	deprecation := checkLegacyAssetsCacheMigration(stateDir)

	msg := fmt.Sprintf("VELOX_STATE_DIR=%s writable (UID=%d)", stateDir, os.Geteuid())
	if deprecation != "" {
		msg += "; " + deprecation
	}
	return pass("state_dir", msg)
}

// checkLegacyAssetsCacheMigration inspects the legacy
// /app/RemoteCodex/assets_cache directory and the canonical
// ${StateDir}/assets_cache directory. Returns a one-shot DEPRECATION
// warning string when the legacy dir is populated AND the canonical
// is empty (operator data stranded in the old bind mount). Returns ""
// otherwise.
//
// The check is intentionally non-blocking: the legacy bind mount may
// be present without content (e.g. fresh container) and we don't
// want to slow startup on every boot just to log the absence.
func checkLegacyAssetsCacheMigration(stateDir string) string {
	legacyDir := "/app/RemoteCodex/assets_cache"
	newDir := filepath.Join(stateDir, "assets_cache")

	// Both absent → no legacy footprint, no deprecation needed.
	if _, err := os.Stat(legacyDir); err != nil {
		if os.IsNotExist(err) {
			return ""
		}
		// Other stat errors (perm denied) we ignore — the worker's own
		// writability check above is the source of truth.
		return ""
	}
	legacyEntries := countDirNonHidden(legacyDir)
	if legacyEntries == 0 {
		// Legacy dir exists but is empty (mount point placeholder) →
		// no deprecation needed; ansible migration already happened.
		return ""
	}
	newEntries := countDirNonHidden(newDir)
	if newEntries > 0 {
		// Worker caught up — no warning.
		return ""
	}
	return fmt.Sprintf(
		"DEPRECATION: %d entries stranded in legacy %s; migrate to %s",
		legacyEntries, legacyDir, newDir)
}

// countDirNonHidden returns the number of non-hidden entries in dir.
// On error (perm denied, ENOENT) returns 0 to avoid false-positive
// deprecation warnings.
func countDirNonHidden(dir string) int {
	f, err := os.Open(dir)
	if err != nil {
		return 0
	}
	defer f.Close()
	names, err := f.Readdirnames(-1)
	if err != nil {
		return 0
	}
	count := 0
	for _, n := range names {
		if strings.HasPrefix(n, ".") {
			continue
		}
		count++
	}
	return count
}
