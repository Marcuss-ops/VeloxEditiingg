// pkg/bootstrap/bundle.go — bundle hash gate (RW-PROD-003 §3 A8).
//
// Thin wrapper around pkg/bundle.BundleHashMatches that maps the
// pkg/bundle error into a StepResult with the canonical code
// "bundle_version_mismatch". The underlying prose from pkg/bundle
// stays intact in Detail so operators who grep for either the code or
// a substring can triage.
//
// This split lets pkg/bundle be reused unmodified by:
//
//   - cmd/velox-worker-agent/main.go (composition root gate)
//   - pkg/doctor/validator.go (when RW-PROD-016 lands)
//   - any future --doctor / --validate-config surfaces
//
// while pkg/bootstrap owns the orchestration / step-result shaping.
package bootstrap

import (
	"time"

	"velox-worker-agent/pkg/bundle"
	"velox-worker-agent/pkg/config"
)

// runBundleHashGate (RW-PROD-003 A8). expected is normally
// cfg.BundleHash (already populated from VELOX_BUNDLE_HASH or read
// from BUNDLE_HASH.txt at composition-root time).
func runBundleHashGate(cfg *config.WorkerConfig, workDir string) StepResult {
	start := time.Now().UTC()
	res := StepResult{
		Name:      "bundle_hash",
		StartedAt: start,
	}

	var expected string
	if cfg != nil {
		expected = cfg.BundleHash
	}

	if err := bundle.BundleHashMatches(expected, workDir); err != nil {
		res.Status = "FAIL"
		res.Code = "bundle_version_mismatch"
		res.Detail = err.Error()
		res.CompletedAt = time.Now().UTC()
		res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
		return res
	}

	res.Status = "OK"
	res.Code = "bundle_version_ok"
	res.Detail = "cfg.BundleHash == on-disk BUNDLE_HASH.txt"
	res.CompletedAt = time.Now().UTC()
	res.DurMs = res.CompletedAt.Sub(start).Milliseconds()
	return res
}
