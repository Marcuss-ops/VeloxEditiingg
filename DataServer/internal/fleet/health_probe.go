// Package fleet — Step 10/15 4-level health probe surface.
//
// Each ProbeLevelX is a PURE top-level function with the shape:
//
//	ProbeLevel{L}(ctx, dep, workerID, now) HealthReport
//
// No mutex, no shared state, no goroutines. Caller decides
// parallelism (the HTTP handler calls levels sequentially to
// keep the response stable; ops scripts can call them in
// goroutines if they need faster aggregation). Tests can call
// each probe in isolation with stub deps — no sqlite, no real
// SSH, no real fleet — so the per-level pure-function contract
// keeps the suite small AND the per-level failure modes
// observable.
//
// The 4-level vocabulary in the user spec:
//
//	A (host)         — ssh up + cpu load + memory + disk + docker + NTP
//	B (container)    — running + /health/ready 200 + image_digest
//	                    match + no restart loop
//	C (master)       — status CONNECTED + session_active +
//	                    executor ads + heartbeat fresh + deployment_state
//	D (smoke)        — application-level smoke on the worker
//
// Each level is independent: a Level A failure (SSH unreachable)
// does NOT block Level C (which reads in-process cached
// Worker) or Level B's image_digest_match (which queries
// deployment_records without SSH). The probes return CheckResult
// per sub-check so the operator dashboard can surface WHERE the
// level failed — not just THAT the level failed.
//
// Atomic scope:
//
//   - Levels B and C are FULLY wired in production bootstrap
//     (deployments repo + registry adapter).
//   - Levels A and D are AUDIT-ONLY: the bootstrap leaves
//     BackendSSHClient / BackendSmokeRunner nil; the probes
//     surface CheckResult{Passed: false, Detail: "<dep> not
//     wired"} so the operator sees the gap rather than a
//     silent 503.
//
// Real wiring for SSH and Smoke runners lands in follow-up
// steps (Step 11+ / 12+). The atomically-shipped endpoint
// already exposes ?level=A|B|C|D; supplying nil deps surfaces
// the audit row without breaking the contract.
//
// File split by responsibility:
//   - health_probe.go         → types, thresholds, helpers, ProbeAll
//   - health_probe_level_a.go → ProbeLevelA (host / SSH)
//   - health_probe_level_b.go → ProbeLevelB (container / SSH + ledger)
//   - health_probe_level_c.go → ProbeLevelC (registry) + gater seam
//   - health_probe_level_d.go → ProbeLevelD (smoke)
package fleet

import (
	"context"
	"strings"
	"time"
)

// HealthLevel is the canonical 4-level probe vocabulary
// surfaced in the JSON envelope's "level" field. The strings
// match the user-spec uppercase A|B|C|D exactly.
type HealthLevel string

const (
	HealthLevelA HealthLevel = "A"
	HealthLevelB HealthLevel = "B"
	HealthLevelC HealthLevel = "C"
	HealthLevelD HealthLevel = "D"
)

// Valid returns true when the level is one of A|B|C|D. The
// HTTP handler uses this to gate the ?level= query-param.
func (l HealthLevel) Valid() bool {
	switch l {
	case HealthLevelA, HealthLevelB, HealthLevelC, HealthLevelD:
		return true
	}
	return false
}

// HealthReport is the canonical JSON envelope returned for
// both single-level (?level=X) and aggregated (no level param)
// responses. The shape:
//
//	{
//	  "worker_id":     "...",
//	  "level":         "A"|"B"|"C"|"D",
//	  "healthy":       true,
//	  "checks":        {"<name>": {<CheckResult>}, ...},
//	  "collected_at":  "2026-07-28T17:00:00Z",
//	  "duration_ms":   12
//	}
//
// Aggregated responses (no level param) emit this struct as
// one element of an outer {worker_id, reports: [A.., B.., C..,
// D..]} envelope owned by the HTTP layer.
type HealthReport struct {
	WorkerID    string                 `json:"worker_id"`
	Level       HealthLevel            `json:"level"`
	Healthy     bool                   `json:"healthy"`
	Checks      map[string]CheckResult `json:"checks"`
	CollectedAt time.Time              `json:"collected_at"`
	DurationMs  int64                  `json:"duration_ms"`
}

// CheckResult is one sub-check within a probe's report. The
// four fields carry everything the operator dashboard needs:
//
//	passed   — true if the sub-check observed an acceptable value
//	value    — the OBSERVED value (e.g. "0.42" for cpu_load_1m)
//	expected — the EXPECTED value (e.g. "<3.20") — grep-friendly
//	detail   — human-readable diagnostic (e.g. "parse: <raw>")
type CheckResult struct {
	Passed   bool   `json:"passed"`
	Value    string `json:"value,omitempty"`
	Expected string `json:"expected,omitempty"`
	Detail   string `json:"detail,omitempty"`
}

// Health thresholds — exposed as package constants so test
// expectations can anchor against them. Operators can tune via
// a follow-up Step that reads these from env / config.
const (
	ThresholdLoad1Max       = 3.20 // 4 cores * 0.8 — generic; per-host override lands Step 11+
	ThresholdMemAvailMin    = 256  // MB; smaller → fail
	ThresholdDiskUsedMax    = 85   // pct; larger → fail
	ThresholdRestartLoopMax = 5    // RestartCount
	HeartbeatFreshnessWin   = 150  // seconds; matches Worker.ConnectionStatus derivation
)

// newReport allocates a fresh HealthReport skeleton with the
// check map pre-sized to the per-level max sub-checks.
func newReport(workerID string, level HealthLevel, now time.Time) HealthReport {
	return HealthReport{
		WorkerID:    workerID,
		Level:       level,
		Checks:      make(map[string]CheckResult, 8),
		CollectedAt: now.UTC(),
	}
}

// finalize derives Healthy from all sub-check Passed values
// and computes DurationMs from the start timestamp. Called
// once per probe so the result is immutable after return.
func finalize(r *HealthReport, start time.Time) {
	r.Healthy = true
	for _, c := range r.Checks {
		if !c.Passed {
			r.Healthy = false
			break
		}
	}
	r.DurationMs = time.Since(start).Milliseconds()
}

// trim is a tiny helper for command-output parsing — Go's
// strings.TrimSpace variant inline so each SSH call is a
// single-line expression.
func trim(s string) string { return strings.TrimSpace(s) }

// ───────────────────────────────────────────────────────────────────────
// ProbeAll — convenience helper for the aggregated (no ?level=)
// HTTP-path. Calls all 4 levels sequentially and returns the
// slice in canonical A, B, C, D order. Calling each in
// sequence keeps responses stable; the dashboard polls the
// aggregated path less frequently than the single-level path
// and the latency budget (~ a few hundred ms) is acceptable.
// ───────────────────────────────────────────────────────────────────────
func ProbeAll(ctx context.Context, ssh BackendSSHClient, deployments BackendDeploymentRepo, registry HealthLevelCGater, smoke BackendSmokeRunner, workerID string, now time.Time) []HealthReport {
	return []HealthReport{
		ProbeLevelA(ctx, ssh, workerID, now),
		ProbeLevelB(ctx, ssh, deployments, workerID, now),
		ProbeLevelC(ctx, registry, workerID, now),
		ProbeLevelD(ctx, smoke, workerID, now),
	}
}
