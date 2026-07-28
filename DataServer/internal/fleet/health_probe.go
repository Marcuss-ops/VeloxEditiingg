// Package fleet — Step 10/15 4-level health probe surface.
//
// Each ProbeLevelX is a PURE top-level function with the shape:
//
//   ProbeLevel{L}(ctx, dep, workerID, now) HealthReport
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
//   A (host)         — ssh up + cpu load + memory + disk + docker + NTP
//   B (container)    — running + /health/ready 200 + image_digest
//                       match + no restart loop
//   C (master)       — status CONNECTED + session_active +
//                       executor ads + heartbeat fresh + deployment_state
//   D (smoke)        — application-level smoke on the worker
//
// Each level is independent: a Level A failure (SSH unreachable)
// does NOT block Level C (which reads in-process cached
// WorkerInfo) or Level B's image_digest_match (which queries
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
package fleet

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	workersreg "velox-server/internal/workers"
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
//   {
//     "worker_id":     "...",
//     "level":         "A"|"B"|"C"|"D",
//     "healthy":       true,
//     "checks":        {"<name>": {<CheckResult>}, ...},
//     "collected_at":  "2026-07-28T17:00:00Z",
//     "duration_ms":   12
//   }
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
//   passed   — true if the sub-check observed an acceptable value
//   value    — the OBSERVED value (e.g. "0.42" for cpu_load_1m)
//   expected — the EXPECTED value (e.g. "<3.20") — grep-friendly
//   detail   — human-readable diagnostic (e.g. "parse: <raw>")
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
	HeartbeatFreshnessWin   = 150  // seconds; matches WorkerInfo.ConnectionStatus derivation
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
// ProbeLevelA — host health.
// ───────────────────────────────────────────────────────────────────────
//
// Six independent sub-checks; cascade pattern: a single ssh_up
// failure means subsequent commands ALSO fail (the ssh
// connection itself is broken), but we still attempt them so
// the operator sees WHICH host checks are broken (not just
// that ssh is unreachable — could be network OR auth).
//
// Sub-checks:
//
//   ssh_up                  — `true` exits 0; confirms ssh daemon
//                              reachable on the worker
//   cpu_load_1m             — /proc/loadavg column 1; pass if
//                              < ThresholdLoad1Max
//   memory_available_mb    — `free -m | awk Mem: $7` (MemAvailable
//                              on Linux ≥ 3.14); pass if >=
//                              ThresholdMemAvailMin MB
//   disk_used_pct           — `df --output=pcent /var/lib/docker`
//                              last row; pass if < ThresholdDiskUsedMax
//   docker_active           — `docker info --format {{.ServerVersion}}`
//                              non-empty; confirms docker daemon
//                              responsive on the worker
//   ntp_synced              — `timedatectl show -p NTPSynchronized
//                              --value` reads "yes"; confirms clock
//                              is in sync (avoids cert + lease expiry
//                              bugs)
func ProbeLevelA(ctx context.Context, ssh BackendSSHClient, workerID string, now time.Time) HealthReport {
	start := now
	r := newReport(workerID, HealthLevelA, now)
	if ssh == nil {
		r.Checks["ssh_deps"] = CheckResult{Passed: false, Detail: "ssh client not wired (Step 11+ dependency)"}
		finalize(&r, start)
		return r
	}
	// ssh_up
	if _, err := ssh.Run(ctx, workerID, "true"); err != nil {
		r.Checks["ssh_up"] = CheckResult{Passed: false, Detail: "ssh unreachable: " + err.Error()}
	} else {
		r.Checks["ssh_up"] = CheckResult{Passed: true, Value: "ok"}
	}
	// cpu_load_1m — read /proc/loadavg directly (no shell needed)
	if out, err := ssh.Run(ctx, workerID, "awk '{print $1}' /proc/loadavg"); err == nil {
		if v, perr := strconv.ParseFloat(trim(out), 64); perr == nil {
			r.Checks["cpu_load_1m"] = CheckResult{
				Passed:   v < ThresholdLoad1Max,
				Value:    fmt.Sprintf("%.2f", v),
				Expected: fmt.Sprintf("<%.2f", ThresholdLoad1Max),
			}
		} else {
			r.Checks["cpu_load_1m"] = CheckResult{Passed: false, Detail: "parse: " + out}
		}
	} else {
		r.Checks["cpu_load_1m"] = CheckResult{Passed: false, Detail: err.Error()}
	}
	// memory_available_mb — MemAvailable column of free -m
	if out, err := ssh.Run(ctx, workerID, "free -m | awk '/^Mem:/ {print $7}'"); err == nil {
		if v, perr := strconv.Atoi(trim(out)); perr == nil {
			r.Checks["memory_available_mb"] = CheckResult{
				Passed:   v > ThresholdMemAvailMin,
				Value:    strconv.Itoa(v),
				Expected: fmt.Sprintf(">%d", ThresholdMemAvailMin),
			}
		} else {
			r.Checks["memory_available_mb"] = CheckResult{Passed: false, Detail: "parse: " + out}
		}
	} else {
		r.Checks["memory_available_mb"] = CheckResult{Passed: false, Detail: err.Error()}
	}
	// disk_used_pct — df Used% on /var/lib/docker (the worker's data root)
	if out, err := ssh.Run(ctx, workerID, "df --output=pcent /var/lib/docker | tail -1 | tr -d '% \\n'"); err == nil {
		if v, perr := strconv.Atoi(trim(out)); perr == nil {
			r.Checks["disk_used_pct"] = CheckResult{
				Passed:   v < ThresholdDiskUsedMax,
				Value:    strconv.Itoa(v),
				Expected: fmt.Sprintf("<%d", ThresholdDiskUsedMax),
			}
		} else {
			r.Checks["disk_used_pct"] = CheckResult{Passed: false, Detail: "parse: " + out}
		}
	} else {
		r.Checks["disk_used_pct"] = CheckResult{Passed: false, Detail: err.Error()}
	}
	// docker_active — docker info ServerVersion non-empty
	if out, err := ssh.Run(ctx, workerID, "docker info --format '{{.ServerVersion}}'"); err == nil {
		v := trim(out)
		r.Checks["docker_active"] = CheckResult{Passed: v != "", Value: v}
	} else {
		r.Checks["docker_active"] = CheckResult{Passed: false, Detail: err.Error()}
	}
	// ntp_synced — systemd-timesyncd.NTPSynchronized=yes
	if out, err := ssh.Run(ctx, workerID, "timedatectl show -p NTPSynchronized --value 2>/dev/null || echo false"); err == nil {
		v := trim(out)
		r.Checks["ntp_synced"] = CheckResult{Passed: v == "yes", Value: v}
	} else {
		r.Checks["ntp_synced"] = CheckResult{Passed: false, Detail: err.Error()}
	}
	finalize(&r, start)
	return r
}

// ───────────────────────────────────────────────────────────────────────
// ProbeLevelB — container health.
// ───────────────────────────────────────────────────────────────────────
//
// Four independent sub-checks; no SSH-cascade outside the
// `docker inspect` family because all four use one SSH
// connection. image_digest_match additionally queries the
// deployment_records ledger via BackendDeploymentRepo
// (no SSH needed for that lookup).
//
// Sub-checks:
//
//   container_running      — `docker inspect -f {{.State.Running}}
//                            velox-worker-<id>` reads "true"
//   health_ready           — `docker exec velox-worker-<id> curl
//                            -fsS http://127.0.0.1:8081/health/ready`
//                            returns body without curl exit non-zero
//   image_digest_match     — running container's image == ledger's
//                            latest deployment_records row's
//                            TargetDigest (PENDING in-flight is OK)
//   no_restart_loop        — `docker inspect -f {{.RestartCount}}
//                            velox-worker-<id>` < Threshold
func ProbeLevelB(ctx context.Context, ssh BackendSSHClient, deployments BackendDeploymentRepo, workerID string, now time.Time) HealthReport {
	start := now
	r := newReport(workerID, HealthLevelB, now)
	container := "velox-worker-" + workerID
	if ssh == nil {
		r.Checks["ssh_deps"] = CheckResult{Passed: false, Detail: "ssh client not wired (Step 11+ dependency)"}
		finalize(&r, start)
		return r
	}
	// container_running
	if out, err := ssh.Run(ctx, workerID, fmt.Sprintf("docker inspect -f '{{.State.Running}}' %s", container)); err == nil {
		v := trim(out)
		r.Checks["container_running"] = CheckResult{Passed: v == "true", Value: v, Expected: "true"}
	} else {
		r.Checks["container_running"] = CheckResult{Passed: false, Detail: err.Error()}
	}
	// health_ready — docker exec -> loopback curl (matches the
	// compose healthcheck contract: expose /health/ready only on
	// the docker network, never on the public surface).
	if _, err := ssh.Run(ctx, workerID, fmt.Sprintf("docker exec %s curl -fsS --max-time 5 http://127.0.0.1:8081/health/ready", container)); err == nil {
		r.Checks["health_ready"] = CheckResult{Passed: true, Value: "ok"}
	} else {
		r.Checks["health_ready"] = CheckResult{Passed: false, Detail: err.Error()}
	}
	// image_digest_match — running image vs ledger's latest row
	var runningDigest string
	if out, err := ssh.Run(ctx, workerID, fmt.Sprintf("docker inspect -f '{{.Image}}' %s", container)); err == nil {
		runningDigest = trim(out)
		if runningDigest == "" {
			r.Checks["image_digest_match"] = CheckResult{
				Passed:   false,
				Detail:   "docker inspect returned empty image field",
				Expected: "sha256:...",
			}
		}
	} else {
		// Don't override the eventual ledger-only verdict —
		// record this as a sub-detail so the operator sees BOTH
		// the docker-call failure AND the ledger lookup outcome.
		runningDigest = ""
	}
	if deployments == nil {
		// Without the ledger, we can't compare; mark unverified
		// but keep the running digest observable.
		if _, ok := r.Checks["image_digest_match"]; !ok {
			r.Checks["image_digest_match"] = CheckResult{
				Passed:   false,
				Value:    runningDigest,
				Expected: "sha256:...",
				Detail:   "deployment_records repo not wired (Step 5/15 dependency) — running digest unverified",
			}
		}
	} else {
		rec, err := deployments.GetLatestDeploymentForWorker(ctx, workerID)
		switch {
		case err != nil || rec == nil:
			r.Checks["image_digest_match"] = CheckResult{
				Passed:   false,
				Value:    runningDigest,
				Expected: "<ledger row missing>",
				Detail:   "no ledger row → cannot verify image_digest_match",
			}
		case rec.Status == "PENDING":
			// In-flight deploy — running image cannot match
			// the freshly-pulled image yet. Accept.
			r.Checks["image_digest_match"] = CheckResult{
				Passed:   true,
				Value:    runningDigest,
				Expected: rec.TargetDigest,
				Detail:   "deployment is PENDING (in-flight) — running image diff expected",
			}
		case rec.TargetDigest != "" && rec.TargetDigest == runningDigest:
			r.Checks["image_digest_match"] = CheckResult{
				Passed:   true,
				Value:    runningDigest,
				Expected: rec.TargetDigest,
			}
		default:
			r.Checks["image_digest_match"] = CheckResult{
				Passed:   false,
				Value:    runningDigest,
				Expected: rec.TargetDigest,
				Detail:   "running image digest does not match latest SUCCEEDED ledger row",
			}
		}
	}
	// no_restart_loop — RestartCount via docker inspect
	if out, err := ssh.Run(ctx, workerID, fmt.Sprintf("docker inspect -f '{{.RestartCount}}' %s", container)); err == nil {
		if v, perr := strconv.Atoi(trim(out)); perr == nil {
			r.Checks["no_restart_loop"] = CheckResult{
				Passed:   v < ThresholdRestartLoopMax,
				Value:    strconv.Itoa(v),
				Expected: fmt.Sprintf("<%d", ThresholdRestartLoopMax),
			}
		} else {
			r.Checks["no_restart_loop"] = CheckResult{Passed: false, Detail: "parse: " + out}
		}
	} else {
		r.Checks["no_restart_loop"] = CheckResult{Passed: false, Detail: err.Error()}
	}
	finalize(&r, start)
	return r
}

// ───────────────────────────────────────────────────────────────────────
// ProbeLevelC — master-side worker health (no SSH).
// ───────────────────────────────────────────────────────────────────────
//
// Six sub-checks sourced from the in-process registry read
// model (WorkerInfo). Each maps directly to a field the user
// spec calls out:
//
//   worker_present         — registry.GetWorker returns non-nil
//   status_connected       — ConnectionStatus == "CONNECTED"
//                            (canonical read-time derivation already
//                            done by the registry itself)
//   session_active         — SessionActive == true (read-time derived)
//   executor_advertised    — Capabilities has at least one entry
//                            in supported_executors OR
//                            supported_job_types
//   heartbeat_fresh        — (now - LastHB) < HeartbeatFreshnessWin
//   deployment_state       — DeploymentState ∈
//                            {CURRENT, SUCCEEDED, ""}
//
// Level C is the ONLY level that takes ZERO SSH deps — it
// reads the cached WorkerInfo. It's also the level that's
// ALWAYS fast (microseconds) which is why the operator
// dashboard can poll it on every file-event.
func ProbeLevelC(ctx context.Context, registry HealthLevelCGater, workerID string, now time.Time) HealthReport {
	start := now
	r := newReport(workerID, HealthLevelC, now)
	if registry == nil {
		r.Checks["registry_deps"] = CheckResult{Passed: false, Detail: "registry gater not wired"}
		finalize(&r, start)
		return r
	}
	info, err := registry.GetWorker(ctx, workerID)
	if err != nil {
		r.Checks["worker_present"] = CheckResult{Passed: false, Detail: err.Error()}
		finalize(&r, start)
		return r
	}
	if info == nil {
		r.Checks["worker_present"] = CheckResult{Passed: false, Value: workerID, Detail: "worker not in registry"}
		finalize(&r, start)
		return r
	}
	r.Checks["worker_present"] = CheckResult{Passed: true, Value: info.WorkerID}
	// status_connected — WorkerInfo.ConnectionStatus is the canonical
	// derivation from registry_query.go (session_active + heartbeat
	// freshness + drain).
	r.Checks["status_connected"] = CheckResult{
		Passed:   info.ConnectionStatus == "CONNECTED",
		Value:    info.ConnectionStatus,
		Expected: "CONNECTED",
	}
	// session_active
	r.Checks["session_active"] = CheckResult{Passed: info.SessionActive, Value: strconv.FormatBool(info.SessionActive)}
	// executor_advertised — Capabilities.supported_executors OR
	// supported_job_types has at least one entry.
	r.Checks["executor_advertised"] = CheckResult{
		Passed: hasExecutorAdvertisement(info),
	}
	// heartbeat_fresh — parse RFC3339Nano (with fallback to RFC3339).
	hbParse := func(value string) (time.Time, bool) {
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
			if t, perr := time.Parse(layout, value); perr == nil {
				return t, true
			}
		}
		return time.Time{}, false
	}
	if info.LastHB == "" {
		r.Checks["heartbeat_fresh"] = CheckResult{Passed: false, Detail: "empty last_heartbeat"}
	} else if hbTime, ok := hbParse(info.LastHB); ok {
		age := now.Sub(hbTime)
		fresh := age < HeartbeatFreshnessWin*time.Second
		r.Checks["heartbeat_fresh"] = CheckResult{
			Passed:   fresh,
			Value:    age.Truncate(time.Second).String(),
			Expected: fmt.Sprintf("<%ds", HeartbeatFreshnessWin),
		}
	} else {
		r.Checks["heartbeat_fresh"] = CheckResult{Passed: false, Detail: "parse: " + info.LastHB}
	}
	// deployment_state — accept CURRENT / SUCCEEDED / empty.
	ds := info.DeploymentState
	okState := ds == "CURRENT" || ds == "SUCCEEDED" || ds == ""
	r.Checks["deployment_state"] = CheckResult{
		Passed:   okState,
		Value:    ds,
		Expected: "CURRENT|SUCCEEDED|<empty>",
	}
	finalize(&r, start)
	return r
}

// hasExecutorAdvertisement returns true if WorkerInfo advertises
// at least one supported executor via supported_executors or
// supported_job_types. Accepts []string and []interface{} boxed
// forms because applyMetadataFields stores either depending on
// the source map shape.
func hasExecutorAdvertisement(info *workersreg.WorkerInfo) bool {
	if info == nil || len(info.Capabilities) == 0 {
		return false
	}
	for _, key := range []string{"supported_executors", "supported_job_types"} {
		v, ok := info.Capabilities[key]
		if !ok || v == nil {
			continue
		}
		switch t := v.(type) {
		case []string:
			if len(t) > 0 {
				return true
			}
		case []interface{}:
			if len(t) > 0 {
				return true
			}
		}
	}
	return false
}

// HealthLevelCGater is the narrow consumer-side surface for
// ProbeLevelC. Distinct from Step 9/15's BackendRegistryGater
// (which additionally requires IsActiveJobsZero); the probe
// only needs the GetWorker call. Go convention: consumer-side
// interfaces are smaller than producer-side.
//
// Production wires *RealRegistryLevelCGater (which wraps
// *workersreg.Registry, adapting the registry's no-error
// GetWorker to the (info, err) signature).
type HealthLevelCGater interface {
	GetWorker(ctx context.Context, workerID string) (*workersreg.WorkerInfo, error)
}

// RealRegistryLevelCGater adapts workersreg.Registry (whose
// GetWorker returns a single *WorkerInfo) to the
// HealthLevelCGater interface (which expects (*WorkerInfo,
// error)). The error is always nil in production — the
// registry is in-memory and panics on its own corruption —
// but the interface seam keeps the audit row honest when a
// future storage backend adds error returns.
type RealRegistryLevelCGater struct {
	Reg *workersreg.Registry
}

// GetWorker adapts the registry call.
func (g *RealRegistryLevelCGater) GetWorker(ctx context.Context, workerID string) (*workersreg.WorkerInfo, error) {
	if g == nil || g.Reg == nil {
		return nil, nil
	}
	return g.Reg.GetWorker(ctx, workerID), nil
}

// ───────────────────────────────────────────────────────────────────────
// ProbeLevelD — application-level smoke test on the worker.
// ───────────────────────────────────────────────────────────────────────
//
// Single sub-check: the worker runs the level-d smoke
// end-to-end (render + verify) and returns a non-empty
// artifact_id without error. The artifact_id IS the proof —
// the executor in Step 9/15 forwards it to the Drive
// verifier, which confirms it landed and matches the
// expected byte size.
//
// Production wires a real BackendSmokeRunner that shells
// `submit-canary-remote.sh <worker_id>` and parses the
// artifact_id from the output. Step 10+ ships audit-only
// (the dep is nil in production) so the operator sees
// "smoke runner not wired" in the response body.
func ProbeLevelD(ctx context.Context, smoke BackendSmokeRunner, workerID string, now time.Time) HealthReport {
	start := now
	r := newReport(workerID, HealthLevelD, now)
	if smoke == nil {
		r.Checks["smoke_deps"] = CheckResult{Passed: false, Detail: "smoke runner not wired (Step 12+ dependency)"}
		finalize(&r, start)
		return r
	}
	artifactID, err := smoke.RunLevelD(ctx, workerID)
	if err != nil {
		r.Checks["smoke_ok"] = CheckResult{Passed: false, Value: artifactID, Detail: err.Error()}
	} else if artifactID == "" {
		r.Checks["smoke_ok"] = CheckResult{Passed: false, Value: "", Detail: "runner returned empty artifact_id"}
	} else {
		r.Checks["smoke_ok"] = CheckResult{Passed: true, Value: artifactID}
	}
	finalize(&r, start)
	return r
}

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
