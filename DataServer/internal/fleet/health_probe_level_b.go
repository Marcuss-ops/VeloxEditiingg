package fleet

import (
	"context"
	"fmt"
	"strconv"
	"time"
)

// health_probe_level_b.go: ProbeLevelB — container health (SSH +
// deployment_records ledger). Split out of health_probe.go; the
// package doc + shared types live in health_probe.go.//
// Four independent sub-checks; no SSH-cascade outside the
// `docker inspect` family because all four use one SSH
// connection. image_digest_match additionally queries the
// deployment_records ledger via BackendDeploymentRepo
// (no SSH needed for that lookup).
//
// Sub-checks:
//
//	container_running      — `docker inspect -f {{.State.Running}}
//	                         velox-worker-<id>` reads "true"
//	health_ready           — `docker exec velox-worker-<id> curl
//	                         -fsS http://127.0.0.1:8081/health/ready`
//	                         returns body without curl exit non-zero
//	image_digest_match     — running container's image == ledger's
//	                         latest deployment_records row's
//	                         TargetDigest (PENDING in-flight is OK)
//	no_restart_loop        — `docker inspect -f {{.RestartCount}}
//	                         velox-worker-<id>` < Threshold
func ProbeLevelB(ctx context.Context, ssh BackendSSHClient, deployments BackendDeploymentRepo, workerID string, now time.Time) HealthReport {
	start := now
	r := newReport(workerID, HealthLevelB, now)
	container := "velox-worker"
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
	if out, err := ssh.Run(ctx, workerID, fmt.Sprintf("docker inspect --format '{{.Config.Image}}' %s", container)); err == nil {
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
