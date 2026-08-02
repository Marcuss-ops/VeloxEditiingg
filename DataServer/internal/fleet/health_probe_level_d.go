package fleet

import (
	"context"
	"time"
)

// health_probe_level_d.go: ProbeLevelD — application-level smoke
// test on the worker. Split out of health_probe.go; the package
// doc + shared types live in health_probe.go.
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
