package fleet

import (
	"context"
	"fmt"
	"strconv"
	"time"

	workersreg "velox-server/internal/workers"
)

// health_probe_level_c.go: ProbeLevelC — master-side worker health
// (registry read model, no SSH) + the HealthLevelCGater seam.
// Split out of health_probe.go; the package doc + shared types
// live in health_probe.go.
//
// Six sub-checks sourced from the in-process registry read
// model (WorkerInfo). Each maps directly to a field the user
// spec calls out:
//
//	worker_present         — registry.GetWorker returns non-nil
//	status_connected       — ConnectionStatus == "CONNECTED"
//	                         (canonical read-time derivation already
//	                         done by the registry itself)
//	session_active         — SessionActive == true (read-time derived)
//	executor_advertised    — Capabilities has at least one entry
//	                         in supported_executors OR
//	                         supported_job_types
//	heartbeat_fresh        — (now - LastHB) < HeartbeatFreshnessWin
//	deployment_state       — DeploymentState ∈
//	                         {CURRENT, SUCCEEDED, ""}
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
	r.Checks["worker_present"] = CheckResult{Passed: true, Value: info.WorkerID.String()}
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
// at least one supported executor via the canonical "executors"
// key (proto-structured list of {id, version} objects), or the
// legacy flat-map keys "supported_executors" / "supported_job_types".
func hasExecutorAdvertisement(info *workersreg.WorkerInfo) bool {
	if info == nil || len(info.Capabilities) == 0 {
		return false
	}
	for _, key := range []string{"executors", "supported_executors", "supported_job_types"} {
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
