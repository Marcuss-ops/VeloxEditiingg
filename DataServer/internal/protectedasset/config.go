// Package protectedasset — Pass 12 ServiceEnv helpers map the
// operator-facing master-side VELOX_CACHE_* env vars onto the
// Service construction parameters.
//
// VELOX_CACHE_LOOKAHEAD_JOBS (default 10) bounds the snapshot size:
// the master reads ListNextDispatchableJobs(_, limit) and unions
// the resulting jobs' Drive IDs into ProtectedAssetSnapshot.
// Higher limits widen the pre-load / pre-cache footprint but also
// guarantee protection coverage for in-flight jobs even when the
// scheduler picks a job whose submission just landed.
//
// VELOX_CACHE_SNAPSHOT_INTERVAL (default 30s) controls how often
// the master Service.Run goroutine refreshes the snapshot. Lower
// intervals tighten the staleness window workers tolerate without
// grace; higher intervals reduce master CPU.
//
// Pass 12 does NOT include a daemon toggle for the master env vars —
// when the master boots, it calls LoadServiceEnv once and threads
// the result into `protectedasset.NewService(repo, env.LookaheadJobs)`
// + `svc.Run(ctx, env.SnapshotInterval)`. The bootstrap module
// (Pass 7 wiring) is the consumer; this file is the type-and-loader
// surface only.

package protectedasset

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"
)

// ServiceEnv carries the operator-facing master tunables for the
// protected-asset snapshot service. LoadServiceEnv is the single
// construction entry point; manual struct-literal initialisation
// is allowed only for the test matrix.
type ServiceEnv struct {
	LookaheadJobs    int
	SnapshotInterval time.Duration
}

// LoadServiceEnv reads VELOX_CACHE_LOOKAHEAD_JOBS and
// VELOX_CACHE_SNAPSHOT_INTERVAL from the process env. Defaults:
// LookaheadJobs=10, SnapshotInterval=30s. Malformed values produce
// a non-nil error — the caller (master bootstrap) MUST surface this
// to the operator rather than silently fall back to defaults so a
// typo like "VELOX_CACHE_LOOKAHEAD_JOBS=ten" doesn't masquerade as
// OK with a 0-row snapshot.
//
// Empty / unset env vars: defaults are used silently (this is the
// common path in CI + dev shells).
func LoadServiceEnv() (ServiceEnv, error) {
	env := ServiceEnv{
		LookaheadJobs:    DefaultLookahead,
		SnapshotInterval: 30 * time.Second,
	}
	if v := strings.TrimSpace(os.Getenv("VELOX_CACHE_LOOKAHEAD_JOBS")); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return env, fmt.Errorf("protectedasset.LoadServiceEnv: VELOX_CACHE_LOOKAHEAD_JOBS=%q: %w", v, err)
		}
		if n <= 0 {
			return env, fmt.Errorf("protectedasset.LoadServiceEnv: VELOX_CACHE_LOOKAHEAD_JOBS must be > 0, got %d", n)
		}
		env.LookaheadJobs = n
	}
	if v := strings.TrimSpace(os.Getenv("VELOX_CACHE_SNAPSHOT_INTERVAL")); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return env, fmt.Errorf("protectedasset.LoadServiceEnv: VELOX_CACHE_SNAPSHOT_INTERVAL=%q: %w", v, err)
		}
		if d <= 0 {
			return env, fmt.Errorf("protectedasset.LoadServiceEnv: VELOX_CACHE_SNAPSHOT_INTERVAL must be > 0, got %v", d)
		}
		env.SnapshotInterval = d
	}
	return env, nil
}
