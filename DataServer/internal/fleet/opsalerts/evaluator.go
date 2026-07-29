package opsalerts

import (
	"fmt"
	"time"
)

// evaluator.go — Step 16/15 pure-rule evaluator for the 15-rule
// fleet-operator alert catalog.
//
// Design contract:
//   - Pure: no side effects (DB writes go through the engine).
//   - Skip-if-missing: any rule whose DataSource.Snapshot field
//     returns "no data" returns nil (no event). This avoids
//     false positives from a partial deployment (e.g., disk
//     pressure data not yet emitted because the host-level
//     metric hasn't been wired into the worker_metric_samples
//     pipeline yet).
//   - Severity-aware: each rule has its own (rule_id, severity)
//     dedup key, so dual-severity rules (disk 85/95, cert
//     15/5) fire independently.
//
// Return shape: (workerID, rule_id, severity, current_value_text,
// message, fired_at_now) for each tripping rule. The engine
// calls Evaluate() once per (tick, worker) and merges results
// into the dedup window + alert_events table.

// WorkerAlertsDataSource is the read surface the evaluator
// consumes. Production wires WorkerAlertsDataSourceAdapter
// (engine.go) from the SQLite store + fleet.WorkersRegistry.
// Tests pass a stub.
type WorkerAlertsDataSource interface {
	// WorkerIDs returns every worker_id registered with the
	// fleet right now (snapshot at call-time; the engine
	// re-reads each tick).
	WorkerIDs(ctx CallCtx) ([]string, error)

	// Snapshot reads all the per-worker data the evaluator
	// needs to score the catalog. Each field is independently
	// nullable; nil means "not collected yet" (skip-if-missing
	// policy).
	Snapshot(ctx CallCtx, workerID string) (*WorkerSnapshot, error)
}

// CallCtx is a minimal context surface so tests can drive
// Evaluate() with a deterministic `now` clock. Production
// passes CallCtx{Now: time.Now().UTC()}.
type CallCtx struct {
	Now time.Time
}

// WorkerSnapshot carries every data field the evaluator
// might consume for a single worker. Each field is
// independently optional; nil = "not collected" →
// skip-if-missing for the rules that need it.
//
// The struct mirrors the data sources documented in
// rule_catalog.go's longDescription per rule. As new data
// sources wire in, add fields here and update the
// corresponding evaluator branch.
type WorkerSnapshot struct {
	// WorkerID echoes the request id; helpful for audit.
	WorkerID string

	// HeartbeatAgeSeconds is the seconds-since-last-hello
	// computed from WorkerCard.last_heartbeat_at (Step 1/15).
	// nil until first heartbeat.
	HeartbeatAgeSeconds *float64

	// ContainerHealthy is the boolean result of Step 10/15
	// Level B health probe (compose healthcheck.healthy).
	// nil when the worker hasn't reported a health probe yet.
	ContainerHealthy *bool

	// RestartsPerHour is the rolling restart count over the
	// last 60 minutes. nil until the restart-event pipeline
	// feeds it (Step 11+ follow-up for non-fleet-emitted
	// counts).
	RestartsPerHour *float64

	// DiskUsedPercent is the host-level disk-usage percentage
	// (0..100). nil until the worker's host metrics pipeline
	// feeds it (Step 11+ follow-up after the SSH client is
	// wired; for atomic 16/15 this is typically nil).
	DiskUsedPercent *float64

	// RAMUsedPercent is host-level RAM usage (0..100). nil
	// today; same data-source caveat as DiskUsedPercent.
	RAMUsedPercent *float64

	// ConsecutiveJobFailures is the consecutive-failures
	// counter at the time of read (Step 13/15 metrics
	// aggregator will feed it; for atomic 16/15 derives a
	// approximation from smoke_runs recent window).
	ConsecutiveJobFailures *float64

	// LatestSmokeStatus is the status of the most-recent
	// smoke_runs row for this worker ("SUCCEEDED" | "FAILED"
	// | "PENDING"), or nil if no smokes have been run.
	LatestSmokeStatus *string

	// LatestSmokeErrorMessage is the row's error_message —
	// used by drive_delivery_failed to detect Drive-specific
	// errors. nil when no smokes have been run.
	LatestSmokeErrorMessage *string

	// LatestSmokeArtifactDriveID indicates Drive delivery
	// completion for the latest smoke: nil = not delivered,
	// non-nil = artifact_drive_id is set.
	LatestSmokeArtifactDriveID *string

	// ImageDigest is WorkerCard.image_digest (sha256:...).
	// nil until first heartbeat.
	ImageDigest *string

	// DesiredVersion is WorkerCard.desired_version (sha256:...
	// from the registry's policy). nil when no desired_version
	// is pinned yet.
	DesiredVersion *string

	// CertExpiresAt is parsed from WorkerCard.cert_expires_at
	// (RFC3339 timestamp). nil when the cert field hasn't been
	// populated in the WorkerCard (Step 11+ follow-up will
	// thread cert_expires_at from the worker registration
	// payload).
	CertExpiresAt *time.Time

	// LatestDeploymentStatus is the status column of the
	// most-recent deployment_records row for this worker
	// ("PENDING" | "SUCCEEDED" | "FAILED" | "ROLLED_BACK").
	LatestDeploymentStatus *string

	// ConnectionStatus is the worker's connection_status
	// ("CONNECTED" | "STALE" | "DISCONNECTED" | "DRAINING").
	// nil until the worker's first heartbeat populates it.
	ConnectionStatus *string

	// LongestRunningJobMinutes is the duration in minutes of
	// the longest-running job currently in RUNNING state on
	// this worker. nil when no jobs are RUNNING or data not
	// yet collected.
	LongestRunningJobMinutes *float64

	// OldestLeaseAgeSeconds is the age in seconds of the
	// oldest active task lease on this worker. nil when no
	// leases are active or data not yet collected.
	OldestLeaseAgeSeconds *float64

	// WorkdirPermissionOK is true when the worker's
	// /var/lib/velox-worker directory has the expected
	// ownership (uid 10001). nil when the host-level check
	// hasn't been performed yet.
	WorkdirPermissionOK *bool
}

// Evaluate runs every rule in the catalog against the given
// Snapshot and returns the firing events. Pure function:
// no DB writes, no log statements. The engine drives
// persistence + dedup + suppression.
func Evaluate(ctx CallCtx, snap *WorkerSnapshot) []AlertEventHit {
	if snap == nil || snap.WorkerID == "" {
		return nil
	}
	var out []AlertEventHit
	rules := AllRules()
	for _, r := range rules {
		if hit, ok := evaluateOne(ctx, snap, r); ok {
			out = append(out, hit)
		}
	}
	return out
}

// evaluateOne scores a single rule against the snapshot. Returns
// (hit, true) on a firing event; (zero, false) on no-fire.
//
// Skip-if-missing semantic: if a rule's required data source
// returns nil, the rule does NOT fire and does NOT log. This is
// the documented anti-false-positive policy.
func evaluateOne(ctx CallCtx, snap *WorkerSnapshot, r AlertRule) (AlertEventHit, bool) {
	switch r.ID {
	case RuleHeartbeatStale:
		if snap.HeartbeatAgeSeconds == nil {
			break
		}
		if r.Above && *snap.HeartbeatAgeSeconds >= r.Threshold {
			return makeHit(snap.WorkerID, r, fmt.Sprintf("heartbeat_age=%.0fs", *snap.HeartbeatAgeSeconds)), true
		}
	case RuleContainerUnhealthy:
		if snap.ContainerHealthy == nil {
			break
		}
		if !r.Above && !*snap.ContainerHealthy {
			return makeHit(snap.WorkerID, r, "container_healthy=false"), true
		}
	case RuleRestartLoop:
		if snap.RestartsPerHour == nil {
			break
		}
		if r.Above && *snap.RestartsPerHour >= r.Threshold {
			return makeHit(snap.WorkerID, r, fmt.Sprintf("restarts_per_hour=%.0f", *snap.RestartsPerHour)), true
		}
	case RuleDiskPressure:
		if snap.DiskUsedPercent == nil {
			break
		}
		if r.Above && *snap.DiskUsedPercent >= r.Threshold {
			return makeHit(snap.WorkerID, r, fmt.Sprintf("disk_used=%.0f%%", *snap.DiskUsedPercent)), true
		}
	case RuleRAMPressure:
		if snap.RAMUsedPercent == nil {
			break
		}
		if r.Above && *snap.RAMUsedPercent >= r.Threshold {
			return makeHit(snap.WorkerID, r, fmt.Sprintf("ram_used=%.0f%%", *snap.RAMUsedPercent)), true
		}
	case RuleConsecutiveJobFailures:
		if snap.ConsecutiveJobFailures == nil {
			break
		}
		if r.Above && *snap.ConsecutiveJobFailures >= r.Threshold {
			return makeHit(snap.WorkerID, r, fmt.Sprintf("consecutive_failures=%.0f", *snap.ConsecutiveJobFailures)), true
		}
	case RuleSmokeFailed:
		if snap.LatestSmokeStatus == nil {
			break
		}
		if !r.Above && *snap.LatestSmokeStatus == "FAILED" {
			val := "smoke=FAILED"
			if snap.LatestSmokeErrorMessage != nil && *snap.LatestSmokeErrorMessage != "" {
				val = "smoke=FAILED err=" + *snap.LatestSmokeErrorMessage
			}
			return makeHit(snap.WorkerID, r, val), true
		}
	case RuleVersionDrift:
		if snap.ImageDigest == nil || snap.DesiredVersion == nil {
			break
		}
		if !r.Above && *snap.ImageDigest != *snap.DesiredVersion {
			return makeHit(snap.WorkerID, r, fmt.Sprintf("image=%s desired=%s", *snap.ImageDigest, *snap.DesiredVersion)), true
		}
	case RuleCertExpiring:
		if snap.CertExpiresAt == nil {
			break
		}
		// Hours-until-expiry compared to the rule's threshold
		// (in hours). cert <15dg = warning (<360h), cert <5d
		// = critical (<120h).
		hoursLeft := snap.CertExpiresAt.Sub(ctx.Now).Hours()
		if !r.Above && hoursLeft <= r.Threshold && hoursLeft > 0 {
			return makeHit(snap.WorkerID, r, fmt.Sprintf("cert_expires_in_hours=%.0f", hoursLeft)), true
		}
	case RuleDriveDeliveryFailed:
		if snap.LatestSmokeStatus == nil {
			break
		}
		if !r.Above && *snap.LatestSmokeStatus == "FAILED" {
			// Drive-specific heuristic: error contains drive
			// or oauth or permission; OR artifact_drive_id
			// is nil on a FAILED smoke.
			if snap.LatestSmokeArtifactDriveID == nil {
				return makeHit(snap.WorkerID, r, "drive=missing (artifact_drive_id IS NULL)"), true
			}
			if snap.LatestSmokeErrorMessage != nil {
				errLow := *snap.LatestSmokeErrorMessage
				if containsAny(errLow, "drive", "oauth", "permission", "unauthorized") {
					return makeHit(snap.WorkerID, r, "drive=errored: "+errLow), true
				}
			}
		}
	case RuleDeploymentRollback:
		if snap.LatestDeploymentStatus == nil {
			break
		}
		if !r.Above && *snap.LatestDeploymentStatus == "ROLLED_BACK" {
			return makeHit(snap.WorkerID, r, "deployment=ROLLED_BACK"), true
		}
	case RuleWorkerDisconnected:
		if snap.ConnectionStatus == nil {
			break
		}
		if !r.Above && *snap.ConnectionStatus == "DISCONNECTED" {
			return makeHit(snap.WorkerID, r, fmt.Sprintf("connection_status=%s", *snap.ConnectionStatus)), true
		}
	case RuleJobStuckRunning:
		if snap.LongestRunningJobMinutes == nil {
			break
		}
		if r.Above && *snap.LongestRunningJobMinutes >= r.Threshold {
			return makeHit(snap.WorkerID, r, fmt.Sprintf("longest_running_job=%.0fmin", *snap.LongestRunningJobMinutes)), true
		}
	case RuleStaleLease:
		if snap.OldestLeaseAgeSeconds == nil {
			break
		}
		if r.Above && *snap.OldestLeaseAgeSeconds >= r.Threshold {
			return makeHit(snap.WorkerID, r, fmt.Sprintf("oldest_lease_age=%.0fs", *snap.OldestLeaseAgeSeconds)), true
		}
	case RuleWorkdirPermissionChange:
		if snap.WorkdirPermissionOK == nil {
			break
		}
		if !r.Above && !*snap.WorkdirPermissionOK {
			return makeHit(snap.WorkerID, r, "workdir_permission=changed"), true
		}
	}
	return AlertEventHit{}, false
}

// AlertEventHit is the firing-event value the evaluator returns.
// `CurrentValueText` is the operator-facing "what was observed"
// string (e.g., "disk_used=92%") that lands in alert_events.current_value.
type AlertEventHit struct {
	WorkerID         string
	RuleID           RuleID
	Severity         Severity
	CurrentValueText string
	Message          string
	FiredAt          time.Time
}

// makeHit builds an AlertEventHit from a rule + observation.
// Truncates the message at 500 chars to keep the alert_events
// row bounded; messages longer than that usually indicate a
// stack-trace leak from the executor and are best inspected at
// the fleet_operations audit row, not in this dashboard.
func makeHit(workerID string, r AlertRule, currentValue string) AlertEventHit {
	msg := r.LongDescription
	if len(msg) > 500 {
		msg = msg[:500] + "…"
	}
	return AlertEventHit{
		WorkerID:         workerID,
		RuleID:           r.ID,
		Severity:         r.Severity,
		CurrentValueText: currentValue,
		Message:          msg,
		FiredAt:          time.Now().UTC(),
	}
}

// containsAny is a small case-insensitive substring check the
// evaluator uses for drive_delivery_failed heuristics. Standard
// slice scan with strings.ToLower once on the haystack; trim
// to avoid importing strings just for this.
func containsAny(haystack string, needles ...string) bool {
	h := []byte(haystack)
	for _, n := range needles {
		if len(n) == 0 {
			continue
		}
		for i := 0; i+len(n) <= len(h); i++ {
			match := true
			for j := 0; j < len(n); j++ {
				hi, ni := h[i+j], n[j]
				if hi >= 'A' && hi <= 'Z' {
					hi += 32
				}
				if ni >= 'A' && ni <= 'Z' {
					ni += 32
				}
				if hi != ni {
					match = false
					break
				}
			}
			if match {
				return true
			}
		}
	}
	return false
}
