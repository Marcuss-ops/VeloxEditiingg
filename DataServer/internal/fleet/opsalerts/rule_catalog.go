// Package opsalerts is the Step 16/15 fleet-operator structured
// alerting engine.
//
// Distinct from internal/alertengine (compute hot-path runtime
// alerts; logs + Slack/Telegram webhook via env vars):
//   - alertengine: compute-level RED-line gauges (error_rate,
//     p95_wall_ms, worker offline, disk_free, ffmpeg_speed_ratio).
//     Per-job-stripe surface, hooks into the existing
//     supervisor.Runner.
//   - opsalerts:    fleet-op structured alerts persisted into the
//     alert_events table for the dashboard (12 rules per the
//     user spec). Cross-worker surface, evaluated per-worker
//     per-tick, written to SQLite for the operator dashboard.
//
// The two engines are complementary. Both run in the same
// supervisor; alert_events writes are exclusive to opsalerts.
//
// Dedup is an in-memory state machine so a CRITICAL alert doesn't
// re-fire every 60s tick — see dedup.go. Severity escalation
// (WARNING → CRITICAL after the boundary crosses) is allowed
// because each (worker_id, rule_id, severity) pair is its own
// dedup key.
package opsalerts

// RuleID is the canonical 15-rule catalog ID per the user spec.
// IDs are stable strings (used as DB row.rule_id AND as
// Prometheus Alert.alert name equivalence). Operator-facing
// dashboards key by these IDs.
type RuleID string

const (
	RuleHeartbeatStale          RuleID = "heartbeat_stale"
	RuleContainerUnhealthy      RuleID = "container_unhealthy"
	RuleRestartLoop             RuleID = "restart_loop"
	RuleDiskPressure            RuleID = "disk_pressure"
	RuleRAMPressure             RuleID = "ram_pressure"
	RuleConsecutiveJobFailures  RuleID = "consecutive_job_failures"
	RuleSmokeFailed             RuleID = "smoke_failed"
	RuleVersionDrift            RuleID = "version_drift"
	RuleCertExpiring            RuleID = "cert_expiring"
	RuleDriveDeliveryFailed     RuleID = "drive_delivery_failed"
	RuleDeploymentRollback      RuleID = "deployment_rollback"
	RuleWorkerDisconnected      RuleID = "worker_disconnected"
	RuleJobStuckRunning         RuleID = "job_stuck_running"
	RuleStaleLease              RuleID = "stale_lease"
	RuleWorkdirPermissionChange RuleID = "workdir_permission_changed"
)

// Severities is the canonical 3-bucket enum per the user spec.
// Mirrored in store.AlertSeverity* string constants; kept
// here as a typed slice so the catalog can iterate.
type Severity string

const (
	Info     Severity = "INFO"
	Warning  Severity = "WARNING"
	Critical Severity = "CRITICAL"
)

// AlertRule is the static catalog entry. The threshold column
// is parsed by the evaluator against the
// DataSource.Snapshot(workerID) surface; if unrecognized, the
// rule returns nil (no event) — see Evaluate() comment on the
// "skip if data missing" policy.
//
// Dual-severity rules (disk >85%/>95%, cert <15d/<5d) carry
// two AlertRule entries with different RuleID-thresholds BUT
// the SAME logical RuleID — dedup keys on (rule_id, severity)
// so the two fire independently.
type AlertRule struct {
	// ID is the stable audit key (RuleID). Used as DB row.rule_id
	// and Prometheus Alert.alert name.
	ID RuleID

	// Severity is the severity bucket this catalog entry emits
	// when triggered.
	Severity Severity

	// Threshold represents either:
	//   - a numeric boundary (heartbeat_age_seconds > 90)
	//   - a percentage boundary (disk >= 85)
	//   - a duration boundary (cert <= 15*24*time.Hour)
	//   - a count boundary (restarts_per_hour >= 3)
	// Interpreted by the evaluator per-rule-kind.
	Threshold float64

	// Above: when true, fires on value >= threshold. When false,
	// fires on value <= threshold (cert expiry, smoke_failed
	// count of zero, etc.).
	Above bool

	// HumanReadable is the canonical short title for the
	// dashboard. Truncated to 80 chars at construction.
	HumanReadable string

	// LongDescription is the operator's diagnostic hint —
	// pipeline to the relevant dashboard panel or kubectl
	// command (no shellout here, just operator-readable text).
	LongDescription string

	// DataSourceKey names a field on the DataSource.Snapshot
	// surface the evaluator pulls. Unexported by intent — the
	// evaluator dispatches by Switch on each rule.
	// Documented inline per evaluator.
}

// AllRules is the canonical 15-rule catalog. Order matches the
// user spec's listing order (heartbeat first, workdir_permission_changed
// last) so the dashboard's grouped-by-rule view has a stable
// ordering without a sort key.
func AllRules() []AlertRule {
	return []AlertRule{
		// 1. heartbeat assente >90s = CRITICAL
		{
			ID: RuleHeartbeatStale, Severity: Critical,
			Threshold: 90, Above: true,
			HumanReadable:   "Worker heartbeat stale (>90s)",
			LongDescription: "Worker heartbeat_age exceeds 90s. Either the worker process is dead, the network is partitioned, or the executor goroutine is stuck. Check `fleet workers` and the worker's last_smoke status.",
		},
		// 2. container unhealthy = CRITICAL
		{
			ID: RuleContainerUnhealthy, Severity: Critical,
			Threshold: 1, Above: false,
			HumanReadable:   "Worker container unhealthy",
			LongDescription: "Step 10/15 Level B health probe reports docker inspect health.status != 'healthy'. The compose restart loop has not recovered within the budget. Inspect docker ps + /health/ready on the host's loopback:8081.",
		},
		// 3. restart loop >= 3/h = CRITICAL
		{
			ID: RuleRestartLoop, Severity: Critical,
			Threshold: 3, Above: true,
			HumanReadable:   "Container restart loop (>=3/h)",
			LongDescription: "The worker container has restarted >=3 times in the last 60 minutes. Almost always a misconfig (image_digest mismatch, env var, port collision). Inspect docker inspect + the deployment_records for the latest forward cascade.",
		},
		// 4a. disco >85% = WARNING (separate entry, same RuleID
		//     but WARNING severity; dedup keys (rule_id, severity)
		//     so the two entries fire independently — and the
		//     CRITICAL 4b auto-escalates when disk crosses 95%).
		{
			ID: RuleDiskPressure, Severity: Warning,
			Threshold: 85, Above: true,
			HumanReadable:   "Disk usage >85%",
			LongDescription: "Disk used percentage crosses 85% threshold. Clean up old artifacts in the worker's run/ temp dir, or expand the volume. CRITICAL escalation when the same metric crosses 95%.",
		},
		// 4b. disco >95% = CRITICAL
		{
			ID: RuleDiskPressure, Severity: Critical,
			Threshold: 95, Above: true,
			HumanReadable:   "Disk usage >95% (CRITICAL)",
			LongDescription: "Disk used percentage crosses 95%. Auto-escalation from the 85% WARNING — the worker cannot produce new artifacts. Drain immediately and either purge or expand the volume.",
		},
		// 5. RAM >90% = CRITICAL
		{
			ID: RuleRAMPressure, Severity: Critical,
			Threshold: 90, Above: true,
			HumanReadable:   "RAM usage >90%",
			LongDescription: "RAM usage crosses 90%. Render artifacts in flight will OOM-kill mid-pipeline. Drain the worker and inspect the largest concurrent artifacts; consider lowering worker_class concurrency.",
		},
		// 6. 3 job consecutivi falliti = CRITICAL
		{
			ID: RuleConsecutiveJobFailures, Severity: Critical,
			Threshold: 3, Above: true,
			HumanReadable:   "3 consecutive job failures",
			LongDescription: "The worker has 3+ consecutive job failures in the recent window. Inspect the worker_metric_samples.consecutive_failures counter and the recent velox_compute_failure_reasons_total{reason=...} breakdown.",
		},
		// 7. smoke fail = CRITICAL
		{
			ID: RuleSmokeFailed, Severity: Critical,
			Threshold: 1, Above: false,
			HumanReadable:   "Latest smoke run FAILED",
			LongDescription: "Latest smoke_runs row for this worker has status=FAILED. Inspect error_message + Drive delivery status. Auto-resolve when a subsequent SUCCEEDED smoke lands.",
		},
		// 8. version drift = WARNING
		{
			ID: RuleVersionDrift, Severity: Warning,
			Threshold: 1, Above: false,
			HumanReadable:   "Worker image_drift (image != desired_version)",
			LongDescription: "WorkerCard.image_digest differs from WorkerCard.desired_version. Either the update hasn't landed yet (transient — auto-resolves on next heartbeat) or a previous update was interrupted. Run `fleetctl update <id> --digest sha256:<desired>` to align.",
		},
		// 9a. cert <15gg = WARNING
		{
			ID: RuleCertExpiring, Severity: Warning,
			Threshold: 15 * 24, Above: false,
			HumanReadable:   "Worker cert expiring in <15d",
			LongDescription: "WorkerCard.cert_expires_at is within 15 days. Rotate the cert via `prepare-host.sh` (regenerates + reissues + cosign re-verify) or run the existing `fleetctl update --digest sha256:<current>` to trigger an automatic cert refresh from the image's baked-in CA.",
		},
		// 9b. cert <5gg = CRITICAL
		{
			ID: RuleCertExpiring, Severity: Critical,
			Threshold: 5 * 24, Above: false,
			HumanReadable:   "Worker cert expiring in <5d (CRITICAL)",
			LongDescription: "WorkerCard.cert_expires_at is within 5 days. After expiry the worker's TLS handshake fails and the worker is auto-disconnected by the matcher. Drain immediately and rotate.",
		},
		// 10. Drive delivery fail = CRITICAL
		{
			ID: RuleDriveDeliveryFailed, Severity: Critical,
			Threshold: 1, Above: false,
			HumanReadable:   "Drive delivery failed (latest smoke)",
			LongDescription: "Latest smoke_runs row's error_message indicates Drive upload failure (status=FAILED + artifact_drive_id IS NULL + error contains 'drive' or 'oauth' or 'permission'). Inspect the worker's Drive token in /opt/velox-worker/secrets/drive-token.",
		},
		// 11. deployment ROLLBACK = CRITICAL
		{
			ID: RuleDeploymentRollback, Severity: Critical,
			Threshold: 1, Above: false,
			HumanReadable:   "Deployment rolled back",
			LongDescription: "Latest deployment_records row for this worker has status='ROLLED_BACK' (is_rollback=true). The forward cascade failed and the worker auto-rolled-back to previous_digest. Inspect the original PENDING row's error_message; the forward-then-rollback sequence is logged.",
		},
		// 12. worker disconnected = WARNING
		{
			ID: RuleWorkerDisconnected, Severity: Warning,
			Threshold: 0, Above: false,
			HumanReadable:   "Worker disconnected (session dead or heartbeat expired)",
			LongDescription: "Worker connection_status is DISCONNECTED. The session is dead or the heartbeat has expired beyond the 5min threshold. The worker will not receive new leases until it reconnects. Check the worker host SSH + docker compose status.",
		},
		// 13. job stuck in RUNNING >10min = CRITICAL
		{
			ID: RuleJobStuckRunning, Severity: Critical,
			Threshold: 10, Above: true,
			HumanReadable:   "Job stuck in RUNNING state (>10min)",
			LongDescription: "At least one job on this worker has been in RUNNING state for more than 10 minutes. The render pipeline may be hung (FFmpeg stall, disk I/O saturation, or network partition during asset download). Inspect the job's task_attempts for the last progress update.",
		},
		// 14. stale lease >5min = WARNING
		{
			ID: RuleStaleLease, Severity: Warning,
			Threshold: 300, Above: true,
			HumanReadable:   "Oldest task lease >5min (stale lease)",
			LongDescription: "The oldest active task lease on this worker is older than 5 minutes. The worker may have accepted a lease but never started (or completed) the associated task. The master-side task-lease-reaper should auto-reclaim it; persistent stale leases suggest a worker-side executor stall.",
		},
		// 15. workdir permission changed = CRITICAL
		{
			ID: RuleWorkdirPermissionChange, Severity: Critical,
			Threshold: 1, Above: false,
			HumanReadable:   "Workdir permissions altered (container cannot write)",
			LongDescription: "The worker's /var/lib/velox-worker directory permissions have changed from the expected uid 10001 (the image's velox user). The container will fail to write artifacts, temp files, or smoke outputs. Run `sudo deploy/runtime/prepare-host.sh` to restore correct ownership.",
		},
	}
}

// AllRulesByID is a lookup table keyed by (rule_id, severity) —
// the dedup key used by dedup.go and the alert_events table.
func AllRulesByID() map[RuleID][]AlertRule {
	out := make(map[RuleID][]AlertRule, 15)
	for _, r := range AllRules() {
		out[r.ID] = append(out[r.ID], r)
	}
	return out
}
