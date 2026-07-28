// Package workers / registry_health.go
//
// Step 3/15 of the fleet-operator rollout: the canonical 9-state
// WorkerHealth enum. Replaces the legacy 4-state ConnectionStatus
// reflection for the operator-facing fleet view (admin WorkerCard,
// future dashboard columns) while ConnectionStatus remains the
// 4-state vocabulary for the diagnostic surface (WorkerResponse)
// — back-compat.
//
// All inputs and outputs are pure: no I/O, no DB. The
// HealthForInfo helper is the canonical shim that callers use to
// populate WorkERInfo.Health from a WorkerInfo struct; the
// registry hydrate path calls HealthForInfo on every List / Get
// so the field is fresh at read time and not subject to
// persistence-leak / cache-staleness mismatch.
//
// WHY 9 STATES (not 4): the legacy ConnectionStatus only
// distinguished "is the worker reachable" (CONNECTED | STALE |
// DISCONNECTED) plus a single operator-set override (DRAINING).
// That single-axis view conflated "the worker is fine but
// currently busy", "the worker is fine but being updated", and
// "the worker is offline" — three operator-facing situations
// that warrant very different responses.
//
// The 9-state enum splits these axes so the operator dashboard
// can drive sequencing directly:
//   HEALTHY     — fresh, idle, otherwise quiet
//   BUSY        — fresh, actively rendering (active_jobs > 0)
//   DRAINING    — operator-set drain (no new leases)
//   UPDATING    — target_digest != current (forward deploy in progress)
//   RESTARTING  — fleet-controller-driven explicit restart
//   DEGRADED    — recent smoke fail OR heartbeat in stale window
//   OFFLINE     — non-live gate (session-dead or heartbeat > 5min)
//   QUARANTINED — operator-mute (excluded from placement)
//   ROLLBACK    — automatic rollback in progress (is_rollback=true)
//
// The 4-state connection_status is preserved on the diagnostic
// surface via ConnectionStatusForInfo; do not migrate that
// surface to the 9-state vocabulary without a coordinated
// dashboard / consumer upgrade.
package workers

import (
	"time"
)

// WorkerHealth canonical 9-state enum. Surfaced on WorkerInfo.Health
// (read-time derived) and propagated to the admin /api/v1/admin/
// workers endpoint via WorkerCard.health (Step 1/15).
const (
	WorkerHealthHealthy     = "HEALTHY"
	WorkerHealthBusy        = "BUSY"
	WorkerHealthDraining    = "DRAINING"
	WorkerHealthUpdating    = "UPDATING"
	WorkerHealthRestarting  = "RESTARTING"
	WorkerHealthDegraded    = "DEGRADED"
	WorkerHealthOffline     = "OFFLINE"
	WorkerHealthQuarantined = "QUARANTINED"
	WorkerHealthRollback    = "ROLLBACK"
)

// HealthDeploymentState* — the deployment_state vocabulary that
// Health() accepts as input. Mapped FROM the deployment_records
// table row by DeriveDeploymentHealthState below. Kept as a
// separate vocabulary so the function input surface stays
// decoupled from the persistent schema.
const (
	HealthDeploymentUpdating   = "UPDATING"
	HealthDeploymentRollback   = "ROLLBACK"
	HealthDeploymentRestarting = "RESTARTING"
)

// HealthReason canonical audit-trail reason codes for non-default
// 9-state outcomes. Future-health-reason taxonomy; the legacy
// 3-element taxonomy (drain / detached_session / heartbeat_stale)
// stays on WorkerInfo.Reason and WorkerResponse.Reason for
// back-compat. Step 6/15 will produce a migration note when the
// 9-state dashboard begins surfacing reasons.
const (
	HealthReasonQuarantine   = "quarantined"
	HealthReasonOffline      = "offline"
	HealthReasonDeploymentUp = "deployment_updating"
	HealthReasonDeploymentDn = "deployment_rolling_back"
	HealthReasonFleetRestart = "fleet_restarting"
	HealthReasonDrain        = "drain"
	HealthReasonSmoke        = "smoke_fail_recent"
	HealthReasonLag          = "heartbeat_lag"
	HealthReasonActive       = "active_jobs"
)

// isOffline is the shared OFFLINE gate. Used by both
// ConnectionStatus (legacy 4-state) and Health (new 9-state) so
// the OFFLINE semantics don't drift between the two surfaces.
//
// OFFLINE is true iff:
//   - session is not active, OR
//   - heartbeat is empty / unparseable, OR
//   - heartbeat age >= ConnectionDisconnectedThreshold (5 min).
//
// Pure function — no I/O.
func isOffline(sessionActive bool, lastHB string, now time.Time) bool {
	if !sessionActive {
		return true
	}
	if lastHB == "" {
		return true
	}
	t, err := time.Parse(time.RFC3339, lastHB)
	if err != nil {
		return true
	}
	return now.Sub(t.UTC()) >= ConnectionDisconnectedThreshold
}

// heartbeatAge returns the duration since lastHB, or 0 if
// unparseable. Pure helper for the Health() function. The zero
// return for unparseable input is intentional — callers
// (Health, isOffline) treat zero-age-or-unparseable as a
// specific input class, distinct from "fresh heartbeat 0s ago".
func heartbeatAge(lastHB string, now time.Time) time.Duration {
	if lastHB == "" {
		return 0
	}
	t, err := time.Parse(time.RFC3339, lastHB)
	if err != nil {
		return 0
	}
	return now.Sub(t.UTC())
}

// Health computes the canonical 9-state operator-facing worker
// status. Pure function — no I/O, no DB. Caller passes `now` so
// the derivation is deterministic in tests.
//
// Precedence (top wins; comment numbering = precedence rank):
//
//  1. QUARANTINED — operator-mute (excluded from placement). Beats
//     every other signal because the operator wants to see
//     "machine is parked for investigation" regardless of what
//     it's actually doing.
//  2. OFFLINE — non-live gate. A non-live worker is non-live
//     regardless of what stale state it's in (an "OFFLINE + drain"
//     worker is still OFFLINE first).
//  3. ROLLBACK — current deployment_records row is PENDING +
//     is_rollback=true. Beats UPDATING because ROLLBACK is an
//     ACTIVE RECOVERY INTERVENTION (a forward deploy failed and
//     the system is restoring the previous digest): the
//     operator dashboard must surface it with the highest
//     urgency and not mask it under a normal forward deploy.
//     Beats DRAINING for the same recovery-intervention reason.
//  4. UPDATING — current deployment_records row is PENDING +
//     is_rollback=false. Beats DRAINING because the operator
//     wants the "actively being changed" signal to survive any
//     concurrent drain plug.
//  5. RESTARTING — fleet-controller-driven explicit restart
//     (Step 7/15 feeds this; Step 3/15 implementer passes "" for
//     the typical case).
//  6. DRAINING — drain=true (operator-blocking flag).
//  7. DEGRADED — EITHER a recent smoke-FAIL within the last hour
//     OR heartbeat age in the legacy STALE window (150s ≤ age <
//     5min). The 4-state STALE bucket maps to DEGRADED here. The
//     smoke-fail-or-stale-heartbeat tie both map to DEGRADED —
//     "not unreachable, but not trustable yet".
//  8. BUSY — active_jobs > 0 (the worker is rendering).
//  9. HEALTHY — fresh heartbeat, idle, otherwise-quiet.
//
// Back-compat note: a worker whose current heartbeat is in the
// legacy STALE window (150s ≤ age < 5min) AND otherwise-quiet
// lands in DEGRADED, not HEALTHY. Operators migrating from the
// 4-state vocabulary see "DEGRADED" where 4-state reported
// "STALE"; CHANGELOG notes the semantic shift.
func Health(
	sessionActive bool,
	drain bool,
	activeJobs int32,
	lastHB string,
	lastSmokeFail time.Time,
	deploymentState string,
	quarantined bool,
	now time.Time,
) string {
	// 1. QUARANTINED — operator-mute.
	if quarantined {
		return WorkerHealthQuarantined
	}
	// 2. OFFLINE — non-live gate.
	if isOffline(sessionActive, lastHB, now) {
		return WorkerHealthOffline
	}
	// 3-5. Deployment-driven states. The deployment_state string
	// is mutually exclusive on the row (SetLatestDeployment),
	// so only one switch arm matches per call.
	// Precedence order in the switch is significant: rank-3
	// (ROLLBACK) MUST come before rank-4 (UPDATING) so the
	// recovery-intervention surface wins over the forward-deploy
	// surface even when both states could feed the function.
	switch deploymentState {
	case HealthDeploymentRollback:
		return WorkerHealthRollback
	case HealthDeploymentUpdating:
		return WorkerHealthUpdating
	case HealthDeploymentRestarting:
		return WorkerHealthRestarting
	}
	// 6. DRAINING — operator-blocking.
	if drain {
		return WorkerHealthDraining
	}
	// 7. DEGRADED — fault signals (smoke-fail OR heartbeat-stale).
	age := heartbeatAge(lastHB, now)
	if !lastSmokeFail.IsZero() && now.Sub(lastSmokeFail) < time.Hour {
		return WorkerHealthDegraded
	}
	if age >= ConnectionStaleThreshold {
		return WorkerHealthDegraded
	}
	// 8. BUSY — actively rendering.
	if activeJobs > 0 {
		return WorkerHealthBusy
	}
	// 9. HEALTHY — fresh, idle.
	return WorkerHealthHealthy
}

// DeriveDeploymentHealthState maps a deployment_records row's
// status + is_rollback to the Health() input vocabulary. Pure
// helper for the future registry hydrate path (Step 6/15 wires
// the SQL query that produces these inputs; not wired in
// Step 3/15 so atomic-commit scope stays tight).
//
//   status=PENDING  + is_rollback=false → HealthDeploymentUpdating
//   status=PENDING  + is_rollback=true  → HealthDeploymentRollback
//   status=SUCCEEDED | FAILED | ROLLED_BACK (terminal) or "" (no row) → ""
//
// The "" return tells Health() to skip the deployment slot and
// fall through to DRAINING / DEGRADED / BUSY / HEALTHY.
func DeriveDeploymentHealthState(status string, isRollback bool) string {
	switch status {
	case "PENDING":
		if isRollback {
			return HealthDeploymentRollback
		}
		return HealthDeploymentUpdating
	}
	return ""
}

// activeJobsFromMetrics parses the canonical "active_tasks" key
// out of the heartbeat metrics map. Returns 0 on missing or
// unconvertible entries. Local helper so the workers package
// doesn't depend on the api package's typed metrics parser.
func activeJobsFromMetrics(raw map[string]interface{}) int32 {
	if raw == nil {
		return 0
	}
	v, ok := raw["active_tasks"]
	if !ok {
		return 0
	}
	switch t := v.(type) {
	case float64:
		return int32(t)
	case float32:
		return int32(t)
	case int:
		return int32(t)
	case int64:
		return int32(t)
	case int32:
		return t
	}
	return 0
}

// HealthForInfo populates info.Health from the canonical inputs.
// Mirror shape of ConnectionStatusForInfo so the registry
// hydrate path can call both side-by-side without threading
// inputs twice.
//
// Input parameters lastSmokeFail and deploymentState are not yet
// wired upstream in the registry hydrate path; callers (tests,
// manual investigation) pass zero values until Step 6/15 / 9/15
// close the loop. The function is forward-compatible — adding the
// upstream queries does NOT change this signature.
func HealthForInfo(info *WorkerInfo, lastSmokeFail time.Time, deploymentState string, now time.Time) {
	if info == nil {
		return
	}
	info.Health = Health(
		info.SessionActive,
		info.Drain,
		activeJobsFromMetrics(info.Metrics),
		info.LastHB,
		lastSmokeFail,
		deploymentState,
		info.Quarantined,
		now,
	)
}
