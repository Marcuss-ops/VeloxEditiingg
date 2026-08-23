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
// populate WorkERInfo.Health from a Worker struct; the
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
//
//	HEALTHY     — fresh, idle, otherwise quiet
//	BUSY        — fresh, actively rendering (active_jobs > 0)
//	DRAINING    — operator-set drain (no new leases)
//	UPDATING    — target_digest != current (forward deploy in progress)
//	RESTARTING  — fleet-controller-driven explicit restart
//	DEGRADED    — recent smoke fail OR heartbeat in stale window
//	OFFLINE     — non-live gate (session-dead or heartbeat > 5min)
//	QUARANTINED — operator-mute (excluded from placement)
//	ROLLBACK    — automatic rollback in progress (is_rollback=true)
//
// The 4-state connection_status is preserved on the diagnostic
// surface via ConnectionStatusForInfo; do not migrate that
// surface to the 9-state vocabulary without a coordinated
// dashboard / consumer upgrade.
package workers

import (
	"time"
)

// WorkerHealth canonical 9-state enum. Surfaced on Worker.Health
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

// HealthReason canonical audit-trail reason codes for non-default
// 9-state outcomes. Future-health-reason taxonomy; the legacy
// 3-element taxonomy (drain / detached_session / heartbeat_stale)
// stays on Worker.Reason and WorkerResponse.Reason for
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
// ConnectionStatus (legacy 4-state) and HealthForInfo (new 9-state) so
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
	t = t.UTC()
	if t.After(now) {
		// A future heartbeat is not proof of liveness: clock skew or
		// malformed producer data must not make an unknown worker
		// eligible for assignment.
		return true
	}
	return now.Sub(t) >= ConnectionDisconnectedThreshold
}

// heartbeatAge returns the duration since lastHB, or 0 if
// unparseable. The zero return for unparseable input is
// intentional — callers treat zero-age-or-unparseable as a
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
func HealthForInfo(info *Worker, lastSmokeFail time.Time, deploymentState string, now time.Time) {
	if info == nil {
		return
	}
	// Compatibility callers may still supply the old deployment string.
	// Convert it once at this boundary; an empty input preserves an already
	// canonical state, while a non-empty input is validated and fails closed.
	if deploymentState != "" {
		info.DeploymentState = NormalizeDeploymentState(deploymentState)
	}
	info.ConnectionState = DeriveConnectionState(info.SessionActive, info.LastHB, now)
	// The lease store owns occupancy. Heartbeat active_tasks is retained
	// for telemetry but cannot promote a worker to BUSY in the canonical
	// state projection.
	info.SchedulingState = DeriveSchedulingState(info.Drain, info.Quarantined, info.Resuming, 0)
	info.HealthState = DeriveHealthState(
		info.ConnectionState,
		info.SchedulingState,
		info.DeploymentState,
		lastSmokeFail,
		now,
	)
	// Keep the old field as a read-only projection for existing clients.
	info.Health = projectHealth9(
		info.ConnectionState,
		info.SchedulingState,
		info.DeploymentState,
		lastSmokeFail,
		now,
	)
}
