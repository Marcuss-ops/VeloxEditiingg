// Package workers / worker_state.go
//
// Canonical worker state model: FOUR independent typed dimensions,
// each derived by a pure function from clear inputs. This is the
// single source of truth for worker state. Displayed vocabularies
// (the legacy 4-state ConnectionStatus and the operator 9-state
// Health) are PROJECTIONS of these dimensions — they never carry
// independent state.
//
//	ConnectionState  — is the worker reachable? (session + heartbeat)
//	SchedulingState  — can it accept work? (drain / quarantine / load)
//	DeploymentState  — what is the release doing? (deployment record)
//	HealthState      — top-level health grade (projection of the above)
//
// Rules that hold everywhere in this package:
//   - No free-form status strings feed the state model. The legacy
//     Free-form worker status strings are not part of this model.
//   - No hidden precedence inside string ladders: precedence lives in
//     projectHealth9 as an explicit, commented, typed switch.
//   - "busy" / "on_task" / "idle" are never states — BUSY is derived
//     from active task count (lease store) at hydration time.
package workers

import "time"

// ConnectionState is the canonical connectivity dimension: whether the
// worker is reachable through its session + heartbeat. drain and
// quarantine do NOT influence connectivity — they are scheduling
// concerns (SchedulingState).
type ConnectionState string

const (
	ConnectionOffline   ConnectionState = "OFFLINE"
	ConnectionConnected ConnectionState = "CONNECTED"
	ConnectionStale     ConnectionState = "STALE"
)

// SchedulingState is the canonical scheduling dimension: whether the
// worker can accept new work. Quarantine and drain are operator-set
// exclusions; BUSY is derived from an active lease count — never from a
// "busy"/"on_task" status string. Registry hydration supplies that count
// from the master lease store; heartbeat active_tasks remains telemetry only.
type SchedulingState string

const (
	SchedulingAvailable   SchedulingState = "AVAILABLE"
	SchedulingBusy        SchedulingState = "BUSY"
	SchedulingDraining    SchedulingState = "DRAINING"
	SchedulingQuarantined SchedulingState = "QUARANTINED"
)

// DeploymentState is the canonical deployment dimension, derived from
// the deployment_records row (status + is_rollback). DeploymentNone
// means "no active deployment signal". DeploymentRestarting is a
// legacy fleet-restart signal kept for the 9-state projection.
type DeploymentState string

const (
	DeploymentNone       DeploymentState = ""
	DeploymentCurrent    DeploymentState = "CURRENT"
	DeploymentUpdating   DeploymentState = "UPDATING"
	DeploymentRollback   DeploymentState = "ROLLBACK"
	DeploymentFailed     DeploymentState = "FAILED"
	DeploymentRestarting DeploymentState = "RESTARTING"
)

// HealthState is the canonical top-level health grade. It is a pure
// PROJECTION of the three upstream dimensions plus an optional smoke
// signal — it is never stored independently. Registry hydration in this
// tranche has no smoke-ledger dependency, so it passes the zero signal;
// HealthForInfo remains the explicit adapter for callers with that data.
type HealthState string

const (
	HealthHealthy  HealthState = "HEALTHY"
	HealthDegraded HealthState = "DEGRADED"
	HealthDown     HealthState = "DOWN"
)

// DeriveConnectionState is the canonical connectivity derivation.
//
//	OFFLINE    — session dead OR heartbeat empty/unparseable OR age ≥ 5min
//	STALE      — heartbeat in [150s, 5min)
//	CONNECTED  — fresh session + heartbeat
func DeriveConnectionState(sessionActive bool, lastHB string, now time.Time) ConnectionState {
	if isOffline(sessionActive, lastHB, now) {
		return ConnectionOffline
	}
	if heartbeatAge(lastHB, now) >= ConnectionStaleThreshold {
		return ConnectionStale
	}
	return ConnectionConnected
}

// DeriveSchedulingState is the canonical scheduling derivation.
// Precedence: quarantine > drain > busy > available.
func DeriveSchedulingState(drain, quarantined bool, activeTasks int) SchedulingState {
	if quarantined {
		return SchedulingQuarantined
	}
	if drain {
		return SchedulingDraining
	}
	if activeTasks > 0 {
		return SchedulingBusy
	}
	return SchedulingAvailable
}

// DeriveDeploymentState maps a deployment_records row (status +
// is_rollback) to the canonical DeploymentState dimension.
//
//	PENDING + is_rollback=false → UPDATING
//	PENDING + is_rollback=true  → ROLLBACK
//	SUCCEEDED                    → CURRENT
//	FAILED                       → FAILED
//	ROLLED_BACK | "" (no row)    → NONE
func DeriveDeploymentState(status string, isRollback bool) DeploymentState {
	switch status {
	case "PENDING":
		if isRollback {
			return DeploymentRollback
		}
		return DeploymentUpdating
	case "SUCCEEDED":
		return DeploymentCurrent
	case "FAILED":
		return DeploymentFailed
	}
	return DeploymentNone
}

// NormalizeDeploymentState validates a deployment state crossing a legacy
// string boundary. Unknown values fail closed to DeploymentNone; callers
// must not let arbitrary heartbeat metadata become a canonical state.
func NormalizeDeploymentState(raw string) DeploymentState {
	switch DeploymentState(raw) {
	case DeploymentNone, DeploymentCurrent, DeploymentUpdating, DeploymentRollback, DeploymentFailed, DeploymentRestarting:
		return DeploymentState(raw)
	default:
		return DeploymentNone
	}
}

// DeriveHealthState is the canonical 3-state health grade — a pure
// projection of the connectivity, scheduling and deployment
// dimensions plus the recent-smoke-fail signal.
//
//	DOWN      — unreachable (OFFLINE) OR parked (QUARANTINED)
//	DEGRADED  — stale heartbeat OR deployment failed/rolling back /
//	            restarting OR recent smoke fail
//	HEALTHY   — everything else (busy is still healthy)
func DeriveHealthState(cs ConnectionState, ss SchedulingState, ds DeploymentState, lastSmokeFail time.Time, now time.Time) HealthState {
	if cs == ConnectionOffline || ss == SchedulingQuarantined {
		return HealthDown
	}
	if cs == ConnectionStale || ds == DeploymentFailed || ds == DeploymentUpdating || ds == DeploymentRollback || ds == DeploymentRestarting ||
		(!lastSmokeFail.IsZero() && now.Sub(lastSmokeFail) < time.Hour) {
		return HealthDegraded
	}
	return HealthHealthy
}

// WireStatus projects the canonical dimensions onto the legacy 4-state
// connection_status wire vocabulary (CONNECTED | STALE | DISCONNECTED |
// DRAINING) consumed by /api/v1/workers and dashboards. DRAINING is a
// scheduling signal surfaced on this wire for back-compat.
func (cs ConnectionState) WireStatus(ss SchedulingState) string {
	if ss == SchedulingDraining {
		return StatusDraining
	}
	switch cs {
	case ConnectionOffline:
		return StatusDisconnected
	case ConnectionStale:
		return StatusStale
	default:
		return StatusConnected
	}
}

// projectHealth9 projects the canonical dimensions onto the operator
// 9-state Health vocabulary. The coarse 3-state grade is derived FIRST
// via DeriveHealthState, so the two projections can never disagree;
// the 9-state vocabulary is a REFINEMENT of that grade. The precedence
// ladder below is explicit and commented (rank 1 wins) — it is the only
// place worker-state precedence is decided.
//
// Refinement matrix (coarse grade → 9-state detail):
//
//	DOWN      → QUARANTINED (operator-mute, rank 1) | OFFLINE
//	DEGRADED  → FAILED → ROLLBACK → UPDATING → RESTARTING → DRAINING → DEGRADED
//	HEALTHY   → DRAINING → BUSY → HEALTHY
//
// The within-grade ordering preserves the legacy ladder exactly:
// quarantine > offline > rollback > updating > restarting > draining
// > degraded > busy > healthy.
func projectHealth9(cs ConnectionState, ss SchedulingState, ds DeploymentState, lastSmokeFail time.Time, now time.Time) string {
	switch DeriveHealthState(cs, ss, ds, lastSmokeFail, now) {
	case HealthDown:
		// 1. QUARANTINED — operator-mute beats every other signal.
		if ss == SchedulingQuarantined {
			return WorkerHealthQuarantined
		}
		// 2. OFFLINE — non-live gate. A failed deployment on a
		// reachable worker is handled by the DEGRADED branch below;
		// the 9-state vocabulary has no dedicated FAILED state.
		return WorkerHealthOffline
	case HealthDegraded:
		// 3-5. Deployment-driven states (ROLLBACK before UPDATING so the
		// recovery intervention wins over the forward deploy).
		switch ds {
		case DeploymentRollback:
			return WorkerHealthRollback
		case DeploymentUpdating:
			return WorkerHealthUpdating
		case DeploymentRestarting:
			return WorkerHealthRestarting
		}
		// 6. DRAINING — operator-blocking.
		if ss == SchedulingDraining {
			return WorkerHealthDraining
		}
		// 7. DEGRADED — smoke-fail OR heartbeat stale window.
		return WorkerHealthDegraded
	default: // HealthHealthy
		// 6. DRAINING — a draining worker is still a scheduling
		// exclusion even when its connectivity grade is healthy.
		if ss == SchedulingDraining {
			return WorkerHealthDraining
		}
		// 8. BUSY — actively rendering.
		if ss == SchedulingBusy {
			return WorkerHealthBusy
		}
		// 9. HEALTHY — fresh, idle, otherwise quiet.
		return WorkerHealthHealthy
	}
}

// IsHeartbeatOffline is the heartbeat-only arm of ConnectionState, for
// hot paths where the session arm is not available (GetEligibleWorkers
// must not run per-worker session queries under the registry lock):
// true iff lastHB is empty, unparseable, or ≥5 minutes old. Session
// reachability is deliberately assumed satisfiable — the heartbeat arm
// alone decides connectivity at eligibility time.
func IsHeartbeatOffline(lastHB string, now time.Time) bool {
	return isOffline(true, lastHB, now)
}
