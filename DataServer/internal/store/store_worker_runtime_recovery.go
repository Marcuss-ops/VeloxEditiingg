package store

import (
	"encoding/json"
	"time"
)

// store_worker_runtime_recovery.go owns the STALE / PARTITIONED_SUSPECTED /
// PARTITIONED state-machine that mirrors workers.ConnectionStatus at
// the persistent layer, plus the public recovery loop entry-point
// (ReconcileWorkerPartitions).
//
// Per-package single-writer tx contract (see store_worker_runtime.go
// for the canonical statement): every helper in this file either
// receives a *sql.Tx parameter (on the heartbeat path) or uses
// s.db.Query / s.db.Exec directly (read + post-commit side effects).
// No function in this file opens its own *sql.Tx — the recovery-path
// BEGIN-TX wrapper has been extracted to store_worker_recovery_tx.go
// (reconcileOnePartition + persistPartitionedStateTx) for physical
// isolation from the heartbeat path. See the file header of
// store_worker_recovery_tx.go for the full rationale on why the
// per-package single-writer invariant permits exactly two
// s.db.BeginTx sites (PersistWorkerHeartbeat + reconcileOnePartition).
//
// What lives in this file:
//
//   - STALE / PARTITIONED_SUSPECTED / PARTITIONED state-machine
//     constants (connectionStateConnected / connectionStateStale /
//     connectionStatePartitionedSuspected / connectionStatePartitioned)
//     and the pure computeConnectionState helper that derives the
//     canonical state from a heartbeat timestamp + threshold pair.
//   - eventJSONDetails: small JSON helper for the audit-trail details
//     column in worker_events (shared with the heartbeat path which
//     also writes audit rows).
//   - reason_code constants: package-level audit-trail labels for
//     both worker-level and task-level events.
//
// The partition-transition logic lives in the sibling file
// store_worker_runtime_partition.go:
//
//   - detectAndPersistPartitionTransition: heartbeat-path detector
//     that RECEIVES *sql.Tx from PersistWorkerHeartbeat. Emits the
//     STALE / PARTITIONED_SUSPECTED / WORKER_PARTITION_RESOLVED
//     audit-trail rows; never writes the bare PARTITIONED state.
//   - ReconcileWorkerPartitions: public recovery entry-point. Scans
//     the workers table for last_heartbeat_at older than
//     PartitionThresholdSeconds, fans out across candidates, and
//     delegates to reconcileOnePartition (in store_worker_recovery_tx.go)
//     for the per-candidate atomic write.
//
// Contract for future maintainers:
//
//   - Do NOT add new s.db.BeginTx call sites to this file. The
//     per-package single-writer contract permits exactly two:
//     PersistWorkerHeartbeat (heartbeat path, store_worker_heartbeat.go)
//     and reconcileOnePartition (recovery path, store_worker_recovery_tx.go).
//   - Do NOT change detectAndPersistPartitionTransition to open its
//     own *sql.Tx — it is a heartbeat-path helper that operates
//     inside the tx owned by PersistWorkerHeartbeat.
//   - Do NOT make ReconcileWorkerPartitions call into the heartbeat
//     path directly — the recovery loop is structurally independent
//     of the heartbeat stream.
//
// State machine:
//
//	CONNECTED       --age>=StaleThreshold-->     STALE
//	STALE           --age>=PartitionThreshold--> PARTITIONED_SUSPECTED
//	CONNECTED       --age>=PartitionThreshold--> PARTITIONED_SUSPECTED
//	                                              (worker resurfaces
//	                                               after a long outage)
//	STALE                  --age<StaleThreshold-->     CONNECTED (recovery)
//	PARTITIONED_SUSPECTED  --age<StaleThreshold-->     CONNECTED (recovery)
//
// Note: connection_state=PARTITIONED is reachable only via the
// reconciler (ReconcileWorkerPartitions) — the heartbeat-time
// detector writes PARTITIONED_SUSPECTED instead. The two states
// are intentionally distinct:
//
//   - PARTITIONED_SUSPECTED: heartbeat-driven suspect after a
//     resurface — the worker MAY still be alive but acknowledged
//     late. Transitions back to CONNECTED on the next fresh
//     heartbeat.
//   - PARTITIONED: reconciler-confirmed unreachable — the heartbeat
//     stream has stopped entirely. Cannot transition to
//     PARTITIONED_SUSPECTED without a fresh heartbeat firing
//     PersistWorkerHeartbeat, at which point the heartbeat-time
//     path takes over.
//
// Events emitted on transitions:
//
//	any                -> STALE                       WORKER_STALE_DETECTED (WARN)
//	any                -> PARTITIONED_SUSPECTED       WORKER_PARTITION_DETECTED (ERROR)
//	PARTITIONED_SUSPECTED -> CONNECTED                 WORKER_PARTITION_RESOLVED (INFO)
//	PARTITIONED       -> CONNECTED                     WORKER_PARTITION_RESOLVED (INFO)
//
// Reconciliation: ReconcileWorkerPartitions is the periodic pass
// that detects workers whose heartbeat stream has stopped entirely
// (no PersistWorkerHeartbeat call). It is the master's recovery
// surface for partitions that don't surface a heartbeat, and the
// only writer of the bare PARTITIONED state from the persistent mirror.

const (
	// connectionStateConnected is the canonical fresh-heartbeat state.
	connectionStateConnected = "CONNECTED"
	// connectionStateStale is the canonical within-grace-but-aging state.
	connectionStateStale = "STALE"
	// connectionStatePartitionedSuspected is the canonical heartbeat-
	// driven suspect state — emitted by detectAndPersistPartitionTransition
	// when last_heartbeat_at crosses WorkersConfig.PartitionThresholdSeconds
	// during a heartbeat-time transition. Distinct from
	// connectionStatePartitioned (which the reconciler writes when the
	// heartbeat stream has stopped entirely).
	connectionStatePartitionedSuspected = "PARTITIONED_SUSPECTED"
	// connectionStatePartitioned is the canonical reconciler-confirmed
	// unreachable state. Only ReconcileWorkerPartitions /
	// reconcileOnePartition writes this value (single-writer
	// separation).
	connectionStatePartitioned = "PARTITIONED"
)

// computeConnectionState derives the canonical state from a
// heartbeat timestamp + threshold pair. Pure function — no I/O, no
// DB — so tests and dashboards can call it directly.
//
//   - lastHB empty / unparseable      → PARTITIONED (we have no
//     signal that the worker is alive; the worst-case state is the
//     safe default for monitoring surfaces).
//   - age >= partitionSeconds         → PARTITIONED
//   - age >= staleSeconds             → STALE
//   - age <  staleSeconds             → CONNECTED
//
// computeConnectionState derives the canonical state from a
// heartbeat timestamp + threshold pair. Pure function — no I/O, no
// DB — so tests and dashboards can call it directly.
// Heartbeat-driven branches emit PARTITIONED_SUSPECTED, NOT
// PARTITIONED; the bare PARTITIONED state is reserved for the
// reconciler code path so dashboards can distinguish "worker came
// back late" from "worker stream stopped entirely".
//
//   - lastHB empty / unparseable      → PARTITIONED_SUSPECTED (the
//     heartbeat time detector cannot prove the worker is dead, only
//     that it has stopped responding within the threshold window)
//   - age >= partitionSeconds         → PARTITIONED_SUSPECTED
//   - age >= staleSeconds             → STALE
//   - age <  staleSeconds             → CONNECTED
func computeConnectionState(lastHB string, now time.Time, staleSeconds, partitionSeconds int) string {
	if lastHB == "" {
		return connectionStatePartitionedSuspected
	}
	t, err := time.Parse(time.RFC3339Nano, lastHB)
	if err != nil {
		return connectionStatePartitionedSuspected
	}
	age := now.Sub(t.UTC())
	switch {
	case age >= time.Duration(partitionSeconds)*time.Second:
		return connectionStatePartitionedSuspected
	case age >= time.Duration(staleSeconds)*time.Second:
		return connectionStateStale
	default:
		return connectionStateConnected
	}
}

// eventJSONDetails is a tiny helper that marshals a map to its
// canonical JSON string form for storage in worker_events.
// details_json. Returns "" if marshalling fails (the event row is
// still inserted with an empty details column so a single bad
// payload never blocks the audit ledger).
func eventJSONDetails(v map[string]any) string {
	b, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(b)
}

// Canonical reason_code values surfaced on the worker_events rows
// emitted by detectAndPersistPartitionTransition /
// appendWorkerPartitionDetectedEvent. Exported (package-level) so
// tests in this package and dashboards in other packages can pin
// the audit-trail string without a re-export shim.
const (
	connectionStateChangeReasonStaleDelayed      = "heartbeat_delayed"
	connectionStateChangeReasonPartitionTimeout  = "heartbeat_timeout"
	connectionStateChangeReasonPartitionResolved = "heartbeat_resumed"
)

// Canonical reason_code values surfaced on the TASK_RUNTIME_DISAPPEARED
// rows emitted by reconcileWorkerRuntime (heartbeat-miss path) and
// bulkEmitTaskRuntimeDisappearedOnPartition (partition-time path).
// Distinct from the WORKER-level reason codes above: those carry a
// per-worker signal; these carry a per-task signal so dashboards can
// filter by event_type=TASK_RUNTIME_DISAPPEARED + reason_code to drill
// into the cause without parsing the details_json.
//
//   - "heartbeat_missing": the row's missing_heartbeats counter crossed 2.
//   - "partition_timeout": the worker's connection_state crossed
//     PARTITIONED_SUSPECTED via the heartbeat-time detector; the
//     bulk-emit fan-out is the per-task mirror of that signal.
const (
	connectionStateChangeReasonHeartbeatMissing     = "heartbeat_missing"
	connectionStateChangeReasonPartitionTimeoutTask = "partition_timeout"
)
