package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
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
//   - detectAndPersistPartitionTransition: heartbeat-path detector
//     that RECEIVES *sql.Tx from PersistWorkerHeartbeat. Emits the
//     STALE / PARTITIONED_SUSPECTED / WORKER_PARTITION_RESOLVED
//     audit-trail rows; never writes the bare PARTITIONED state.
//   - ReconcileWorkerPartitions: public recovery entry-point. Scans
//     the workers table for last_heartbeat_at older than
//     PartitionThresholdSeconds, fans out across candidates, and
//     delegates to reconcileOnePartition (in store_worker_recovery_tx.go)
//     for the per-candidate atomic write.
//   - eventJSONDetails: small JSON helper for the audit-trail details
//     column in worker_events (shared with the heartbeat path which
//     also writes audit rows).
//   - reason_code constants: package-level audit-trail labels for
//     both worker-level and task-level events.
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

// detectAndPersistPartitionTransition reads the worker's prior
// connection_state, computes the new state from the heartbeat
// timestamp, emits a worker_events row if the state transitioned,
// and writes the new state back to the workers row.
//
// All work happens inside the supplied *sql.Tx so the state
// transition and the events ledger stay atomic with the heartbeat
// snapshot upsert that owns this transaction (single-writer
// contract).
//
// Returns the NEW connection_state so the caller can log it.
// Returns ("", nil) if the worker row does not yet exist (e.g.,
// first heartbeat on a freshly-registered worker — the upsert
// earlier in PersistWorkerHeartbeat created the row).
func detectAndPersistPartitionTransition(
	ctx context.Context,
	tx *sql.Tx,
	workerID string,
	lastHB string,
	now time.Time,
	staleSeconds, partitionSeconds int,
) (string, error) {
	if workerID == "" {
		return "", nil
	}
	newState := computeConnectionState(lastHB, now, staleSeconds, partitionSeconds)

	var oldState string
	row := tx.QueryRowContext(ctx, `SELECT connection_state FROM workers WHERE worker_id=?`, workerID)
	if err := row.Scan(&oldState); err != nil {
		if err == sql.ErrNoRows {
			// First heartbeat on a worker row that has not been
			// upserted yet (rare — EnsureWorkerRecord normally
			// creates it before the first heartbeat). The default
			// column value is CONNECTED, so the transition is from
			// "" (unset) to newState.
			oldState = ""
		} else {
			return "", fmt.Errorf("partition detection: read state: %w", err)
		}
	}

	// Same-state short-circuit avoids emitting redundant events and
	// avoids bumping last_state_change_at on every heartbeat.
	if oldState == newState {
		return newState, nil
	}

	// Emit the canonical event for the transition. The heartbeat-
	// time detector specifically emits PARTITIONED_SUSPECTED (not
	// PARTITIONED); the bare PARTITIONED reach is reserved for the
	// reconciler code path. Recovery from either terminal state
	// iterates back to CONNECTED + WORKER_PARTITION_RESOLVED.
	nowRFC3339 := now.UTC().Format(time.RFC3339Nano)
	switch {
	case oldState != connectionStateStale && newState == connectionStateStale:
		if err := appendWorkerStaleDetectedEvent(ctx, tx, workerID, lastHB, staleSeconds, nowRFC3339); err != nil {
			return "", err
		}
	case oldState != connectionStatePartitionedSuspected && newState == connectionStatePartitionedSuspected:
		if err := appendWorkerPartitionDetectedEvent(ctx, tx, workerID, lastHB, partitionSeconds, nowRFC3339); err != nil {
			return "", err
		}
	case (oldState == connectionStatePartitionedSuspected || oldState == connectionStatePartitioned) && newState == connectionStateConnected:
		if err := appendWorkerPartitionResolvedEvent(ctx, tx, workerID, lastHB, nowRFC3339); err != nil {
			return "", err
		}
	}

	// Persist the new state + bump last_state_change_at. UPDATE
	// rather than UPSERT because the heartbeat path always calls
	// upsertWorkerExec first, which creates the row if needed.
	if _, err := tx.ExecContext(ctx,
		`UPDATE workers SET connection_state=?, last_state_change_at=? WHERE worker_id=?`,
		newState, nowRFC3339, workerID,
	); err != nil {
		return "", fmt.Errorf("partition detection: write state: %w", err)
	}
	return newState, nil
}

// ReconcileWorkerPartitions reconciles workers whose heartbeat
// stream has stopped entirely — the recovery-side counterpart to
// the heartbeat-path PersistWorkerHeartbeat. It scans the workers
// table for rows whose last_heartbeat_at is older than
// partitionSeconds and whose connection_state is not already
// PARTITIONED, then transitions each match to PARTITIONED inside
// its own *sql.Tx, emits WORKER_PARTITION_DETECTED, and bumps
// last_state_change_at.
//
// # Canonical caller
//
// This method is intended to be invoked on a wall-clock cadence
// from the MASTER scheduler (master cron loop) or a dedicated
// background goroutine on the master process. It is NOT driven by
// worker activity and MUST NOT be called from inside the heartbeat
// path: the candidates it surfaces — workers whose heartbeat
// stream has stopped entirely — cannot produce a PersistWorkerHeartbeat
// call by definition, so there is no heartbeat-driven tx to
// piggyback on. Typical invocation is from master-side cron at a
// cadence shorter than partitionSeconds so the catch-up lag stays
// bounded.
//
// # Why no shared tx with PersistWorkerHeartbeat
//
// PersistWorkerHeartbeat owns the heartbeat-path *sql.Tx — it
// composes the heartbeat snapshot upsert with the
// detectAndPersistPartitionTransition call (in this same file)
// that may also write PARTITIONED_SUSPECTED, plus the canonical
// worker_events audit row, all in one heartbeat-scoped tx.
//
// ReconcileWorkerPartitions CANNOT reuse that transaction for
// three structural reasons:
//
//   - There is no PersistWorkerHeartbeat call for the candidates
//     ReconcileWorkerPartitions targets. A worker whose heartbeat
//     stream has stopped entirely never fires the heartbeat path;
//     the recovery loop exists precisely to make the persistent
//     mirror catch up when no heartbeat activity is happening.
//     Coupling master reconciliation to a per-worker heartbeat
//     transaction would require master to drive heartbeat calls
//     — out of scope, and would defeat the recovery semantics
//     by coupling master to a path that the partitioned worker
//     is structurally absent from.
//   - Master coordination with per-worker heartbeat transactions
//     would introduce cross-process coupling that contradicts the
//     recovery loop's purpose. The recovery loop is intentionally
//     lightweight and self-contained: it reads the workers table
//     and writes back the bare PARTITIONED state without any
//     shared state with the heartbeat path.
//   - Per-candidate atomicity — the PARTITIONED state transition
//     plus the WORKER_PARTITION_DETECTED audit row, written
//     together inside reconcileOnePartition (in
//     store_worker_recovery_tx.go) — is the recovery-side
//     contract. Piggybacking on PersistWorkerHeartbeat would
//     forfeit the bounded per-candidate commit boundary that
//     lets ONE candidate's failure NOT abort the rest of the
//     pass.
//
// The per-package single-writer invariant permits exactly two
// s.db.BeginTx sites: PersistWorkerHeartbeat on the heartbeat
// path (store_worker_heartbeat.go) and reconcileOnePartition on
// the recovery path (store_worker_recovery_tx.go). This method
// itself opens no transaction; it fans out per-candidate work
// to reconcileOnePartition, which is the SOLE BEGIN-TX caller
// on the recovery side.
//
// # Why PARTITIONED is disjoint from PARTITIONED_SUSPECTED
//
// The workers.connection_state column has two writer paths in
// this file and its sibling:
//
//   - PARTITIONED_SUSPECTED — written by
//     detectAndPersistPartitionTransition (heartbeat path,
//     RECEIVES *sql.Tx from PersistWorkerHeartbeat) when a fresh
//     heartbeat arrives for a worker whose last_heartbeat_at
//     crossed the partition threshold. PARTITIONED_SUSPECTED is
//     the suspect signal: the worker MAY still be alive, just
//     acknowledged late. It reverts to CONNECTED on the next
//     fresh heartbeat (WORKER_PARTITION_RESOLVED event).
//   - PARTITIONED — written ONLY by reconcileOnePartition (via
//     persistPartitionedStateTx, in store_worker_recovery_tx.go)
//     when the master reconciler confirms the heartbeat stream
//     has stopped entirely. PARTITIONED is the unreachable
//     signal: NO fresh heartbeat is expected. Recovery requires
//     the worker to surface a new heartbeat — which fires
//     PersistWorkerHeartbeat and transitions
//     PARTITIONED → PARTITIONED_SUSPECTED → CONNECTED via the
//     heartbeat-time detector (never the bare PARTITIONED write,
//     which is reserved for the reconciler).
//
// The two states target DISJOINT value sets, so the per-package
// single-writer invariant (only one writer of
// workers.connection_state at a time) holds even though the
// underlying column is shared. Dashboards can distinguish
// "worker came back late" (PARTITIONED_SUSPECTED) from "worker
// stream stopped entirely" (PARTITIONED) by reading
// connection_state directly — no need to parse details_json on
// the audit row.
//
// # Returns
//
//   - transitioned (int): the count of workers transitioned to
//     PARTITIONED during the pass.
//   - err (error): nil on a fully-successful pass. If a per-worker
//     failure occurred during the fan-out (a candidate whose
//     s.db.BeginTx failed, whose state UPDATE failed, etc.), the
//     FIRST error encountered is returned so the caller can
//     surface a partial-failure count alongside `transitioned`.
//     A partial failure does NOT abort the rest of the pass —
//     each candidate is its own *sql.Tx, rolled back
//     independently on failure, so a single broken worker row
//     cannot delay the recovery of the rest of the fleet.
//
// # Argument validation
//
//   - staleSeconds <= 0 OR partitionSeconds <= 0 — returns
//     (0, nil) as a no-op. The caller has not configured
//     thresholds yet, so the pass should neither side-effect
//     nor escalate.
//   - staleSeconds > partitionSeconds — returns
//     (0, descriptive-error) WITHOUT touching the database so a
//     misconfiguration surfaces immediately rather than as a
//     silent "always-partitioned" pass.
//
// # Cross-references
//
//   - PersistWorkerHeartbeat: store_worker_heartbeat.go (#1 of 2
//     per-package BEGIN-TX sites).
//   - detectAndPersistPartitionTransition: this file (heartbeat
//     detector; RECEIVES *sql.Tx from PersistWorkerHeartbeat).
//   - reconcileOnePartition: store_worker_recovery_tx.go (#2 of 2
//     per-package BEGIN-TX sites; the recovery-path wrapper).
//   - persistPartitionedStateTx: store_worker_recovery_tx.go
//     (private *sql.Tx helper that writes the bare PARTITIONED
//     UPDATE).
//   - appendWorkerPartitionDetectedEvent: store_worker_events.go
//     (audit-trail row that reconciles the bare PARTITIONED
//     transition).
func (s *SQLiteStore) ReconcileWorkerPartitions(ctx context.Context, staleSeconds, partitionSeconds int) (int, error) {
	if staleSeconds <= 0 || partitionSeconds <= 0 {
		return 0, nil
	}
	if staleSeconds > partitionSeconds {
		// Validate the threshold pair before touching the DB so a
		// misconfiguration surfaces immediately rather than as a
		// silent "always-partitioned" pass.
		return 0, fmt.Errorf("reconcile partitions: staleSeconds (%d) > partitionSeconds (%d)", staleSeconds, partitionSeconds)
	}
	now := time.Now().UTC()
	cutoff := now.Add(-time.Duration(partitionSeconds) * time.Second).Format(time.RFC3339Nano)
	nowRFC3339 := now.Format(time.RFC3339Nano)

	rows, err := s.db.QueryContext(ctx,
		`SELECT worker_id, COALESCE(last_heartbeat_at,'')
		   FROM workers
		  WHERE connection_state != ?
		    AND (last_heartbeat_at IS NULL OR last_heartbeat_at = '' OR last_heartbeat_at < ?)`,
		connectionStatePartitioned, cutoff,
	)
	if err != nil {
		return 0, fmt.Errorf("reconcile partitions: scan: %w", err)
	}
	defer rows.Close()

	type candidate struct {
		workerID string
		lastHB   string
	}
	var candidates []candidate
	for rows.Next() {
		var c candidate
		if err := rows.Scan(&c.workerID, &c.lastHB); err != nil {
			rows.Close()
			return 0, fmt.Errorf("reconcile partitions: scan row: %w", err)
		}
		candidates = append(candidates, c)
	}
	if err := rows.Err(); err != nil {
		return 0, fmt.Errorf("reconcile partitions: rows: %w", err)
	}

	transitioned := 0
	var firstErr error
	for _, c := range candidates {
		if err := s.reconcileOnePartition(ctx, c.workerID, c.lastHB, staleSeconds, partitionSeconds, nowRFC3339); err != nil {
			// Per-worker tx failure does not abort the rest of
			// the pass; we keep going so a single broken row
			// does not delay the recovery of the rest of the
			// fleet. The first error is reported at the end so
			// the caller can surface a partial-failure count.
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		transitioned++
	}
	return transitioned, firstErr
}

// reconcileOnePartition is the per-worker tx wrapper used by
// ReconcileWorkerPartitions. Extracted to store_worker_recovery_tx.go
// for physical isolation from the heartbeat path — see the file
// header of store_worker_recovery_tx.go for the full rationale on
// why the per-package single-writer invariant permits exactly two
// s.db.BeginTx sites (PersistWorkerHeartbeat on the heartbeat path
// + reconcileOnePartition on the recovery path).

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
