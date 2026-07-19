package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

// store_worker_heartbeat.go owns the worker-side heartbeat persistence
// pipeline. It is the ONLY place in this refactor where the SQLite
// transaction is opened (via s.db.BeginTx); every helper living in the
// sibling files (store_worker_runtime_projection.go, store_worker_metrics.go,
// store_worker_events.go, store_worker_runtime_recovery.go) receives
// the *sql.Tx as a parameter, never opens its own. This single-writer
// invariant is what keeps the runtime projection, metric sampling,
// event emit, AND partition detection atomic with respect to the
// worker snapshot upsert.

// PersistWorkerHeartbeat writes the structured projections associated with a
// heartbeat in one SQLite transaction. workers.raw_json remains a compatibility
// snapshot; runtime rows are volatile and are reconciled from active_jobs.
//
// The pipeline (single tx):
//
//  1. BeginTx + parse payload.
//  2. SELECT status, current_job, connection_state from workers (so
//     state-change events can be compared against the just-arrived
//     heartbeat).
//  3. upsertWorkerExec — write the heartbeat snapshot.
//  4. touch worker_sessions.last_seen if a sessionID was supplied.
//  5. reconcileWorkerRuntime — INSERT/UPDATE worker_task_runtime +
//     bump missing_heartbeats + emit TASK_RUNTIME_DISAPPEARED for
//     stale rows.
//  6. maybeInsertWorkerMetric — throttled sample insert.
//  7. pruneWorkerMetricSamples + pruneWorkerEvents — retention prune
//     (opt-out via days <= 0).
//  8. detectAndPersistPartitionTransition — recompute
//     connection_state from last_heartbeat_at, emit
//     WORKER_STALE_DETECTED / WORKER_PARTITION_DETECTED /
//     WORKER_PARTITION_RESOLVED on transitions, write the new state.
//  9. appendWorkerStateChangedEvent — emit on status / current_job
//     transitions.
//
// 10. Commit.
func (s *SQLiteStore) PersistWorkerHeartbeat(ctx context.Context, raw []byte, sessionID string) error {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	workerID := asString(m["worker_id"])
	if workerID == "" {
		return fmt.Errorf("persist worker heartbeat: missing worker_id")
	}
	now := time.Now().UTC()
	nowRFC3339Nano := now.Format(time.RFC3339Nano)
	lastHBAt := asString(m["last_heartbeat"])
	if lastHBAt == "" {
		lastHBAt = nowRFC3339Nano
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var oldStatus, oldJob, oldConnState string
	_ = tx.QueryRowContext(ctx, `SELECT status,current_job,connection_state FROM workers WHERE worker_id=?`, workerID).Scan(&oldStatus, &oldJob, &oldConnState)
	if err := s.upsertWorkerExec(tx, m, raw, nowRFC3339Nano); err != nil {
		return fmt.Errorf("persist worker snapshot: %w", err)
	}
	if sessionID != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE worker_sessions
			SET last_seen=?, last_seen_at=?, status='ACTIVE', revoked=0
			WHERE session_id=?`, nowRFC3339Nano, nowRFC3339Nano, sessionID); err != nil {
			return fmt.Errorf("touch worker session: %w", err)
		}
	}

	active := activeTasksFromSnapshot(m)
	if err := reconcileWorkerRuntime(ctx, tx, workerID, sessionID, active, nowRFC3339Nano); err != nil {
		return err
	}
	if err := maybeInsertWorkerMetric(ctx, tx, m, workerID, sessionID, oldStatus != asString(m["status"]), nowRFC3339Nano); err != nil {
		return err
	}
	if err := pruneWorkerMetricSamples(ctx, tx, s.retentionDays.Metrics); err != nil {
		return err
	}
	if err := pruneWorkerEvents(ctx, tx, s.retentionDays.Events); err != nil {
		return err
	}
	staleSec, partitionSec := s.partitionThresholds()
	newConnState, err := detectAndPersistPartitionTransition(ctx, tx, workerID, lastHBAt, now, staleSec, partitionSec)
	if err != nil {
		return err
	}
	_ = oldConnState // captured for symmetry with the state-change helpers; reserved for future diff-driven events.
	_ = newConnState // logged via the worker_events rows emitted by detectAndPersistPartitionTransition.
	if oldStatus != "" && (oldStatus != asString(m["status"]) || oldJob != asString(m["current_job"])) {
		if err := appendWorkerStateChangedEvent(ctx, tx, workerID, sessionID,
			oldStatus, asString(m["status"]), oldJob, asString(m["current_job"]), nowRFC3339Nano); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// partitionThresholds returns the (stale, partition) thresholds
// effective for this store. Defaults match the canonical read-side
// thresholds in workers/registry_query.go (ConnectionStaleThreshold =
// 150s, ConnectionDisconnectedThreshold = 5min) so the persistent
// mirror and the read-time derivation stay aligned.
//
// SetRetention + SetPartitionThresholds (called from production
// bootstrap) override these values; in tests the defaults apply.
func (s *SQLiteStore) partitionThresholds() (staleSec, partitionSec int) {
	stale := s.partitionKnobs.Stale
	partition := s.partitionKnobs.Partition
	if stale <= 0 {
		stale = defaultStaleThresholdSeconds
	}
	if partition <= 0 {
		partition = defaultPartitionThresholdSeconds
	}
	if stale > partition {
		// Misconfiguration: clamp to safe defaults so the
		// partition detector never silently flips every worker to
		// PARTITIONED on a bad config. Logged at the bootstrap
		// layer (cmd/server/bootstrap.go) via SetPartitionThresholds'
		// precondition check.
		return defaultStaleThresholdSeconds, defaultPartitionThresholdSeconds
	}
	return stale, partition
}

// SetRetention is the canonical entry point for the master boot
// path to wire config.RetentionConfig into the store. Idempotent;
// safe to call before NewSQLiteStoreFromHandle (no-op on a nil
// receiver).
//
// Tests can also use this to override defaults — see
// store_worker_runtime_test.go for the partition-detection +
// retention-prune coverage.
func (s *SQLiteStore) SetRetention(metricsDays, eventsDays int) {
	if s == nil {
		return
	}
	s.retentionDays.Metrics = metricsDays
	s.retentionDays.Events = eventsDays
}

// SetPartitionThresholds is the canonical entry point for the
// master boot path to wire config.WorkersConfig.{Stale,
// Partition}ThresholdSeconds into the store. Idempotent.
func (s *SQLiteStore) SetPartitionThresholds(staleSeconds, partitionSeconds int) {
	if s == nil {
		return
	}
	if staleSeconds <= 0 {
		staleSeconds = defaultStaleThresholdSeconds
	}
	if partitionSeconds <= 0 {
		partitionSeconds = defaultPartitionThresholdSeconds
	}
	if staleSeconds > partitionSeconds {
		staleSeconds = defaultStaleThresholdSeconds
		partitionSeconds = defaultPartitionThresholdSeconds
	}
	s.partitionKnobs.Stale = staleSeconds
	s.partitionKnobs.Partition = partitionSeconds
}

// retentionDays is the per-table retention window pair used by the
// prune helpers. Lives as a value type (not a pointer) so the zero
// value (0, 0) is a valid opt-out.
type retentionDays struct {
	Metrics int
	Events  int
}

// partitionThresholds is the threshold pair used by
// detectAndPersistPartitionTransition. Same value-type choice as
// retentionDays.
type partitionThresholds struct {
	Stale     int
	Partition int
}

// Default threshold constants. The canonical single source of truth for
// heartbeat staleness across the system (handlers, registry, and store).
//
// Aligned with the worker-side heartbeat idle interval (60s on the
// producer side, heartbeat_intervals.go in RemoteCodex) so the
// read-side derivation and the persist-side mirror stay in lock-step
// with the heartbeat cadence. STALE = 2.5x idle; PARTITIONED = 5x idle.
// The idle interval is the producer's heartbeat frequency; bumping
// the STALE threshold relative to it tolerates normal scheduling
// jitter + network hiccups without flipping fresh workers to STALE.
//
// The workers package's ConnectionStaleThreshold +
// ConnectionDisconnectedThreshold are compile-time aliases of these
// constants (const of const) — there is no second copy anywhere.
const (
	DefaultStaleThreshold     = 150 * time.Second
	DefaultPartitionThreshold = 5 * time.Minute

	defaultStaleThresholdSeconds     = int(DefaultStaleThreshold / time.Second)
	defaultPartitionThresholdSeconds = int(DefaultPartitionThreshold / time.Second)
)

// sql is imported to satisfy the unused-import detector for the
// ErrNoRows sentinel reference. Kept here (rather than in the
// helpers above) so the persistWorkerHeartbeat signature stays
// flat and any future ErrNoRows handling has a single import.
var _ = sql.ErrNoRows
