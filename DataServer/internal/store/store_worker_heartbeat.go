package store

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

// store_worker_heartbeat.go owns the worker-side heartbeat persistence
// pipeline. It is the ONLY place in this refactor where the SQLite
// transaction is opened (via s.db.BeginTx); every helper living in the
// sibling files (store_worker_runtime_projection.go, store_worker_metrics.go,
// store_worker_events.go) receives the *sql.Tx as a parameter, never opens
// its own. This single-writer invariant is what keeps the runtime
// projection, metric sampling, and event emit atomic with respect to the
// worker snapshot upsert.

// PersistWorkerHeartbeat writes the structured projections associated with a
// heartbeat in one SQLite transaction. workers.raw_json remains a compatibility
// snapshot; runtime rows are volatile and are reconciled from active_jobs.
func (s *SQLiteStore) PersistWorkerHeartbeat(ctx context.Context, raw []byte, sessionID string) error {
	var m map[string]any
	if err := json.Unmarshal(raw, &m); err != nil {
		return err
	}
	workerID := asString(m["worker_id"])
	if workerID == "" {
		return fmt.Errorf("persist worker heartbeat: missing worker_id")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	var oldStatus, oldJob string
	_ = tx.QueryRowContext(ctx, `SELECT status,current_job FROM workers WHERE worker_id=?`, workerID).Scan(&oldStatus, &oldJob)
	if err := s.upsertWorkerExec(tx, m, raw, now); err != nil {
		return fmt.Errorf("persist worker snapshot: %w", err)
	}
	if sessionID != "" {
		if _, err := tx.ExecContext(ctx, `UPDATE worker_sessions
			SET last_seen=?, last_seen_at=?, status='ACTIVE', revoked=0
			WHERE session_id=?`, now, now, sessionID); err != nil {
			return fmt.Errorf("touch worker session: %w", err)
		}
	}

	active := activeTasksFromSnapshot(m)
	if err := reconcileWorkerRuntime(ctx, tx, workerID, sessionID, active, now); err != nil {
		return err
	}
	if err := maybeInsertWorkerMetric(ctx, tx, m, workerID, sessionID, oldStatus != asString(m["status"]), now); err != nil {
		return err
	}
	if err := pruneWorkerMetricSamples(ctx, tx); err != nil {
		return err
	}
	if oldStatus != "" && (oldStatus != asString(m["status"]) || oldJob != asString(m["current_job"])) {
		if err := appendWorkerStateChangedEvent(ctx, tx, workerID, sessionID,
			oldStatus, asString(m["status"]), oldJob, asString(m["current_job"]), now); err != nil {
			return err
		}
	}
	return tx.Commit()
}
