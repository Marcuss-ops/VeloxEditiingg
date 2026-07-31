package store

import (
	"context"
	"database/sql"
)

// resolveWorkerRuntimeIdentityTx recovers the canonical runtime identity for
// the legacy ClaimNextWithAttemptAtomic API, whose signature predates session
// and snapshot IDs. The push placement path does not use this fallback: it
// supplies both IDs explicitly to ClaimTaskForWorkerAtomic.
//
// Historical/minimal test schemas may not contain worker_sessions or
// worker_runtime_snapshots. In that case the attempt remains backward
// compatible with empty identity fields rather than making an otherwise
// unrelated claim fail.
func resolveWorkerRuntimeIdentityTx(ctx context.Context, tx *sql.Tx, workerID string) (sessionID, snapshotID string) {
	if tx == nil || workerID == "" {
		return "", ""
	}

	err := tx.QueryRowContext(ctx, `
		SELECT session_id
		  FROM worker_sessions
		 WHERE worker_id = ?
		   AND session_type = 'control'
		   AND status = 'ACTIVE'
		   AND revoked = 0
		 ORDER BY COALESCE(last_seen_at, created_at) DESC, session_id DESC
		 LIMIT 1`, workerID).Scan(&sessionID)
	if err != nil || sessionID == "" {
		return "", ""
	}

	err = tx.QueryRowContext(ctx, `
		SELECT snapshot_id
		  FROM worker_runtime_snapshots
		 WHERE worker_id = ? AND session_id = ?
		 LIMIT 1`, workerID, sessionID).Scan(&snapshotID)
	if err != nil {
		return sessionID, ""
	}
	return sessionID, snapshotID
}
