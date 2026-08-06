package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// runtimeIdentitySchemaAvailableTx reports whether production runtime identity
// tables are present. Minimal historical test schemas predate these tables and
// remain compatible with empty identity fields.
func runtimeIdentitySchemaAvailableTx(ctx context.Context, tx *sql.Tx) (bool, error) {
	var count int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM sqlite_master
		 WHERE type = 'table'
		   AND name IN ('worker_sessions', 'worker_runtime_snapshots')`).Scan(&count); err != nil {
		return false, err
	}
	if count == 1 {
		return false, fmt.Errorf("runtime identity schema is incomplete: expected worker_sessions and worker_runtime_snapshots")
	}
	return count == 2, nil
}

// validateWorkerRuntimeIdentityTx enforces the canonical identity tuple used
// when minting a TaskAttempt. The session must be an active control session
// for workerID, and the snapshot must belong to the same worker/session pair.
// It runs before the task CAS/attempt INSERT, so a spoofed or stale tuple
// cannot leave a claimed task or partial attempt behind.
//
// Minimal schemas without the runtime identity tables are intentionally
// accepted for backward-compatible repository fixtures and pre-migration DBs.
func validateWorkerRuntimeIdentityTx(
	ctx context.Context,
	tx *sql.Tx,
	workerID, sessionID, snapshotID string,
) error {
	available, err := runtimeIdentitySchemaAvailableTx(ctx, tx)
	if err != nil {
		return wrapDBInfrastructure("runtime identity schema probe", err)
	}
	if !available {
		return nil
	}
	if workerID == "" || sessionID == "" || snapshotID == "" {
		return fmt.Errorf("canonical runtime identity requires worker_id, worker_session_id, and worker_snapshot_id")
	}

	var sessionCount int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM worker_sessions
		 WHERE session_id = ? AND worker_id = ?
		   AND session_type = 'control'
		   AND status = 'ACTIVE' AND revoked = 0`,
		sessionID, workerID).Scan(&sessionCount); err != nil {
		return wrapDBInfrastructure("canonical worker session lookup", err)
	}
	if sessionCount != 1 {
		return fmt.Errorf("canonical worker session %q is not active for worker %q", sessionID, workerID)
	}

	var snapshotCount int
	if err := tx.QueryRowContext(ctx, `
		SELECT COUNT(*) FROM worker_runtime_snapshots
		 WHERE snapshot_id = ? AND worker_id = ? AND session_id = ?`,
		snapshotID, workerID, sessionID).Scan(&snapshotCount); err != nil {
		return wrapDBInfrastructure("canonical worker snapshot lookup", err)
	}
	if snapshotCount != 1 {
		return fmt.Errorf("canonical worker snapshot %q is not bound to worker %q/session %q", snapshotID, workerID, sessionID)
	}
	return nil
}

// resolveWorkerRuntimeIdentityTx recovers the canonical runtime identity for
// the legacy ClaimNextWithAttemptAtomic API, whose signature predates session
// and snapshot IDs. The placement path supplies both IDs explicitly.
//
// When the runtime tables are unavailable, empty identity fields preserve
// compatibility with minimal historical schemas. When they are available,
// the caller validates the resolved tuple before inserting the attempt.
func resolveWorkerRuntimeIdentityTx(ctx context.Context, tx *sql.Tx, workerID string) (sessionID, snapshotID string, err error) {
	if tx == nil || workerID == "" {
		return "", "", nil
	}
	available, err := runtimeIdentitySchemaAvailableTx(ctx, tx)
	if err != nil {
		return "", "", wrapDBInfrastructure("runtime identity schema probe", err)
	}
	if !available {
		return "", "", nil
	}

	err = tx.QueryRowContext(ctx, `
		SELECT session_id
		  FROM worker_sessions
		 WHERE worker_id = ?
		   AND session_type = 'control'
		   AND status = 'ACTIVE'
		   AND revoked = 0
		 ORDER BY COALESCE(last_seen_at, created_at) DESC, session_id DESC
		 LIMIT 1`, workerID).Scan(&sessionID)
	if errors.Is(err, sql.ErrNoRows) {
		return "", "", nil
	}
	if err != nil {
		return "", "", wrapDBInfrastructure("resolve worker session", err)
	}
	if sessionID == "" {
		return "", "", nil
	}

	err = tx.QueryRowContext(ctx, `
		SELECT snapshot_id
		  FROM worker_runtime_snapshots
		 WHERE worker_id = ? AND session_id = ?
		 LIMIT 1`, workerID, sessionID).Scan(&snapshotID)
	if errors.Is(err, sql.ErrNoRows) {
		return sessionID, "", nil
	}
	if err != nil {
		return "", "", wrapDBInfrastructure("resolve worker snapshot", err)
	}
	return sessionID, snapshotID, nil
}
