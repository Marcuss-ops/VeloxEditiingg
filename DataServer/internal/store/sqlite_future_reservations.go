package store

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"velox-server/internal/taskgraph"
)

var _ taskgraph.FutureReservationStore = (*SQLiteTaskRepository)(nil)

func (r *SQLiteTaskRepository) TryReserveFutureTask(ctx context.Context, reservation taskgraph.FutureReservation) (bool, error) {
	if r == nil || r.store == nil || r.store.db == nil {
		return false, fmt.Errorf("future reservation store: not initialized")
	}
	if reservation.TaskID == "" || reservation.WorkerID == "" || reservation.ReservationID == "" || reservation.Distance <= 0 {
		return false, fmt.Errorf("future reservation: incomplete identity")
	}
	if reservation.State == "" {
		reservation.State = taskgraph.ReservationReserved
	}
	now := time.Now().UTC()
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return false, err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `DELETE FROM future_task_reservations WHERE expires_at <= ?`, now.Format(time.RFC3339)); err != nil {
		return false, err
	}
	res, err := tx.ExecContext(ctx, `INSERT INTO future_task_reservations(task_id,job_id,worker_id,reservation_id,task_revision,distance,state,expires_at,created_at,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(task_id) DO NOTHING`, reservation.TaskID, reservation.JobID, reservation.WorkerID, reservation.ReservationID, reservation.TaskRevision, reservation.Distance, string(reservation.State), reservation.ExpiresAt.UTC().Format(time.RFC3339), now.Format(time.RFC3339), now.Format(time.RFC3339))
	if err != nil {
		return false, err
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, err
	}
	if rows == 0 {
		var owner string
		err = tx.QueryRowContext(ctx, `SELECT worker_id FROM future_task_reservations WHERE task_id = ?`, reservation.TaskID).Scan(&owner)
		if err != nil && err != sql.ErrNoRows {
			return false, err
		}
		if owner != reservation.WorkerID {
			return false, nil
		}
	}
	if err := tx.Commit(); err != nil {
		return false, err
	}
	return true, nil
}

func (r *SQLiteTaskRepository) ReconcileFutureReservations(ctx context.Context, workerID string, desired []taskgraph.FutureReservation) error {
	if r == nil || r.store == nil || r.store.db == nil {
		return fmt.Errorf("future reservation store: not initialized")
	}
	if strings.TrimSpace(workerID) == "" {
		return fmt.Errorf("future reservation: worker_id is required")
	}
	tx, err := r.store.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	keep := make(map[string]struct{}, len(desired))
	for _, item := range desired {
		keep[item.TaskID] = struct{}{}
	}
	rows, err := tx.QueryContext(ctx, `SELECT task_id FROM future_task_reservations WHERE worker_id = ?`, workerID)
	if err != nil {
		return err
	}
	var remove []string
	for rows.Next() {
		var taskID string
		if err := rows.Scan(&taskID); err != nil {
			_ = rows.Close()
			return err
		}
		if _, ok := keep[taskID]; !ok {
			remove = append(remove, taskID)
		}
	}
	_ = rows.Close()
	for _, taskID := range remove {
		if _, err := tx.ExecContext(ctx, `DELETE FROM future_task_reservations WHERE task_id = ? AND worker_id = ?`, taskID, workerID); err != nil {
			return err
		}
	}
	for _, item := range desired {
		if item.WorkerID != workerID || item.ExpiresAt.IsZero() {
			return fmt.Errorf("future reservation: invalid desired item %s", item.TaskID)
		}
		if item.State == "" {
			item.State = taskgraph.ReservationReserved
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO future_task_reservations(task_id,job_id,worker_id,reservation_id,task_revision,distance,state,expires_at,created_at,updated_at)
VALUES(?,?,?,?,?,?,?,?,?,?) ON CONFLICT(task_id) DO UPDATE SET job_id=excluded.job_id,worker_id=excluded.worker_id,reservation_id=excluded.reservation_id,task_revision=excluded.task_revision,distance=excluded.distance,state=CASE WHEN future_task_reservations.reservation_id=excluded.reservation_id THEN future_task_reservations.state ELSE excluded.state END,expires_at=excluded.expires_at,updated_at=excluded.updated_at`, item.TaskID, item.JobID, item.WorkerID, item.ReservationID, item.TaskRevision, item.Distance, string(item.State), item.ExpiresAt.UTC().Format(time.RFC3339), nowRFC3339(), nowRFC3339()); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (r *SQLiteTaskRepository) ListFutureReservations(ctx context.Context, workerID string) ([]taskgraph.FutureReservationWithPayload, error) {
	if r == nil || r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("future reservation store: not initialized")
	}
	query := `SELECT r.task_id,r.job_id,r.worker_id,r.reservation_id,r.task_revision,r.distance,r.expires_at,COALESCE(s.payload_json,''),COALESCE(r.state,'')
FROM future_task_reservations r LEFT JOIN task_specs s ON s.task_id=r.task_id WHERE r.expires_at > ?`
	args := []interface{}{nowRFC3339()}
	if workerID != "" {
		query += ` AND r.worker_id = ?`
		args = append(args, workerID)
	}
	query += ` ORDER BY r.worker_id,r.distance`
	rows, err := r.store.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []taskgraph.FutureReservationWithPayload
	for rows.Next() {
		var item taskgraph.FutureReservationWithPayload
		var expires, payload string
		if err := rows.Scan(&item.TaskID, &item.JobID, &item.WorkerID, &item.ReservationID, &item.TaskRevision, &item.Distance, &expires, &payload, &item.State); err != nil {
			return nil, err
		}
		item.ExpiresAt, err = time.Parse(time.RFC3339, expires)
		if err != nil {
			return nil, err
		}
		item.Payload = []byte(payload)
		out = append(out, item)
	}
	return out, rows.Err()
}

// TransferFutureTask atomically moves an active preparation reservation
// from expectedWorkerID to reservation.WorkerID. It is the fallback CAS:
// a reconnect/race that changes the owner causes (false, nil), never an
// overwrite of a newer reservation.
func (r *SQLiteTaskRepository) TransferFutureTask(ctx context.Context, taskID, expectedWorkerID string, reservation taskgraph.FutureReservation) (bool, error) {
	if r == nil || r.store == nil || r.store.db == nil {
		return false, fmt.Errorf("future reservation store: not initialized")
	}
	if taskID == "" || expectedWorkerID == "" || reservation.TaskID != taskID || reservation.WorkerID == "" || reservation.ReservationID == "" || reservation.ExpiresAt.IsZero() {
		return false, fmt.Errorf("future reservation: incomplete transfer")
	}
	if reservation.WorkerID == expectedWorkerID {
		return false, fmt.Errorf("future reservation: transfer target must differ from current worker")
	}
	res, err := r.store.db.ExecContext(ctx, `UPDATE future_task_reservations
SET job_id = ?, worker_id = ?, reservation_id = ?, task_revision = ?, distance = ?, state = ?, expires_at = ?, updated_at = ?
WHERE task_id = ? AND worker_id = ? AND expires_at > ?`,
		reservation.JobID, reservation.WorkerID, reservation.ReservationID, reservation.TaskRevision,
		reservation.Distance, string(taskgraph.ReservationReserved), reservation.ExpiresAt.UTC().Format(time.RFC3339), nowRFC3339(),
		taskID, expectedWorkerID, nowRFC3339())
	if err != nil {
		return false, wrapDBInfrastructure("future reservation transfer", err)
	}
	rows, err := res.RowsAffected()
	if err != nil {
		return false, wrapDBInfrastructure("future reservation transfer rows", err)
	}
	return rows == 1, nil
}

func (r *SQLiteTaskRepository) FutureTaskPayload(ctx context.Context, taskID string) ([]byte, error) {
	if r == nil || r.store == nil || r.store.db == nil {
		return nil, fmt.Errorf("future reservation store: not initialized")
	}
	var payload string
	err := r.store.db.QueryRowContext(ctx, `SELECT COALESCE(payload_json,'') FROM task_specs WHERE task_id = ?`, taskID).Scan(&payload)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	return []byte(payload), err
}

func reservationStateTransitionAllowed(from, to taskgraph.ReservationState) bool {
	if to == "" {
		return false
	}
	if from == to {
		return true
	}
	if from == "" {
		return true
	}
	switch from {
	case taskgraph.ReservationReserved:
		return to == taskgraph.ReservationPlanning || to == taskgraph.ReservationPreparing || to == taskgraph.ReservationPrepared || to == taskgraph.ReservationExpired
	case taskgraph.ReservationPlanning:
		return to == taskgraph.ReservationPreparing || to == taskgraph.ReservationPrepared || to == taskgraph.ReservationExpired
	case taskgraph.ReservationPreparing:
		return to == taskgraph.ReservationPrepared || to == taskgraph.ReservationExpired
	case taskgraph.ReservationPrepared, taskgraph.ReservationExpired:
		return false
	default:
		return false
	}
}

// UpdateReservationState advances the reservation lifecycle state.
// The state column was added in migration 164; older databases without
// the column return an error. State transitions are monotonic: a refresh may
// never downgrade PREPARING/PREPARED evidence back to PLANNING/RESERVED.
func (r *SQLiteTaskRepository) UpdateReservationState(ctx context.Context, reservationID string, state taskgraph.ReservationState) error {
	if r == nil || r.store == nil || r.store.db == nil {
		return fmt.Errorf("future reservation store: not initialized")
	}
	if reservationID == "" || state == "" {
		return nil
	}
	var currentRaw string
	err := r.store.db.QueryRowContext(ctx,
		`SELECT COALESCE(state,'') FROM future_task_reservations WHERE reservation_id = ?`,
		reservationID).Scan(&currentRaw)
	if err == sql.ErrNoRows {
		return nil
	}
	if err != nil {
		return wrapDBInfrastructure("future reservation state read", err)
	}
	current := taskgraph.ReservationState(currentRaw)
	if !reservationStateTransitionAllowed(current, state) {
		return fmt.Errorf("future reservation: invalid state transition %s -> %s", current, state)
	}
	res, err := r.store.db.ExecContext(ctx,
		`UPDATE future_task_reservations SET state = ?, updated_at = ? WHERE reservation_id = ? AND COALESCE(state,'') = ?`,
		string(state), nowRFC3339(), reservationID, currentRaw)
	if err != nil {
		return wrapDBInfrastructure("future reservation state update", err)
	}
	// A zero-row CAS means another event advanced the reservation between the
	// read and write. Treat that as a replay-safe no-op rather than forcing a
	// stale transition over newer evidence.
	_, _ = res.RowsAffected()
	return nil
}
