package completion

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

type AttemptCommitState struct {
	CommitID     string
	Status       string
	TaskRevision int
}

func (f FenceTuple) Read(ctx context.Context, tx *sql.Tx) (*AttemptCommitState, error) {
	return readFenceForTest(ctx, tx, f, false)
}

func (f FenceTuple) ReadOrMissing(ctx context.Context, tx *sql.Tx) (*AttemptCommitState, error) {
	return readFenceForTest(ctx, tx, f, true)
}

func readFenceForTest(ctx context.Context, tx *sql.Tx, f FenceTuple, allowMissing bool) (*AttemptCommitState, error) {
	if err := f.Validate(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrFenceMismatch, err)
	}
	var commitID, status, worker, lease string
	var revision int
	err := tx.QueryRowContext(ctx, `SELECT commit_id,status,worker_id,lease_id,task_revision FROM attempt_commits WHERE task_id=? AND attempt_id=?`, f.TaskID, f.AttemptID).Scan(&commitID, &status, &worker, &lease, &revision)
	if errors.Is(err, sql.ErrNoRows) {
		if allowMissing {
			return nil, nil
		}
		return nil, fmt.Errorf("%w: no attempt_commits row", ErrAttemptCommitNotFound)
	}
	if err != nil {
		return nil, err
	}
	if worker != f.WorkerID.String() || lease != f.LeaseID || revision != f.Revision {
		return nil, fmt.Errorf("%w: fence mismatch (stored worker_id=%q lease_id=%q revision=%d status=%q; supplied worker_id=%q lease_id=%q revision=%d)", ErrTransitionConflict, worker, lease, revision, status, f.WorkerID, f.LeaseID, f.Revision)
	}
	return &AttemptCommitState{CommitID: commitID, Status: status, TaskRevision: revision}, nil
}
