package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"velox-server/internal/repository"
	"velox-server/internal/sqliteerr"
)

// CompletionStore is re-exported from the repository leaf package.
type CompletionStore = repository.CompletionStore

// CompletionTx is re-exported from the repository leaf package.
type CompletionTx = repository.CompletionTx

type CompletionFence = repository.CompletionFence
type CompletionAttemptState = repository.CompletionAttemptState
type CompletionAttemptRow = repository.CompletionAttemptRow
type CompletionDeclareParams = repository.CompletionDeclareParams
type CompletionDeclarationParams = repository.CompletionDeclarationParams
type CompletionUploadState = repository.CompletionUploadState
type CompletionArtifactVerdict = repository.CompletionArtifactVerdict

const (
	CompletionKeepVerifying = repository.CompletionKeepVerifying
	CompletionReady         = repository.CompletionReady
)

type CompletionCommitResult = repository.CompletionCommitResult
type CompletionUploadBinding = repository.CompletionUploadBinding
type CompletionReconcileCandidate = repository.CompletionReconcileCandidate

var (
	ErrCompletionAttemptNotFound    = repository.ErrCompletionAttemptNotFound
	ErrCompletionTransitionConflict = repository.ErrCompletionTransitionConflict
	ErrCompletionCanonicalConflict  = repository.ErrCompletionCanonicalConflict
	ErrCompletionBindingConflict    = repository.ErrCompletionBindingConflict
)

type SQLiteCompletionStore struct {
	db            *sql.DB
	retryObserver interface{ RecordDBRetry() }
}

const completionBusyRetries = 4

func NewSQLiteCompletionStore(db *sql.DB) *SQLiteCompletionStore {
	if db == nil {
		panic("store: NewSQLiteCompletionStore requires a non-nil database")
	}
	return &SQLiteCompletionStore{db: db}
}

// SetDBRetryObserver wires the shared operational telemetry without making
// the completion package depend on the metrics implementation.
func (s *SQLiteCompletionStore) SetDBRetryObserver(observer interface{ RecordDBRetry() }) {
	if s != nil {
		s.retryObserver = observer
	}
}

func (s *SQLiteCompletionStore) Run(ctx context.Context, fn func(CompletionTx) error) error {
	if fn == nil {
		return fmt.Errorf("store: completion transaction callback is nil")
	}
	for attempt := 0; ; attempt++ {
		tx, err := s.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			if sqliteerr.IsBusy(err) && attempt < completionBusyRetries {
				if s.retryObserver != nil {
					s.retryObserver.RecordDBRetry()
				}
				if err := waitCompletionRetry(ctx, attempt); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("store: completion begin: %w", err)
		}
		ct := &sqliteCompletionTx{tx: tx}
		if err := fn(ct); err != nil {
			_ = tx.Rollback()
			if sqliteerr.IsBusy(err) && attempt < completionBusyRetries {
				if s.retryObserver != nil {
					s.retryObserver.RecordDBRetry()
				}
				if waitErr := waitCompletionRetry(ctx, attempt); waitErr != nil {
					return waitErr
				}
				continue
			}
			return err
		}
		if err := tx.Commit(); err != nil {
			if sqliteerr.IsBusy(err) && attempt < completionBusyRetries {
				if s.retryObserver != nil {
					s.retryObserver.RecordDBRetry()
				}
				if waitErr := waitCompletionRetry(ctx, attempt); waitErr != nil {
					return waitErr
				}
				continue
			}
			return fmt.Errorf("store: completion commit: %w", err)
		}
		return nil
	}
}

func waitCompletionRetry(ctx context.Context, attempt int) error {
	d := time.Duration(25*(1<<attempt)) * time.Millisecond
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-t.C:
		return nil
	}
}

type sqliteCompletionTx struct{ tx *sql.Tx }

var _ CompletionStore = (*SQLiteCompletionStore)(nil)
