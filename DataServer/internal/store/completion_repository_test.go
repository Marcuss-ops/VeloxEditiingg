package store

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"

	"velox-server/internal/store/migrations"

	_ "github.com/mattn/go-sqlite3"
)

func openCompletionRepositoryTestDB(t *testing.T) (*SQLiteCompletionStore, *sql.DB) {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "completion-repository.db")
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	if err := migrations.RunMigrations(db, migrations.SQLiteMigrationsFS(), "sqlite"); err != nil {
		t.Fatal(err)
	}
	return NewSQLiteCompletionStore(db), db
}

func completionDeclareParams(commitID string) CompletionDeclareParams {
	return CompletionDeclareParams{
		CommitID:            commitID,
		TaskID:              "task-completion-test",
		AttemptID:           "attempt-completion-test",
		JobID:               "job-completion-test",
		WorkerID:            "worker-completion-test",
		LeaseID:             "lease-completion-test",
		Revision:            7,
		RequiredOutputCount: 1,
		TokenHash:           "token-hash",
		Deadline:            "2026-08-10T12:00:00Z",
		Now:                 "2026-08-10T11:00:00Z",
	}
}

func TestCompletionRepository_RunCommitsAndRollsBackAsOneUnit(t *testing.T) {
	repo, db := openCompletionRepositoryTestDB(t)
	ctx := context.Background()

	params := completionDeclareParams("commit-completion-commit")
	if err := repo.Run(ctx, func(tx CompletionTx) error {
		got, err := tx.InsertCompletionAttempt(ctx, params)
		if err != nil {
			return err
		}
		if got != params.CommitID {
			t.Fatalf("InsertCompletionAttempt returned %q, want %q", got, params.CommitID)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	var committed int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM attempt_commits WHERE commit_id=?`, params.CommitID).Scan(&committed); err != nil {
		t.Fatal(err)
	}
	if committed != 1 {
		t.Fatalf("committed attempt count=%d, want 1", committed)
	}

	rollbackParams := completionDeclareParams("commit-completion-rollback")
	sentinel := errors.New("force completion transaction rollback")
	if err := repo.Run(ctx, func(tx CompletionTx) error {
		if _, err := tx.InsertCompletionAttempt(ctx, rollbackParams); err != nil {
			return err
		}
		return sentinel
	}); !errors.Is(err, sentinel) {
		t.Fatalf("Run rollback error=%v, want %v", err, sentinel)
	}

	var rolledBack int
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM attempt_commits WHERE commit_id=?`, rollbackParams.CommitID).Scan(&rolledBack); err != nil {
		t.Fatal(err)
	}
	if rolledBack != 0 {
		t.Fatalf("rolled-back attempt count=%d, want 0", rolledBack)
	}
}

func TestCompletionRepository_ReadCompletionFenceRejectsStaleIdentity(t *testing.T) {
	repo, _ := openCompletionRepositoryTestDB(t)
	ctx := context.Background()
	params := completionDeclareParams("commit-completion-fence")
	if err := repo.Run(ctx, func(tx CompletionTx) error {
		_, err := tx.InsertCompletionAttempt(ctx, params)
		return err
	}); err != nil {
		t.Fatal(err)
	}

	goodFence := CompletionFence{
		TaskID:    params.TaskID,
		AttemptID: params.AttemptID,
		WorkerID:  params.WorkerID,
		LeaseID:   params.LeaseID,
		Revision:  params.Revision,
	}
	if err := repo.Run(ctx, func(tx CompletionTx) error {
		state, err := tx.ReadCompletionFence(ctx, goodFence, false)
		if err != nil {
			return err
		}
		if state == nil || state.CommitID != params.CommitID || state.Status != "DECLARED" || state.TaskRevision != params.Revision {
			t.Fatalf("unexpected fence state: %+v", state)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	staleFence := goodFence
	staleFence.LeaseID = "stale-lease"
	if err := repo.Run(ctx, func(tx CompletionTx) error {
		_, err := tx.ReadCompletionFence(ctx, staleFence, false)
		return err
	}); !errors.Is(err, ErrCompletionTransitionConflict) {
		t.Fatalf("stale fence error=%v, want ErrCompletionTransitionConflict", err)
	}

	missingFence := goodFence
	missingFence.AttemptID = "missing-attempt"
	if err := repo.Run(ctx, func(tx CompletionTx) error {
		state, err := tx.ReadCompletionFence(ctx, missingFence, true)
		if err != nil {
			return err
		}
		if state != nil {
			t.Fatalf("allowMissing returned state: %+v", state)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}
