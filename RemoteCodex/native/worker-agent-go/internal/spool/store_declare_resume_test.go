package spool

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// TestInsert_PersistsOutputKind pins the new manifest-ledger column: the
// OutputKind stamped at Insert survives the round-trip so the declare-resume
// loop can rebuild TaskOutputDeclared after a restart.
func TestInsert_PersistsOutputKind(t *testing.T) {
	s := newInMemoryTestStore(t)
	in := SpoolEntry{
		TaskID:         "task-ok",
		AttemptID:      "attempt-ok",
		WorkerSpoolKey: "wsk-ok",
		OutputKind:     "final_video",
		LocalPath:      "/var/scratch/ok.mp4",
	}
	out, err := s.Insert(context.Background(), in)
	if err != nil {
		t.Fatalf("Insert: %v", err)
	}
	got, err := s.Get(context.Background(), out.SpoolID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.OutputKind != "final_video" {
		t.Fatalf("OutputKind = %q; want final_video", got.OutputKind)
	}
}

// TestRecordUploadFailure_OutputReadyIsResumable pins the declare-phase ledger:
// an OUTPUT_READY row (declare receipt failed) records the failure + backoff
// and stays OUTPUT_READY (never terminal).
func TestRecordUploadFailure_OutputReadyIsResumable(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()
	e := mustInsertBasic(t, s, "declare-fail")
	_ = s.MarkReady(ctx, e.SpoolID, strings.Repeat("e", 64), 123)

	next := time.Now().UTC().Add(2 * time.Second)
	if err := s.RecordUploadFailure(ctx, e.SpoolID, "declare receipt: transport is closed", next); err != nil {
		t.Fatalf("RecordUploadFailure on OUTPUT_READY: %v", err)
	}
	got, err := s.Get(ctx, e.SpoolID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != StatusOutputReady {
		t.Fatalf("status = %q; want OUTPUT_READY (declare failure is resumable)", got.Status)
	}
	if got.UploadAttemptCount != 1 {
		t.Fatalf("upload_attempt_count = %d; want 1", got.UploadAttemptCount)
	}
	if got.LastError != "declare receipt: transport is closed" {
		t.Fatalf("last_error = %q; want the declare failure", got.LastError)
	}
	if got.NextUploadAttemptAt.IsZero() || !got.NextUploadAttemptAt.Equal(next) {
		t.Fatalf("next_upload_attempt_at = %v; want %v", got.NextUploadAttemptAt, next)
	}
}

// TestListDeclareResumeCandidates_OnlyUndeclaredReady pins the declare-resume
// query: OUTPUT_READY rows WITHOUT a persisted upload target are candidates;
// rows with a target (mid-upload) and terminal rows are excluded.
func TestListDeclareResumeCandidates_OnlyUndeclaredReady(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()

	undeclared := mustInsertBasic(t, s, "undeclared")
	_ = s.MarkReady(ctx, undeclared.SpoolID, strings.Repeat("f", 64), 10)

	stashed := mustInsertBasic(t, s, "stashed")
	_ = s.MarkReady(ctx, stashed.SpoolID, strings.Repeat("a", 64), 10)
	_ = s.StashUploadPlan(ctx, stashed.SpoolID, "commit", "up-stashed", "{}", "token")

	done := mustInsertBasic(t, s, "done")
	_ = s.MarkReady(ctx, done.SpoolID, strings.Repeat("b", 64), 10)
	_ = s.MarkRejected(ctx, done.SpoolID, "E_X", "rejected")

	got, err := s.ListDeclareResumeCandidates(ctx, 32)
	if err != nil {
		t.Fatalf("ListDeclareResumeCandidates: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("candidates = %v; want only the undeclared OUTPUT_READY row", got)
	}
	if got[0].SpoolID != undeclared.SpoolID {
		t.Fatalf("candidate spool_id = %q; want %q", got[0].SpoolID, undeclared.SpoolID)
	}
}

// TestEnsureOutputKindColumn_MigratesAndIsIdempotent proves the roll-forward
// migration adds output_kind to a pre-existing spool DB and tolerates a re-run.
func TestEnsureOutputKindColumn_MigratesAndIsIdempotent(t *testing.T) {
	const oldDDL = `CREATE TABLE worker_output_spool (
		spool_id        TEXT PRIMARY KEY,
		task_id         TEXT NOT NULL,
		attempt_id      TEXT NOT NULL,
		commit_id       TEXT NOT NULL DEFAULT '',
		worker_spool_key TEXT NOT NULL,
		local_path      TEXT NOT NULL DEFAULT '',
		sha256          TEXT NOT NULL DEFAULT '',
		size_bytes      INTEGER NOT NULL DEFAULT 0,
		upload_id       TEXT NOT NULL DEFAULT '',
		uploaded_bytes  INTEGER NOT NULL DEFAULT 0,
		status          TEXT NOT NULL,
		storage_tier    TEXT NOT NULL DEFAULT 'NVME_DURABLE',
		last_error      TEXT NOT NULL DEFAULT '',
		created_at      TEXT NOT NULL,
		updated_at      TEXT NOT NULL,
		UNIQUE(task_id, attempt_id, worker_spool_key)
	)`
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("sql.Open: %v", err)
	}
	defer db.Close()
	if _, err := db.Exec(oldDDL); err != nil {
		t.Fatalf("create old schema: %v", err)
	}

	if err := ensureOutputKindColumn(db); err != nil {
		t.Fatalf("first ensureOutputKindColumn: %v", err)
	}
	if err := ensureOutputKindColumn(db); err != nil {
		t.Fatalf("second ensureOutputKindColumn (idempotent): %v", err)
	}

	if _, err := db.Exec(`INSERT INTO worker_output_spool
		(spool_id, task_id, attempt_id, worker_spool_key, status, created_at, updated_at)
		VALUES ('s1','T1','A1','K1','RENDERING','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("raw insert: %v", err)
	}
	var outputKind string
	if err := db.QueryRow(`SELECT output_kind FROM worker_output_spool WHERE spool_id = 's1'`).Scan(&outputKind); err != nil {
		t.Fatalf("read output_kind column: %v", err)
	}
	if outputKind != "" {
		t.Fatalf("migrated output_kind = %q; want empty default", outputKind)
	}
}
