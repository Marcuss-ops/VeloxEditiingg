package spool

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// TestStashUploadPlan_PersistsTargetAndToken locks the durable resume key:
// StashUploadPlan stamps commit_id + upload_id + the opaque target JSON + the
// commit token WITHOUT moving the row off OUTPUT_READY.
func TestStashUploadPlan_PersistsTargetAndToken(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()
	e := mustInsertBasic(t, s, "stash")
	if err := s.MarkReady(ctx, e.SpoolID, strings.Repeat("a", 64), 100); err != nil {
		t.Fatalf("MarkReady: %v", err)
	}

	targetJSON := `{"declaration_id":"d1","upload_id":"up1","transport_id":"master-stream.v1"}`
	if err := s.StashUploadPlan(ctx, e.SpoolID, "commit-1", "up1", targetJSON, "secret-token"); err != nil {
		t.Fatalf("StashUploadPlan: %v", err)
	}

	got, err := s.Get(ctx, e.SpoolID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != StatusOutputReady {
		t.Errorf("status = %q; want OUTPUT_READY (stash must not advance the state)", got.Status)
	}
	if got.CommitID != "commit-1" {
		t.Errorf("commit_id = %q; want commit-1", got.CommitID)
	}
	if got.UploadID != "up1" {
		t.Errorf("upload_id = %q; want up1", got.UploadID)
	}
	if got.UploadTargetJSON != targetJSON {
		t.Errorf("upload_target_json = %q; want %q", got.UploadTargetJSON, targetJSON)
	}
	if got.CommitToken != "secret-token" {
		t.Errorf("commit_token = %q; want secret-token", got.CommitToken)
	}
}

// TestStashUploadPlan_NotOutputReady_Conflicts pins the CAS gate: the stash
// only runs between MarkReady and MarkUploading.
func TestStashUploadPlan_NotOutputReady_Conflicts(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()
	e := mustInsertBasic(t, s, "stash-cas")
	// Row is still RENDERING.
	err := s.StashUploadPlan(ctx, e.SpoolID, "commit", "up", "{}", "token")
	if err == nil || !strings.Contains(err.Error(), "lifecycle CAS conflict") {
		t.Fatalf("StashUploadPlan on RENDERING: got %v; want ErrCASConflict", err)
	}
}

// TestRecordUploadFailure_BumpsAndSchedules pins the retry ledger: a failure
// increments the bounded counter, schedules next_upload_attempt_at, and leaves
// the row mid-upload (resumable), never terminal.
func TestRecordUploadFailure_BumpsAndSchedules(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()
	e := mustInsertBasic(t, s, "fail")
	_ = s.MarkReady(ctx, e.SpoolID, strings.Repeat("b", 64), 200)
	if err := s.StashUploadPlan(ctx, e.SpoolID, "commit-1", "up1", "{}", "token"); err != nil {
		t.Fatalf("StashUploadPlan: %v", err)
	}
	_ = s.MarkUploadPending(ctx, e.SpoolID, "up1")
	_ = s.MarkUploading(ctx, e.SpoolID, 0)

	next := time.Now().UTC().Add(4 * time.Second)
	if err := s.RecordUploadFailure(ctx, e.SpoolID, "connection reset", next); err != nil {
		t.Fatalf("RecordUploadFailure: %v", err)
	}

	got, err := s.Get(ctx, e.SpoolID)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.Status != StatusUploading {
		t.Errorf("status = %q; want UPLOADING (resumable)", got.Status)
	}
	if got.UploadAttemptCount != 1 {
		t.Errorf("upload_attempt_count = %d; want 1", got.UploadAttemptCount)
	}
	if got.LastError != "connection reset" {
		t.Errorf("last_error = %q; want connection reset", got.LastError)
	}
	if got.NextUploadAttemptAt.IsZero() || !got.NextUploadAttemptAt.Equal(next) {
		t.Errorf("next_upload_attempt_at = %v; want %v", got.NextUploadAttemptAt, next)
	}
}

// TestRecordUploadFailure_TerminalRow_Conflicts: a terminal row cannot be
// re-opened by a late failure stamp.
func TestRecordUploadFailure_TerminalRow_Conflicts(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()
	e := mustInsertBasic(t, s, "fail-term")
	_ = s.MarkReady(ctx, e.SpoolID, strings.Repeat("c", 64), 300)
	if err := s.MarkRejected(ctx, e.SpoolID, "E_X", "already rejected"); err != nil {
		t.Fatalf("MarkRejected: %v", err)
	}
	err := s.RecordUploadFailure(ctx, e.SpoolID, "late", time.Now())
	if err == nil || !strings.Contains(err.Error(), "lifecycle CAS conflict") {
		t.Fatalf("RecordUploadFailure on REJECTED: got %v; want ErrCASConflict", err)
	}
}

// TestListUploadResumeCandidates_OnlyTargetedResumable pins the resume query:
// rows WITH a persisted target are candidates while upload or commit completion
// is still resumable; committed rows and rows without a target are excluded.
func TestListUploadResumeCandidates_OnlyTargetedMidUpload(t *testing.T) {
	s := newInMemoryTestStore(t)
	ctx := context.Background()

	mk := func(key string, target bool) *SpoolEntry {
		e := mustInsertBasic(t, s, key)
		_ = s.MarkReady(ctx, e.SpoolID, strings.Repeat("d", 64), 10)
		if target {
			_ = s.StashUploadPlan(ctx, e.SpoolID, "commit", "up-"+key, "{}", "token")
			_ = s.MarkUploadPending(ctx, e.SpoolID, "up-"+key)
			_ = s.MarkUploading(ctx, e.SpoolID, 0)
		}
		return e
	}

	targeted := mk("targeted", true)
	_ = mk("no-target", false)       // OUTPUT_READY, no stash → excluded
	uploaded := mk("uploaded", true) // bytes done, commit ack still resumable
	_ = s.MarkUploaded(ctx, uploaded.SpoolID)
	done := mk("done", true) // terminal committed row is excluded
	_ = s.MarkUploaded(ctx, done.SpoolID)
	_ = s.MarkCommitted(ctx, done.SpoolID)

	got, err := s.ListUploadResumeCandidates(ctx, 32)
	if err != nil {
		t.Fatalf("ListUploadResumeCandidates: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("candidates = %v; want targeted + uploaded rows", got)
	}
	seen := map[string]bool{}
	for _, candidate := range got {
		seen[candidate.SpoolID] = true
	}
	if !seen[targeted.SpoolID] || !seen[uploaded.SpoolID] || seen[done.SpoolID] {
		t.Fatalf("candidates = %v; want upload=%s + uploaded=%s, not committed=%s", got, targeted.SpoolID, uploaded.SpoolID, done.SpoolID)
	}
}

// TestEnsureUploadResumeColumns_MigratesAndIsIdempotent proves the roll-forward
// migration adds the 4 ledger columns to a pre-existing spool DB and tolerates
// a re-run (fresh DBs already carry them).
func TestEnsureUploadResumeColumns_MigratesAndIsIdempotent(t *testing.T) {
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

	if err := ensureUploadResumeColumns(db); err != nil {
		t.Fatalf("first ensureUploadResumeColumns: %v", err)
	}
	if err := ensureUploadResumeColumns(db); err != nil {
		t.Fatalf("second ensureUploadResumeColumns (idempotent): %v", err)
	}

	if _, err := db.Exec(`INSERT INTO worker_output_spool
		(spool_id, task_id, attempt_id, worker_spool_key, status, created_at, updated_at)
		VALUES ('s1','T1','A1','K1','RENDERING','2026-01-01T00:00:00Z','2026-01-01T00:00:00Z')`); err != nil {
		t.Fatalf("raw insert: %v", err)
	}
	var targetJSON, token, nextAt string
	var count int
	if err := db.QueryRow(`SELECT upload_target_json, commit_token, upload_attempt_count, next_upload_attempt_at FROM worker_output_spool WHERE spool_id = 's1'`).
		Scan(&targetJSON, &token, &count, &nextAt); err != nil {
		t.Fatalf("read ledger columns: %v", err)
	}
	if targetJSON != "" || token != "" || count != 0 || nextAt != "" {
		t.Errorf("migrated ledger defaults = %q/%q/%d/%q; want all-empty/zero", targetJSON, token, count, nextAt)
	}
}
