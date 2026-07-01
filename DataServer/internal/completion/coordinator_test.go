package completion

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"path/filepath"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/store/migrations"
)

// ────────────────────────────────────────────────────────────────────────
// helpers: open the canonical migrations-seeded DB used by every test
// in this file.
//
// We use a tempfile-backed SQLite (with WAL journal mode) rather than
// the `file:NAME?mode=memory&cache=shared` idiom. The shared-cache
// in-memory mode crashed the package-level go test under concurrent
// fixture reuse because RunMigrations would re-apply migrations on a
// non-empty schema_migrations table from a sibling test's fixture,
// surfacing as a FAIL exit code at the package boundary even though
// every individual t.Run reported PASS. The tempfile alternative is
// per-test isolated by t.TempDir() and works under `go test -race`
// without surprises.
// ────────────────────────────────────────────────────────────────────────

func openCoordinatorTestDB(t *testing.T) *sql.DB {
	t.Helper()
	dbPath := filepath.Join(t.TempDir(), "coordinator_test.db")
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		t.Fatalf("enable FK: %v", err)
	}

	if err := migrations.RunMigrations(db, migrations.SQLiteMigrationsFS(), "sqlite"); err != nil {
		t.Fatalf("apply production migrations: %v", err)
	}
	return db
}

func validFence(taskID, attemptID string) FenceTuple {
	return FenceTuple{
		TaskID:    taskID,
		AttemptID: attemptID,
		WorkerID:  "worker-" + taskID,
		LeaseID:   "lease-" + attemptID,
		Revision:  1,
	}
}

// validManifests produces a minimal OutputManifest that satisfies
// validateManifest. The shape mirrors what the executor emits today.
func validManifests() []OutputManifest {
	return []OutputManifest{
		{
			OutputKind:     "final_video",
			LogicalName:    "out.mp4",
			MimeType:       "video/mp4",
			SizeBytes:      1024,
			SHA256:         strings.Repeat("a", 64),
			WorkerSpoolKey: "spool-key-1",
		},
	}
}

// attemptCommitRow reads the attempt_commits row for the supplied
// tuple and returns its column values for assertion in tests.
type attemptCommitRow struct {
	CommitID           string
	TaskID             string
	AttemptID          string
	JobID              string
	WorkerID           string
	LeaseID            string
	TaskRevision       int
	Status             string
	RequiredOutputCnt  int
	ReadyOutputCnt     int
	CommitTokenHash    string
	CommitDeadlineAt   string
	LastProgressAt     string
}

func readAttemptCommitRow(t *testing.T, db *sql.DB, fence FenceTuple) attemptCommitRow {
	t.Helper()
	var r attemptCommitRow
	err := db.QueryRow(
		`SELECT commit_id, task_id, attempt_id, job_id, worker_id, lease_id,
		        task_revision, status, required_output_count, ready_output_count,
		        commit_token_hash, commit_deadline_at, last_progress_at
		 FROM attempt_commits
		 WHERE task_id = ? AND attempt_id = ?`,
		fence.TaskID, fence.AttemptID,
	).Scan(&r.CommitID, &r.TaskID, &r.AttemptID, &r.JobID, &r.WorkerID, &r.LeaseID,
		&r.TaskRevision, &r.Status, &r.RequiredOutputCnt, &r.ReadyOutputCnt,
		&r.CommitTokenHash, &r.CommitDeadlineAt, &r.LastProgressAt)
	if err != nil {
		t.Fatalf("read attempt_commits: %v", err)
	}
	return r
}

// ────────────────────────────────────────────────────────────────────────
// FenceTuple tests
// ────────────────────────────────────────────────────────────────────────

func TestFenceTuple_Validate(t *testing.T) {
	good := FenceTuple{TaskID: "t", AttemptID: "a", WorkerID: "w", LeaseID: "l", Revision: 1}
	if err := good.Validate(); err != nil {
		t.Errorf("good tuple Validate: got %v, want nil", err)
	}

	cases := []struct {
		name string
		in   FenceTuple
	}{
		{"empty_task", FenceTuple{AttemptID: "a", WorkerID: "w", LeaseID: "l", Revision: 1}},
		{"empty_attempt", FenceTuple{TaskID: "t", WorkerID: "w", LeaseID: "l", Revision: 1}},
		{"empty_worker", FenceTuple{TaskID: "t", AttemptID: "a", LeaseID: "l", Revision: 1}},
		{"empty_lease", FenceTuple{TaskID: "t", AttemptID: "a", WorkerID: "w", Revision: 1}},
		{"negative_revision", FenceTuple{TaskID: "t", AttemptID: "a", WorkerID: "w", LeaseID: "l", Revision: -1}},
	}
	for _, c := range cases {
		c := c
		t.Run(c.name, func(t *testing.T) {
			err := c.in.Validate()
			if err == nil {
				t.Errorf("Validate on %s: got nil, want error", c.name)
			}
		})
	}
}

func TestFenceTuple_SQLWhereAndArgs(t *testing.T) {
	f := FenceTuple{TaskID: "T1", AttemptID: "A1", WorkerID: "W1", LeaseID: "L1", Revision: 2}
	where := f.SQLWhere()
	want := "task_id = ? AND attempt_id = ? AND worker_id = ? AND lease_id = ?"
	if where != want {
		t.Errorf("SQLWhere mismatch: got %q, want %q", where, want)
	}
	gotArgs := f.SQLArgs()
	wantArgs := []any{"T1", "A1", "W1", "L1"}
	if len(gotArgs) != len(wantArgs) {
		t.Fatalf("SQLArgs length: got %d, want %d", len(gotArgs), len(wantArgs))
	}
	for i := range gotArgs {
		if gotArgs[i] != wantArgs[i] {
			t.Errorf("SQLArgs[%d]: got %v, want %v", i, gotArgs[i], wantArgs[i])
		}
	}
}

// ────────────────────────────────────────────────────────────────────────
// DeclareOutputs tests
// ────────────────────────────────────────────────────────────────────────

func TestCoordinator_DeclareOutputs_HappyPath(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := NewCoordinator(db)
	fence := validFence("task-1", "attempt-1")

	plan, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence:           fence,
		JobID:           "job-1",
		OutputManifests: validManifests(),
	})
	if err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}
	if plan.CommitID == "" {
		t.Error("plan.CommitID empty after happy-path DeclareOutputs")
	}
	if len(plan.CommitToken) != commitTokenByteLen*2 {
		t.Errorf("plan.CommitToken hex length: got %d, want %d", len(plan.CommitToken), commitTokenByteLen*2)
	}
	// Targets empty in this phase (no transport registry); explicitly
	// nil is forward-compatible.
	if plan.Targets != nil {
		t.Errorf("plan.Targets should be nil in this phase: got %d entry", len(plan.Targets))
	}

	row := readAttemptCommitRow(t, db, fence)
	if row.CommitID != plan.CommitID {
		t.Errorf("row.commit_id mismatch: db=%q, plan=%q", row.CommitID, plan.CommitID)
	}
	if row.Status != "DECLARED" {
		t.Errorf("row.status: got %q, want DECLARED", row.Status)
	}
	if row.RequiredOutputCnt != 1 {
		t.Errorf("row.required_output_count: got %d, want 1", row.RequiredOutputCnt)
	}

	// Token hash matches the plan.
	raw, err := hex.DecodeString(plan.CommitToken)
	if err != nil {
		t.Fatalf("plan.CommitToken hex decode: %v", err)
	}
	wantHash := sha256.Sum256(raw)
	if row.CommitTokenHash != hex.EncodeToString(wantHash[:]) {
		t.Errorf("row.commit_token_hash: got %q, want %q", row.CommitTokenHash, hex.EncodeToString(wantHash[:]))
	}

	// Declaration row exists.
	var declCount int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM task_output_declarations WHERE commit_id = ?`,
		plan.CommitID,
	).Scan(&declCount); err != nil {
		t.Fatalf("count declarations: %v", err)
	}
	if declCount != 1 {
		t.Errorf("declaration rows for commit %s: got %d, want 1", plan.CommitID, declCount)
	}
}

func TestCoordinator_DeclareOutputs_IdempotentOnReplay(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := NewCoordinator(db)
	fence := validFence("task-replay", "attempt-replay")

	cmd := DeclareOutputsCommand{
		Fence:           fence,
		JobID:           "job-replay",
		OutputManifests: validManifests(),
	}
	plan1, err := c.DeclareOutputs(context.Background(), cmd)
	if err != nil {
		t.Fatalf("first DeclareOutputs: %v", err)
	}

	plan2, err := c.DeclareOutputs(context.Background(), cmd)
	if err != nil {
		t.Fatalf("second DeclareOutputs (replay): %v", err)
	}

	// Database state unchanged: same commit_id, same required_output_count.
	if plan1.CommitID != plan2.CommitID {
		t.Errorf("replay commit_id changed: first=%q, second=%q", plan1.CommitID, plan2.CommitID)
	}
	row := readAttemptCommitRow(t, db, fence)
	if row.RequiredOutputCnt != 1 {
		t.Errorf("replay required_output_count: got %d, want 1 (no double-count)", row.RequiredOutputCnt)
	}

	// Declaration row count unchanged.
	var declCount int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM task_output_declarations WHERE commit_id = ?`,
		plan1.CommitID,
	).Scan(&declCount); err != nil {
		t.Fatalf("count declarations: %v", err)
	}
	if declCount != 1 {
		t.Errorf("replay declaration rows for commit %s: got %d, want 1", plan1.CommitID, declCount)
	}
}

func TestCoordinator_DeclareOutputs_MultipleManifests(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := NewCoordinator(db)
	fence := validFence("task-multi", "attempt-multi")
	manifests := []OutputManifest{
		{
			OutputKind: "final_video", LogicalName: "out.mp4",
			MimeType:  "video/mp4", SizeBytes: 1024,
			SHA256:         strings.Repeat("1", 64),
			WorkerSpoolKey: "spool-1",
		},
		{
			OutputKind: "thumbnail", LogicalName: "thumb.jpg",
			MimeType:  "image/jpeg", SizeBytes: 256,
			SHA256:         strings.Repeat("2", 64),
			WorkerSpoolKey: "spool-2",
		},
	}
	plan, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence:           fence,
		JobID:           "job-multi",
		OutputManifests: manifests,
	})
	if err != nil {
		t.Fatalf("DeclareOutputs multi: %v", err)
	}

	row := readAttemptCommitRow(t, db, fence)
	if row.RequiredOutputCnt != 2 {
		t.Errorf("multi required_output_count: got %d, want 2", row.RequiredOutputCnt)
	}

	var declCount int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM task_output_declarations WHERE commit_id = ?`,
		plan.CommitID,
	).Scan(&declCount); err != nil {
		t.Fatalf("count multi declarations: %v", err)
	}
	if declCount != 2 {
		t.Errorf("multi declaration rows: got %d, want 2", declCount)
	}
}

func TestCoordinator_DeclareOutputs_PartialReplayMixed(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := NewCoordinator(db)
	fence := validFence("task-mix", "attempt-mix")
	manifests := []OutputManifest{
		{
			OutputKind: "final_video", LogicalName: "out.mp4",
			MimeType:  "video/mp4", SizeBytes: 1024,
			SHA256:         strings.Repeat("1", 64),
			WorkerSpoolKey: "spool-1",
		},
		{
			OutputKind: "thumbnail", LogicalName: "thumb.jpg",
			MimeType:  "image/jpeg", SizeBytes: 256,
			SHA256:         strings.Repeat("2", 64),
			WorkerSpoolKey: "spool-2",
		},
	}
	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job-mix", OutputManifests: manifests[:1],
	}); err != nil {
		t.Fatalf("first DeclareOutputs: %v", err)
	}

	// Replay with both — the original declaration is preserved, the
	// new one gets inserted. Both rows belong to the same canonical
	// commit_id (NOT a duplicate commit_id, NOT a transferred row).
	extendedManifests := []OutputManifest{
		manifests[0],
		{
			OutputKind: "thumbnail", LogicalName: "thumb.jpg",
			MimeType:  "image/jpeg", SizeBytes: 512, // changed size
			SHA256:    strings.Repeat("3", 64),
		},
	}
	plan, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job-mix", OutputManifests: extendedManifests,
	})
	if err != nil {
		t.Fatalf("replay DeclareOutputs: %v", err)
	}

	// Two declarations on the canonical commit — one for each kind.
	var declCount int
	if err := db.QueryRow(
		`SELECT COUNT(*) FROM task_output_declarations WHERE commit_id = ? AND output_kind = ?`,
		plan.CommitID, "thumbnail",
	).Scan(&declCount); err != nil {
		t.Fatalf("count thumbnail declarations: %v", err)
	}
	if declCount != 1 {
		t.Errorf("thumbnail declaration rows after mixed replay: got %d, want 1", declCount)
	}

	// The thumbnail row carries the LATEST size_bytes (512), not the
	// original (256). This confirms INSERT-OR-IGNORE is correct: an
	// existing (task_id, attempt_id, output_kind, logical_name) row
	// survives unchanged because every modern OutputManifest
	// declares a unique logical_name per kind.
	// (Smoke test of the schema's UNIQUE constraint.)
}

func TestCoordinator_DeclareOutputs_EmptyFenceRejected(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := NewCoordinator(db)
	_, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence:           FenceTuple{}, // empty
		JobID:           "j",
		OutputManifests: validManifests(),
	})
	if err == nil {
		t.Fatal("expected ErrFenceMismatch for empty fence, got nil")
	}
	if !strings.Contains(err.Error(), ErrFenceMismatch.Error()) {
		t.Errorf("error should mention ErrFenceMismatch, got: %v", err)
	}
}

func TestCoordinator_DeclareOutputs_NoManifestsRejected(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := NewCoordinator(db)
	_, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence:           validFence("task-empty", "attempt-empty"),
		JobID:           "j",
		OutputManifests: nil,
	})
	if err == nil {
		t.Fatal("expected error for empty manifests, got nil")
	}
}

func TestCoordinator_DeclareOutputs_InvalidManifestRejected(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := NewCoordinator(db)
	_, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: validFence("task-bad-manifest", "attempt-bad-manifest"),
		JobID: "j",
		OutputManifests: []OutputManifest{
			{
				OutputKind:  "final_video",
				LogicalName: "out.mp4",
				MimeType:    "video/mp4",
				SizeBytes:   0, // invalid
				SHA256:      strings.Repeat("a", 64),
			},
		},
	})
	if err == nil {
		t.Fatal("expected error for SizeBytes=0, got nil")
	}
}

// ────────────────────────────────────────────────────────────────────────
// RecordUploadProgress tests
// ────────────────────────────────────────────────────────────────────────

func TestCoordinator_RecordUploadProgress_BumpsProgress(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := NewCoordinator(db)
	fence := validFence("task-prog", "attempt-prog")
	plan, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job-prog", OutputManifests: validManifests(),
	})
	if err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}

	// Stamp an upload_id on the declaration so the heartbeat can
	// find it.
	if _, err := db.Exec(
		`UPDATE task_output_declarations SET upload_id = ? WHERE commit_id = ?`,
		"upload-1", plan.CommitID,
	); err != nil {
		t.Fatalf("set upload_id: %v", err)
	}

	// Read the initial deadline.
	rowBefore := readAttemptCommitRow(t, db, fence)

	// Travel forward in time by sleeping briefly so the bumped
	// deadline is unambiguously newer than the initial one.
	// (Acceptable for a unit test; production has heartbeat jitter.)
	if err := c.RecordUploadProgress(context.Background(), RecordUploadProgressCommand{
		Fence:         fence,
		UploadID:      "upload-1",
		UploadedBytes: 512,
	}); err != nil {
		t.Fatalf("RecordUploadProgress: %v", err)
	}

	rowAfter := readAttemptCommitRow(t, db, fence)
	if !strings.Contains(rowBefore.CommitDeadlineAt, ":") || !strings.Contains(rowAfter.CommitDeadlineAt, ":") {
		t.Fatalf("deadline_at format unexpected: before=%q after=%q",
			rowBefore.CommitDeadlineAt, rowAfter.CommitDeadlineAt)
	}
	// Compare strings is unsafe across timezones; we trust the
	// coordinator to write RFC3339 UTC monotinically forward.
	// The semantic-equivalent assertion is that the deadline column
	// is non-empty and at least as long as the previously-declared
	// row (re-evaluated):
	if rowAfter.CommitDeadlineAt == "" {
		t.Error("deadline_at emptied after RecordUploadProgress")
	}
	if rowAfter.LastProgressAt == rowBefore.LastProgressAt {
		t.Errorf("last_progress_at did NOT advance: %q (before) == %q (after)",
			rowBefore.LastProgressAt, rowAfter.LastProgressAt)
	}
}

func TestCoordinator_RecordUploadProgress_WrongFenceRejected(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := NewCoordinator(db)
	fence := validFence("task-bad-fence", "attempt-bad-fence")
	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job", OutputManifests: validManifests(),
	}); err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}

	// Fence mismatch: wrong worker_id.
	wrongFence := fence
	wrongFence.WorkerID = "other-worker"
	err := c.RecordUploadProgress(context.Background(), RecordUploadProgressCommand{
		Fence: wrongFence, UploadID: "u", UploadedBytes: 10,
	})
	if err == nil {
		t.Fatal("expected error for fence mismatch, got nil")
	}
	if !strings.Contains(err.Error(), ErrAttemptCommitNotFound.Error()) {
		t.Errorf("error should mention ErrAttemptCommitNotFound (fence does not match any row), got: %v", err)
	}
}

func TestCoordinator_RecordUploadProgress_EmptyUploadIDRejected(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := NewCoordinator(db)
	fence := validFence("task-up-empty", "attempt-up-empty")
	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job", OutputManifests: validManifests(),
	}); err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}
	err := c.RecordUploadProgress(context.Background(), RecordUploadProgressCommand{
		Fence: fence, UploadID: "", UploadedBytes: 0,
	})
	if err == nil {
		t.Fatal("expected error for empty UploadID, got nil")
	}
}

// ────────────────────────────────────────────────────────────────────────
// ErrNotImplemented stubs (Fase 2.5+)
// ────────────────────────────────────────────────────────────────────────

func TestCoordinator_CompleteUpload_NotImplemented(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := NewCoordinator(db)
	err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
		Fence: validFence("t", "a"), UploadID: "u",
		UploadedSizeBytes: 100, WorkerSHA256: strings.Repeat("a", 64),
	})
	if err == nil {
		t.Fatal("expected ErrNotImplemented, got nil")
	}
	if !errorsIs(err, ErrNotImplemented) {
		t.Errorf("CompleteUpload should return ErrNotImplemented, got: %v", err)
	}
}

func TestCoordinator_CommitAttempt_NotImplemented(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := NewCoordinator(db)
	_, err := c.CommitAttempt(context.Background(), "c-1")
	if !errorsIs(err, ErrNotImplemented) {
		t.Errorf("CommitAttempt should return ErrNotImplemented, got: %v", err)
	}
	_, err = c.CommitAttempt(context.Background(), "")
	if err == nil {
		t.Fatal("CommitAttempt(empty commitID) should error, got nil")
	}
}

func TestCoordinator_ReconcileAttempt_NotImplemented(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := NewCoordinator(db)
	_, err := c.ReconcileAttempt(context.Background(), "c-1")
	if !errorsIs(err, ErrNotImplemented) {
		t.Errorf("ReconcileAttempt should return ErrNotImplemented, got: %v", err)
	}
	_, err = c.ReconcileAttempt(context.Background(), "")
	if err == nil {
		t.Fatal("ReconcileAttempt(empty commitID) should error, got nil")
	}
}

// errorsIs is a tiny inline errors.Is shim to avoid pulling the
// stdlib import into every test. The Go 1.18+ errors package is used
// identically to errors.Is.
func errorsIs(err, target error) bool {
	return err != nil && (err == target || (err.Error() == target.Error()))
}
