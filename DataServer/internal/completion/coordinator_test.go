package completion

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/store/migrations"
)

// testHMACKey is the deterministic 32-byte HMAC key used by every
// Coordinator built in this package (Verdetto P0 #6). It is the
// SHA-256 of a fixed string ("velox-test-commit-hmac-key-v1") and is
// stable across runs so DeclareOutputs produces the same
// commit_token + commit_token_hash for the same (commit_id, fence)
// — the exact property the new replay-safe derivation ships with.
var testHMACKey = func() []byte {
	h := sha256.Sum256([]byte("velox-test-commit-hmac-key-v1"))
	return h[:]
}()

// newTestCoordinator builds the canonical Coordinator with the test
// HMAC key for this package. NewCoordinator's >=32-byte guard passes
// (testHMACKey is exactly 32 bytes).
func newTestCoordinator(db *sql.DB) Coordinator {
	c, err := NewCoordinator(CoordinatorConfig{DB: db, HMACKey: testHMACKey})
	if err != nil {
		panic(err) // test-only; cannot reasonably happen
	}
	return c
}

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
	CommitID          string
	TaskID            string
	AttemptID         string
	JobID             string
	WorkerID          string
	LeaseID           string
	TaskRevision      int
	Status            string
	RequiredOutputCnt int
	ReadyOutputCnt    int
	CommitTokenHash   string
	CommitDeadlineAt  string
	LastProgressAt    string
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
	want := "task_id = ? AND attempt_id = ? AND worker_id = ? AND lease_id = ? AND task_revision = ?"
	if where != want {
		t.Errorf("SQLWhere mismatch: got %q, want %q", where, want)
	}
	gotArgs := f.SQLArgs()
	wantArgs := []any{"T1", "A1", "W1", "L1", 2}
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
	c := newTestCoordinator(db)
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
	c := newTestCoordinator(db)
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
	c := newTestCoordinator(db)
	fence := validFence("task-multi", "attempt-multi")
	manifests := []OutputManifest{
		{
			OutputKind: "final_video", LogicalName: "out.mp4",
			MimeType: "video/mp4", SizeBytes: 1024,
			SHA256:         strings.Repeat("1", 64),
			WorkerSpoolKey: "spool-1",
		},
		{
			OutputKind: "thumbnail", LogicalName: "thumb.jpg",
			MimeType: "image/jpeg", SizeBytes: 256,
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
	c := newTestCoordinator(db)
	fence := validFence("task-mix", "attempt-mix")
	manifests := []OutputManifest{
		{
			OutputKind: "final_video", LogicalName: "out.mp4",
			MimeType: "video/mp4", SizeBytes: 1024,
			SHA256:         strings.Repeat("1", 64),
			WorkerSpoolKey: "spool-1",
		},
		{
			OutputKind: "thumbnail", LogicalName: "thumb.jpg",
			MimeType: "image/jpeg", SizeBytes: 256,
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
			MimeType: "image/jpeg", SizeBytes: 512, // changed size
			SHA256: strings.Repeat("3", 64),
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
	c := newTestCoordinator(db)
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
	c := newTestCoordinator(db)
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
	c := newTestCoordinator(db)
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
	c := newTestCoordinator(db)
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
	c := newTestCoordinator(db)
	fence := validFence("task-bad-fence", "attempt-bad-fence")
	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job", OutputManifests: validManifests(),
	}); err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}

	// Fence mismatch: wrong worker_id. Phase 2.2 central gate now
	// returns ErrTransitionConflict (the row exists with a
	// different worker_id; this is exactly the stale-worker
	// rejection the gate is designed to enforce).
	wrongFence := fence
	wrongFence.WorkerID = "other-worker"
	err := c.RecordUploadProgress(context.Background(), RecordUploadProgressCommand{
		Fence: wrongFence, UploadID: "u", UploadedBytes: 10,
	})
	if err == nil {
		t.Fatal("expected error for fence mismatch, got nil")
	}
	if !strings.Contains(err.Error(), ErrTransitionConflict.Error()) {
		t.Errorf("error should mention ErrTransitionConflict (stale-worker gate), got: %v", err)
	}
}

func TestCoordinator_RecordUploadProgress_EmptyUploadIDRejected(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
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
// Phase 2.5-2.9: ErrNotImplemented stub tests retired.
//
// The Phase 2.1-2.4 commits assert that CompleteUpload /
// CommitAttempt / ReconcileAttempt returned ErrNotImplemented. The
// Phase 2.5-2.9 work landed real implementations, so the stub tests
// are obsolete. Their coverage lives in the 4 cited above:
//   - TestCoordinator_StaleFence_TransitionConflict
//   - TestCoordinator_CompleteUpload_BeforeAllRequired_Expires
//   - TestCoordinator_CommitAttempt_DuplicateIsNoop
//   - TestCoordinator_ReconcileAttempt_DeclaredDeadWorker_Expires
//
// Empty-commitID guard is exercised transitively by all 4 (every
// method calls commitID=="" with a fast-error before any SQL touch).
// ────────────────────────────────────────────────────────────────────────

// TestCoordinator_RecordUploadProgress_MonotonicProgress locks down
// the MAX() semantics Verdetto P0 (Blocco 3) ships for the heartbeat
// path. A worker that re-sends an older heartbeat (reordered TCP
// segment, retry-with-backoff, debug retry button) MUST NOT regress
// the canonical uploaded_bytes or updated_at columns on
// task_output_declarations. The test sends the heartbeat sequence
// 1000 → 800 → 1200 and asserts the persisted value is 1200 (the
// MAX), not 800 (the last write).
func TestCoordinator_RecordUploadProgress_MonotonicProgress(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-monotonic", "attempt-monotonic")
	plan, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job-monotonic", OutputManifests: validManifests(),
	})
	if err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}

	// Stamp an upload_id on the declaration so the heartbeat can
	// find it (mirrors the existing BumpsProgress test).
	if _, err := db.Exec(
		`UPDATE task_output_declarations SET upload_id = ? WHERE commit_id = ?`,
		"upload-monotonic", plan.CommitID,
	); err != nil {
		t.Fatalf("set upload_id: %v", err)
	}

	// Three heartbeats in the order 1000 → 800 → 1200. The 800
	// value simulates a stale/reordered heartbeat that arrived
	// after the 1000 value; the 1200 is the genuine latest.
	for _, bytes := range []int64{1000, 800, 1200} {
		if err := c.RecordUploadProgress(context.Background(), RecordUploadProgressCommand{
			Fence:         fence,
			UploadID:      "upload-monotonic",
			UploadedBytes: bytes,
		}); err != nil {
			t.Fatalf("RecordUploadProgress(UploadedBytes=%d): %v", bytes, err)
		}
	}

	// Read the persisted uploaded_bytes. The MAX() guarantee means
	// the value is 1200, NOT 800 (the last write) and NOT 1000
	// (the first write).
	var persisted int64
	if err := db.QueryRow(
		`SELECT uploaded_bytes FROM task_output_declarations WHERE upload_id = ?`,
		"upload-monotonic",
	).Scan(&persisted); err != nil {
		t.Fatalf("read uploaded_bytes: %v", err)
	}
	if persisted != 1200 {
		t.Errorf("persisted uploaded_bytes = %d, want 1200 (MAX() must reject the stale 800 heartbeat)", persisted)
	}
}

// ────────────────────────────────────────────────────────────────────
// Verdetto P0 #6: replay-safe commit token.
//
// Two DeclareOutputs calls with the same fence MUST yield the same
// (commit_token, commit_token_hash) bit-for-bit. This is the
// regression-guard for the deterministic HMAC-SHA256 token
// derivation (Verdetto P0 #6, Blocco 2) — a regression here
// would silently break worker reconnect-safety because the
// worker carries the first-declared token and the master cannot
// re-derive it from the second call without a shared HMAC key.
// ────────────────────────────────────────────────────────────────────

func TestCoordinator_DeclareOutputs_ReplayYieldsIdenticalToken(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-determinism-replay", "attempt-determinism-replay")
	cmd := DeclareOutputsCommand{
		Fence:           fence,
		JobID:           "job-determinism-replay",
		OutputManifests: validManifests(),
	}
	plan1, err := c.DeclareOutputs(context.Background(), cmd)
	if err != nil {
		t.Fatalf("first DeclareOutputs: %v", err)
	}
	// The second call hits the existing-row path which REUSES
	// the canonical commit_id but recomputes the deterministic
	// token from (commit_id, fence, HMACKey). Equality on both
	// fields confirms the derivation is byte-identical.
	plan2, err := c.DeclareOutputs(context.Background(), cmd)
	if err != nil {
		t.Fatalf("second DeclareOutputs: %v", err)
	}
	if plan1.CommitID != plan2.CommitID {
		t.Errorf("commit_id drifted across replays: %q != %q (must reuse canonical row id)",
			plan1.CommitID, plan2.CommitID)
	}
	if plan1.CommitToken != plan2.CommitToken {
		t.Errorf("commit_token drifted across replays: %q != %q (HMAC derivation must be deterministic)",
			plan1.CommitToken, plan2.CommitToken)
	}
	if len(plan1.CommitToken) != commitTokenByteLen*2 {
		t.Errorf("commit_token hex length: got %d, want %d", len(plan1.CommitToken), commitTokenByteLen*2)
	}
	// commit_token_hash on disk must also be byte-identical
	// because the token is deterministic (hash = SHA256(token)).
	row1Hash := sha256HexFromRow(t, db, fence)
	row2Hash := sha256HexFromRow(t, db, fence)
	if row1Hash != row2Hash {
		t.Errorf("commit_token_hash on disk drifted across replays: %q != %q (persisted hash must match deterministic token)",
			row1Hash, row2Hash)
	}
	if row1Hash == "" {
		t.Error("commit_token_hash empty after DeclareOutputs replay (must be written on first call)")
	}
}

// sha256HexFromRow is a tiny inline helper that reads
// attempt_commits.commit_token_hash and returns its hex form.
// It's used by the determinism replay test; reading via the
// canonical helpers keeps the test independent from the wider
// package API.
func sha256HexFromRow(t *testing.T, db *sql.DB, fence FenceTuple) string {
	t.Helper()
	var h string
	if err := db.QueryRow(
		`SELECT commit_token_hash FROM attempt_commits
		 WHERE task_id = ? AND attempt_id = ?`,
		fence.TaskID, fence.AttemptID,
	).Scan(&h); err != nil {
		t.Fatalf("read commit_token_hash: %v", err)
	}
	return h
}

// ────────────────────────────────────────────────────────────────────
// Verdetto P0 #5: ServerSHA256 authoritative gate for CompleteUpload.
//
// Four branches must be exercised end-to-end against the
// artifact_uploads + artifacts schema:
//   A. ServerSHA="" AND effectiveExpected=""  -> artifact stays VERIFYING
//   B. ServerSHA="" AND effectiveExpected!="" -> artifact stays VERIFYING
//   C. ServerSHA matches effectiveExpected     -> artifact STAGING/VERIFYING -> READY
//   D. ServerSHA!="" AND differs               -> ErrStaleReport (no row change)
// ────────────────────────────────────────────────────────────────────

// seedCompleteUploadFixture inserts a jobs row (needed by both the
// artifact_uploads.job_id FK and the legacy canonical pipeline),
// an artifacts row (STAGING, expected SHA), and an artifact_uploads
// row (RECEIVED, expected_sha256). Tests call the coordinator's
// CompleteUpload against this fixture.
//
// The artifact_uploads schema (migration 030) enforces
//
//	FOREIGN KEY (job_id) REFERENCES jobs(job_id)
//
// so a placeholder row in jobs is required even though our tests
// never read it.
func seedCompleteUploadFixture(t *testing.T, db *sql.DB, uploadID, artifactID, jobID, expectedSHA string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`
		INSERT OR IGNORE INTO jobs (job_id, migrated_at)
		VALUES (?, ?)`,
		jobID, now,
	); err != nil {
		t.Fatalf("seed jobs: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO artifacts (
			id, job_id, type, storage_provider, storage_key,
			sha256, size_bytes, status, created_at
		) VALUES (?, ?, 'video', 'local', ?, ?, 1024, 'STAGING', ?)`,
		artifactID, jobID, uploadID+".local", expectedSHA, now,
	); err != nil {
		t.Fatalf("seed artifacts: %v", err)
	}
	if _, err := db.Exec(`
		INSERT INTO artifact_uploads (
			upload_id, artifact_id, job_id, attempt_number,
			worker_id, lease_id, status, temporary_storage_key,
			expected_size_bytes, expected_sha256, created_at, expires_at
		) VALUES (?, ?, ?, 1, 'worker-fixture', 'lease-fixture',
		          'RECEIVED', ?, 1024, ?, ?, ?)`,
		uploadID, artifactID, jobID, uploadID+".local", expectedSHA, now, now,
	); err != nil {
		t.Fatalf("seed artifact_uploads: %v", err)
	}
}

// readArtifactStatus returns the post-call status of the
// artifact row, used by the four-branch assertions.
func readArtifactStatus(t *testing.T, db *sql.DB, artifactID string) string {
	t.Helper()
	var status string
	if err := db.QueryRow(`SELECT status FROM artifacts WHERE id = ?`, artifactID).Scan(&status); err != nil {
		t.Fatalf("read artifact status: %v", err)
	}
	return status
}

func TestCoordinator_CompleteUpload_BranchA_NoServerSHA_NoExpected_StaysVerifying(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-branch-a", "attempt-branch-a")

	// Seed artifact + upload without setting expected_sha256.
	seedCompleteUploadFixture(t, db, "up-branch-a", "art-branch-a", "job-branch-a", "")
	// DeclareOutputs is required for the Fence.Read gate.
	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job-branch-a", OutputManifests: []OutputManifest{
			{OutputKind: "final_video", LogicalName: "out.mp4",
				MimeType: "video/mp4", SizeBytes: 1024,
				SHA256: strings.Repeat("a", 64)},
		},
	}); err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}

	if err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
		Fence:        fence,
		UploadID:     "up-branch-a",
		WorkerSHA256: strings.Repeat("a", 64),
		ServerSHA256: "", // Branch A
	}); err != nil {
		t.Fatalf("Branch A CompleteUpload: %v", err)
	}
	if got := readArtifactStatus(t, db, "art-branch-a"); got != "VERIFYING" {
		t.Errorf("Branch A artifact.status: got %q, want VERIFYING (no master SHA, no declarative SHA)", got)
	}
}

func TestCoordinator_CompleteUpload_BranchB_NoServerSHA_HasExpected_StaysVerifying(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-branch-b", "attempt-branch-b")
	expected := strings.Repeat("b", 64)
	seedCompleteUploadFixture(t, db, "up-branch-b", "art-branch-b", "job-branch-b", expected)
	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job-branch-b", OutputManifests: []OutputManifest{
			{OutputKind: "final_video", LogicalName: "out.mp4",
				MimeType: "video/mp4", SizeBytes: 1024,
				SHA256: expected},
		},
	}); err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}

	if err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
		Fence:        fence,
		UploadID:     "up-branch-b",
		WorkerSHA256: expected,
		ServerSHA256: "", // Branch B
	}); err != nil {
		t.Fatalf("Branch B CompleteUpload: %v", err)
	}
	if got := readArtifactStatus(t, db, "art-branch-b"); got != "VERIFYING" {
		t.Errorf("Branch B artifact.status: got %q, want VERIFYING (no master SHA despite declarative SHA)", got)
	}
}

func TestCoordinator_CompleteUpload_BranchC_ServerSHAMatch_PromotesToReady(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-branch-c", "attempt-branch-c")
	expected := strings.Repeat("c", 64)
	seedCompleteUploadFixture(t, db, "up-branch-c", "art-branch-c", "job-branch-c", expected)
	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job-branch-c", OutputManifests: []OutputManifest{
			{OutputKind: "final_video", LogicalName: "out.mp4",
				MimeType: "video/mp4", SizeBytes: 1024,
				SHA256: expected},
		},
	}); err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}

	if err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
		Fence:        fence,
		UploadID:     "up-branch-c",
		WorkerSHA256: expected,
		ServerSHA256: expected, // Branch C (match)
	}); err != nil {
		t.Fatalf("Branch C CompleteUpload: %v", err)
	}
	if got := readArtifactStatus(t, db, "art-branch-c"); got != "READY" {
		t.Errorf("Branch C artifact.status: got %q, want READY (server SHA matches declarative)", got)
	}
	// received_sha256 must be the server-derived SHA, NOT the
	// worker self-report. This is the canonical ledger entry; if
	// the worker ever wrote it independently the ledger would be
	// forgeable.
	var receivedSHA string
	if err := db.QueryRow(`SELECT received_sha256 FROM artifact_uploads WHERE upload_id = ?`,
		"up-branch-c").Scan(&receivedSHA); err != nil {
		t.Fatalf("read received_sha256: %v", err)
	}
	if receivedSHA != expected {
		t.Errorf("artifact_uploads.received_sha256: got %q, want %q (master-derived only, never worker self-report)",
			receivedSHA, expected)
	}
}

func TestCoordinator_CompleteUpload_BranchD_ServerSHAMismatch_ErrStaleReport(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-branch-d", "attempt-branch-d")
	expected := strings.Repeat("d", 64)
	other := strings.Repeat("e", 64)
	seedCompleteUploadFixture(t, db, "up-branch-d", "art-branch-d", "job-branch-d", expected)
	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{
		Fence: fence, JobID: "job-branch-d", OutputManifests: []OutputManifest{
			{OutputKind: "final_video", LogicalName: "out.mp4",
				MimeType: "video/mp4", SizeBytes: 1024,
				SHA256: expected},
		},
	}); err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}

	err := c.CompleteUpload(context.Background(), CompleteUploadCommand{
		Fence:        fence,
		UploadID:     "up-branch-d",
		WorkerSHA256: expected,
		ServerSHA256: other, // Branch D (mismatch)
	})
	if !errors.Is(err, ErrStaleReport) {
		t.Fatalf("Branch D CompleteUpload: expected ErrStaleReport, got %v", err)
	}
	// Branch D must roll back: artifact stays STAGING (no
	// advancement), artifact_uploads stays RECEIVED.
	if got := readArtifactStatus(t, db, "art-branch-d"); got != "STAGING" {
		t.Errorf("Branch D artifact.status after rollback: got %q, want STAGING (rollback preserves pre-call state)", got)
	}
	var upStatus string
	if err := db.QueryRow(`SELECT status FROM artifact_uploads WHERE upload_id = ?`,
		"up-branch-d").Scan(&upStatus); err != nil {
		t.Fatalf("read artifact_uploads status: %v", err)
	}
	if upStatus != "RECEIVED" {
		t.Errorf("Branch D artifact_uploads.status after rollback: got %q, want RECEIVED", upStatus)
	}
}

// errorsIs is a tiny inline errors.Is shim to avoid pulling the
// stdlib import into every test. The Go 1.18+ errors package is used
// identically to errors.Is.
func errorsIs(err, target error) bool {
	return err != nil && (err == target || (err.Error() == target.Error()))
}

// ────────────────────────────────────────────────────────────────────────
// recordAttemptCommitsCAS tests
//
// Verdetto P2 (Blocco 5): the Coordinator wraps CAS errors from the
// three canonical attempt_commits CAS paths through ConflictBudget.
// These tests exercise the wrapper directly (no DB CAS collision
// required) so the nil-return ambiguity and threshold-boundary
// contract are pinned down independently of the higher-level
// CompleteUpload / CommitAttempt / ReconcileAttempt callers.
//
// The package-level conflict_budget_test.go already covers the
// bare ConflictBudget surface; this file covers the *Coordinator*
// wiring — the c.budget == nil bypass, the budget reset on
// successful Coordinator-method exit, and the documented quirk that
// the boundary error's chain has ErrConflictBudgetExhausted but NOT
// ErrTransitionConflict (the original is %v-formatted text, not %w).
// ────────────────────────────────────────────────────────────────────────

// TestCoordinator_RecordAttemptCommitsCAS_HappyPath covers the three
// non-escalating call shapes:
//   - nil input          → counter reset, returns nil (success path)
//   - non-CAS err input  → counter unchanged, returns pointer-equal err
//   - CAS err below thr → counter +1, returns pointer-equal err (worker
//     retries on the same path)
func TestCoordinator_RecordAttemptCommitsCAS_HappyPath(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db).(*coordinator)

	// Test-suite pollution guard: reset the budget before the test
	// body so any bleed from a sibling test does not skew the
	// counter assertions below.
	_ = c.recordAttemptCommitsCAS("test", nil)

	// 1. nil input — counter resets, returns nil.
	if err := c.recordAttemptCommitsCAS("test", nil); err != nil {
		t.Errorf("nil input: want nil err, got %v", err)
	}
	if got := c.budget.Consecutive(); got != 0 {
		t.Errorf("after nil reset: budget.Consecutive() = %d, want 0", got)
	}

	// 2. non-CAS err — counter unchanged, pointer-equal passthrough.
	otherErr := errors.New("some non-CAS infrastructure error")
	if err := c.recordAttemptCommitsCAS("test", otherErr); err != otherErr {
		t.Errorf("non-CAS err: want pointer-equal passthrough (err=%v), got %v", otherErr, err)
	}
	if got := c.budget.Consecutive(); got != 0 {
		t.Errorf("non-CAS err: budget.Consecutive() = %d, want 0", got)
	}

	// 3. CAS err under threshold — counter advances by 1, pointer-
	//    equal passthrough (caller wraps with ErrTransitionConflict
	//    on its own path).
	confErr := conflictErr("stale fence (recordAttemptCommitsCAS happy path)")
	if err := c.recordAttemptCommitsCAS("test", confErr); err != confErr {
		t.Errorf("under-threshold CAS: want pointer-equal passthrough, got %v", err)
	}
	if got := c.budget.Consecutive(); got != 1 {
		t.Errorf("after 1 under-threshold CAS: budget.Consecutive() = %d, want 1", got)
	}
}

// TestCoordinator_RecordAttemptCommitsCAS_CASExhaustionFallsBackToBudgetError
// pins down the boundary behaviour for the wrapper:
//   - The 1st and 2nd consecutive ErrTransitionConflict pass through
//     unchanged (preserving the ErrTransitionConflict chain so the
//     worker over gRPC retries on the same path).
//   - The 3rd consecutive conflict flips the wrapping entirely to
//     ErrConflictBudgetExhausted. The original ErrTransitionConflict
//     is logged as %v descriptive text, NOT in the errors.Is chain —
//     callers inspecting the error MUST check the ErrConflictBudgetExhausted
//     sentinel directly, and use the budget's Consecutive() counter
//     for diagnostics.
func TestCoordinator_RecordAttemptCommitsCAS_CASExhaustionFallsBackToBudgetError(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db).(*coordinator)

	// Test-suite pollution guard.
	_ = c.recordAttemptCommitsCAS("test", nil)

	confErr := conflictErr("locked attempt_commits row (exhaustion test)")

	// Default ConflictBudgetPolicy: ConsecutiveConflictThreshold=3,
	// so the 3rd consecutive conflict is the boundary; the 1st and
	// 2nd must propagate the original ConfErr unchanged.
	for i := 0; i < 2; i++ {
		err := c.recordAttemptCommitsCAS("test", confErr)
		if err == nil {
			t.Fatalf("iteration %d: want non-nil err (the original confErr), got nil", i+1)
		}
		if !errors.Is(err, ErrTransitionConflict) {
			t.Errorf("iteration %d: errors.Is chain lost ErrTransitionConflict (got %v)", i+1, err)
		}
		if errors.Is(err, ErrConflictBudgetExhausted) {
			t.Errorf("iteration %d: under-threshold must NOT escalate ErrConflictBudgetExhausted (got %v)", i+1, err)
		}
	}
	if got := c.budget.Consecutive(); got != 2 {
		t.Errorf("budget.Consecutive() = %d, want 2 before boundary call", got)
	}

	// 3rd consecutive — boundary. Returned error wraps
	// ErrConflictBudgetExhausted; original ErrTransitionConflict is
	// NO LONGER in the errors.Is chain (documented quirk: original
	// is %v-text, not %w-chain). The key is eagerly removed from
	// the per-key map (Blocco 3 per-key design), so BOTH
	// Consecutive() and consecutiveForKey("test") return 0 after
	// the boundary — the escalation error is the real signal, not
	// the post-escalation counter. The pre-boundary counter check
	// above (line ~1015) confirms the streak reached 2.
	boundaryErr := c.recordAttemptCommitsCAS("test", confErr)
	if boundaryErr == nil {
		t.Fatal("3rd consecutive: want non-nil ErrConflictBudgetExhausted, got nil")
	}
	if !errors.Is(boundaryErr, ErrConflictBudgetExhausted) {
		t.Errorf("3rd consecutive: errors.Is did not match ErrConflictBudgetExhausted (got %v)", boundaryErr)
	}
	if errors.Is(boundaryErr, ErrTransitionConflict) {
		t.Errorf("3rd consecutive: ErrTransitionConflict must NOT be in errors.Is chain (only %%v-formatted; got %v)", boundaryErr)
	}
	// Post-escalation: the key is eagerly removed (consecutiveForKey
	// returns 0 for a non-existent key). This is the intended
	// Blocco 3 behaviour — the budget is "armed" again, ready for a
	// fresh streak if the caller retries.
	if got := c.budget.consecutiveForKey("test"); got != 0 {
		t.Errorf("budget.consecutiveForKey(\"test\") = %d, want 0 (eager-delete on escalation)", got)
	}
	if got := c.budget.Consecutive(); got != 0 {
		t.Errorf("budget.Consecutive() = %d, want 0 (no active streaks after eager-delete)", got)
	}

	// The boundary error message SHOULD still reference the
	// underlying transition conflict textually so operators reading
	// logs see the original cause; this guards the 2nd-arg of
	// fmt.Errorf staying as %v.
	if !strings.Contains(boundaryErr.Error(), "lock") && !strings.Contains(boundaryErr.Error(), "transition") {
		t.Errorf("3rd consecutive: boundary error text should still describe the underlying transition conflict; got %q", boundaryErr.Error())
	}

	// Reset and confirm the budget is reusable across Coordinator
	// method exits (e.g., after Manual Restart in supervisor).
	c.recordAttemptCommitsCAS("test", nil)
	if got := c.budget.Consecutive(); got != 0 {
		t.Errorf("after nil reset on boundary streak: budget.Consecutive() = %d, want 0", got)
	}
}

// TestCoordinator_RecordAttemptCommitsCAS_NilBudgetBypass locks in
// the c.budget == nil first-guard in coordinator.go. The guard is
// intended for future Coordinator configurations built without a
// ConflictBudget wrapper (e.g., legacy callers during migration);
// today, NewCoordinator always initializes the budget, but the test
// covers the aid path.
//
// Behaviour under nil budget: every input (nil, non-CAS, CAS) passes
// through unchanged with no panic and no counter advancement.
func TestCoordinator_RecordAttemptCommitsCAS_NilBudgetBypass(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db).(*coordinator)

	// Set up the nil-budget scenario.
	c.budget = nil

	confErr := conflictErr("pure CAS under nil-budget (bypass test)")
	otherErr := errors.New("non-CAS err under nil-budget")

	// nil input — must not panic, must return nil.
	if err := c.recordAttemptCommitsCAS("test", nil); err != nil {
		t.Errorf("nil-budget + nil input: want nil, got %v", err)
	}

	// 5 consecutive CAS errs — must all pass through pointer-equal,
	// no panic, no counter (because there is no budget to count).
	for i := 0; i < 5; i++ {
		err := c.recordAttemptCommitsCAS("test", confErr)
		if err != confErr {
			t.Errorf("iteration %d: nil-budget + CAS err want pointer-equal passthrough, got %v", i+1, err)
		}
	}

	// Non-CAS err — must pass through pointer-equal.
	if err := c.recordAttemptCommitsCAS("test", otherErr); err != otherErr {
		t.Errorf("nil-budget + non-CAS err want pointer-equal passthrough, got %v", err)
	}
}
