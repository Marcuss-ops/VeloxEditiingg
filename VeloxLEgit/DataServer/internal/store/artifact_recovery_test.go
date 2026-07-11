// Package store / artifact_recovery_test.go
//
// Unit test for store.RegisterRecoveryUploadSession — validates the
// idempotency contract documented in artifact_recovery.go:
//
//   - "INSERT OR IGNORE returns 0 affected rows on a re-run; the CLI
//     verifies before each step that the same upload_id is reused so
//     the gating CAS in CompleteUpload continues to advance the row."
//
// Why a temp-dir file path (instead of `:memory:` DSN):
// store.NewSQLiteStore was designed for production file-backed DBs;
// forging an in-memory DSN through it depends on the underlying
// platform/database.Open accepting ":memory:" as SQLitePath, which is
// not a documented invariant. Using a per-test temp dir yields a
// real on-disk DB that exercises the same boot path production runs:
// database.Open + sqliteTunePragmas + migrations.RunMigrations. The
// disk cost is negligible (per-test file is removed by t.Cleanup) and
// the test becomes decoupled from any sqlite driver-DSN quirks.
package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"testing"
	"time"
)

// TestRegisterRecoveryUploadSession_Idempotency covers three observations:
//
//  1. First call inserts exactly one row per table.
//  2. Second call with the same (upload_id, artifact_id) PKs succeeds
//     AND does not duplicate the rows — this is the contract that the
//     recovery CLI relies on:
//     "INSERT OR IGNORE returns 0 affected rows on a re-run; the CLI
//     verifies before each step that the same upload_id is reused".
//  3. A different (upload_id, artifact_id) pair inserts a fresh row —
//     confirms idempotency is keyed correctly (per-PK). Without this
//     observation a regression where RegisterRecoveryUploadSession
//     silently dropped ALL inserts after the first (e.g., a buggy
//     conditional) would still pass observation #2, because it would
//     return nil for the 2nd same-PK call too. Observation #3 catches
//     that the per-PK keying is preserved.
//
// Additionally the test round-trips the SHA256 / storage_key / status
// / type / created_at-format columns on the first-vs-second insert so
// a regression that breaks the row CONTENT (not just the row COUNT)
// surfaces as a failing test. The CLI's downstream CompleteUpload
// relies on these columns being byte-for-byte identical between
// attempts so it can re-derive the same canonical storage_key.
func TestRegisterRecoveryUploadSession_Idempotency(t *testing.T) {
	tmp := t.TempDir()
	dbPath := filepath.Join(tmp, "test.db")

	s, err := NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore(%q): %v", dbPath, err)
	}
	t.Cleanup(func() { _ = s.Close() })
	db := s.DB()

	ctx := context.Background()

	sess := RecoveryUploadSession{
		UploadID:   "recover-commit-abc",
		ArtifactID: "art_recover_commit-abc",
		JobID:      "job-1",
		WorkerID:   "worker-1",
		LeaseID:    "lease-1",
		FilePath:   "/tmp/rendered.mp4",
		SizeBytes:  1234,
		SHA256:     "abcd1234abcd1234abcd1234abcd1234",
	}

	// Seed the FK-parent row BEFORE calling the helper. Migration
	// 030_artifact_uploads.sql declares artifact_uploads.job_id with
	// FOREIGN KEY (job_id) REFERENCES jobs(job_id); the helper does
	// not insert into jobs because in the real recovery CLI this row
	// arrives earlier via the Coordinator.DeclareOutputs step
	// (formerly cmd/worker/recover_output.go step 3). The test
	// replicates that ordering locally so the FK is satisfied.
	seedJobsRow(t, db, sess.JobID)

	// ── 1. First call: insert both rows ──
	if err := RegisterRecoveryUploadSession(ctx, db, sess); err != nil {
		t.Fatalf("first RegisterRecoveryUploadSession: %v", err)
	}
	assertRowCount(t, db, "artifacts", "id", sess.ArtifactID, 1, "after first call")
	assertRowCount(t, db, "artifact_uploads", "upload_id", sess.UploadID, 1, "after first call")

	// Capture content snapshot after the FIRST insert so we can
	// re-verify byte-for-byte identity after the SECOND insert
	// (idempotent caller should NOT mutate any column value).
	first := snapshotArtifact(t, db, sess.ArtifactID)
	firstUpload := snapshotArtifactUpload(t, db, sess.UploadID)

	// SHA256 / file_path / type / status / created_at-format round-trip.
	assertArtifact(t, first, sess.FilePath, sess.SHA256, sess.SizeBytes)
	assertArtifactUpload(t, firstUpload, sess.FilePath, sess.SHA256, sess.SizeBytes)

	// created_at must be RFC3339Nano (preserves byte-for-byte parity
	// with the original recover_output.go CLI which used
	// time.RFC3339Nano; downstream readers in CompleteUpload parse
	// with time.RFC3339Nano).
	assertRFC3339Nano(t, first.CreatedAt, "artifacts.created_at after first insert")
	assertRFC3339Nano(t, firstUpload.CreatedAt, "artifact_uploads.created_at after first insert")

	// Regression lock for the expires_at = created_at bug: the helper
	// must stamp expires_at strictly AFTER created_at, otherwise
	// migration 030's reconciler EXPIRES the row on the very next
	// pass. See artifact_recovery.go's `expires_at contract`
	// docstring for the full failure-mode description.
	assertExpiresAtAfterCreatedAt(t, firstUpload, "after first call (regression lock for expires_at = created_at bug)")

	// ── 2. Second call with the SAME PKs: must succeed AND must NOT duplicate ──
	if err := RegisterRecoveryUploadSession(ctx, db, sess); err != nil {
		t.Fatalf("second RegisterRecoveryUploadSession: %v (expected idempotent success, not error)", err)
	}
	assertRowCount(t, db, "artifacts", "id", sess.ArtifactID, 1, "after second call (idempotency violated if != 1)")
	assertRowCount(t, db, "artifact_uploads", "upload_id", sess.UploadID, 1, "after second call (idempotency violated if != 1)")

	// After the SECOND insert: every column MUST match the first
	// snapshot exactly. Any drift means INSERT OR IGNORE silently
	// swallowed an UPDATE — silent regressions like that are the
	// kind of bug a re-running recovery CLI is most exposed to.
	second := snapshotArtifact(t, db, sess.ArtifactID)
	if first != second {
		t.Errorf("artifacts row drift across idempotent re-insert:\n  first:  %+v\n  second: %+v", first, second)
	}
	secondUpload := snapshotArtifactUpload(t, db, sess.UploadID)
	if firstUpload != secondUpload {
		t.Errorf("artifact_uploads row drift across idempotent re-insert:\n  first:  %+v\n  second: %+v", firstUpload, secondUpload)
	}

	// Belt-and-suspenders: after the idempotent re-insert the TTL
	// contract must still hold. Since the second INSERT OR IGNORE
	// doesn't mutate any column, this is a structural check that
	// `firstUpload.ExpiresAt` and `secondUpload.ExpiresAt` are both
	// strictly AFTER their respective `CreatedAt` (they share the
	// same `CreatedAt` and `ExpiresAt` strings after the no-op
	// re-insert, so this would only fail if the helper's TTL
	// computation changed under a same-PK re-insert).
	assertExpiresAtAfterCreatedAt(t, secondUpload, "after second call (idempotency preserved expires_at > created_at)")

	// ── 3. Distinct PKs: a different (upload_id, artifact_id) pair MUST insert fresh rows ──
	//
	// Defends against a regression where RegisterRecoveryUploadSession
	// silently drops ALL inserts after the first (e.g., a buggy
	// conditional guarding the INSERT). Observation #2 alone would
	// pass that regression because the 2nd same-PK call would simply
	// no-op on a "we already inserted something" guard. Observation
	// #3 catches it by demanding a fresh insertion lands.
	altSess := sess
	altSess.UploadID = "recover-commit-def"
	altSess.ArtifactID = "art_recover_commit-def"
	altSess.JobID = "job-2"
	// Distinct FilePath from observation #1's. Migration 030 declares a
	// UNIQUE INDEX idx_artifacts_storage_key ON
	// artifacts(storage_provider, storage_key) WHERE storage_key <>
	// ''; the helper inserts storage_key=s.FilePath verbatim, so
	// reusing /tmp/rendered.mp4 would silently no-op the INSERT OR
	// IGNORE (guardian of the deduplication contract) and the
	// observation #3 artifact_uploads insert would then fail its
	// artifact_id FK — a false-negative poison unrelated to
	// idempotency. Production never feeds the same FilePath under
	// distinct artifact_ids (a distinct commit_id derives a distinct
	// rendered file path on the master host), so a divergent path
	// here mirrors production semantics.
	altSess.FilePath = "/tmp/rendered-alt.mp4"
	seedJobsRow(t, db, altSess.JobID)
	if err := RegisterRecoveryUploadSession(ctx, db, altSess); err != nil {
		t.Fatalf("third RegisterRecoveryUploadSession (distinct PKs): %v", err)
	}
	assertRowCount(t, db, "artifacts", "id", altSess.ArtifactID, 1, "distinct PKs should insert a new row")
	assertRowCount(t, db, "artifact_uploads", "upload_id", altSess.UploadID, 1, "distinct PKs should insert a new row")

	// Sanity: the original session's row count is unaffected by the
	// insertion of the alternative session.
	assertRowCount(t, db, "artifacts", "id", sess.ArtifactID, 1, "original session row should be unaffected by alt insertion")
	assertRowCount(t, db, "artifact_uploads", "upload_id", sess.UploadID, 1, "original upload row should be unaffected by alt insertion")
}

// ── helpers ─────────────────────────────────────────────────────────────
//
// The `assert*` helpers below format `table` and `keyCol` directly into
// the SQL string. They are safe for the static string literals used
// here (and any future test that follows the same pattern) but MUST
// NOT be reused with user-derived table/column names. The reader is
// the only attack surface today; future callers should add an
// allow-list switch if exposing these helpers more broadly.

func assertRowCount(t *testing.T, db *sql.DB, table, keyCol, keyVal string, want int, msg string) {
	t.Helper()
	var n int
	row := db.QueryRow("SELECT COUNT(*) FROM "+table+" WHERE "+keyCol+" = ?", keyVal)
	if err := row.Scan(&n); err != nil {
		t.Fatalf("count %s where %s=? %q (%s): %v", table, keyCol, keyVal, msg, err)
	}
	if n != want {
		t.Errorf("%s where %s=%q: row count=%d, want %d (%s)", table, keyCol, keyVal, n, want, msg)
	}
}

// artifactRow is a content snapshot of an `artifacts` table row
// selected by primary key. Used to compare first-vs-second insert
// identity (the idempotency contract).
type artifactRow struct {
	Status     string
	Type       string
	StorageKey string
	SHA256     string
	SizeBytes  int64
	CreatedAt  string
}

func snapshotArtifact(t *testing.T, db *sql.DB, id string) artifactRow {
	t.Helper()
	var r artifactRow
	err := db.QueryRow(`
		SELECT status, type, storage_key, sha256, size_bytes, created_at
		FROM artifacts WHERE id = ?`, id).Scan(
		&r.Status, &r.Type, &r.StorageKey, &r.SHA256, &r.SizeBytes, &r.CreatedAt,
	)
	if err != nil {
		t.Fatalf("snapshot artifacts(id=%q): %v", id, err)
	}
	return r
}

type artifactUploadRow struct {
	Status            string
	StorageKey        string
	ExpectedSHA256    string
	ExpectedSizeBytes int64
	CreatedAt         string
	// ExpiresAt is captured here so the regression-locking
	// assertion below (assertExpiresAtAfterCreatedAt) can verify
	// the helper's TTL contract end-to-end. Migration 030's
	// idx_artifact_uploads_expiry feeds the reconciler rule
	// "staging session troppo vecchio -> EXPIRED", so a flat
	// expires_at == created_at would EXPIRE the row on the very
	// next reconciler pass — the exact bug this fix removes.
	ExpiresAt string
}

func snapshotArtifactUpload(t *testing.T, db *sql.DB, uploadID string) artifactUploadRow {
	t.Helper()
	var r artifactUploadRow
	err := db.QueryRow(`
		SELECT status, temporary_storage_key, expected_sha256, expected_size_bytes, created_at, expires_at
		FROM artifact_uploads WHERE upload_id = ?`, uploadID).Scan(
		&r.Status, &r.StorageKey, &r.ExpectedSHA256, &r.ExpectedSizeBytes, &r.CreatedAt, &r.ExpiresAt,
	)
	if err != nil {
		t.Fatalf("snapshot artifact_uploads(upload_id=%q): %v", uploadID, err)
	}
	return r
}

func assertArtifact(t *testing.T, r artifactRow, wantFilePath, wantSHA string, wantSize int64) {
	t.Helper()
	if r.Status != "STAGING" {
		t.Errorf("artifacts.status=%q, want STAGING", r.Status)
	}
	if r.Type != "video" {
		t.Errorf("artifacts.type=%q, want video (hard-coded in helper)", r.Type)
	}
	if r.StorageKey != wantFilePath {
		t.Errorf("artifacts.storage_key=%q, want %q (file path verbatim)", r.StorageKey, wantFilePath)
	}
	if r.SHA256 != wantSHA {
		t.Errorf("artifacts.sha256=%q, want %q", r.SHA256, wantSHA)
	}
	if r.SizeBytes != wantSize {
		t.Errorf("artifacts.size_bytes=%d, want %d", r.SizeBytes, wantSize)
	}
}

func assertArtifactUpload(t *testing.T, r artifactUploadRow, wantFilePath, wantSHA string, wantSize int64) {
	t.Helper()
	if r.Status != "CREATED" {
		t.Errorf("artifact_uploads.status=%q, want CREATED", r.Status)
	}
	if r.StorageKey != wantFilePath {
		t.Errorf("artifact_uploads.temporary_storage_key=%q, want %q", r.StorageKey, wantFilePath)
	}
	if r.ExpectedSHA256 != wantSHA {
		t.Errorf("artifact_uploads.expected_sha256=%q, want %q", r.ExpectedSHA256, wantSHA)
	}
	if r.ExpectedSizeBytes != wantSize {
		t.Errorf("artifact_uploads.expected_size_bytes=%d, want %d", r.ExpectedSizeBytes, wantSize)
	}
}

func assertRFC3339Nano(t *testing.T, raw, msg string) {
	t.Helper()
	if raw == "" {
		t.Errorf("%s: empty timestamp (RFC3339Nano expected)", msg)
		return
	}
	if _, err := time.Parse(time.RFC3339Nano, raw); err != nil {
		t.Errorf("%s: timestamp %q does not parse as RFC3339Nano: %v", msg, raw, err)
	}
}

// assertExpiresAtAfterCreatedAt is the regression lock for the
// expires_at = created_at bug that motivated the helper's TTL field
// (RecoveryUploadSession.ExpiresAtTTL). The helper must stamp
// expires_at STRICTLY AFTER created_at; otherwise migration 030's
// idx_artifact_uploads_expiry-backed reconciler rule "staging
// session troppo vecchio -> EXPIRED" would fire on the very next
// pass, killing the recovery session before CompleteUpload's CAS
// could advance it. This assertion parses both RFC3339Nano strings
// back to time.Time and fails the test if expires_at <= created_at,
// with the actual delta in the error message so a future regression
// surface area is debuggable in seconds. A future regression that
// flattens the two columns back to equal (e.g. accidentally passing
// a zero TTL to time.Time.Add) would surface as a failing test
// immediately.
func assertExpiresAtAfterCreatedAt(t *testing.T, r artifactUploadRow, msg string) {
	t.Helper()
	if r.CreatedAt == "" || r.ExpiresAt == "" {
		t.Fatalf("%s: created_at and expires_at both must be non-empty (got created_at=%q expires_at=%q)", msg, r.CreatedAt, r.ExpiresAt)
	}
	created, err := time.Parse(time.RFC3339Nano, r.CreatedAt)
	if err != nil {
		t.Fatalf("%s: parse created_at=%q: %v", msg, r.CreatedAt, err)
	}
	expires, err := time.Parse(time.RFC3339Nano, r.ExpiresAt)
	if err != nil {
		t.Fatalf("%s: parse expires_at=%q: %v", msg, r.ExpiresAt, err)
	}
	if !expires.After(created) {
		t.Errorf("%s: expires_at=%s must be STRICTLY AFTER created_at=%s (delta=%s); this is a regression of the helper's TTL contract — see artifact_recovery.go's `expires_at contract` docstring for the failure mode", msg, expires.Format(time.RFC3339Nano), created.Format(time.RFC3339Nano), expires.Sub(created))
	}
}

// seedJobsRow inserts the FK-parent row that migration 030 requires
// before artifact_uploads accepts any row. The columns match the
// minimal working set the recovery CLI's Coordinator.DeclareOutputs
// would have stamped on a real recovery session. We use a sentinel
// status of "RUNNING" which is the recovery CLI's documented
// pre-finalization interval. Note that worker_id / lease_id are
// intentionally NOT seeded here — migration
// 048_drop_jobs_runtime_columns dropped those columns off `jobs`
// (worker assignment and lease identity moved to the `tasks` table
// per the task-native dispatch model). Those identities DO still
// surface on artifact_uploads.worker_id / artifact_uploads.lease_id
// (migration 030 keeps those columns and RegisterRecoveryUploadSession
// populates them from sess.WorkerID / sess.LeaseID in the
// artifact_uploads INSERT); the test sets those sess fields so that
// the helper's input-validation gate (`if s.WorkerID == "" ||
// s.LeaseID == ""` early-return) does not trip, but it's harmless
// from the jobs-row perspective — that's why this helper only needs
// jobID.
func seedJobsRow(t *testing.T, db *sql.DB, jobID string) {
	t.Helper()
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`
		INSERT INTO jobs (job_id, status, revision, created_at, updated_at, migrated_at)
		VALUES (?, 'RUNNING', 0, ?, ?, ?)`,
		jobID, now, now, now,
	); err != nil {
		t.Fatalf("seed jobs(job_id=%q): %v", jobID, err)
	}
}
