// Package artifacts / service_test.go
//
// Integration tests for the artifacts.Service trust boundary.
//
// Each test owns a file-backed SQLite DB in t.TempDir() with all
// SQLite-schema migrations applied (via migrations.RunMigrations against
// the recursive migrations/sqlite/*.sql embed exposed by
// migrations.SQLiteMigrationsFS() — see migrations/runner.go), plus
// a FilesystemBlobStore rooted in temp subdirs. Per-test isolation;
// no shared state.

package artifacts

import (
	"bytes"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/require"

	"velox-server/internal/store"
	"velox-server/internal/store/migrations"
)

// =====================================================================
// FIXTURES
// =====================================================================

const (
	testWorkerID = "worker-test-1"
	testLeaseID  = "lease-test-1"
	testRevision = 5
)

type testEnv struct {
	t              *testing.T
	db             *sql.DB
	bs             store.BlobStore
	svc            *Service
	repo           store.UploadRepository
	uploadWriter   UploadSessionWriter
	finalizeWriter FinalizationWriter
	artifactReader ArtifactReader
	clock          *manualClock
	tmpDir         string
}

// manualClock satisfies Clock for tests; advance via Advance().
type manualClock struct {
	mu sync.Mutex
	t  time.Time
}

func newManualClock() *manualClock {
	return &manualClock{t: time.Date(2026, 6, 18, 12, 0, 0, 0, time.UTC)}
}
func (c *manualClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}
func (c *manualClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func setupTestEnv(t *testing.T) *testEnv {
	t.Helper()
	tmp := t.TempDir()

	dbPath := filepath.Join(tmp, "test.db")
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL")
	require.NoError(t, err, "open sqlite")

	for _, p := range []string{
		"PRAGMA synchronous = NORMAL",
		"PRAGMA foreign_keys = ON",
	} {
		_, err := db.Exec(p)
		require.NoError(t, err, "pragma "+p)
	}
	db.SetMaxOpenConns(4)

	err = migrations.RunMigrations(db, migrations.SQLiteMigrationsFS(), "sqlite")
	require.NoError(t, err, "migrations.RunMigrations")

	// Seed a delivery destination for FinalizeVerified tests.
	_, _ = db.Exec(`INSERT OR IGNORE INTO delivery_destinations (destination_id, provider, name, enabled, created_at, updated_at) VALUES ('primary', 'test', 'Test', 1, '', '')`)

	staging := filepath.Join(tmp, "staging")
	final := filepath.Join(tmp, "final")
	bs, err := store.NewFilesystemBlobStore(staging, final)
	require.NoError(t, err, "NewFilesystemBlobStore")

	clk := newManualClock()
	// store.NewSQLiteUploadRepository is the typed artifact_uploads +
	// artifact_upload_chunks CRUD surface. Sharing the same *sql.DB
	// with the artifacts-package writers lets the finalize tx join
	// with concurrent UpdateUploadStatus on artifact_uploads.
	repo := store.NewSQLiteUploadRepository(db)
	artifactReader := store.NewSQLiteArtifactReader(db)
	authReader := NewSQLiteAuthReader(db)
	uploadWriter := NewSQLiteUploadSessionWriter(db)
	finalizeWriter := NewSQLiteFinalizeWriter(db, artifactReader, nil)
	jobCounter := NewSQLiteJobDeliveryCounter(db)
	svc := NewService(repo, uploadWriter, finalizeWriter, artifactReader, bs, authReader, clk, jobCounter)

	t.Cleanup(func() {
		_ = db.Close()
	})
	return &testEnv{
		t: t, db: db, bs: bs, svc: svc, repo: repo,
		uploadWriter:   uploadWriter,
		finalizeWriter: finalizeWriter,
		artifactReader: artifactReader,
		clock:          clk, tmpDir: tmp,
	}
}

// seedJob inserts a jobs row with the columns Service.loadJob queries.
// Other job columns get their DEFAULT values from the migration. We use
// a minimal schema-defensive INSERT so future migrations that add
// additional columns don't break this test.
//
// canonical-cutover: assigned_to, lease_id, lease_expiry were DROPPED
// from the jobs table; identity is now tracked strictly on
// task_attempts / artifact_uploads. The signature is preserved so the
// 6 caller call-sites (Begin/Receive/Finalize tests) don't all need
// editing — the dropped values are simply no longer persisted here.
// The attempt row inserted via seedAttempt continues to be the
// authoritative source for (worker, lease) identity assertions.
func (e *testEnv) seedJob(jobID, status, assignedTo, leaseID string, revision int, leaseExpiry time.Time) {
	e.t.Helper()
	_ = assignedTo // dropped by migration 048; retained for caller-signature stability
	_ = leaseID    // dropped by migration 048; authority now lives on task_attempts
	_ = leaseExpiry
	now := e.clock.Now().UTC().Format(time.RFC3339)
	_, err := e.db.Exec(`
		INSERT INTO jobs (
			job_id, status, revision,
			created_at, updated_at, migrated_at
		) VALUES (?, ?, ?, ?, ?, ?)`,
		jobID, status, revision, now, now, now,
	)
	require.NoError(e.t, err, "seedJob "+jobID)
}

func (e *testEnv) seedAttempt(jobID string, attemptNumber int, status, workerID, leaseID string) {
	e.t.Helper()
	now := e.clock.Now().UTC().Format(time.RFC3339)
	taskID := jobID + "-task"

	// Seed the parent task first (loadAttempt JOINs task_attempts→tasks).
	_, err := e.db.Exec(`
		INSERT OR IGNORE INTO tasks (task_id, job_id, status, created_at, updated_at)
		VALUES (?, ?, 'RUNNING', ?, ?)`,
		taskID, jobID, now, now,
	)
	require.NoError(e.t, err, "seedAttempt task "+jobID)

	// Seed the attempt row that loadAttempt reads.
	_, err = e.db.Exec(`
		INSERT INTO task_attempts (
			id, task_id, attempt_number, worker_id, lease_id, status,
			started_at, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		jobID+"-attempt", taskID, attemptNumber, workerID, leaseID, status, now, now, now,
	)
	require.NoError(e.t, err, "seedAttempt "+jobID)
}

// markPreExistingReady inserts a READY artifact row of the given kind
// for the given job. Used to test the "no existing READY of same kind"
// gate in BeginUpload.
func (e *testEnv) markPreExistingReady(jobID, kind, artifactID string) {
	e.t.Helper()
	now := e.clock.Now().UTC().Format(time.RFC3339)
	_, err := e.db.Exec(`
		INSERT INTO artifacts (
			id, job_id, attempt_id, type, storage_provider, storage_key,
			sha256, size_bytes, status, created_at
		) VALUES (?, ?, 1, ?, 'local', ?, ?, 0, 'READY', ?)`,
		artifactID, jobID, kind, "artifacts/sha256/aa/"+artifactID+".mp4",
		strings.Repeat("a", 64), now,
	)
	require.NoError(e.t, err)
}

func nullableTime(t time.Time) interface{} {
	if t.IsZero() {
		return nil
	}
	return t.UTC().Format(time.RFC3339)
}

func beginUploadDefaultCmd(jobID string) BeginUploadCommand {
	return BeginUploadCommand{
		JobID:            jobID,
		WorkerID:         testWorkerID,
		LeaseID:          testLeaseID,
		AttemptNumber:    1,
		ExpectedRevision: testRevision,
		Kind:             "video",
		MimeType:         "video/mp4",
	}
}

// uploadBytes returns a reader over data; Receive uses io.Copy internally.
// uploadBytes takes only data (the uploadID is no longer needed at this
// layer: the service resolves the upload session by uploadID from the
// command argument, not from the byte stream).
func uploadBytes(data []byte) io.Reader {
	return bytes.NewReader(data)
}

func sha256Hex(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:])
}

// Test cases are split by lifecycle concern into service_begin_upload_test.go, service_receive_test.go and service_finalize_test.go.
