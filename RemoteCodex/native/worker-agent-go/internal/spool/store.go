// Package spool is the worker-side durable tracker for output
// artifacts produced during the Artifact Commit Protocol (Phase 3.1 of
// docs/completion-protocol.md).
//
// The spool survives worker crashes: every output the encoder
// produces is registered with the row-level state machine so a
// supervisor restart can pick up where the previous incarnation left
// off (resume multipart upload, re-emit DeclareOutputs, audit
// orphans, etc.).
//
// State machine (8 states, CAS-gated transitions):
//
//	RENDERING       ─┐   worker is encoding the artifact
//	                 │
//	OUTPUT_READY    ─┤   sha256 + size captured, file on local fs
//	                 │
//	UPLOAD_PENDING  ─┤   master returned upload_id in ArtifactUploadPlan
//	                 │
//	UPLOADING       ─┘   bytes flowing through transport
//	UPLOADED            master CompleteUpload acked
//	COMMITTED           master committed the attempt; keep the file
//	                    until the supervisor marks it CLEANED
//	REJECTED            worker or master error; keep the row for
//	                    forensics
//	CLEANED             local file deleted; row kept for audit
//
// The transitions are enforced with bordered
// `UPDATE ... WHERE id=? AND status=expected_from` CAS statements so
// a late upload thread cannot overwrite a final REJECTED or CLEANED
// state. The (`task_id`, `attempt_id`, `worker_spool_key`) UNIQUE
// tuple guarantees idempotent registration per task-attempt: a worker
// that re-encodes the same logical output lands on the same row.
//
// SQLite is the durability substrate (matches DataServer convention;
// the worker already exists in a Go ecosystem where the same
// `mattn/go-sqlite3` driver is the production default). WAL + busy
// timeout are applied at Open so concurrent writer goroutines do not
// trip `database is locked`.
package spool

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// ────────────────────────────────────────────────────────────────────────
// Status vocabulary — closed enum.
// ────────────────────────────────────────────────────────────────────────

// Status is the spool row's lifecycle marker. Persisted as TEXT so it
// survives restart; the application layer (this package) is the only
// writer.
type Status string

const (
	StatusRendering     Status = "RENDERING"
	StatusOutputReady   Status = "OUTPUT_READY"
	StatusUploadPending Status = "UPLOAD_PENDING"
	StatusUploading     Status = "UPLOADING"
	StatusUploaded      Status = "UPLOADED"
	StatusCommitted     Status = "COMMITTED"
	StatusRejected      Status = "REJECTED"
	StatusCleaned       Status = "CLEANED"
)

// AllStatuses lists the closed vocabulary in lifecycle order. Callers
// use this for supervisor scans + observability bursts.
var AllStatuses = []Status{
	StatusRendering,
	StatusOutputReady,
	StatusUploadPending,
	StatusUploading,
	StatusUploaded,
	StatusCommitted,
	StatusRejected,
	StatusCleaned,
}

// IsValid reports whether s is in the closed vocabulary.
func (s Status) IsValid() bool {
	for _, v := range AllStatuses {
		if v == s {
			return true
		}
	}
	return false
}

// Sentinel errors so callers can branch on syscall-equivalent
// conditions from the store layer. Use errors.Is, not str match.
var (
	ErrNotFound       = errors.New("spool: row not found")
	ErrCASConflict    = errors.New("spool: lifecycle CAS conflict")
	ErrInvalidStatus  = errors.New("spool: invalid status input")
	ErrDuplicateSpool = errors.New("spool: duplicate (task_id, attempt_id, worker_spool_key)")
)

// ────────────────────────────────────────────────────────────────────────
// SpoolEntry — the row shape.
// ────────────────────────────────────────────────────────────────────────

// SpoolEntry is the worker_output_spool row as exposed to callers.
// All 13 columns the spec lists are present (SpoolID is the
// surrogate primary key; the user-listed (task_id, attempt_id,
// worker_spool_key) UNIQUE is enforced separately).
type SpoolEntry struct {
	SpoolID        string
	TaskID         string
	AttemptID      string
	CommitID       string
	WorkerSpoolKey string
	LocalPath      string
	SHA256         string
	SizeBytes      int64
	UploadID       string
	UploadedBytes  int64
	Status         Status
	LastError      string
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

// ────────────────────────────────────────────────────────────────────────
// Store — the SQLite-backed implementation.
// ────────────────────────────────────────────────────────────────────────

// Store wraps a *sql.DB whose schema guarantees match the spec.
type Store struct {
	db *sql.DB
}

// Open creates (or opens) the spool database at path and applies the
// inline schema. WAL mode + busy timeout are tuned at open so
// concurrent writer goroutines from the encoder / publisher /
// supervisor don't trip on locked-write errors.
//
// An in-memory path may be supplied for tests via ":memory:" with the
// shared-cache convention. Production deployments point at a
// persistent file inside the worker's data dir.
func Open(path string) (*Store, error) {
	dsn := path
	if path != ":memory:" {
		// Append standard DSN init params; matches DataServer
		// convention so operators don't have to learn two flavors.
		dsn = path + "?_busy_timeout=5000&_journal_mode=WAL&_synchronous=NORMAL"
	}
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return nil, fmt.Errorf("spool.Open: sql.Open: %w", err)
	}
	if _, err := db.Exec(`PRAGMA foreign_keys = ON`); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("spool.Open: PRAGMA foreign_keys: %w", err)
	}
	if _, err := db.Exec(schemaDDL); err != nil {
		_ = db.Close()
		return nil, fmt.Errorf("spool.Open: apply schema: %w", err)
	}
	return &Store{db: db}, nil
}

// Close releases the underlying *sql.DB. The store cannot be reused
// after Close.
func (s *Store) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// DB exposes the underlying *sql.DB. Reserved for advanced migration
// scripts and supervisor scans that want to join across tables.
func (s *Store) DB() *sql.DB { return s.db }

// schemaDDL is the inline DDL. Inline (rather than a .sql file +
// migration framework) because the worker spool is local state with
// no version history expectations beyond the rollforward shutdown
// guarantee from PR-PROD-040.
const schemaDDL = `
CREATE TABLE IF NOT EXISTS worker_output_spool (
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
    last_error      TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE(task_id, attempt_id, worker_spool_key)
);
CREATE INDEX IF NOT EXISTS idx_spool_status
    ON worker_output_spool(status);
CREATE INDEX IF NOT EXISTS idx_spool_task_attempt
    ON worker_output_spool(task_id, attempt_id);
`

// ────────────────────────────────────────────────────────────────────────
// Insert / lookup / list.
// ────────────────────────────────────────────────────────────────────────

// Insert registers a new spool entry in StatusRendering. The unique
// tuple (task_id, attempt_id, worker_spool_key) prevents the same
// worker from double-spooling the same logical output.
//
// Returns the SpoolEntry with SpoolID + CreatedAt stamped.
func (s *Store) Insert(ctx context.Context, e SpoolEntry) (*SpoolEntry, error) {
	if e.TaskID == "" || e.AttemptID == "" || e.WorkerSpoolKey == "" {
		return nil, fmt.Errorf("spool.Insert: TaskID, AttemptID, WorkerSpoolKey are required")
	}
	if e.Status == "" {
		e.Status = StatusRendering
	}
	if !e.Status.IsValid() {
		return nil, fmt.Errorf("%w: %q", ErrInvalidStatus, e.Status)
	}
	if e.SpoolID == "" {
		e.SpoolID = newSpoolID()
	}
	now := time.Now().UTC()
	nowStr := now.Format(time.RFC3339Nano)
	e.CreatedAt = now
	e.UpdatedAt = now

	_, err := s.db.ExecContext(ctx, `
		INSERT INTO worker_output_spool (
		    spool_id, task_id, attempt_id, commit_id, worker_spool_key,
		    local_path, sha256, size_bytes, upload_id, uploaded_bytes,
		    status, last_error, created_at, updated_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		e.SpoolID, e.TaskID, e.AttemptID, e.CommitID, e.WorkerSpoolKey,
		e.LocalPath, e.SHA256, e.SizeBytes, e.UploadID, e.UploadedBytes,
		string(e.Status), e.LastError, nowStr, nowStr,
	)
	if err != nil {
		if isUniqueConflict(err) {
			return nil, fmt.Errorf("%w: (task_id=%s attempt_id=%s worker_spool_key=%s)",
				ErrDuplicateSpool, e.TaskID, e.AttemptID, e.WorkerSpoolKey)
		}
		return nil, fmt.Errorf("spool.Insert: %w", err)
	}
	return &e, nil
}

// Get returns the row by SpoolID, or ErrNotFound.
func (s *Store) Get(ctx context.Context, spoolID string) (*SpoolEntry, error) {
	row := s.db.QueryRowContext(ctx, selectSpoolBySpoolID, spoolID)
	return scanSpool(row)
}

// ListByStatus returns all rows in a given status. Used by supervisor
// scans + observability bursts.
func (s *Store) ListByStatus(ctx context.Context, status Status) ([]SpoolEntry, error) {
	rows, err := s.db.QueryContext(ctx, selectSpoolByStatus, string(status))
	if err != nil {
		return nil, fmt.Errorf("spool.ListByStatus: %w", err)
	}
	defer rows.Close()
	var out []SpoolEntry
	for rows.Next() {
		e, err := scanSpool(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

// ListByAttempt returns all rows for (TaskID, AttemptID), in time
// order.
func (s *Store) ListByAttempt(ctx context.Context, taskID, attemptID string) ([]SpoolEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+selectSpoolCols+` FROM worker_output_spool
		  WHERE task_id = ? AND attempt_id = ?
		  ORDER BY created_at ASC`, taskID, attemptID)
	if err != nil {
		return nil, fmt.Errorf("spool.ListByAttempt: %w", err)
	}
	defer rows.Close()
	var out []SpoolEntry
	for rows.Next() {
		e, err := scanSpool(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

// ListResumeCandidates returns rows that are eligible for resume on
// worker restart: anything between OUTPUT_READY and UPLOADED (mid-
// upload states). REJECTED / COMMITTED / CLEANED are excluded.
func (s *Store) ListResumeCandidates(ctx context.Context) ([]SpoolEntry, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT `+selectSpoolCols+` FROM worker_output_spool
		  WHERE status IN ('OUTPUT_READY','UPLOAD_PENDING','UPLOADING','UPLOADED')
		  ORDER BY created_at ASC`)
	if err != nil {
		return nil, fmt.Errorf("spool.ListResumeCandidates: %w", err)
	}
	defer rows.Close()
	var out []SpoolEntry
	for rows.Next() {
		e, err := scanSpool(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *e)
	}
	return out, rows.Err()
}

const selectSpoolCols = `spool_id, task_id, attempt_id, commit_id,
    worker_spool_key, local_path, sha256, size_bytes, upload_id,
    uploaded_bytes, status, last_error, created_at, updated_at`

const selectSpoolBySpoolID = `SELECT ` + selectSpoolCols +
	` FROM worker_output_spool WHERE spool_id = ?`
const selectSpoolByStatus = `SELECT ` + selectSpoolCols +
	` FROM worker_output_spool WHERE status = ? ORDER BY created_at ASC`

// scanDBI abstracts *sql.Row + *sql.Rows so both Get and the iterator
// callers share one scanner.
type scanDBI interface {
	Scan(...interface{}) error
}

func scanSpool(r scanDBI) (*SpoolEntry, error) {
	var (
		e       SpoolEntry
		sizeB   sql.NullInt64
		uploadB sql.NullInt64
		statusS string
		created string
		updated string
	)
	err := r.Scan(
		&e.SpoolID, &e.TaskID, &e.AttemptID, &e.CommitID, &e.WorkerSpoolKey,
		&e.LocalPath, &e.SHA256, &sizeB, &e.UploadID, &uploadB,
		&statusS, &e.LastError, &created, &updated,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("spool.scanSpool: %w", err)
	}
	e.SizeBytes = sizeB.Int64
	e.UploadedBytes = uploadB.Int64
	e.Status = Status(statusS)
	if e.CreatedAt, err = parseRFC3339Nano(created); err != nil {
		return nil, fmt.Errorf("spool.scanSpool: created_at: %w", err)
	}
	if e.UpdatedAt, err = parseRFC3339Nano(updated); err != nil {
		return nil, fmt.Errorf("spool.scanSpool: updated_at: %w", err)
	}
	return &e, nil
}

// ────────────────────────────────────────────────────────────────────────
// Lifecycle transitions — every step is CAS-gated on the
// expected_from status.
// ────────────────────────────────────────────────────────────────────────

// MarkReady transitions RENDERING → OUTPUT_READY, stamping the
// SHA-256 (mandatory) and SizeBytes. Idempotent if the row is already
// OUTPUT_READY (returns nil ErrCASConflict).
func (s *Store) MarkReady(ctx context.Context, spoolID, sha256Hex string, sizeBytes int64) error {
	if len(sha256Hex) != 64 {
		return fmt.Errorf("spool.MarkReady: sha256 must be 64 hex chars (got %d)", len(sha256Hex))
	}
	return s.transition(ctx, spoolID, StatusRendering, StatusOutputReady, map[string]any{
		"sha256":     sha256Hex,
		"size_bytes": sizeBytes,
	})
}

// MarkUploadPending transitions OUTPUT_READY → UPLOAD_PENDING,
// stamping the master-assigned upload_id.
func (s *Store) MarkUploadPending(ctx context.Context, spoolID, uploadID string) error {
	if uploadID == "" {
		return fmt.Errorf("spool.MarkUploadPending: upload_id empty")
	}
	return s.transition(ctx, spoolID, StatusOutputReady, StatusUploadPending, map[string]any{
		"upload_id": uploadID,
	})
}

// MarkUploading transitions UPLOAD_PENDING → UPLOADING plus stashes
// the running bytes counter.
func (s *Store) MarkUploading(ctx context.Context, spoolID string, uploadedBytes int64) error {
	return s.transition(ctx, spoolID, StatusUploadPending, StatusUploading, map[string]any{
		"uploaded_bytes": uploadedBytes,
	})
}

// RecordProgress bumps UploadedBytes while still in UPLOADING. NOT a
// status transition; idempotent.
func (s *Store) RecordProgress(ctx context.Context, spoolID string, uploadedBytes int64) error {
	if uploadedBytes < 0 {
		return fmt.Errorf("spool.RecordProgress: uploaded_bytes < 0")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, `
		UPDATE worker_output_spool
		   SET uploaded_bytes = ?, updated_at = ?
		 WHERE spool_id = ? AND status = ?`,
		uploadedBytes, now, spoolID, string(StatusUploading),
	)
	if err != nil {
		return fmt.Errorf("spool.RecordProgress: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: spool=%s (expected status=UPLOADING)", ErrCASConflict, spoolID)
	}
	return nil
}

// MarkUploaded transitions UPLOADING → UPLOADED. The supervisor's
// audit contract binds this state to the master CompleteUpload ack.
func (s *Store) MarkUploaded(ctx context.Context, spoolID string) error {
	return s.transition(ctx, spoolID, StatusUploading, StatusUploaded, nil)
}

// MarkCommitted transitions UPLOADED → COMMITTED. The row stays
// alive until MarkCleaned runs after the row was acknowledged by
// the master and the local file was deleted.
func (s *Store) MarkCommitted(ctx context.Context, spoolID string) error {
	return s.transition(ctx, spoolID, StatusUploaded, StatusCommitted, nil)
}

// MarkRejected transitions any mid-upload state to REJECTED. The
// LastError field is populated; the row stays alive for forensics.
//
// Per spec the reject path is `any_of(OUTPUT_READY | UPLOAD_PENDING |
// UPLOADING | UPLOADED) → REJECTED`. RENDERING (no artifact on disk
// yet) and the terminal states (COMMITTED, CLEANED, REJECTED) are
// explicitly excluded so a late reject cannot overwrite an
// already-final state and so a render that never produced output
// does not get a phantom REJECTED row.
func (s *Store) MarkRejected(ctx context.Context, spoolID, code, message string) error {
	if spoolID == "" {
		return fmt.Errorf("spool.MarkRejected: spool_id empty")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	lastError := stringsOrDash(code, message)
	res, err := s.db.ExecContext(ctx, `
		UPDATE worker_output_spool
		   SET status = ?, last_error = ?, updated_at = ?
		 WHERE spool_id = ? AND status IN ('OUTPUT_READY','UPLOAD_PENDING','UPLOADING','UPLOADED')`,
		string(StatusRejected), lastError, now, spoolID,
	)
	if err != nil {
		return fmt.Errorf("spool.MarkRejected: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: spool=%s (expected mid-upload state)", ErrCASConflict, spoolID)
	}
	return nil
}

// MarkCleaned transitions COMMITTED | REJECTED → CLEANED. After
// Cleaned the row is audit-only and the local_path is expected to be
// empty (caller is responsible for unlinking the file).
func (s *Store) MarkCleaned(ctx context.Context, spoolID string) error {
	if spoolID == "" {
		return fmt.Errorf("spool.MarkCleaned: spool_id empty")
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)
	res, err := s.db.ExecContext(ctx, `
		UPDATE worker_output_spool
		   SET status = ?, local_path = '', updated_at = ?
		 WHERE spool_id = ? AND status IN ('COMMITTED','REJECTED')`,
		string(StatusCleaned), now, spoolID,
	)
	if err != nil {
		return fmt.Errorf("spool.MarkCleaned: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: spool=%s (expected COMMITTED|REJECTED)", ErrCASConflict, spoolID)
	}
	return nil
}

// Delete hard-deletes the row. Reserved for cleanup tools and tests.
func (s *Store) Delete(ctx context.Context, spoolID string) error {
	if _, err := s.db.ExecContext(ctx,
		`DELETE FROM worker_output_spool WHERE spool_id = ?`, spoolID); err != nil {
		return fmt.Errorf("spool.Delete: %w", err)
	}
	return nil
}

// transition is the canonical CAS-gated status move. Sets the optional
// column overrides (pass nil if no extra columns are needed) and
// stamps updated_at. Column overrides are iterated in deterministic
// alphabetical key order so the SQL placeholder sequence is stable
// across runs (helps test debugging and log diffing).
func (s *Store) transition(ctx context.Context, spoolID string, from, to Status, extras map[string]any) error {
	if !to.IsValid() {
		return fmt.Errorf("%w: %q", ErrInvalidStatus, to)
	}
	now := time.Now().UTC().Format(time.RFC3339Nano)

	// Build the SET clause from the optional overrides. The two
	// shapes collapse into one — always iterate `extras` (Go's
	// randomized map iteration is OK because we sort the keys
	// below for placeholder stability).
	keys := make([]string, 0, len(extras))
	for k := range extras {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var setExtras string
	var args []any
	for _, k := range keys {
		setExtras += ", " + k + " = ?"
		args = append(args, extras[k])
	}
	args = append([]any{string(to)}, args...)
	args = append(args, now, spoolID, string(from))

	q := `UPDATE worker_output_spool
	         SET status = ?` + setExtras + `, updated_at = ?
	        WHERE spool_id = ? AND status = ?`
	res, err := s.db.ExecContext(ctx, q, args...)
	if err != nil {
		return fmt.Errorf("spool.transition: %w", err)
	}
	if n, _ := res.RowsAffected(); n != 1 {
		return fmt.Errorf("%w: spool=%s (expected status=%s)", ErrCASConflict, spoolID, from)
	}
	return nil
}

// ────────────────────────────────────────────────────────────────────────
// helpers
// ────────────────────────────────────────────────────────────────────────

// newSpoolID returns a 16-byte hex sequence. Same construction idiom
// as DataServer/internal/completion/coordinator.go::newUUIDLowerHex;
// collision property is fine for a local single-instance database.
func newSpoolID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		for i := range b {
			b[i] = byte(i + 1)
		}
	}
	return hex.EncodeToString(b[:])
}

// parseRFC3339Nano accepts RFC3339Nano (with nanos) and plain RFC3339
// (second precision) — both forms can land from older code paths.
func parseRFC3339Nano(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

// isUniqueConflict returns true when err is a SQLite UNIQUE constraint
// violation. The mattn/go-sqlite3 driver reports this with the
// sub-string "UNIQUE constraint failed".
func isUniqueConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, frag := range []string{"UNIQUE constraint failed", "constraint failed"} {
		if containsCI(msg, frag) {
			return true
		}
	}
	return false
}

func containsCI(haystack, needle string) bool {
	if len(needle) == 0 {
		return true
	}
	if len(haystack) < len(needle) {
		return false
	}
	// case-insensitive substring match
	h := []byte(haystack)
	n := []byte(needle)
	for i := 0; i+len(n) <= len(h); i++ {
		match := true
		for j := 0; j < len(n); j++ {
			hh := h[i+j]
			nn := n[j]
			if hh >= 'A' && hh <= 'Z' {
				hh += 32
			}
			if nn >= 'A' && nn <= 'Z' {
				nn += 32
			}
			if hh != nn {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}

// stringsOrDash canonicalizes a (code, message) tuple into the
// LastError column. Either component missing becomes "-" so the
// audit string stays single-line and grep-friendly.
func stringsOrDash(code, message string) string {
	if code == "" && message == "" {
		return "-"
	}
	if message == "" {
		return code
	}
	if code == "" {
		return "- " + message
	}
	return code + ": " + message
}
