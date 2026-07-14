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
//
// Layering note (refactor): the 8 lifecycle transitions (MarkReady
// ... MarkCleaned) + the CAS helper live in `store_transitions.go`,
// and the read-side (Insert, Get, List, scanSpool) lives in
// `store_queries.go`. The orchestrator code here stays minimal: the
// closed `Status` enum, the row shape, the Store wrapper, the Open
// lifecycle, and the inline schema DDL. Same `package spool` so
// transitions + queries retain cross-file private-symbol access
// without re-export.
package spool

import (
	"database/sql"
	"errors"
	"fmt"
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
