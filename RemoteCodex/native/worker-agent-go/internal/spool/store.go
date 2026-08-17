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
	"strings"
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

// statusSet is the O(1) membership view of AllStatuses. It is derived once
// at init so the per-row IsValid() gate never scans the lifecycle-ordered
// slice (mirrors the telemetry originSet/scopeSet idiom).
var statusSet = func() map[Status]struct{} {
	m := make(map[Status]struct{}, len(AllStatuses))
	for _, v := range AllStatuses {
		m[v] = struct{}{}
	}
	return m
}()

// IsValid reports whether s is in the closed vocabulary.
func (s Status) IsValid() bool {
	_, ok := statusSet[s]
	return ok
}

// ────────────────────────────────────────────────────────────────────────
// StorageTier — the physical medium backing a spooled artifact.
// ────────────────────────────────────────────────────────────────────────

// StorageTier records whether the spooled artifact lives on volatile tmpfs
// (lost on hard crash / reboot) or durable NVMe (survives restart). It is
// captured at Insert time from the output URI so the post-commit cleanup and
// the graceful-shutdown spill know which durability contract applies.
type StorageTier string

const (
	// StorageTierNvmeDurable is the durable medium (always survives restart).
	StorageTierNvmeDurable StorageTier = "NVME_DURABLE"
	// StorageTierTmpfsVolatile is the volatile RAM staging medium (lost on
	// hard crash / reboot; must be re-rendered or spilled before shutdown).
	StorageTierTmpfsVolatile StorageTier = "TMPFS_VOLATILE"
)

// IsValid reports whether t is one of the two closed tiers.
func (t StorageTier) IsValid() bool {
	return t == StorageTierNvmeDurable || t == StorageTierTmpfsVolatile
}

// Volatile reports whether t is backed by tmpfs (and therefore lost on a
// hard crash / reboot).
func (t StorageTier) Volatile() bool {
	return t == StorageTierTmpfsVolatile
}

// Sentinel errors so callers can branch on syscall-equivalent
// conditions from the store layer. Use errors.Is, not str match.
var (
	ErrNotFound       = errors.New("spool: row not found")
	ErrCASConflict    = errors.New("spool: lifecycle CAS conflict")
	ErrInvalidStatus  = errors.New("spool: invalid status input")
	ErrDuplicateSpool = errors.New("spool: duplicate (task_id, attempt_id, worker_spool_key)")
	// ErrIncompatibleSpool is returned by Ensure when a row already
	// exists for the (task_id, attempt_id, worker_spool_key) identity
	// but its content (sha256 / size_bytes) differs from the incoming
	// entry — a real conflict (the same logical output re-encoded to
	// different bytes), not a benign retry.
	ErrIncompatibleSpool = errors.New("spool: existing row has incompatible content")
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
	// StorageTier records the physical backing (tmpfs vs NVMe) of
	// LocalPath. Defaults to NVME_DURABLE on Insert when empty.
	StorageTier StorageTier
	LastError   string
	// UploadTargetJSON is the serialized upload target from the master's
	// ArtifactUploadPlan (transport_id, upload_url, declaration_id, …). It is
	// opaque to this package (the worker marshals/unmarshals it) and empty
	// until StashUploadPlan runs. It is the durable resume key: without it an
	// upload cannot be re-driven after a restart.
	UploadTargetJSON string
	// CommitToken is the short-lived master-issued token authorizing the
	// upload. It is a secret: never log it, never serialize it into
	// UploadTargetJSON.
	CommitToken string
	// UploadAttemptCount is the bounded retry counter for the artifact
	// upload resume loop.
	UploadAttemptCount int
	// NextUploadAttemptAt is the earliest instant the resume loop may retry
	// this row's upload. Zero means "due immediately".
	NextUploadAttemptAt time.Time
	CreatedAt           time.Time
	UpdatedAt           time.Time
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
	// Roll-forward migration for spool DBs created before storage_tier
	// existed: the column is in schemaDDL for fresh databases, and added
	// idempotently here for pre-existing ones.
	if err := ensureStorageTierColumn(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	// Roll-forward migration for spool DBs created before the artifact-
	// upload resume ledger existed (upload_target_json / commit_token /
	// upload_attempt_count / next_upload_attempt_at).
	if err := ensureUploadResumeColumns(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return &Store{db: db}, nil
}

// ensureStorageTierColumn adds storage_tier to pre-existing spool DBs.
// Fresh DBs already carry the column (schemaDDL), so the ALTER fails with
// "duplicate column name" and is ignored.
func ensureStorageTierColumn(db *sql.DB) error {
	if _, err := db.Exec(`ALTER TABLE worker_output_spool ADD COLUMN storage_tier TEXT NOT NULL DEFAULT 'NVME_DURABLE'`); err != nil {
		if strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
			return nil
		}
		return fmt.Errorf("spool.Open: add storage_tier column: %w", err)
	}
	return nil
}

// ensureUploadResumeColumns adds the artifact-upload resume ledger columns to
// pre-existing spool DBs. Fresh DBs already carry them (schemaDDL), so each
// ALTER fails with "duplicate column name" and is ignored. The columns are
// additive and default-empty, so an existing spool keeps working unchanged.
func ensureUploadResumeColumns(db *sql.DB) error {
	cols := []struct{ name, ddl string }{
		{"upload_target_json", `ALTER TABLE worker_output_spool ADD COLUMN upload_target_json TEXT NOT NULL DEFAULT ''`},
		{"commit_token", `ALTER TABLE worker_output_spool ADD COLUMN commit_token TEXT NOT NULL DEFAULT ''`},
		{"upload_attempt_count", `ALTER TABLE worker_output_spool ADD COLUMN upload_attempt_count INTEGER NOT NULL DEFAULT 0`},
		{"next_upload_attempt_at", `ALTER TABLE worker_output_spool ADD COLUMN next_upload_attempt_at TEXT NOT NULL DEFAULT ''`},
	}
	for _, col := range cols {
		if _, err := db.Exec(col.ddl); err != nil {
			if strings.Contains(strings.ToLower(err.Error()), "duplicate column") {
				continue
			}
			return fmt.Errorf("spool.Open: add %s column: %w", col.name, err)
		}
	}
	return nil
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
    storage_tier    TEXT NOT NULL DEFAULT 'NVME_DURABLE',
    last_error      TEXT NOT NULL DEFAULT '',
    upload_target_json TEXT NOT NULL DEFAULT '',
    commit_token    TEXT NOT NULL DEFAULT '',
    upload_attempt_count INTEGER NOT NULL DEFAULT 0,
    next_upload_attempt_at TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    UNIQUE(task_id, attempt_id, worker_spool_key)
);
CREATE INDEX IF NOT EXISTS idx_spool_status
    ON worker_output_spool(status);
CREATE INDEX IF NOT EXISTS idx_spool_task_attempt
    ON worker_output_spool(task_id, attempt_id);
CREATE TABLE IF NOT EXISTS task_result_outbox (
    task_id         TEXT NOT NULL,
    attempt_id      TEXT NOT NULL,
    report_hash     TEXT NOT NULL,
    payload         BLOB NOT NULL,
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (task_id, attempt_id, report_hash)
);
CREATE INDEX IF NOT EXISTS idx_task_result_outbox_due
    ON task_result_outbox(next_attempt_at, created_at);
`
