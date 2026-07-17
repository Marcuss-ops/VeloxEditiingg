// Package store provides database access layers for Velox.
// SQLite is the single database used for jobs, workers, analytics, calendar,
// drive links, and dark editor projects.
package store

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"velox-shared/payload"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/outbox"
	"velox-server/internal/platform/database"
	"velox-server/internal/store/migrations"
)

type SQLiteStore struct {
	db     *sql.DB
	path   string
	outbox OutboxEmitter // optional; nil disables ARTIFACT_READY/JOB_SUCCEEDED emission
}

// OutboxEmitter is the minimal interface SQLiteStore uses to write
// outbox events (ARTIFACT_READY from FinalizeArtifactVerified and
// JOB_SUCCEEDED / JOB_FAILED from PR 3 transactional methods).
// Bootstrap wires an *outbox.Store; nil is a safe no-op (log + skip)
// so callers that have not yet completed the cutover still work.
//
// The `txn` parameter lets the producer enqueue the outbox row in the
// same transaction as its state-change writes — this guarantees the
// "atomic write-then-enqueue" guarantee of the transactional outbox
// pattern. Pass nil for auto-commit (the helper uses s.db).
//
// The interface lives in store/sqlite.go (rather than being a method
// on *outbox.Store) so test fakes in the store package only need a
// one-method stub and so callers in the store package don't pull the
// full outbox surface area — they only need the producer side.
type OutboxEmitter interface {
	Insert(ctx context.Context, txn outbox.Executor, params outbox.InsertParams) (string, error)
}

// SetOutbox wires (or unwires, when o is nil) the outbox emitter. Idempotent.
func (s *SQLiteStore) SetOutbox(o OutboxEmitter) { s.outbox = o }

// emitOutbox writes a PENDING outbox event via the wired emitter.
//
// Returns the wrapped error from the emitter's Insert when the write
// fails. Callers MUST check this and rollback their surrounding *sql.Tx
// to honor the transactional outbox guarantee — if the state change
// committed but the event INSERT failed, downstream handlers would
// never see the transition.
//
// `txn` is forwarded to the emitter so callers in a *sql.Tx can keep
// the outbox enqueue atomic with their state-change writes. Pass nil
// for auto-commit (the helper uses s.db).
//
// PR 2 (bootstrap hardening): when no emitter is wired, this returns
// an error so callers MUST rollback their transaction.  A nil outbox
// emitter is a bootstrap-level misconfiguration — the master should
// fail-fast at startup rather than silently dropping events.
func (s *SQLiteStore) emitOutbox(ctx context.Context, txn outbox.Executor, p outbox.InsertParams) error {
	if s.outbox == nil {
		return fmt.Errorf("store: emitOutbox %s aggregate=%s: outbox not wired — SetOutbox must be called at bootstrap", p.EventType, p.AggregateID)
	}
	if txn == nil {
		txn = s.db
	}
	if _, err := s.outbox.Insert(ctx, txn, p); err != nil {
		return fmt.Errorf("store: emitOutbox %s aggregate=%s: %w", p.EventType, p.AggregateID, err)
	}
	return nil
}

// sqliteTunePragmas lists the runtime PRAGMAs applied post-init to
// each pooled connection. Connection-init-level PRAGMAs (_busy_timeout
// and _journal_mode) are appended to the SQLite DSN inside
// platform/database.openSQLite so they fire on every spawned
// connection — runtime db.Exec PRAGMAs only affect the single
// connection that ran them, never the others in the pool. The
// distinction is non-trivial for MaxOpenConns>=2 deployments where
// concurrent writers from different connections would otherwise get
// busy_timeout=0 and immediately throw SQLITE_BUSY.
//
// page_size is included for historical parity with the legacy
// NewSQLiteStore call sequence; it is a no-op at runtime once the
// database is created and stays here only for diff minimisation.
var sqliteTunePragmas = []string{
	"PRAGMA synchronous = NORMAL",      // Faster writes, safe with WAL
	"PRAGMA cache_size = -32000",       // 32MB cache (negative = KB)
	"PRAGMA temp_store = MEMORY",       // In-memory temp tables
	"PRAGMA mmap_size = 268435456",     // 256MB memory-mapped I/O
	"PRAGMA page_size = 4096",          // Larger pages for better I/O (no-op at runtime)
	"PRAGMA foreign_keys = ON",         // Enforce referential integrity
	"PRAGMA wal_autocheckpoint = 2000", // Checkpoint every 2000 pages
}

// sqliteStorePoolSize returns the (max-open, max-idle, conn-max-
// lifetime) defaults the legacy NewSQLiteStore applied after
// `sql.Open`. Used when constructing an internal Config for the
// SQLiteStore path so production retains the historically-tested
// tuning without leaking Velox-specific opinions into
// platform/database (which uses conservative 1/1/1h defaults).
func sqliteStorePoolSize() (int, int, time.Duration) {
	return 8, 4, 5 * time.Minute
}

// NewSQLiteStoreFromHandle builds a *SQLiteStore from an already-open
// *database.Handle. The Handle is the canonical entry point for the
// platform/database abstraction; production bootstrap (cmd/server/
// bootstrap.go) calls platform/database.Open for both SQLite and
// Postgres backends and routes to this constructor when Handle.Driver
// is DriverSQLite. The 30-or-so test callers of NewSQLiteStore(path)
// still go through NewSQLiteStore (which now delegates to
// platform/database.Open then this function), so the SQLite god-object
// is wired exactly once across the entire codebase.
//
// The handle is taken by reference so the caller retains Close()
// ownership — bootstrap owns the connection lifetime so teardown can
// sequence against the background goroutines that share ctx.
//
// MigrateOnStart gates the schema bootstrap at boot. The flag's intent
// is orthogonal to the driver dispatch in bootstrap.go (driver = sqlite
// vs postgres is decided by VELOX_DB_DRIVER; migration opt-in/out is
// decided by VELOX_DB_MIGRATE_ON_START). Two paths fall out:
//
//   - migrateOnStart == true (legacy default, tests, default for ops
//     who do NOT run an external migration tool): run
//     migrations.RunMigrations + postMigrationAdjustments. The runner
//     is idempotent (checksums + schema_migrations tracking prevent
//     double-apply) so a caller that previously held the DB open sees
//     no change on subsequent opens.
//
//   - migrateOnStart == false (forward-only tool mode, when an external
//     tool like Atlas / goose / sql-migrate / a hand-rolled Ansible
//     playbook owns the schema): skip both. The store still boots;
//     schema_migrations is queried via AppliedVersions and the result
//     is logged so operators running an external tool can see what
//     version is in the DB at boot. Errors at first SQL execution
//     from a stale or partial schema are surfaced naturally via the
//     underlying SQLite calls.
func NewSQLiteStoreFromHandle(handle *database.Handle, path string, migrateOnStart bool) (*SQLiteStore, error) {
	if handle == nil || handle.DB == nil {
		return nil, fmt.Errorf("store: nil sqlite handle")
	}
	if handle.Driver != database.DriverSQLite {
		return nil, fmt.Errorf("store: NewSQLiteStoreFromHandle requires driver=sqlite, got %q", handle.Driver)
	}
	db := handle.DB

	// Apply runtime tuning PRAGMAs. Connection-init PRAGMAs
	// (_busy_timeout, _journal_mode) are already on the DSN. We DO
	// NOT apply any BEGIN IMMEDIATE-style lock here because the
	// Mattn driver + MaxOpenConns=4 retains pooled connections; a
	// silent exclusivity upgrade would break concurrent reads.
	for _, pragma := range sqliteTunePragmas {
		if _, err := db.Exec(pragma); err != nil {
			// Non-fatal, preserve legacy NewSQLiteStore's tolerance.
			log.Printf("SQLite PRAGMA failed: %s - %v", pragma, err)
		}
	}

	s := &SQLiteStore{db: db, path: path}

	if !migrateOnStart {
		// Forward-only tool mode: an external tool owns the schema.
		// Log current applied version (or "untouched DB") so operators
		// running with a real migration tool can see what version is
		// in the DB at boot. The runner is intentionally NOT invoked
		// and postMigrationAdjustments is intentionally NOT invoked.
		logSQLiteForwardOnlySummary(db, path)
		return s, nil
	}

	// Run schema migrations through the dialect-aware accessors
	// declared by migrations/runner.go (SQLiteMigrationsFS() +
	// "sqlite" dir). The runner is idempotent (checksums +
	// schema_migrations tracking prevent double-apply) so a caller
	// that previously held the DB open sees no change on subsequent
	// opens.
	if err := migrations.RunMigrations(db, migrations.SQLiteMigrationsFS(), "sqlite"); err != nil {
		return nil, fmt.Errorf("store: run migrations: %w", err)
	}

	// Post-migration schema adjustments (ensureColumn for existing columns).
	if err := s.postMigrationAdjustments(); err != nil {
		return nil, fmt.Errorf("store: post-migration: %w", err)
	}

	return s, nil
}

// logSQLiteForwardOnlySummary logs the current applied migration
// versions when boot is in forward-only tool mode. Forward-only mode
// means an external tool owns schema state; this summary lets operators
// running that tool see what version the master booted against. If the
// schema_migrations table does not exist (the DB has never been touched
// by Velox), a one-line notice is logged instead so the operator knows
// they need to run their external migration tool against a fresh DB.
//
// Errors querying schema_migrations are logged as "unable to read" but
// are NOT returned — forward-only mode is a trust-the-operator posture,
// and bailing out of boot on a metadata read failure would block
// operators whose external tool doesn't track versions in exactly the
// way Velox does.
func logSQLiteForwardOnlySummary(db *sql.DB, path string) {
	versions, err := migrations.AppliedVersions(db)
	if err != nil {
		// Includes the no-such-table case for a brand-new DB.
		log.Printf("[STORE] forward-only schema mode (path=%s): schema_migrations unreadable — %v — "+
			"verify your external migration tool has applied the expected schema",
			path, err)
		return
	}
	if len(versions) == 0 {
		log.Printf("[STORE] forward-only schema mode (path=%s): schema_migrations empty — "+
			"verify your external migration tool has applied the expected schema",
			path)
		return
	}
	log.Printf("[STORE] forward-only schema mode (path=%s): applied migration versions = %v "+
		"(skip of NewSQLiteStoreFromHandle's own migrations; post-migration adjustments also skipped)",
		path, versions)
}

func NewSQLiteStore(path string) (*SQLiteStore, error) {
	return NewSQLiteStoreFromPath(path, true)
}

// NewSQLiteStoreFromPath is the migration-aware variant of NewSQLiteStore.
// It exists so tests that need to opt out of the embedded migration runner
// (forward-only tool mode for the same DB) can do so without bypassing
// the platform/database.Open / NewSQLiteStoreFromHandle composition.
// Default callers (production boot, ~30 test suites) should continue to
// use NewSQLiteStore(path) which preserves migrateOnStart=true semantics.
func NewSQLiteStoreFromPath(path string, migrateOnStart bool) (*SQLiteStore, error) {
	if path == "" {
		return nil, fmt.Errorf("store: empty sqlite path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("store: create directory: %w", err)
	}

	legacyOpen, legacyIdle, legacyLifetime := sqliteStorePoolSize()
	ctx := context.Background()
	handle, err := database.Open(ctx, database.Config{
		Driver:          database.DriverSQLite,
		SQLitePath:      path,
		MaxOpenConns:    legacyOpen,
		MaxIdleConns:    legacyIdle,
		ConnMaxLifetime: legacyLifetime,
	})
	if err != nil {
		return nil, fmt.Errorf("store: open database: %w", err)
	}
	return NewSQLiteStoreFromHandle(handle, path, migrateOnStart)
}

func (s *SQLiteStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// postMigrationAdjustments handles schema additions that can't be done via CREATE TABLE IF NOT EXISTS
// (e.g., adding columns to existing tables, backfilling). This runs after all migrations.
func (s *SQLiteStore) postMigrationAdjustments() error {
	// Dark Editor: ensure folder_id column on existing databases
	if err := s.ensureColumn("dark_editor_projects", "folder_id", "TEXT"); err != nil {
		return fmt.Errorf("store: post-migration adjustments: %w", err)
	}

	// Calendar: backfill schema additions
	calendarColumns := []struct {
		table      string
		column     string
		definition string
	}{
		{"calendar_events", "external_id", "TEXT DEFAULT ''"},
		{"calendar_events", "source", "TEXT DEFAULT ''"},
		{"calendar_events", "status", "TEXT DEFAULT 'draft'"},
		{"calendar_events", "titles_json", "TEXT DEFAULT '[]'"},
		{"calendar_events", "script_text", "TEXT DEFAULT ''"},
		{"calendar_events", "voiceover_paths_json", "TEXT DEFAULT '[]'"},
		{"calendar_events", "category", "TEXT DEFAULT ''"},
		{"calendar_events", "job_id", "TEXT DEFAULT ''"},
		{"calendar_events", "job_status", "TEXT DEFAULT ''"},
		{"calendar_events", "queued_at", "TEXT"},
		{"calendar_events", "queue_error", "TEXT DEFAULT ''"},
	}
	for _, col := range calendarColumns {
		if err := s.ensureColumn(col.table, col.column, col.definition); err != nil {
			return err
		}
	}

	// YouTube metrics: ensure calendar_output columns
	if err := s.ensureColumn("calendar_events", "output_video_path", "TEXT"); err != nil {
		return err
	}
	if err := s.ensureColumn("calendar_events", "output_video_url", "TEXT"); err != nil {
		return err
	}
	if err := s.ensureColumn("calendar_events", "publish_status", "TEXT"); err != nil {
		return err
	}

	return nil
}

func toISO(v any) string {
	switch t := v.(type) {
	case float64:
		return time.Unix(int64(t), 0).UTC().Format(time.RFC3339)
	case int64:
		return time.Unix(t, 0).UTC().Format(time.RFC3339)
	case int:
		return time.Unix(int64(t), 0).UTC().Format(time.RFC3339)
	case string:
		return t
	default:
		return ""
	}
}

func asString(v any) string {
	return payload.AsString(v)
}

func asInt(v any) int {
	return payload.AsInt(v)
}

func (s *SQLiteStore) columnExists(table, column string) (bool, error) {
	rows, err := s.db.Query(fmt.Sprintf("PRAGMA table_info(%s)", table))
	if err != nil {
		return false, err
	}
	defer rows.Close()

	var (
		cid       int
		name      string
		dataType  string
		notnull   int
		dfltValue sql.NullString
		pk        int
	)
	for rows.Next() {
		if err := rows.Scan(&cid, &name, &dataType, &notnull, &dfltValue, &pk); err != nil {
			continue
		}
		if name == column {
			return true, nil
		}
	}
	return false, nil
}

func (s *SQLiteStore) ensureColumn(table, column, definition string) error {
	exists, err := s.columnExists(table, column)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}
	_, err = s.db.Exec(fmt.Sprintf("ALTER TABLE %s ADD COLUMN %s %s", table, column, definition))
	return err
}

// Ping tests the database connection
func (s *SQLiteStore) Ping() error {
	return s.db.Ping()
}

// DB returns the underlying sql.DB handle for direct queries (e.g. maintenance tasks)
func (s *SQLiteStore) DB() *sql.DB {
	return s.db
}

// Path returns the on-disk file path this SQLiteStore was opened against.
// Used by the /api/v1/audit/persistence endpoint to surface the live DB path
// and detect duplicate copies (the previous dual-DB issue caused groups to
// silently disappear because the runtime was reading a stale source copy).
func (s *SQLiteStore) Path() string {
	if s == nil {
		return ""
	}
	return s.path
}

// nullIfEmpty returns nil for empty strings, otherwise the string itself.
// Used by delivery and asset writers to avoid storing zero-length strings.
func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
