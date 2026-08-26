// Package store provides database access layers for Velox.
// SQLite is the single database used for jobs, workers, analytics,
// calendar, and drive links.
// COMPATIBILITY:
// Owner:        P0.4 store-facade migration
// Remove after: 2026-09-30
// Read-only:    yes

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

	"velox-server/internal/deliverystore"
	"velox-server/internal/forwardingstore"
	"velox-server/internal/outbox"
	"velox-server/internal/platform/database"
	"velox-server/internal/repository"
	"velox-server/internal/store/migrations"
)

type SQLiteStore struct {
	db                *sql.DB
	path              string
	outbox            OutboxEmitter // optional; nil disables ARTIFACT_READY/JOB_SUCCEEDED emission
	retentionDays     retentionDays // configurable retention windows (see SetRetention)
	resourceRetention resourceRetention
	dbTelemetry       DBTelemetry
	// partitionKnobs is the (Stale, Partition) threshold pair used by
	// detectAndPersistPartitionTransition + ReconcileWorkerPartitions.
	// Renamed from `partitionThresholds` to avoid a name collision with
	// the `partitionThresholds()` helper method on the same receiver.
	partitionKnobs partitionThresholds
	// forwarding is the leaf SQLite persistence for creator_forwardings.
	// The forwarding methods on SQLiteStore delegate to it. The
	// cross-domain Job+Task creator is injected into the leaf so
	// AtomicForwardAndEnqueue can own the whole creator_forwardings +
	// job/task transaction without importing store.
	forwarding *forwardingstore.SQLiteForwardingStore
	// delivery is the leaf SQLite persistence for job_deliveries +
	// delivery_destinations. The delivery methods on SQLiteStore delegate
	// to it. The cross-domain parent-job finalizer is injected into the
	// leaf so the terminal MarkDelivery* transitions can own the
	// job_deliveries + jobs transaction without importing store.
	delivery *deliverystore.SQLiteDeliveryStore
}

// OutboxEmitter is re-exported from the repository leaf package. The
// canonical declaration lives in internal/repository so callers do not pull
// the full outbox surface area — they only need the producer side.
type OutboxEmitter = repository.OutboxEmitter

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
// bootstrap.go) calls platform/database.Open and routes to this
// constructor when Handle.Driver is DriverSQLite. The 30-or-so test
// callers of NewSQLiteStore(path) still go through NewSQLiteStore
// (which now delegates to platform/database.Open then this function),
// so the SQLite god-object is wired exactly once across the entire
// codebase.
//
// The handle is taken by reference so the caller retains Close()
// ownership — bootstrap owns the connection lifetime so teardown can
// sequence against the background goroutines that share ctx.
//
// MigrateOnStart gates the schema bootstrap at boot. The flag's intent
// is orthogonal to the driver dispatch in bootstrap.go (driver = sqlite
// is decided by VELOX_DB_DRIVER; migration opt-in/out is decided by
// VELOX_DB_MIGRATE_ON_START). Two paths fall out:
//
//   - migrateOnStart == true (legacy default, tests, default for ops
//     who do NOT run an external migration tool): run
//     migrations.RunMigrations. The runner
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
	s.forwarding = forwardingstore.NewSQLiteForwardingStore(db).WithJobTaskCreator(NewAtomicJobTaskCreator(s))
	s.delivery = deliverystore.NewSQLiteDeliveryStore(db).WithParentJobFinalizer(s)

	if !migrateOnStart {
		// Forward-only tool mode: an external tool owns the schema.
		// Log current applied version (or "untouched DB") so operators
		// running that tool can see what version is in the DB at
		// boot. The runner is intentionally NOT invoked.
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

// postMigrationAdjustments and its helpers (ensureColumn, columnExists)
// have been removed. All columns formerly added by ensureColumn are
// already defined in migration 001_initial.sql (calendar_events table
// includes every column). The migrations.RunMigrations path is the
// single source of schema truth.

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

// Ping tests the database connection
func (s *SQLiteStore) Ping() error {
	return s.db.Ping()
}

// DB returns the underlying sql.DB handle for direct queries (e.g. maintenance tasks)
func (s *SQLiteStore) DB() *sql.DB {
	return s.db
}

// Forwarding exposes the creator_forwardings leaf persistence so the
// composition root can wire leaf consumers (the forwarding runner, the
// creatorflow resolver) directly against forwardingstore instead of going
// through the store facade. The leaf is constructed once in
// NewSQLiteStoreFromHandle with the injected Job+Task creator.
func (s *SQLiteStore) Forwarding() *forwardingstore.SQLiteForwardingStore {
	if s == nil {
		return nil
	}
	return s.forwarding
}

// Delivery exposes the job_deliveries leaf persistence so the composition
// root can wire leaf consumers directly against deliverystore instead of
// going through the store facade. The leaf is constructed once in
// NewSQLiteStoreFromHandle with the injected parent-job finalizer, and lazily
// for directly-constructed test stores.
func (s *SQLiteStore) Delivery() *deliverystore.SQLiteDeliveryStore {
	if s == nil {
		return nil
	}
	return s.deliveryStore()
}

// deliveryStore returns the delivery leaf, lazily constructing it for
// directly-constructed *SQLiteStore values (test fixtures) so the facade
// methods never nil-panic.
func (s *SQLiteStore) deliveryStore() *deliverystore.SQLiteDeliveryStore {
	if s.delivery == nil && s.db != nil {
		s.delivery = deliverystore.NewSQLiteDeliveryStore(s.db).WithParentJobFinalizer(s)
	}
	return s.delivery
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
