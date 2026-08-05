package workercache

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// cache_helpers.go owns the storage plumbing of the workercache package:
// schema creation/migration, the row projection, the scanner and shared
// predicates. The public CRUD surface lives in cache.go.

const currentSchemaVersion = 2

// schemaDDL is the canonical schema for new databases. Lease ownership is
// represented only by cached_asset_leases; cached_assets has no lease mirror.
const schemaDDL = `
CREATE TABLE IF NOT EXISTS cached_assets (
    drive_file_id      TEXT PRIMARY KEY,
    local_path         TEXT NOT NULL,
    size_bytes         INTEGER NOT NULL DEFAULT 0,
    download_complete  INTEGER NOT NULL DEFAULT 0,
    created_at         TEXT NOT NULL,
    last_used_at       TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cached_assets_last_used_at
    ON cached_assets(last_used_at);
CREATE TABLE IF NOT EXISTS cached_asset_leases (
    drive_file_id TEXT NOT NULL,
    job_id        TEXT NOT NULL,
    acquired_at   TEXT NOT NULL,
    PRIMARY KEY (drive_file_id, job_id),
    FOREIGN KEY (drive_file_id) REFERENCES cached_assets(drive_file_id) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_cached_asset_leases_asset
    ON cached_asset_leases(drive_file_id);
`

const selectCols = `drive_file_id, local_path, size_bytes,
    download_complete, created_at, last_used_at,
    (SELECT COUNT(1) FROM cached_asset_leases l WHERE l.drive_file_id = cached_assets.drive_file_id),
    COALESCE((SELECT MIN(job_id) FROM cached_asset_leases l2 WHERE l2.drive_file_id = cached_assets.drive_file_id), '')`

// applySchema creates the current schema or upgrades a legacy database. The
// migration is deliberately forward-only: once version 2 is committed the
// legacy column is gone and older binaries are not supported against it.
// Deployment contract: roll out the v2 worker before opening the upgraded DB,
// and do not downgrade that worker against the same DB after this migration.
func applySchema(db *sql.DB) error {
	// A single connection makes PRAGMA and the migration transaction operate
	// on the same SQLite connection, and is also safer for :memory: databases.
	db.SetMaxOpenConns(1)

	exists, err := tableExists(db, "cached_assets")
	if err != nil {
		return err
	}
	if !exists {
		if _, err := db.Exec(schemaDDL); err != nil {
			return fmt.Errorf("create cache schema: %w", err)
		}
		return setSchemaVersion(db, currentSchemaVersion)
	}

	legacy, err := columnExists(db, "cached_assets", "active_job_id")
	if err != nil {
		return err
	}
	if legacy {
		if err := migrateLegacySchema(db); err != nil {
			return fmt.Errorf("migrate cache schema: %w", err)
		}
		return nil
	}

	if _, err := db.Exec(schemaDDL); err != nil {
		return fmt.Errorf("ensure cache schema: %w", err)
	}
	return setSchemaVersion(db, currentSchemaVersion)
}

func tableExists(db *sql.DB, name string) (bool, error) {
	var exists int
	err := db.QueryRow(`SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&exists)
	return exists != 0, err
}

func columnExists(db *sql.DB, table, column string) (bool, error) {
	rows, err := db.Query(`PRAGMA table_info(` + quoteSQLiteIdentifier(table) + `)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull, primaryKey int
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func quoteSQLiteIdentifier(identifier string) string {
	// All call sites use compile-time identifiers. Doubling quotes keeps this
	// helper safe if another migration later supplies a validated identifier.
	out := "\""
	for _, r := range identifier {
		if r == '"' {
			out += "\"\""
		} else {
			out += string(r)
		}
	}
	return out + "\""
}

func setSchemaVersion(db *sql.DB, version int) error {
	if _, err := db.Exec(fmt.Sprintf("PRAGMA user_version = %d", version)); err != nil {
		return fmt.Errorf("set cache schema version %d: %w", version, err)
	}
	return nil
}

// migrateLegacySchema preserves every cached asset, backfills the many-to-
// many lease table from the old single-owner column, and rebuilds the parent
// table without that column. All DDL and data movement is one transaction;
// any failure rolls back the upgrade and leaves the legacy database usable.
func migrateLegacySchema(db *sql.DB) error {
	return migrateLegacySchemaWithHook(db, nil)
}

// migrateLegacySchemaWithHook exists to prove rollback after destructive DDL
// in tests. Production callers use migrateLegacySchema, which supplies no
// hook and follows the same transaction path.
func migrateLegacySchemaWithHook(db *sql.DB, afterAssetsRebuild func() error) error {
	if _, err := db.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		return fmt.Errorf("disable foreign keys for migration: %w", err)
	}
	defer func() { _, _ = db.Exec(`PRAGMA foreign_keys = ON`) }()

	tx, err := db.Begin()
	if err != nil {
		return err
	}
	rollback := func(cause error) error {
		_ = tx.Rollback()
		return cause
	}

	legacyLeasesExists, err := tableExistsTx(tx, "cached_asset_leases")
	if err != nil {
		return rollback(err)
	}
	if !legacyLeasesExists {
		if _, err := tx.Exec(`CREATE TABLE cached_asset_leases (
			drive_file_id TEXT NOT NULL,
			job_id TEXT NOT NULL,
			acquired_at TEXT NOT NULL,
			PRIMARY KEY (drive_file_id, job_id)
		)`); err != nil {
			return rollback(err)
		}
	}
	if _, err := tx.Exec(`INSERT OR IGNORE INTO cached_asset_leases
		(drive_file_id, job_id, acquired_at)
		SELECT drive_file_id, active_job_id, last_used_at
		FROM cached_assets
		WHERE active_job_id IS NOT NULL AND active_job_id != ''`); err != nil {
		return rollback(fmt.Errorf("backfill legacy leases: %w", err))
	}

	if _, err := tx.Exec(`DROP INDEX IF EXISTS idx_cached_asset_leases_asset`); err != nil {
		return rollback(fmt.Errorf("drop legacy lease index: %w", err))
	}
	if _, err := tx.Exec(`ALTER TABLE cached_asset_leases RENAME TO cached_asset_leases_legacy`); err != nil {
		return rollback(fmt.Errorf("preserve backfilled leases: %w", err))
	}
	if _, err := tx.Exec(`CREATE TABLE cached_assets_new (
		drive_file_id TEXT PRIMARY KEY,
		local_path TEXT NOT NULL,
		size_bytes INTEGER NOT NULL DEFAULT 0,
		download_complete INTEGER NOT NULL DEFAULT 0,
		created_at TEXT NOT NULL,
		last_used_at TEXT NOT NULL
	)`); err != nil {
		return rollback(fmt.Errorf("create migrated cache table: %w", err))
	}
	if _, err := tx.Exec(`INSERT INTO cached_assets_new
		(drive_file_id, local_path, size_bytes, download_complete, created_at, last_used_at)
		SELECT drive_file_id, local_path, size_bytes, download_complete, created_at, last_used_at
		FROM cached_assets`); err != nil {
		return rollback(fmt.Errorf("copy cached assets: %w", err))
	}
	if _, err := tx.Exec(`DROP TABLE cached_assets`); err != nil {
		return rollback(fmt.Errorf("drop legacy cache table: %w", err))
	}
	if _, err := tx.Exec(`ALTER TABLE cached_assets_new RENAME TO cached_assets`); err != nil {
		return rollback(fmt.Errorf("rename migrated cache table: %w", err))
	}
	if _, err := tx.Exec(`CREATE INDEX idx_cached_assets_last_used_at ON cached_assets(last_used_at)`); err != nil {
		return rollback(fmt.Errorf("create cache timestamp index: %w", err))
	}
	if afterAssetsRebuild != nil {
		if err := afterAssetsRebuild(); err != nil {
			return rollback(fmt.Errorf("injected migration failure after assets rebuild: %w", err))
		}
	}
	if _, err := tx.Exec(`CREATE TABLE cached_asset_leases (
		drive_file_id TEXT NOT NULL,
		job_id TEXT NOT NULL,
		acquired_at TEXT NOT NULL,
		PRIMARY KEY (drive_file_id, job_id),
		FOREIGN KEY (drive_file_id) REFERENCES cached_assets(drive_file_id) ON DELETE CASCADE
	)`); err != nil {
		return rollback(fmt.Errorf("create migrated lease table: %w", err))
	}
	if _, err := tx.Exec(`INSERT INTO cached_asset_leases (drive_file_id, job_id, acquired_at)
		SELECT drive_file_id, job_id, acquired_at FROM cached_asset_leases_legacy`); err != nil {
		return rollback(fmt.Errorf("restore migrated leases: %w", err))
	}
	if _, err := tx.Exec(`DROP TABLE cached_asset_leases_legacy`); err != nil {
		return rollback(fmt.Errorf("drop temporary lease table: %w", err))
	}
	if _, err := tx.Exec(`CREATE INDEX idx_cached_asset_leases_asset ON cached_asset_leases(drive_file_id)`); err != nil {
		return rollback(fmt.Errorf("create lease index: %w", err))
	}
	if _, err := tx.Exec(`PRAGMA user_version = 2`); err != nil {
		return rollback(fmt.Errorf("set migrated schema version: %w", err))
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit cache migration: %w", err)
	}
	return nil
}

func tableExistsTx(tx *sql.Tx, name string) (bool, error) {
	var exists int
	err := tx.QueryRow(`SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&exists)
	return exists != 0, err
}

// scanDBI lets scanEntry work for both *sql.Row and *sql.Rows.
type scanDBI interface {
	Scan(...interface{}) error
}

func scanEntry(r scanDBI) (*Entry, error) {
	var (
		e          Entry
		dlInt      int
		createdS   string
		usedS      string
		leaseCount int
		leaseJob   string
	)
	err := r.Scan(
		&e.DriveFileID, &e.LocalPath, &e.SizeBytes,
		&dlInt, &createdS, &usedS, &leaseCount, &leaseJob,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("workercache.scanEntry: %w", err)
	}
	if leaseCount > 0 {
		e.ActiveLeaseCount = leaseCount
		e.ActiveJobID = leaseJob
	}
	e.DownloadComplete = dlInt != 0
	if e.CreatedAt, err = parseRFC3339Nano(createdS); err != nil {
		return nil, fmt.Errorf("workercache.scanEntry: created_at: %w", err)
	}
	if e.LastUsedAt, err = parseRFC3339Nano(usedS); err != nil {
		return nil, fmt.Errorf("workercache.scanEntry: last_used_at: %w", err)
	}
	return &e, nil
}

// mustHaveAffected returns ErrNotFound if the result affected zero rows.
func mustHaveAffected(res sql.Result, driveFileID, op string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("workercache.%s(%q): rows affected: %w", op, driveFileID, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: drive_file_id=%s", ErrNotFound, driveFileID)
	}
	return nil
}

func parseRFC3339Nano(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

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
	if needle == "" {
		return true
	}
	if len(haystack) < len(needle) {
		return false
	}
	h := []byte(haystack)
	n := []byte(needle)
	for i := 0; i+len(n) <= len(h); i++ {
		match := true
		for j := 0; j < len(n); j++ {
			hh := h[i+j]
			if hh >= 'A' && hh <= 'Z' {
				hh += 32
			}
			nn := n[j]
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
