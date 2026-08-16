package workercache

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"velox-shared/assetref"
)

// cache_helpers.go owns the storage plumbing of the workercache package:
// schema creation/migration, the row projection, the scanner and shared
// predicates. The public CRUD surface lives in cache.go.

const currentSchemaVersion = 5

// legacyBlobKeyPrefix names the degenerate content identity used for a cache
// entry whose verified SHA-256 is unknown (legacy callers, test fixtures,
// folder-backed assets). Such an entry cannot be deduplicated against any
// other asset, so its physical blob is keyed by the asset itself. The prefix
// keeps those synthetic keys from ever colliding with a real 64-hex digest,
// and displayContentHash strips it back to "" for callers.
const legacyBlobKeyPrefix = "legacy:"

func legacyBlobKey(assetKey string) string { return legacyBlobKeyPrefix + assetKey }

// displayContentHash maps a stored blob key back to the caller-facing content
// identity: a synthetic legacy key has no verified digest, so it is reported
// as empty.
func displayContentHash(stored string) string {
	if strings.HasPrefix(stored, legacyBlobKeyPrefix) {
		return ""
	}
	return stored
}

// schemaDDL is the canonical schema for new databases (v5). The logical
// identity (asset_key → content_hash) lives in cached_assets; the physical
// bytes (content_hash → local_path/size/verification) live in cached_blobs.
// Lease ownership remains only in cached_asset_leases, keyed by asset_key.
const schemaDDL = `
CREATE TABLE IF NOT EXISTS cached_blobs (
    content_hash      TEXT PRIMARY KEY,
    local_path        TEXT NOT NULL UNIQUE,
    size_bytes        INTEGER NOT NULL DEFAULT 0,
    created_at        TEXT NOT NULL,
    last_used_at      TEXT NOT NULL,
    verified_at       TEXT NOT NULL DEFAULT '',
    download_complete INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_cached_blobs_last_used
    ON cached_blobs(last_used_at);
CREATE TABLE IF NOT EXISTS cached_assets (
    asset_key    TEXT PRIMARY KEY,
    content_hash TEXT NOT NULL DEFAULT '',
    created_at   TEXT NOT NULL,
    last_used_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_cached_assets_hash
    ON cached_assets(content_hash);
CREATE INDEX IF NOT EXISTS idx_cached_assets_last_used_at
    ON cached_assets(last_used_at);
CREATE TABLE IF NOT EXISTS cached_asset_reservations (
    asset_key TEXT NOT NULL,
    reservation_id TEXT NOT NULL,
    expires_at TEXT NOT NULL,
    PRIMARY KEY (asset_key, reservation_id),
    FOREIGN KEY (asset_key) REFERENCES cached_assets(asset_key) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_cached_asset_reservations_expiry
    ON cached_asset_reservations(expires_at);
CREATE TABLE IF NOT EXISTS cached_asset_leases (
    asset_key TEXT NOT NULL,
    job_id        TEXT NOT NULL,
    acquired_at   TEXT NOT NULL,
    PRIMARY KEY (asset_key, job_id),
    FOREIGN KEY (asset_key) REFERENCES cached_assets(asset_key) ON DELETE CASCADE
);
CREATE INDEX IF NOT EXISTS idx_cached_asset_leases_asset
    ON cached_asset_leases(asset_key);
CREATE TABLE IF NOT EXISTS pending_lease_releases (
    asset_key       TEXT NOT NULL,
    job_id          TEXT NOT NULL,
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TEXT NOT NULL,
    last_error      TEXT NOT NULL DEFAULT '',
    created_at      TEXT NOT NULL,
    updated_at      TEXT NOT NULL,
    PRIMARY KEY (asset_key, job_id)
);
CREATE INDEX IF NOT EXISTS idx_pending_lease_releases_due
    ON pending_lease_releases(next_attempt_at, created_at);
`

// selectCols projects the composite Entry read-model from the two-table join.
// The physical columns come from cached_blobs; lease/reservation counts are
// still derived from the asset_key side. Callers must append selectFrom.
const selectCols = `a.asset_key, a.content_hash,
    COALESCE(b.local_path, ''), COALESCE(b.size_bytes, 0),
    COALESCE(b.download_complete, 0), a.created_at, a.last_used_at,
    (SELECT COUNT(1) FROM cached_asset_leases l WHERE l.asset_key = a.asset_key),
    COALESCE((SELECT MIN(job_id) FROM cached_asset_leases l2 WHERE l2.asset_key = a.asset_key), ''),
    (SELECT COUNT(1) FROM cached_asset_reservations r WHERE r.asset_key = a.asset_key AND julianday(r.expires_at) > julianday('now'))`

// selectFrom is the canonical FROM/JOIN clause for selectCols.
const selectFrom = ` FROM cached_assets a LEFT JOIN cached_blobs b ON b.content_hash = a.content_hash `

// applySchema creates the current schema or upgrades a legacy database. The
// migration is deliberately forward-only: legacy drive_file_id /
// active_job_id columns are gone after upgrade and older binaries are not
// supported against the migrated DB.
// Deployment contract: roll out the new worker before opening the upgraded DB,
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

	legacyKey, err := columnExists(db, "cached_assets", "drive_file_id")
	if err != nil {
		return err
	}
	legacyLease, err := columnExists(db, "cached_assets", "active_job_id")
	if err != nil {
		return err
	}
	if legacyKey || legacyLease {
		if err := migrateLegacySchema(db); err != nil {
			return fmt.Errorf("migrate cache schema: %w", err)
		}
		return nil
	}

	// v3/v4 canonical schema: ensure the content_hash column exists before the
	// blob split reads it.
	hasContentHash, err := columnExists(db, "cached_assets", "content_hash")
	if err != nil {
		return err
	}
	if !hasContentHash {
		if _, err := db.Exec(`ALTER TABLE cached_assets ADD COLUMN content_hash TEXT NOT NULL DEFAULT ''`); err != nil {
			return fmt.Errorf("add content_hash column: %w", err)
		}
	}
	blobsExist, err := tableExists(db, "cached_blobs")
	if err != nil {
		return err
	}
	if !blobsExist {
		if err := migrateToBlobsSchema(db); err != nil {
			return fmt.Errorf("migrate to blob schema: %w", err)
		}
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

// migrateToBlobsSchema splits a v3/v4 single-table cache into the v5 two-table
// model. Physical columns move to cached_blobs (deduplicated by content_hash;
// a hashless row becomes a per-asset legacy blob), and cached_assets is
// rebuilt down to the logical asset_key → content_hash mapping. Leases and
// reservations are untouched: they already key on asset_key, and the
// FOREIGN KEY definitions re-resolve to the rebuilt table by name.
func migrateToBlobsSchema(db *sql.DB) error {
	if _, err := db.Exec(`PRAGMA foreign_keys = OFF`); err != nil {
		return fmt.Errorf("disable foreign keys for blob migration: %w", err)
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

	if _, err := tx.Exec(`CREATE TABLE cached_blobs (
		content_hash TEXT PRIMARY KEY,
		local_path TEXT NOT NULL UNIQUE,
		size_bytes INTEGER NOT NULL DEFAULT 0,
		created_at TEXT NOT NULL,
		last_used_at TEXT NOT NULL,
		verified_at TEXT NOT NULL DEFAULT '',
		download_complete INTEGER NOT NULL DEFAULT 0
	)`); err != nil {
		return rollback(fmt.Errorf("create blob table: %w", err))
	}
	if _, err := tx.Exec(`INSERT OR IGNORE INTO cached_blobs
		(content_hash, local_path, size_bytes, created_at, last_used_at, verified_at, download_complete)
		SELECT
			CASE WHEN content_hash = '' THEN '` + legacyBlobKeyPrefix + `' || asset_key ELSE content_hash END,
			local_path, size_bytes, created_at, last_used_at,
			CASE WHEN download_complete = 1 THEN last_used_at ELSE '' END,
			download_complete
		FROM cached_assets`); err != nil {
		return rollback(fmt.Errorf("populate blobs: %w", err))
	}
	if _, err := tx.Exec(`CREATE INDEX idx_cached_blobs_last_used ON cached_blobs(last_used_at)`); err != nil {
		return rollback(fmt.Errorf("create blob timestamp index: %w", err))
	}

	if _, err := tx.Exec(`CREATE TABLE cached_assets_new (
		asset_key TEXT PRIMARY KEY,
		content_hash TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL,
		last_used_at TEXT NOT NULL
	)`); err != nil {
		return rollback(fmt.Errorf("create migrated cache table: %w", err))
	}
	if _, err := tx.Exec(`INSERT INTO cached_assets_new
		(asset_key, content_hash, created_at, last_used_at)
		SELECT asset_key,
			CASE WHEN content_hash = '' THEN '` + legacyBlobKeyPrefix + `' || asset_key ELSE content_hash END,
			created_at, last_used_at
		FROM cached_assets`); err != nil {
		return rollback(fmt.Errorf("copy cached assets: %w", err))
	}
	if _, err := tx.Exec(`DROP TABLE cached_assets`); err != nil {
		return rollback(fmt.Errorf("drop legacy cache table: %w", err))
	}
	if _, err := tx.Exec(`ALTER TABLE cached_assets_new RENAME TO cached_assets`); err != nil {
		return rollback(fmt.Errorf("rename migrated cache table: %w", err))
	}
	if _, err := tx.Exec(`CREATE INDEX idx_cached_assets_hash ON cached_assets(content_hash)`); err != nil {
		return rollback(fmt.Errorf("create cache hash index: %w", err))
	}
	if _, err := tx.Exec(`CREATE INDEX idx_cached_assets_last_used_at ON cached_assets(last_used_at)`); err != nil {
		return rollback(fmt.Errorf("create cache timestamp index: %w", err))
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit blob migration: %w", err)
	}
	return nil
}

// migrateLegacySchema preserves every cached asset, backfills the many-to-
// many lease table from the old single-owner column, splits the physical
// columns into cached_blobs, and rebuilds the parent table down to the
// logical asset_key → content_hash mapping. All DDL and data movement is one
// transaction; any failure rolls back the upgrade and leaves the legacy
// database usable.
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
	leaseAssetColumn := "drive_file_id"
	if !legacyLeasesExists {
		if _, err := tx.Exec(`CREATE TABLE cached_asset_leases_legacy (
			drive_file_id TEXT NOT NULL,
			job_id TEXT NOT NULL,
			acquired_at TEXT NOT NULL,
			PRIMARY KEY (drive_file_id, job_id)
		)`); err != nil {
			return rollback(err)
		}
	} else {
		legacyLeaseKey, err := columnExistsTx(tx, "cached_asset_leases", "drive_file_id")
		if err != nil {
			return rollback(err)
		}
		canonicalLeaseKey, err := columnExistsTx(tx, "cached_asset_leases", "asset_key")
		if err != nil {
			return rollback(err)
		}
		switch {
		case legacyLeaseKey:
			leaseAssetColumn = "drive_file_id"
		case canonicalLeaseKey:
			leaseAssetColumn = "asset_key"
		default:
			return rollback(fmt.Errorf("cached_asset_leases has neither drive_file_id nor asset_key"))
		}
		if _, err := tx.Exec(`DROP INDEX IF EXISTS idx_cached_asset_leases_asset`); err != nil {
			return rollback(fmt.Errorf("drop legacy lease index: %w", err))
		}
		if _, err := tx.Exec(`ALTER TABLE cached_asset_leases RENAME TO cached_asset_leases_legacy`); err != nil {
			return rollback(fmt.Errorf("preserve legacy leases: %w", err))
		}
	}
	if active, err := columnExistsTx(tx, "cached_assets", "active_job_id"); err != nil {
		return rollback(err)
	} else if active {
		if _, err := tx.Exec(`INSERT OR IGNORE INTO cached_asset_leases_legacy
			(drive_file_id, job_id, acquired_at)
			SELECT drive_file_id, active_job_id, last_used_at
			FROM cached_assets
			WHERE active_job_id IS NOT NULL AND active_job_id != ''`); err != nil {
			return rollback(fmt.Errorf("backfill legacy leases: %w", err))
		}
	}

	// Split physical columns into cached_blobs, keyed by the legacy per-asset
	// identity (a legacy row has no verified digest to deduplicate against).
	if _, err := tx.Exec(`CREATE TABLE cached_blobs (
		content_hash TEXT PRIMARY KEY,
		local_path TEXT NOT NULL UNIQUE,
		size_bytes INTEGER NOT NULL DEFAULT 0,
		created_at TEXT NOT NULL,
		last_used_at TEXT NOT NULL,
		verified_at TEXT NOT NULL DEFAULT '',
		download_complete INTEGER NOT NULL DEFAULT 0
	)`); err != nil {
		return rollback(fmt.Errorf("create blob table: %w", err))
	}
	if _, err := tx.Exec(`INSERT INTO cached_blobs
		(content_hash, local_path, size_bytes, created_at, last_used_at, verified_at, download_complete)
		SELECT '` + legacyBlobKeyPrefix + `' || drive_file_id, local_path, size_bytes, created_at, last_used_at,
			CASE WHEN download_complete = 1 THEN last_used_at ELSE '' END,
			download_complete
		FROM cached_assets`); err != nil {
		return rollback(fmt.Errorf("populate blobs: %w", err))
	}
	if _, err := tx.Exec(`CREATE INDEX idx_cached_blobs_last_used ON cached_blobs(last_used_at)`); err != nil {
		return rollback(fmt.Errorf("create blob timestamp index: %w", err))
	}

	if _, err := tx.Exec(`CREATE TABLE cached_assets_new (
		asset_key TEXT PRIMARY KEY,
		content_hash TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL,
		last_used_at TEXT NOT NULL
	)`); err != nil {
		return rollback(fmt.Errorf("create migrated cache table: %w", err))
	}
	if _, err := tx.Exec(`INSERT INTO cached_assets_new
		(asset_key, content_hash, created_at, last_used_at)
		SELECT drive_file_id, '` + legacyBlobKeyPrefix + `' || drive_file_id, created_at, last_used_at
		FROM cached_assets`); err != nil {
		return rollback(fmt.Errorf("copy cached assets: %w", err))
	}
	if _, err := tx.Exec(`DROP TABLE cached_assets`); err != nil {
		return rollback(fmt.Errorf("drop legacy cache table: %w", err))
	}
	if _, err := tx.Exec(`ALTER TABLE cached_assets_new RENAME TO cached_assets`); err != nil {
		return rollback(fmt.Errorf("rename migrated cache table: %w", err))
	}
	if _, err := tx.Exec(`CREATE INDEX idx_cached_assets_hash ON cached_assets(content_hash)`); err != nil {
		return rollback(fmt.Errorf("create cache hash index: %w", err))
	}
	if _, err := tx.Exec(`CREATE INDEX idx_cached_assets_last_used_at ON cached_assets(last_used_at)`); err != nil {
		return rollback(fmt.Errorf("create cache timestamp index: %w", err))
	}
	if afterAssetsRebuild != nil {
		if err := afterAssetsRebuild(); err != nil {
			return rollback(fmt.Errorf("injected migration failure after assets rebuild: %w", err))
		}
	}
	if _, err := tx.Exec(`CREATE TABLE cached_asset_reservations (
		asset_key TEXT NOT NULL,
		reservation_id TEXT NOT NULL,
		expires_at TEXT NOT NULL,
		PRIMARY KEY (asset_key, reservation_id),
		FOREIGN KEY (asset_key) REFERENCES cached_assets(asset_key) ON DELETE CASCADE
	)`); err != nil {
		return rollback(fmt.Errorf("create migrated reservation table: %w", err))
	}
	if _, err := tx.Exec(`CREATE TABLE cached_asset_leases (
		asset_key TEXT NOT NULL,
		job_id TEXT NOT NULL,
		acquired_at TEXT NOT NULL,
		PRIMARY KEY (asset_key, job_id),
		FOREIGN KEY (asset_key) REFERENCES cached_assets(asset_key) ON DELETE CASCADE
	)`); err != nil {
		return rollback(fmt.Errorf("create migrated lease table: %w", err))
	}
	if _, err := tx.Exec(`INSERT INTO cached_asset_leases (asset_key, job_id, acquired_at)
		SELECT ` + quoteSQLiteIdentifier(leaseAssetColumn) + ` AS asset_key, job_id, acquired_at FROM cached_asset_leases_legacy`); err != nil {
		return rollback(fmt.Errorf("restore migrated leases: %w", err))
	}
	if _, err := tx.Exec(`DROP TABLE cached_asset_leases_legacy`); err != nil {
		return rollback(fmt.Errorf("drop temporary lease table: %w", err))
	}
	if _, err := tx.Exec(`CREATE INDEX idx_cached_asset_leases_asset ON cached_asset_leases(asset_key)`); err != nil {
		return rollback(fmt.Errorf("create lease index: %w", err))
	}
	if _, err := tx.Exec(`CREATE TABLE pending_lease_releases (
		asset_key TEXT NOT NULL,
		job_id TEXT NOT NULL,
		attempt_count INTEGER NOT NULL DEFAULT 0,
		next_attempt_at TEXT NOT NULL,
		last_error TEXT NOT NULL DEFAULT '',
		created_at TEXT NOT NULL,
		updated_at TEXT NOT NULL,
		PRIMARY KEY (asset_key, job_id)
	)`); err != nil {
		return rollback(fmt.Errorf("create lease reconciliation table: %w", err))
	}
	if _, err := tx.Exec(`CREATE INDEX idx_pending_lease_releases_due ON pending_lease_releases(next_attempt_at, created_at)`); err != nil {
		return rollback(fmt.Errorf("create lease reconciliation index: %w", err))
	}
	if _, err := tx.Exec(fmt.Sprintf(`PRAGMA user_version = %d`, currentSchemaVersion)); err != nil {
		return rollback(fmt.Errorf("set migrated schema version: %w", err))
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit cache migration: %w", err)
	}
	return nil
}

func columnExistsTx(tx *sql.Tx, table, column string) (bool, error) {
	rows, err := tx.Query(`PRAGMA table_info(` + quoteSQLiteIdentifier(table) + `)`)
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
		e                Entry
		assetKey         string
		storedHash       string
		dlInt            int
		createdS         string
		usedS            string
		leaseCount       int
		leaseJob         string
		reservationCount int
	)
	err := r.Scan(
		&assetKey, &storedHash, &e.LocalPath, &e.SizeBytes,
		&dlInt, &createdS, &usedS, &leaseCount, &leaseJob, &reservationCount,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("workercache.scanEntry: %w", err)
	}
	e.AssetKey = assetref.AssetKey(assetKey)
	e.ContentHash = assetref.ContentHash(displayContentHash(storedHash))
	e.storedContentHash = storedHash
	e.ActiveLeaseCount = leaseCount
	e.ActiveJobID = leaseJob
	e.ActiveReservationCount = reservationCount
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
func mustHaveAffected(res sql.Result, assetKey, op string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("workercache.%s(%q): rows affected: %w", op, assetKey, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: asset_key=%s", ErrNotFound, assetKey)
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
