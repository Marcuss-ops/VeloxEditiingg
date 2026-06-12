// Package store provides database access layers for Velox.
//
// Database Strategy:
//   - SQLite (sqlite.go): Primary database for all environments. Used for jobs, workers,
//     analytics, calendar, drive links, and YouTube data. Configured via VELOX_DB_DSN.
//   - PostgreSQL (postgres_store.go): Optional enterprise store for projects, assets,
//     templates, and folders. Only used when VELOX_DB_DRIVER=postgres.
//
// SQLite is the default and recommended database. PostgreSQL support exists for
// enterprise deployments requiring concurrent access or advanced features.
package store

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"velox-shared/payload"
)

type SQLiteStore struct {
	db *sql.DB
}

func NewSQLiteStore(path string) (*SQLiteStore, error) {
	if path == "" {
		return nil, fmt.Errorf("store: empty sqlite path")
	}
	if err := os.MkdirAll(filepath.Dir(path), 0755); err != nil {
		return nil, fmt.Errorf("store: create directory: %w", err)
	}

	db, err := sql.Open("sqlite3", path+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		return nil, fmt.Errorf("store: open database: %w", err)
	}
	if err := db.Ping(); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("sqlite: close after ping failure: %v", closeErr)
		}
		return nil, fmt.Errorf("store: ping database: %w", err)
	}

	// Performance PRAGMAs for optimal query speed
	pragmas := []string{
		"PRAGMA synchronous = NORMAL",         // Faster writes, safe with WAL
		"PRAGMA cache_size = -16000",          // 16MB cache (negative = KB)
		"PRAGMA temp_store = MEMORY",           // In-memory temp tables
		"PRAGMA mmap_size = 268435456",        // 256MB memory-mapped I/O
		"PRAGMA page_size = 4096",              // Larger pages for better I/O
		"PRAGMA foreign_keys = ON",             // Enforce referential integrity
		"PRAGMA wal_autocheckpoint = 1000",     // Checkpoint every 1000 pages
	}

	for _, pragma := range pragmas {
		if _, err := db.Exec(pragma); err != nil {
			// Non-fatal, log and continue
			log.Printf("SQLite PRAGMA failed: %s - %v", pragma, err)
		}
	}

	// Connection pool tuning for optimal throughput
	db.SetMaxOpenConns(4)  // SQLite handles concurrent reads well, limit writes
	db.SetMaxIdleConns(2)  // Keep 2 idle connections ready
	db.SetConnMaxLifetime(5 * time.Minute) // Recycle connections periodically

	s := &SQLiteStore{db: db}
	if err := s.initSchema(); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("sqlite: close after schema failure: %v", closeErr)
		}
		return nil, err
	}
	return s, nil
}

func (s *SQLiteStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

func (s *SQLiteStore) initSchema() error {
	ddl := `
CREATE TABLE IF NOT EXISTS jobs (
  job_id TEXT PRIMARY KEY,
  status TEXT,
  video_name TEXT,
  project_id TEXT,
  created_at TEXT,
  updated_at TEXT,
  assigned_to TEXT,
  retry_count INTEGER,
  last_error TEXT,
  completed_at TEXT,
  raw_json TEXT NOT NULL,
  migrated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_updated ON jobs(updated_at);

CREATE TABLE IF NOT EXISTS job_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL,
  status TEXT,
  event_ts TEXT,
  worker_id TEXT,
  message TEXT,
  raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_job_history_job_id ON job_history(job_id);

CREATE TABLE IF NOT EXISTS job_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL,
  log_ts TEXT,
  message TEXT,
  worker_id TEXT,
  is_error INTEGER DEFAULT 0,
  raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_job_logs_job_id ON job_logs(job_id);

CREATE TABLE IF NOT EXISTS workers (
  worker_id TEXT PRIMARY KEY,
  worker_name TEXT,
  status TEXT,
  last_heartbeat TEXT,
  schedulable INTEGER,
  drain INTEGER,
  worker_group TEXT,
  raw_json TEXT NOT NULL,
  migrated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_workers_last_hb ON workers(last_heartbeat);

CREATE TABLE IF NOT EXISTS worker_flags (
  worker_id TEXT PRIMARY KEY,
  revoked INTEGER DEFAULT 0,
  quarantined INTEGER DEFAULT 0,
  raw_json TEXT NOT NULL,
  migrated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS analytics_cache (
  cache_key TEXT PRIMARY KEY,
  ts REAL,
  data_json TEXT NOT NULL,
  migrated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS drive_links (
  id TEXT PRIMARY KEY,
  parent_id TEXT,
  name TEXT,
  link TEXT,
  language TEXT,
  created_at TEXT,
  updated_at TEXT,
  raw_json TEXT NOT NULL,
  migrated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_drive_links_parent ON drive_links(parent_id);
`
	if _, err := s.db.Exec(ddl); err != nil {
		return fmt.Errorf("store: init schema: %w", err)
	}
	// Initialize Dark Editor tables
	if err := s.initDarkEditorSchema(); err != nil {
		return fmt.Errorf("store: init dark editor schema: %w", err)
	}
	// Initialize YouTube historical tables
	if err := s.initYouTubeSchema(); err != nil {
		return fmt.Errorf("store: init youtube schema: %w", err)
	}
	// Initialize Calendar tables
	if err := s.initCalendarSchema(); err != nil {
		return fmt.Errorf("store: init calendar schema: %w", err)
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
