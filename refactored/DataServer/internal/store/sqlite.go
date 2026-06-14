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
	"velox-server/internal/store/migrations"
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

	// Run schema migrations
	if err := migrations.RunMigrations(db, migrationsFS, "migrations"); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("sqlite: close after migration failure: %v", closeErr)
		}
		return nil, fmt.Errorf("store: run migrations: %w", err)
	}

	// Post-migration schema adjustments (ensureColumn for existing columns)
	if err := s.postMigrationAdjustments(); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("sqlite: close after post-migration: %v", closeErr)
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
		{"calendar_events", "youtube_group", "TEXT DEFAULT ''"},
		{"calendar_events", "titles_json", "TEXT DEFAULT '[]'"},
		{"calendar_events", "script_text", "TEXT DEFAULT ''"},
		{"calendar_events", "youtube_links_json", "TEXT DEFAULT '[]'"},
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
