// Package backup provides verified, filesystem-level SQLite snapshots.
//
// A backup is created with SQLite's VACUUM INTO command, which produces a
// consistent snapshot while the live database remains open. The resulting
// file is promoted atomically and verified before it is reported as usable.
package backup

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"

	_ "github.com/mattn/go-sqlite3"
)

var (
	ErrInvalidPath   = errors.New("backup: invalid path")
	ErrIntegrity     = errors.New("backup: integrity check failed")
	ErrSchemaMissing = errors.New("backup: schema_migrations table missing")
)

// Verification is the evidence recorded for a backup or restored copy.
type Verification struct {
	Path                 string
	SHA256               string
	SizeBytes            int64
	IntegrityOK          bool
	SchemaMigrationCount int64
	TableCounts          map[string]int64
}

// BackupSQLite creates a consistent snapshot at destination and verifies it.
// An existing destination is replaced only after the new snapshot is ready.
func BackupSQLite(ctx context.Context, db *sql.DB, destination string) (Verification, error) {
	if db == nil {
		return Verification{}, fmt.Errorf("%w: nil database", ErrInvalidPath)
	}
	destination, err := normalizedPath(destination)
	if err != nil {
		return Verification{}, err
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return Verification{}, fmt.Errorf("backup: create destination directory: %w", err)
	}

	tmp, err := temporaryPath(destination)
	if err != nil {
		return Verification{}, err
	}
	defer os.Remove(tmp)

	if _, err := db.ExecContext(ctx, "VACUUM INTO ?", tmp); err != nil {
		return Verification{}, fmt.Errorf("backup: vacuum into: %w", err)
	}
	if err := syncFile(tmp); err != nil {
		return Verification{}, fmt.Errorf("backup: sync snapshot: %w", err)
	}
	if err := atomicPromote(tmp, destination); err != nil {
		return Verification{}, err
	}
	return VerifySQLite(ctx, destination)
}

// RestoreSQLite copies a verified backup to destination and promotes it
// atomically. The caller must stop/quiesce the runtime before replacing its
// live database; this function deliberately operates on a path only.
func RestoreSQLite(ctx context.Context, source, destination string) (Verification, error) {
	source, err := normalizedPath(source)
	if err != nil {
		return Verification{}, err
	}
	destination, err = normalizedPath(destination)
	if err != nil {
		return Verification{}, err
	}
	if source == destination {
		return Verification{}, fmt.Errorf("%w: source and destination are identical", ErrInvalidPath)
	}
	if _, err := VerifySQLite(ctx, source); err != nil {
		return Verification{}, fmt.Errorf("backup: source verification: %w", err)
	}
	if err := os.MkdirAll(filepath.Dir(destination), 0o755); err != nil {
		return Verification{}, fmt.Errorf("backup: create restore directory: %w", err)
	}
	tmp, err := temporaryPath(destination)
	if err != nil {
		return Verification{}, err
	}
	defer os.Remove(tmp)

	if err := copyFile(ctx, source, tmp); err != nil {
		return Verification{}, fmt.Errorf("backup: copy restore: %w", err)
	}
	if err := syncFile(tmp); err != nil {
		return Verification{}, fmt.Errorf("backup: sync restore: %w", err)
	}
	if err := atomicPromote(tmp, destination); err != nil {
		return Verification{}, err
	}
	return VerifySQLite(ctx, destination)
}

// VerifySQLite opens path read-only and checks SQLite integrity plus the
// application migration ledger and core state tables.
func VerifySQLite(ctx context.Context, path string) (Verification, error) {
	path, err := normalizedPath(path)
	if err != nil {
		return Verification{}, err
	}
	info, err := os.Stat(path)
	if err != nil {
		return Verification{}, fmt.Errorf("backup: stat %s: %w", path, err)
	}
	if !info.Mode().IsRegular() || info.Size() == 0 {
		return Verification{}, fmt.Errorf("%w: %s is not a non-empty regular file", ErrInvalidPath, path)
	}

	fileDB, err := sql.Open("sqlite3", sqliteReadOnlyDSN(path))
	if err != nil {
		return Verification{}, fmt.Errorf("backup: open read-only: %w", err)
	}
	defer fileDB.Close()
	if err := fileDB.PingContext(ctx); err != nil {
		return Verification{}, fmt.Errorf("backup: ping read-only copy: %w", err)
	}

	var integrity string
	if err := fileDB.QueryRowContext(ctx, "PRAGMA integrity_check").Scan(&integrity); err != nil {
		return Verification{}, fmt.Errorf("backup: integrity query: %w", err)
	}
	if !strings.EqualFold(integrity, "ok") {
		return Verification{}, fmt.Errorf("%w: %s", ErrIntegrity, integrity)
	}

	var migrationCount int64
	if err := fileDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM schema_migrations").Scan(&migrationCount); err != nil {
		return Verification{}, fmt.Errorf("%w: %v", ErrSchemaMissing, err)
	}
	counts := make(map[string]int64, len(coreTables))
	for _, table := range coreTables {
		var count int64
		if err := fileDB.QueryRowContext(ctx, "SELECT COUNT(*) FROM \""+table+"\"").Scan(&count); err != nil {
			return Verification{}, fmt.Errorf("backup: count %s: %w", table, err)
		}
		counts[table] = count
	}

	digest, err := fileSHA256(path)
	if err != nil {
		return Verification{}, fmt.Errorf("backup: hash: %w", err)
	}
	return Verification{
		Path:                 path,
		SHA256:               digest,
		SizeBytes:            info.Size(),
		IntegrityOK:          true,
		SchemaMigrationCount: migrationCount,
		TableCounts:          counts,
	}, nil
}

var coreTables = []string{"jobs", "tasks", "artifacts", "job_deliveries"}

func normalizedPath(path string) (string, error) {
	if strings.TrimSpace(path) == "" {
		return "", fmt.Errorf("%w: empty path", ErrInvalidPath)
	}
	abs, err := filepath.Abs(filepath.Clean(path))
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrInvalidPath, err)
	}
	return abs, nil
}

func temporaryPath(destination string) (string, error) {
	f, err := os.CreateTemp(filepath.Dir(destination), "."+filepath.Base(destination)+".tmp-*")
	if err != nil {
		return "", fmt.Errorf("backup: create temporary file: %w", err)
	}
	path := f.Name()
	if err := f.Close(); err != nil {
		_ = os.Remove(path)
		return "", fmt.Errorf("backup: close temporary file: %w", err)
	}
	if err := os.Remove(path); err != nil {
		return "", fmt.Errorf("backup: prepare temporary path: %w", err)
	}
	return path, nil
}

func atomicPromote(source, destination string) error {
	if err := os.Rename(source, destination); err != nil {
		return fmt.Errorf("backup: atomic promote: %w", err)
	}
	dir, err := os.Open(filepath.Dir(destination))
	if err == nil {
		_ = dir.Sync()
		_ = dir.Close()
	}
	return nil
}

func syncFile(path string) error {
	f, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func copyFile(ctx context.Context, source, destination string) error {
	in, err := os.Open(source)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.OpenFile(destination, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o600)
	if err != nil {
		return err
	}
	defer out.Close()
	buf := make([]byte, 128*1024)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		n, readErr := in.Read(buf)
		if n > 0 {
			if _, err := out.Write(buf[:n]); err != nil {
				return err
			}
		}
		if readErr == io.EOF {
			return nil
		}
		if readErr != nil {
			return readErr
		}
	}
}

func fileSHA256(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", err
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func sqliteReadOnlyDSN(path string) string {
	return (&url.URL{Scheme: "file", Path: filepath.ToSlash(path), RawQuery: "mode=ro&immutable=1"}).String() + "&_busy_timeout=5000"
}
