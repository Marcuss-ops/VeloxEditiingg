package migrations

import (
	"database/sql"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// TestObservabilityDashboardsSQL validates the operator SQL against the same
// schema used by production. Static checks catch dashboard shape/cardinality;
// this test catches renamed or removed SQLite columns and unsupported SQL.
func TestObservabilityDashboardsSQL(t *testing.T) {
	db := openObservabilityDashboardDB(t)
	if err := RunMigrations(db, SQLiteMigrationsFS(), "sqlite"); err != nil {
		t.Fatalf("apply SQLite migrations: %v", err)
	}

	sqlText := readObservabilityDashboardSQL(t)
	sectionCount := 0
	for _, line := range strings.Split(sqlText, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "-- ") && len(trimmed) >= 5 && trimmed[3] >= '1' && trimmed[3] <= '8' && trimmed[4] == '.' {
			sectionCount++
		}
	}
	if sectionCount != 8 {
		t.Fatalf("dashboard sections = %d, want 8", sectionCount)
	}

	statements := splitObservabilitySQL(sqlText)
	if len(statements) != 13 {
		t.Fatalf("dashboard SQL statements = %d, want 13", len(statements))
	}

	for i, statement := range statements {
		statement = strings.TrimSpace(statement)
		if statement == "" {
			continue
		}
		params := make([]interface{}, 0, 3)
		if strings.Contains(statement, ":attempt_id") {
			params = append(params, sql.Named("attempt_id", "attempt-test"))
		}
		if strings.Contains(statement, ":from_day") {
			params = append(params, sql.Named("from_day", ""))
		}
		if strings.Contains(statement, ":to_day") {
			params = append(params, sql.Named("to_day", ""))
		}
		if _, err := db.Exec("EXPLAIN QUERY PLAN "+statement, params...); err != nil {
			t.Errorf("dashboard statement %d failed schema/SQL validation: %v\n%s", i+1, err, statement)
		}
	}
}

func openObservabilityDashboardDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", "file:observability-dashboard-"+t.Name()+"?mode=memory&cache=shared&_busy_timeout=5000")
	if err != nil {
		t.Fatalf("open SQLite: %v", err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	if _, err := db.Exec("PRAGMA foreign_keys = ON"); err != nil {
		t.Fatalf("enable foreign keys: %v", err)
	}
	return db
}

func readObservabilityDashboardSQL(t *testing.T) string {
	t.Helper()
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("locate migration test source")
	}
	path := filepath.Join(filepath.Dir(sourceFile), "..", "..", "..", "..", "prometheus", "observability-dashboards.sql")
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return string(content)
}

func splitObservabilitySQL(sqlText string) []string {
	// Remove complete-line comments before splitting. Comments in this file
	// contain semicolons, so splitting first would turn comment continuations
	// into invalid SQL fragments.
	kept := make([]string, 0)
	for _, line := range strings.Split(sqlText, "\n") {
		if strings.HasPrefix(strings.TrimSpace(line), "--") {
			continue
		}
		kept = append(kept, line)
	}

	parts := strings.Split(strings.Join(kept, "\n"), ";")
	statements := make([]string, 0, len(parts))
	for _, part := range parts {
		if statement := strings.TrimSpace(part); statement != "" {
			statements = append(statements, statement)
		}
	}
	return statements
}
