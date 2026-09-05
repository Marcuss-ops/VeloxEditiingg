// Package migrations / 172_render_performance_day_index_test.go
//
// Per-migration unit test for 172_render_performance_day_index.sql.
// Coverage:
//   - The expression index is created and queryable via sqlite_master.
//   - PRAGMA integrity_check returns "ok" after apply.
//   - EXPLAIN QUERY PLAN pins that the rollup day filter
//     (substr(COALESCE(NULLIF(completed_at,”), updated_at),1,10)) actually
//     USES the index for both the `=` (current day) and `<` (prior-day
//     baseline) variants emitted by render_performance_rollup.go.
package migrations

import (
	"database/sql"
	"os"
	"strings"
	"testing"

	// Required for //go:embed parsing.
	_ "embed"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed sqlite/172_render_performance_day_index.sql
var sqliteSQL172 string

// TestMigration172_IndexExists asserts the expression index lands on a fresh
// DB and survives a re-apply (IF NOT EXISTS idempotency).
func TestMigration172_IndexExists(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// task_attempts is created by migration 041 in production; the per-
	// migration test isolates this file, so create the table it indexes.
	if _, err := db.Exec(`CREATE TABLE task_attempts (
		id TEXT PRIMARY KEY,
		task_id TEXT NOT NULL,
		completed_at TEXT NOT NULL DEFAULT '',
		updated_at TEXT NOT NULL DEFAULT ''
	)`); err != nil {
		t.Fatalf("create task_attempts: %v", err)
	}

	applyMigrationSQL(t, db, sqliteSQL172)
	applyMigrationSQL(t, db, sqliteSQL172) // idempotent replay

	var name string
	if err := db.QueryRow(
		`SELECT name FROM sqlite_master WHERE type='index' AND name='idx_task_attempts_perf_day'`,
	).Scan(&name); err != nil {
		t.Fatalf("expression index missing after apply: %v", err)
	}

	var result string
	if err := db.QueryRow(`PRAGMA integrity_check`).Scan(&result); err != nil {
		t.Fatalf("PRAGMA integrity_check: %v", err)
	}
	if result != "ok" {
		t.Errorf("PRAGMA integrity_check returned %q, want \"ok\"", result)
	}
}

// TestMigration172_UsesIndexInQueryPlan pins that the planner actually picks
// the expression index for the rollup's WHERE shape, both `=` and `<`.
// Regression guard: if the SQL text in render_performance_rollup.go drifts
// away from the indexed expression, the plan flips back to SCAN and the
// non-sargable full-scan behavior returns silently.
func TestMigration172_UsesIndexInQueryPlan(t *testing.T) {
	db := openTestDB(t)
	defer db.Close()

	// task_attempts must exist with the columns the expression references.
	if _, err := db.Exec(`CREATE TABLE task_attempts (
		id TEXT PRIMARY KEY,
		task_id TEXT NOT NULL,
		completed_at TEXT NOT NULL DEFAULT '',
		updated_at TEXT NOT NULL DEFAULT ''
	)`); err != nil {
		t.Fatalf("create task_attempts: %v", err)
	}
	applyMigrationSQL(t, db, sqliteSQL172)

	for _, operator := range []string{"=", "<"} {
		plan, err := explainQueryPlan(t, db,
			`SELECT id FROM task_attempts WHERE substr(COALESCE(NULLIF(completed_at, ''), updated_at), 1, 10) `+operator+` ?`,
			"2026-09-05")
		if err != nil {
			t.Fatalf("explain (%s): %v", operator, err)
		}
		if !strings.Contains(plan, "idx_task_attempts_perf_day") {
			t.Errorf("query plan (%s) does not use the expression index:\n%s", operator, plan)
		}
		if strings.Contains(plan, "SCAN task_attempts") && !strings.Contains(plan, "USING") {
			t.Errorf("query plan (%s) falls back to a full scan:\n%s", operator, plan)
		}
	}
}

// TestMigration172_RollupSourceUsesIndexedExpression is a source pin: the
// EQP assertions above use the index expression hardcoded in THIS test file,
// so if the actual rollup query drifts away from the indexed expression the
// migration would silently stop being used with no EQP regression here. This
// test reads the rollup source and asserts the live WHERE expression matches
// the indexed expression (same pattern as the artifacts scan_test.go SQL
// fragment pins), so a rollup-side drift fails loudly at CI time.
func TestMigration172_RollupSourceUsesIndexedExpression(t *testing.T) {
	src, err := os.ReadFile("../../metrics/render_performance_rollup.go")
	if err != nil {
		t.Fatalf("read render_performance_rollup.go: %v", err)
	}
	srcText := string(src)

	// The rollup's WHERE filter (both `=` current-day and `<` prior-day
	// variants are built from this literal via operator concatenation).
	if !strings.Contains(srcText, `substr(COALESCE(NULLIF(a.completed_at, ''), a.updated_at), 1, 10)`) {
		t.Fatal(`render_performance_rollup.go no longer filters by "substr(COALESCE(NULLIF(a.completed_at, ''), a.updated_at), 1, 10)" — ` +
			`the migration 172 day index no longer matches the live rollup query`)
	}

	// The migration's index must express the SAME expression (alias-free),
	// otherwise SQLite will not match the query expression to the index.
	if !strings.Contains(sqliteSQL172, `substr(COALESCE(NULLIF(completed_at, ''), updated_at), 1, 10)`) {
		t.Fatal(`migration 172 no longer indexes "substr(COALESCE(NULLIF(completed_at, ''), updated_at), 1, 10)" — ` +
			`the rollup query cannot use it`)
	}
}

func explainQueryPlan(t *testing.T, db *sql.DB, query string, args ...any) (string, error) {
	t.Helper()
	rows, err := db.Query("EXPLAIN QUERY PLAN "+query, args...)
	if err != nil {
		return "", err
	}
	defer rows.Close()
	var b strings.Builder
	for rows.Next() {
		var id, parent, notused int
		var detail string
		if err := rows.Scan(&id, &parent, &notused, &detail); err != nil {
			return "", err
		}
		b.WriteString(detail)
		b.WriteString("\n")
	}
	return b.String(), rows.Err()
}
