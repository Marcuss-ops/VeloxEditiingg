// Package main — DataServer/cmd/seed-velox-db-fixture
//
// Phase 1.5 CI fixture helper. Applies the canonical SQLite
// migrations (via internal/store/migrations.RunMigrations against
// migrations.SQLiteMigrationsFS) to a fresh on-disk DB at the path
// given on the command line. The resulting DB has the EMPTY schema
// shape — all tables exist, zero rows — which the
// check-completion-protocol-invariants.sh script then runs the 4
// invariant queries against. Empty DB → 0 rows → script exits 0
// (positive CI signal). Violation injections are scripted in the
// test harness separately.
//
// Usage:
//   go run ./DataServer/cmd/seed-velox-db-fixture <DB_PATH>
//
// Exit codes:
//   0 — DB created and migrations applied
//   2 — missing DB_PATH argument
//   3 — sqlite open failure
//   4 — migration application failure
//
// The helper captures the canonical migration order through the
// migrations.RunMigrations engine; do NOT inline the `.sql` files
// here, otherwise migration-order drift between the seed helper
// and the production SQLiteStore path is a likely footgun.
package main

import (
	"database/sql"
	"fmt"
	"os"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/store/migrations"
)

func main() {
	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "usage: %s <DB_PATH>\n", os.Args[0])
		os.Exit(2)
	}
	dbPath := os.Args[1]

	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: open %q: %v\n", dbPath, err)
		os.Exit(3)
	}
	defer func() { _ = db.Close() }()

	if err := migrations.RunMigrations(db, migrations.SQLiteMigrationsFS(), "sqlite"); err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: RunMigrations failed: %v\n", err)
		os.Exit(4)
	}

	fmt.Printf("OK %s\n", dbPath)
}
