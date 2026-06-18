package main

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/config"
	integrationsYoutube "velox-server/internal/integrations/youtube"
	"velox-server/internal/store/migrations"
)

// migrateUsage prints the usage text for the `migrate` subcommand.
//
// Note for the operator: `migrate youtube-oauth-json` is FILESYSTEM-ONLY
// — it moves legacy Token files into the canonical path and prunes
// empty legacy directories. It does NOT write to SQLite. After this
// command succeeds the operator must still import the canonical JSON
// into youtube_oauth_tokens (currently via Service.BackfillOAuthTokensFromJSON,
// either invoked directly by the operator or wired into the next
// planned `velox-server migrate youtube-oauth-sqlite` subcommand).
func migrateUsage() {
	fmt.Fprintf(os.Stderr, "Usage: velox-server migrate <subcommand> [<args>]\n\n")
	fmt.Fprintf(os.Stderr, "Subcommands:\n")
	fmt.Fprintf(os.Stderr, "  status\n")
	fmt.Fprintf(os.Stderr, "      Show status of all schema migrations (applied/pending/checksum_mismatch).\n")
	fmt.Fprintf(os.Stderr, "  youtube-oauth-json [--dry-run] [--data-dir=PATH]\n")
	fmt.Fprintf(os.Stderr, "      Move legacy OAuth token files under <DataDir>/youtube/\n")
	fmt.Fprintf(os.Stderr, "      into the canonical path <DataDir>/%s/.\n", integrationsYoutube.CanonicalOAuthTokenSubPath)
	fmt.Fprintf(os.Stderr, "      Prints a summary: Found/Moved/Merged/DeletedLegacyFiles/RemovedEmptyDirs/Errors.\n")
	fmt.Fprintf(os.Stderr, "      FILESYSTEM-ONLY: SQL import into youtube_oauth_tokens is NOT performed.\n")
	fmt.Fprintf(os.Stderr, "      To import the canonical JSON into youtube_oauth_tokens, run a\n")
	fmt.Fprintf(os.Stderr, "      future `velox-server migrate youtube-oauth-sqlite` subcommand\n")
	fmt.Fprintf(os.Stderr, "      (planned) or invoke Service.BackfillOAuthTokensFromJSON manually\n")
	fmt.Fprintf(os.Stderr, "      in a host process with the cipher mounted.\n")
}

// runMigrate dispatches `velox-server migrate <sub> [<args>]`. Each
// subcommand is a one-shot command — the HTTP server is NEVER started.
//
// Returns nil on success (including a non-error ConsolidateOAuthTokens
// call with per-file Errors). Returns non-nil error for malformed args
// or subcommand-internal failures.
func runMigrate(cfg *config.Config, args []string) error {
	if len(args) == 0 {
		migrateUsage()
		return fmt.Errorf("migrate: missing subcommand")
	}
	switch args[0] {
	case "status":
		return runMigrateStatus(cfg)
	case "youtube-oauth-json":
		return runMigrateOAuthJSON(cfg, args[1:])
	case "--help", "-h", "help":
		migrateUsage()
		return nil
	default:
		migrateUsage()
		return fmt.Errorf("migrate: unknown subcommand: %s", args[0])
	}
}

// runMigrateStatus implements `velox-server migrate status`.
// Opens the database, discovers all embedded migrations, and prints their status.
func runMigrateStatus(cfg *config.Config) error {
	db, err := openMigrateDB(cfg.Database.DBPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	// Ensure the schema_migrations table exists so we can query it.
	if err := migrations.EnsureSchemaTable(db); err != nil {
		return fmt.Errorf("ensure schema table: %w", err)
	}

	statuses, err := migrations.ListMigrationStatus(db, migrations.MigrationsFS, ".")
	if err != nil {
		return fmt.Errorf("list migration status: %w", err)
	}

	if len(statuses) == 0 {
		fmt.Println("No migrations found.")
		return nil
	}

	// Print header.
	fmt.Printf("%-8s %-30s %-12s  CHECKSUM\n", "VERSION", "NAME", "STATUS")
	fmt.Println(strings.Repeat("-", 80))

	applied := 0
	pending := 0
	mismatch := 0
	for _, ms := range statuses {
		statusStr := ms.Status
		checksumStr := ms.Checksum
		if len(checksumStr) > 16 {
			checksumStr = checksumStr[:16] + "..."
		}
		switch ms.Status {
		case "applied":
			applied++
		case "pending":
			pending++
		case "checksum_mismatch":
			mismatch++
			statusStr = "MISMATCH"
		}
		fmt.Printf("%-8d %-30s %-12s  %s\n", ms.Version, ms.Name, statusStr, checksumStr)
	}

	fmt.Println(strings.Repeat("-", 80))
	fmt.Printf("Total: %d | Applied: %d | Pending: %d | Checksum mismatch: %d\n",
		len(statuses), applied, pending, mismatch)

	return nil
}

// openMigrateDB opens a SQLite database for migration commands.
func openMigrateDB(dbPath string) (*sql.DB, error) {
	db, err := sql.Open("sqlite3", dbPath+"?_busy_timeout=5000&_journal_mode=WAL")
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		db.Close()
		return nil, err
	}
	return db, nil
}

// runMigrateOAuthJSON implements `velox-server migrate youtube-oauth-json`.
//
// Flags:
//
//	--dry-run             Count discovered files and report the move/merge
//	                      plan without touching the filesystem.
//	--data-dir=PATH       Override cfg.Runtime.DataDir for this invocation only.
//	                      Falls back to $VELOX_DATA_DIR via cfg.FromEnv()
//	                      when the flag is absent.
//
// Exit semantics: returns nil when consolidation completes (per-file
// errors are printed but do NOT fail the command — they are part of
// the summary the operator is asking to see). Returns non-nil only
// for malformed args or a workflow-level failure (e.g. dataDir
// unreadable for the discover step).
func runMigrateOAuthJSON(cfg *config.Config, args []string) error {
	dryRun := false
	for _, a := range args {
		switch {
		case a == "--dry-run":
			dryRun = true
		case a == "--help" || a == "-h":
			migrateUsage()
			return nil
		case strings.HasPrefix(a, "--data-dir="):
			cfg.Runtime.DataDir = strings.TrimPrefix(a, "--data-dir=")
		default:
			migrateUsage()
			return fmt.Errorf("migrate youtube-oauth-json: unknown arg: %s", a)
		}
	}
	if cfg.Runtime.DataDir == "" {
		return fmt.Errorf("migrate youtube-oauth-json: VELOX_DATA_DIR (or --data-dir=PATH) must be set")
	}

	fmt.Printf("[MIGRATE] youtube-oauth-json: dataDir=%s dryRun=%v canonical=%s\n",
		cfg.Runtime.DataDir, dryRun,
		fmt.Sprintf("%s/%s", cfg.Runtime.DataDir, integrationsYoutube.CanonicalOAuthTokenSubPath))

	res, err := integrationsYoutube.ConsolidateOAuthTokens(cfg.Runtime.DataDir, dryRun)
	if err != nil {
		return fmt.Errorf("migrate youtube-oauth-json: %w", err)
	}

	fmt.Printf("[MIGRATE] Found=%d Moved=%d Merged=%d DeletedLegacyFiles=%d RemovedEmptyDirs=%d Errors=%d\n",
		res.Found, res.Moved, res.Merged, res.DeletedLegacyFiles, res.RemovedEmptyDirs, len(res.Errors))
	if len(res.Errors) > 0 {
		fmt.Println("[MIGRATE] Per-file errors (require operator reconciliation):")
		for _, e := range res.Errors {
			fmt.Printf("  - %s\n", e)
		}
	}
	if !dryRun && res.Found > 0 {
		log.Printf("[MIGRATE] youtube-oauth-json: %d legacy files relocated. SQL import is a separate operator action — see migration plan S6/S11 for the future `velox-server migrate youtube-oauth-sqlite` subcommand.", res.Moved+res.Merged)
	}
	if len(res.Errors) > 0 {
		log.Printf("[MIGRATE] youtube-oauth-json: reconciliation required for %d per-file errors (see stdout above).", len(res.Errors))
	}
	return nil
}
