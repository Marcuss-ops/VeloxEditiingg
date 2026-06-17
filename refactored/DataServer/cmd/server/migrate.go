package main

import (
	"fmt"
	"log"
	"os"
	"strings"

	"velox-server/internal/config"
	integrationsYoutube "velox-server/internal/integrations/youtube"
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
