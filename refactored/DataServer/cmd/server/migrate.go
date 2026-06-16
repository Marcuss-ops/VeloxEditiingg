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
func migrateUsage() {
	fmt.Fprintf(os.Stderr, "Usage: velox-server migrate <subcommand> [<args>]\n\n")
	fmt.Fprintf(os.Stderr, "Subcommands:\n")
	fmt.Fprintf(os.Stderr, "  youtube-oauth-json [--dry-run] [--data-dir=PATH]\n")
	fmt.Fprintf(os.Stderr, "      Move legacy OAuth token files under <DataDir>/youtube/\n")
	fmt.Fprintf(os.Stderr, "      into the canonical path <DataDir>/%s/.\n", integrationsYoutube.CanonicalOAuthTokenSubPath)
	fmt.Fprintf(os.Stderr, "      Prints a summary: Found/Moved/Merged/DeletedLegacyFiles/RemovedEmptyDirs/Errors.\n")
	fmt.Fprintf(os.Stderr, "      SQL import into youtube_oauth_tokens is NOT performed;\n")
	fmt.Fprintf(os.Stderr, "      run `velox-server migrate youtube-oauth-sqlite` (TODO) afterwards.\n")
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
//	--data-dir=PATH       Override cfg.DataDir for this invocation only.
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
			cfg.DataDir = strings.TrimPrefix(a, "--data-dir=")
		default:
			migrateUsage()
			return fmt.Errorf("migrate youtube-oauth-json: unknown arg: %s", a)
		}
	}
	if cfg.DataDir == "" {
		return fmt.Errorf("migrate youtube-oauth-json: VELOX_DATA_DIR (or --data-dir=PATH) must be set")
	}

	fmt.Printf("[MIGRATE] youtube-oauth-json: dataDir=%s dryRun=%v canonical=%s\n",
		cfg.DataDir, dryRun,
		fmt.Sprintf("%s/%s", cfg.DataDir, integrationsYoutube.CanonicalOAuthTokenSubPath))

	res, err := integrationsYoutube.ConsolidateOAuthTokens(cfg.DataDir, dryRun)
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
	if !dryRun && (res.Found > 0 || len(res.Errors) > 0) {
		log.Printf("[MIGRATE] youtube-oauth-json: legacy files relocated; now run migration tools to import the canonical JSON into youtube_oauth_tokens (TODO: velox-server migrate youtube-oauth-sqlite).")
	}
	return nil
}
