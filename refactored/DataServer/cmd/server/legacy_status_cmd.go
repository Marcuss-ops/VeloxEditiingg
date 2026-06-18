package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	_ "github.com/mattn/go-sqlite3"

	"velox-server/internal/config"
	migrations "velox-server/internal/migrations"
)

// runLegacyStatus implements `velox-server migrate legacy-status`.
//
// Read-only — does not modify the database. Counts every category of
// legacy state, legacy embedded field, and legacy column. Exits non-zero
// if --strict was passed AND the snapshot reports EraseSafe() == false.
// In default (non-strict) mode it always exits zero so it can be used
// as a CI diagnostic without blocking.
//
// Flags:
//
//	--json     write JSON instead of human-readable
//	--strict   exit non-zero when EraseSafe is false
//	--db PATH  override cfg.Database.DBPath (primarily for testing)
func runLegacyStatus(cfg *config.Config, args []string, stdout, stderr io.Writer) int {
	jsonOut := false
	strict := false
	dbPath := ""

	for i := 0; i < len(args); i++ {
		switch args[i] {
		case "--json":
			jsonOut = true
		case "--strict":
			strict = true
		case "--db":
			if i+1 >= len(args) {
				fmt.Fprintln(stderr, "migrate legacy-status: --db requires a path argument")
				return 2
			}
			dbPath = args[i+1]
			i++
		default:
			fmt.Fprintf(stderr, "migrate legacy-status: unknown flag %q\n", args[i])
			return 2
		}
	}

	if dbPath == "" && cfg != nil {
		dbPath = cfg.Database.DBPath
	}
	if dbPath == "" {
		fmt.Fprintln(stderr, "migrate legacy-status: no DB path configured (set VELOX_DB_PATH or pass --db)")
		return 2
	}
	abs, _ := filepath.Abs(dbPath)
	if _, err := os.Stat(abs); err != nil {
		fmt.Fprintf(stderr, "migrate legacy-status: cannot open db at %s: %v\n", abs, err)
		return 2
	}

	db, err := sql.Open("sqlite3", abs+"?_busy_timeout=5000&mode=ro")
	if err != nil {
		fmt.Fprintf(stderr, "migrate legacy-status: open db: %v\n", err)
		return 1
	}
	defer db.Close()

	counts, err := migrations.CountLegacyStatus(context.Background(), db)
	if err != nil {
		fmt.Fprintf(stderr, "migrate legacy-status: snapshot failed: %v\n", err)
		return 1
	}

	if jsonOut {
		enc := json.NewEncoder(stdout)
		enc.SetIndent("", "  ")
		_ = enc.Encode(struct {
			DB              string                    `json:"db"`
			EraseSafe       bool                      `json:"erase_safe"`
			BlockingReasons []string                  `json:"blocking_reasons,omitempty"`
			Counts          migrations.LegacyStatusCounts `json:"counts"`
		}{
			DB:              abs,
			EraseSafe:       counts.EraseSafe(),
			BlockingReasons: counts.BlockingReasons(),
			Counts:          counts,
		})
	} else {
		fmt.Fprintf(stdout, "DB: %s\n", abs)
		fmt.Fprintln(stdout, "")
		fmt.Fprintf(stdout, "Legacy state rows in jobs:\n")
		fmt.Fprintf(stdout, "  PROCESSING          : %d\n", counts.JobsProcessing)
		fmt.Fprintf(stdout, "  COMPLETED           : %d\n", counts.JobsCompleted)
		fmt.Fprintf(stdout, "  AWAITING_ARTIFACT   : %d\n", counts.JobsAwaitingArt)
		fmt.Fprintf(stdout, "  RENDER_FINISHED     : %d\n", counts.JobsRenderFin)
		fmt.Fprintln(stdout, "")
		fmt.Fprintf(stdout, "Legacy embedded values in jobs:\n")
		fmt.Fprintf(stdout, "  master_video_path   : %d\n", counts.JobsWithMasterPathEmbed)
		fmt.Fprintf(stdout, "  drive_url           : %d\n", counts.JobsWithDriveURLEmbed)
		fmt.Fprintf(stdout, "  youtube_url         : %d\n", counts.JobsWithYouTubeEmbed)
		fmt.Fprintf(stdout, "  artifact_id         : %d\n", counts.JobsWithArtifactIDEmbed)
		fmt.Fprintf(stdout, "  output_sha256       : %d\n", counts.JobsWithOutputSHA256Embed)
		fmt.Fprintf(stdout, "  idempotency_key     : %d\n", counts.JobsWithIdempotencyEmbed)
		fmt.Fprintf(stdout, "  video_uploaded=1    : %d\n", counts.JobsWithVideoUploadedCol)
		fmt.Fprintf(stdout, "  raw_json non-empty  : %d\n", counts.JobsWithRawJSONNonEmpty)
		fmt.Fprintln(stdout, "")
		fmt.Fprintf(stdout, "Other:\n")
		fmt.Fprintf(stdout, "  job_deliveries legacy status  : %d\n", counts.JobDeliveriesLegacyStatus)
		fmt.Fprintf(stdout, "  workflow_runs total           : %d\n", counts.WorkflowRunsCount)
		fmt.Fprintf(stdout, "  workflow_runs raw_json != {}  : %d\n", counts.WorkflowRunsRawJSONNonEmpty)
		fmt.Fprintln(stdout, "")
		fmt.Fprintf(stdout, "Schema:\n")
		fmt.Fprintf(stdout, "  jobs has legacy columns       : %v\n", counts.JobsHasLegacyColumns)
		if counts.JobsHasLegacyColumns {
			fmt.Fprintf(stdout, "  jobs legacy columns present   : %v\n", counts.JobsLegacyColumnsPresent)
		}
		fmt.Fprintln(stdout, "")
		if counts.EraseSafe() {
			fmt.Fprintln(stdout, "Result: ERASE-SAFE — legacy purge may proceed.")
		} else {
			fmt.Fprintln(stdout, "Result: NOT ERASE-SAFE — legacy data still present:")
			for _, r := range counts.BlockingReasons() {
				fmt.Fprintf(stdout, "  - %s\n", r)
			}
		}
	}

	if strict && !counts.EraseSafe() {
		return 1
	}
	return 0
}
