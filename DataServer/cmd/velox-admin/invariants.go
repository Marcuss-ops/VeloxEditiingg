package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"velox-server/internal/invariants"
)

func runInvariantAudit(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("audit-invariants", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "", "SQLite database path")
	outputPath := fs.String("output", "", "write JSON report to this path (default: stdout)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*dbPath) == "" {
		return fmt.Errorf("--db is required")
	}

	db, err := invariants.OpenReadOnly(*dbPath)
	if err != nil {
		return err
	}
	defer db.Close()
	report, err := invariants.Audit(context.Background(), db, *dbPath, time.Now().UTC())
	if err != nil {
		return fmt.Errorf("audit invariants: %w", err)
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode invariant report: %w", err)
	}
	encoded = append(encoded, '\n')
	if strings.TrimSpace(*outputPath) == "" {
		if _, err := stdout.Write(encoded); err != nil {
			return fmt.Errorf("write invariant report: %w", err)
		}
	} else if err := os.WriteFile(*outputPath, encoded, 0o600); err != nil {
		return fmt.Errorf("write invariant report: %w", err)
	}
	if !report.OK {
		return fmt.Errorf("invariant audit found %d violation(s)", len(report.Findings))
	}
	return nil
}
