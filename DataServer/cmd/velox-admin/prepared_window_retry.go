package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"strings"

	"velox-server/internal/store"
)

func runPreparedWindowRetry(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("retry-prepared-windows", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "", "SQLite database path")
	jobID := fs.String("job", "", "running Master job ID")
	apply := fs.Bool("apply", false, "requeue eligible failed preparation tasks")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*dbPath) == "" || strings.TrimSpace(*jobID) == "" {
		return fmt.Errorf("--db and --job are required")
	}
	if !*apply {
		return fmt.Errorf("--apply is required; this operation changes task states")
	}
	db, err := store.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()
	ids, err := db.RetryFailedPreparedWindows(context.Background(), strings.TrimSpace(*jobID))
	if err != nil {
		return err
	}
	for _, id := range ids {
		if _, err := fmt.Fprintln(stdout, id); err != nil {
			return err
		}
	}
	_, err = fmt.Fprintf(stdout, "requeued=%d job=%s\n", len(ids), strings.TrimSpace(*jobID))
	return err
}
