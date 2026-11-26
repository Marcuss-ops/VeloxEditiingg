package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"strings"

	"velox-server/internal/store"
)

func runLegacyVideoDeadLetterReconcile(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("reconcile-legacy-video-dead-letters", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "", "SQLite database path")
	apply := fs.Bool("apply", false, "reconcile terminal legacy rows; default is read-only dry-run")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*dbPath) == "" {
		return fmt.Errorf("--db is required")
	}
	db, err := store.NewSQLiteStoreFromPath(strings.TrimSpace(*dbPath), false)
	if err != nil {
		return fmt.Errorf("open database without migrations: %w", err)
	}
	defer db.Close()
	result, err := db.ReconcileLegacyVideoDeadLetters(context.Background(), *apply)
	if err != nil {
		return err
	}
	result.Applied = *apply
	encoded, err := json.Marshal(result)
	if err != nil {
		return fmt.Errorf("encode reconciliation result: %w", err)
	}
	_, err = fmt.Fprintln(stdout, string(encoded))
	return err
}
