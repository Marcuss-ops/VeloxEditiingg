// Package main provides small, non-destructive Velox operator commands.
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

	"velox-server/internal/audittrail"
	"velox-server/internal/store"
)

type duplicateManifest struct {
	GeneratedAt string                          `json:"generated_at"`
	Mode        string                          `json:"mode"`
	Records     []store.DuplicateDeliveryRecord `json:"records"`
}

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "velox-admin:", err)
		os.Exit(2)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 || args[0] == "--help" || args[0] == "-h" {
		fmt.Fprintln(stderr, "usage: velox-admin duplicate-delivery-manifest --db PATH [--output PATH] [--dry-run]")
		fmt.Fprintln(stderr, "       velox-admin reconcile-stale-executions --db PATH (--dry-run|--apply) [--output PATH] [--limit N] [--actor ID]")
		return nil
	}
	if args[0] == "reconcile-stale-executions" {
		return runStaleExecutionReconcile(args[1:], stdout, stderr)
	}
	if args[0] != "duplicate-delivery-manifest" {
		return fmt.Errorf("unknown command %q", args[0])
	}

	fs := flag.NewFlagSet("duplicate-delivery-manifest", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "", "SQLite database path")
	outputPath := fs.String("output", "", "write JSON manifest to this path (default: stdout)")
	dryRun := fs.Bool("dry-run", true, "audit only; never delete remote files (default true)")
	actor := fs.String("actor", "velox-admin", "audit actor identifier")
	if err := fs.Parse(args[1:]); err != nil {
		return err
	}
	if strings.TrimSpace(*dbPath) == "" {
		return fmt.Errorf("--db is required")
	}
	if !*dryRun {
		return fmt.Errorf("duplicate delivery manifest is dry-run only; remote deletion requires a separately reviewed operation")
	}

	db, err := store.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	ctx := context.Background()
	records, err := db.BuildDuplicateDeliveryManifest(ctx)
	if err != nil {
		return fmt.Errorf("build duplicate delivery manifest: %w", err)
	}
	manifest := duplicateManifest{
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Mode:        "dry-run",
		Records:     records,
	}
	encoded, err := json.MarshalIndent(manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("encode manifest: %w", err)
	}
	encoded = append(encoded, '\n')

	if strings.TrimSpace(*outputPath) == "" {
		if _, err := stdout.Write(encoded); err != nil {
			return fmt.Errorf("write manifest: %w", err)
		}
	} else if err := os.WriteFile(*outputPath, encoded, 0o600); err != nil {
		return fmt.Errorf("write manifest file: %w", err)
	}

	metadata, _ := json.Marshal(map[string]interface{}{
		"mode":             "dry-run",
		"record_count":     len(records),
		"output_path":      strings.TrimSpace(*outputPath),
		"remote_deletions": 0,
	})
	if err := db.AppendAuditEvent(ctx, audittrail.Event{
		ActorType:    "operator",
		ActorID:      strings.TrimSpace(*actor),
		Action:       "DUPLICATE_DELIVERY_MANIFEST_GENERATED",
		ResourceType: "duplicate_delivery_manifest",
		ResourceID:   "dry-run",
		MetadataJSON: string(metadata),
	}); err != nil {
		return fmt.Errorf("append manifest audit event: %w", err)
	}
	return nil
}

func runStaleExecutionReconcile(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("reconcile-stale-executions", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "", "SQLite database path")
	outputPath := fs.String("output", "", "write JSON report to this path (default: stdout)")
	dryRun := fs.Bool("dry-run", false, "scan and report without mutations")
	apply := fs.Bool("apply", false, "apply the idempotent reconciliation plan")
	limit := fs.Int("limit", 500, "maximum findings to process")
	actor := fs.String("actor", "velox-admin", "audit actor identifier")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*dbPath) == "" {
		return fmt.Errorf("--db is required")
	}
	if *dryRun == *apply {
		return fmt.Errorf("exactly one of --dry-run or --apply is required")
	}
	if *limit <= 0 {
		return fmt.Errorf("--limit must be greater than zero")
	}

	db, err := store.NewSQLiteStore(*dbPath)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()
	reconciler, err := store.NewStaleExecutionReconciler(db)
	if err != nil {
		return err
	}
	report, err := reconciler.Reconcile(context.Background(), time.Now().UTC(), *limit, *apply, *actor)
	if err != nil {
		return fmt.Errorf("reconcile stale executions: %w", err)
	}
	encoded, err := json.MarshalIndent(report, "", "  ")
	if err != nil {
		return fmt.Errorf("encode reconciliation report: %w", err)
	}
	encoded = append(encoded, '\n')
	if strings.TrimSpace(*outputPath) == "" {
		_, err = stdout.Write(encoded)
	} else {
		err = os.WriteFile(*outputPath, encoded, 0o600)
	}
	if err != nil {
		return fmt.Errorf("write reconciliation report: %w", err)
	}
	return nil
}
