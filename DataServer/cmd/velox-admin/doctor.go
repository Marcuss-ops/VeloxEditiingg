package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"time"

	"velox-server/internal/invariants"
	"velox-server/internal/jobdoctor"
)

// runDoctor prints a read-only, evidence-backed diagnosis for one job. It
// never mutates the database: the handle is opened with SQLite mode=ro.
func runDoctor(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("doctor", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dbPath := fs.String("db", "", "SQLite database path (read-only)")
	jobID := fs.String("job", "", "job id to diagnose")
	jsonOutput := fs.Bool("json", false, "emit the structured JSON report")
	outputPath := fs.String("output", "", "write the report to this path (JSON)")
	stallAfter := fs.Duration("stall-after", 30*time.Second, "no-progress threshold for STALLED")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if strings.TrimSpace(*dbPath) == "" {
		return fmt.Errorf("--db is required")
	}
	if strings.TrimSpace(*jobID) == "" {
		return fmt.Errorf("--job is required")
	}

	db, err := invariants.OpenReadOnly(*dbPath)
	if err != nil {
		return err
	}
	defer db.Close()

	diagnosis, err := jobdoctor.Diagnose(context.Background(), db, *jobID, time.Now().UTC(), jobdoctor.Thresholds{NoProgressAfter: *stallAfter})
	if err != nil {
		return fmt.Errorf("diagnose job: %w", err)
	}
	if !*jsonOutput && strings.TrimSpace(*outputPath) == "" {
		writeHumanDiagnosis(stdout, diagnosis)
		return nil
	}
	encoded, err := json.MarshalIndent(diagnosis, "", "  ")
	if err != nil {
		return fmt.Errorf("encode diagnosis: %w", err)
	}
	encoded = append(encoded, '\n')
	if strings.TrimSpace(*outputPath) == "" {
		if _, err := stdout.Write(encoded); err != nil {
			return fmt.Errorf("write diagnosis: %w", err)
		}
		return nil
	}
	if err := os.WriteFile(*outputPath, encoded, 0o600); err != nil {
		return fmt.Errorf("write diagnosis file: %w", err)
	}
	return nil
}

func writeHumanDiagnosis(w io.Writer, diagnosis jobdoctor.Diagnosis) {
	fmt.Fprintf(w, "JOB %s\n", diagnosis.JobID)
	fmt.Fprintf(w, "VERDICT %s\n", diagnosis.Verdict)
	fmt.Fprintf(w, "job status: %s\n", diagnosis.JobStatus)
	fmt.Fprintf(w, "tasks: %d total, %d READY, %d RUNNING\n", diagnosis.Tasks, diagnosis.ReadyTasks, diagnosis.RunningTasks)
	statuses := make([]string, 0, len(diagnosis.TaskCounts))
	for status := range diagnosis.TaskCounts {
		statuses = append(statuses, status)
	}
	sort.Strings(statuses)
	for _, status := range statuses {
		fmt.Fprintf(w, "  %-12s %d\n", status, diagnosis.TaskCounts[status])
	}
	slots := "unknown"
	if diagnosis.SlotsKnown {
		slots = fmt.Sprintf("%d", diagnosis.FreeSlots)
	}
	fmt.Fprintf(w, "workers: %d available / %d total, %s free slot(s)\n", diagnosis.WorkersAvailable, diagnosis.WorkersTotal, slots)
	if diagnosis.LastProgressAt == "" {
		fmt.Fprintln(w, "last progress: unknown")
	} else {
		fmt.Fprintf(w, "last progress: %s (%s ago)\n", diagnosis.LastProgressAt, time.Duration(diagnosis.ProgressAgeMS)*time.Millisecond)
	}
	if diagnosis.Cause == "" {
		fmt.Fprintln(w, "cause: none")
	} else {
		fmt.Fprintf(w, "cause: %s\n", diagnosis.Cause)
		fmt.Fprintf(w, "detail: %s\n", diagnosis.CauseDetail)
	}
	if len(diagnosis.Evidence) > 0 {
		fmt.Fprintln(w, "evidence:")
		for _, evidence := range diagnosis.Evidence {
			fmt.Fprintf(w, "  - [%s] %s\n", evidence.Source, evidence.Detail)
		}
	}
	if len(diagnosis.Warnings) > 0 {
		fmt.Fprintln(w, "warnings:")
		for _, warning := range diagnosis.Warnings {
			fmt.Fprintf(w, "  - %s\n", warning)
		}
	}
	if diagnosis.InsufficientEvidence {
		fmt.Fprintln(w, "note: evidence is insufficient to name a single cause")
	}
}
