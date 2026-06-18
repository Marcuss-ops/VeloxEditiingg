// Package workflow/migrate — CLI command
//
//	velox-server migrate workflows-v2 --dry-run
//	velox-server migrate workflows-v2 --apply
//
// Reads legacy orchestrator_jobs.raw_json MultiStepJob blobs and replays
// them as workflow_runs + workflow_steps + workflow_dependencies rows.
// Per PR 9 §Migrazione dei workflow esistenti:
//
//   runs_found: 32
//   runs_migrated: 31
//   steps_found: 184
//   steps_migrated: 181
//   invalid_runs: 1
//   invalid_steps: 3
//
// Each error includes: job_id, cause, JSON key, suggested action.
package workflow

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
	"time"
)

// LegacyMultiStepJob is the shape of orchestrator_jobs.raw_json pre-PR8.
// We unmarshal into a tolerant representation so the migrator can either
// accept the bulk of rows or report precise JSON-keyed causes.
type LegacyMultiStepJob struct {
	JobID        string             `json:"job_id"`
	PipelineType string             `json:"pipeline_type"`
	Status       string             `json:"status"`
	Steps        []LegacyStep       `json:"steps"`
	Metadata     map[string]any     `json:"metadata,omitempty"`
	CreatedAt    string             `json:"created_at"`
	UpdatedAt    string             `json:"updated_at"`
	StartedAt    *string            `json:"started_at,omitempty"`
	CompletedAt  *string            `json:"completed_at,omitempty"`
}

// LegacyStep is the inner shape of MultiStepJob.Steps[i].
type LegacyStep struct {
	StepID       string         `json:"step_id"`
	StepName     string         `json:"step_name"`
	StepOrder    int            `json:"step_order"`
	Status       string         `json:"status"`
	JobType      string         `json:"job_type"`
	Payload      map[string]any `json:"payload,omitempty"`
	Dependencies []string       `json:"dependencies,omitempty"`
	Result       map[string]any `json:"result,omitempty"`
	Error        string         `json:"error,omitempty"`
	RetryCount   int            `json:"retry_count"`
	MaxRetries   int            `json:"max_retries"`
}

// MigrateResult is the summary printed by the CLI.
type MigrateResult struct {
	RunsFound     int            `json:"runs_found"`
	RunsMigrated  int            `json:"runs_migrated"`
	StepsFound    int            `json:"steps_found"`
	StepsMigrated int            `json:"steps_migrated"`
	InvalidRuns   int            `json:"invalid_runs"`
	InvalidSteps  int            `json:"invalid_steps"`
	Errors        []MigrateError `json:"errors,omitempty"`
}

// MigrateError is the per-row failure record from PR 9 §Migrazione.
type MigrateError struct {
	JobID         string `json:"job_id"`
	StepID        string `json:"step_id,omitempty"`
	JSONKey       string `json:"json_key,omitempty"`
	Cause         string `json:"cause"`
	SuggestedFlag string `json:"suggested_action,omitempty"`
}

// errorf builds a MigrateError with a key, cause, and suggested action.
func (r *MigrateResult) errorf(jobID, stepID, key, cause, action string) {
	r.Errors = append(r.Errors, MigrateError{
		JobID:         jobID,
		StepID:        stepID,
		JSONKey:       key,
		Cause:         cause,
		SuggestedFlag: action,
	})
}

// DryRun returns the would-be migration summary without writing.
func DryRun(ctx context.Context, repo Repository, rawJSONs [][]byte) (MigrateResult, error) {
	return migrateInternal(ctx, repo, rawJSONs, true)
}

// Apply persists the migrations from rawJSONs.
func Apply(ctx context.Context, repo Repository, rawJSONs [][]byte) (MigrateResult, error) {
	return migrateInternal(ctx, repo, rawJSONs, false)
}

func migrateInternal(ctx context.Context, repo Repository, rawJSONs [][]byte, dryRun bool) (MigrateResult, error) {
	var res MigrateResult
	type plannedRun struct {
		spec WorkflowSpec
	}
	plans := map[string]plannedRun{}

	for _, raw := range rawJSONs {
		res.RunsFound++
		var legacy LegacyMultiStepJob
		if err := json.Unmarshal(raw, &legacy); err != nil {
			res.InvalidRuns++
			res.errorf("<unmarshalable>", "", "raw_json", err.Error(),
				"verify the raw_json blob is valid JSON and re-run --apply")
			continue
		}
		if legacy.JobID == "" {
			res.InvalidRuns++
			res.errorf("<empty>", "", "job_id", "missing job_id",
				"re-export legacy orchestrator_jobs to fix missing job_id")
			continue
		}
		if len(legacy.Steps) == 0 {
			res.InvalidRuns++
			res.errorf(legacy.JobID, "", "steps", "no steps in legacy run",
				"check whether the run was abandoned pre-submit; ignore if expected empty")
			continue
		}

		spec := WorkflowSpec{
			RunID:        legacy.JobID,
			WorkflowType: legacy.PipelineType,
			Input:        legacy.Metadata,
			Steps:        make([]WorkflowStepSpec, 0, len(legacy.Steps)),
		}

		// Map of legacy step_id -> step_key (StepKey must be unique; we
		// fall back to step_name or step_order-index if needed).
		seenKeys := map[string]bool{}
		for i, ls := range legacy.Steps {
			res.StepsFound++
			key := strings.TrimSpace(ls.StepName)
			if key == "" {
				key = strings.TrimSpace(ls.StepID)
			}
			if key == "" {
				key = fmt.Sprintf("step-%d", i)
			}
			if seenKeys[key] {
				res.InvalidSteps++
				dupeSuf := 2
				for {
					candidate := fmt.Sprintf("%s-r%d", key, dupeSuf)
					if !seenKeys[candidate] {
						key = candidate
						break
					}
					dupeSuf++
				}
				res.errorf(legacy.JobID, ls.StepID, "steps[].step_name",
					"duplicate step_key resolved by suffix",
					"apply was retained under suffixed key")
			}
			seenKeys[key] = true

			maxAttempts := ls.MaxRetries + 1
			if maxAttempts <= 0 {
				maxAttempts = 3
			}
			input := ls.Payload
			spec.Steps = append(spec.Steps, WorkflowStepSpec{
				StepKey:       key,
				JobType:       ls.JobType,
				Input:         input,
				DependsOnKeys: ls.Dependencies,
				MaxAttempts:   maxAttempts,
			})
		}
		// Sanity: ensure dependency refs were resolved.
		present := map[string]bool{}
		for _, s := range spec.Steps {
			present[s.StepKey] = true
		}
		for i, s := range spec.Steps {
			for _, d := range s.DependsOnKeys {
				if !present[d] {
					// Best-effort: legacy used step_id rather than step_name.
					// If we can resolve by mapping the legacy step_id we tried earlier, we'd have caught it.
					// We just record this and skip the dep so --apply doesn't break.
					res.errorf(legacy.JobID, ls_legacy_step_id(legacy, i), "steps[].dependencies",
						fmt.Sprintf("dependency %q not resolvable", d),
						"resaved without this edge — re-add manually via update")
					spec.Steps[i].DependsOnKeys = nil
				}
			}
		}

		// Build a legacy step_id → spec.Steps[i] map so dependency
		// resolution works whether the legacy blob used step_id or
		// step_name keys. Then fix-up the spec.Steps[i].DependsOnKeys
		// to point at the v2 step_key (wF-step_key).
		legacyIDToIndex := make(map[string]int, len(legacy.Steps))
		legacyIDToKey := make(map[string]string, len(legacy.Steps))
		for i, ls := range legacy.Steps {
			if ls.StepID != "" {
				legacyIDToIndex[ls.StepID] = i
			}
		}
		for i, s := range spec.Steps {
			if i < len(legacy.Steps) {
				if ls := legacy.Steps[i]; ls.StepID != "" {
					legacyIDToKey[ls.StepID] = s.StepKey
				}
			}
		}
		for i, dep := range legacy.Steps {
			if len(dep.Dependencies) == 0 {
				continue
			}
			var resolved []string
			for _, d := range dep.Dependencies {
				if _, nameOK := present[d]; nameOK {
					resolved = append(resolved, d)
					continue
				}
				if targetIdx, ok := legacyIDToIndex[d]; ok {
					resolved = append(resolved, spec.Steps[targetIdx].StepKey)
					continue
				}
				res.errorf(legacy.JobID, dep.StepID,
					fmt.Sprintf("steps[%d].dependencies", i),
					fmt.Sprintf("dependency %q not resolvable", d),
					"re-add manually via workflow_store_runs_edges or leave empty if not needed")
				res.InvalidSteps++
			}
			if resolved != nil {
				spec.Steps[i].DependsOnKeys = resolved
			}
		}

		res.RunsMigrated++
		res.StepsMigrated += len(spec.Steps)
		plans[legacy.JobID] = plannedRun{spec: spec}
	}

	if dryRun {
		return res, nil
	}

	for _, p := range plans {
		if _, err := repo.CreateRun(ctx, p.spec); err != nil {
			// If it already exists the migration is idempotent; surface
			// any other failure as a per-run error.
			if !strings.Contains(err.Error(), "UNIQUE") {
				res.errorf(p.spec.RunID, "", "create_run", err.Error(),
					"re-run --dry-run to inspect offending rows")
				res.RunsMigrated--
			}
		}
	}

	return res, nil
}

// ls_legacy_step_id looks up the legacy step_id of an index in spec.Steps.
func ls_legacy_step_id(legacy LegacyMultiStepJob, i int) string {
	if i >= 0 && i < len(legacy.Steps) {
		return legacy.Steps[i].StepID
	}
	return ""
}// Command is the entrypoint for `velox-server migrate workflows-v2`.
// It is wired in cmd/server/bootstrap.go main(). It only writes to
// stdout — no database side effects until --apply is given.
func Command(args []string, repo Repository, rawJSONProvider func(ctx context.Context) ([][]byte, error), out io.Writer) error {
	if out == nil {
		out = os.Stdout
	}
	fs := flag.NewFlagSet("workflows-v2", flag.ContinueOnError)
	dryRun := fs.Bool("dry-run", true, "report what would be applied (default)")
	withApply := fs.Bool("apply", false, "actually write the migration into workflow_runs")
	if err := fs.Parse(args); err != nil {
		return err
	}
	apply := *withApply && !*dryRun

	ctx := context.Background()
	raw, err := rawJSONProvider(ctx)
	if err != nil {
		return fmt.Errorf("migrate workflows-v2: read legacy rows: %w", err)
	}

	var res MigrateResult
	if apply {
		res, err = Apply(ctx, repo, raw)
	} else {
		res, err = DryRun(ctx, repo, raw)
	}
	if err != nil {
		return err
	}
	return printMigrateResult(out, res, apply)
}

func printMigrateResult(out io.Writer, r MigrateResult, applied bool) error {
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	if applied {
		fmt.Fprintln(out, "// workflow-v2 migration applied at", time.Now().UTC().Format(time.RFC3339))
	} else {
		fmt.Fprintln(out, "// dry run — pass --apply to actually write rows")
	}
	if err := enc.Encode(r); err != nil {
		return err
	}
	if r.InvalidRuns > 0 || r.InvalidSteps > 0 {
		return errors.New("workflow-v2: invalid rows — re-run --apply after investigation")
	}
	return nil
}
