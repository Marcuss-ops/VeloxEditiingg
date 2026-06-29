// Package bootstrap — boot report types.
//
// Run() returns a *Report. Each sub-step writes a StepResult with
// timing (dur_ms). The Report can be serialised to JSON via AsJSON().
// Operators can grep the JSON for "status":"FAIL" + "code" to triage
// which sub-step blocked the boot.
package bootstrap

import (
	"encoding/json"
	"time"
)

// Report is the canonical, JSON-stable boot record. Field names use
// snake_case so dashboards (Grafana / Loki / local jq) can index them
// without per-field renaming. The structured payload is intentionally
// SIMPLE — every Step is a flat record, no nested error trees.
type Report struct {
	WorkerID    string       `json:"worker_id"`
	BundleHash  string       `json:"bundle_hash,omitempty"`
	OutputDir   string       `json:"output_dir"`
	Verdict     string       `json:"verdict"` // "OK" | "FAIL"
	StartedAt   time.Time    `json:"started_at"`
	CompletedAt time.Time    `json:"completed_at"`
	DurMs       int64        `json:"dur_ms"`
	Steps       []StepResult `json:"steps"`
}

// HasFailure returns true iff at least one Step ended in FAIL. The
// bootstrap package's invariant (RW-PROD-003 §4 acceptance criterion
// "NESSUN fallback silenzioso") means: this is the only signal Run()
// uses to decide whether the worker can continue.
func (r *Report) HasFailure() bool {
	if r == nil {
		return true
	}
	for _, s := range r.Steps {
		if s.Status == "FAIL" {
			return true
		}
	}
	return false
}

// StepResult is one row of the report. Code is a stable, greppable
// reason identifier (e.g. "engine_missing", "tools.ffprobe_missing",
// "output_dir.readonly", "bundle_version_mismatch", "engine_selftest_baseline_mismatch").
// Operators can wire alerts on individual codes — Dashboarding for
// residency review does not need to parse the prose Error field.
type StepResult struct {
	Name        string    `json:"name"`             // engine_self_render | ffmpeg | output_dir | bundle_hash
	Status      string    `json:"status"`           // "OK" | "FAIL" | "SKIP"
	Code        string    `json:"code,omitempty"`   // stable error code
	Detail      string    `json:"detail,omitempty"` // human-readable
	StartedAt   time.Time `json:"started_at"`
	CompletedAt time.Time `json:"completed_at"`
	DurMs       int64     `json:"dur_ms"`
}

// AsJSON marshals the report to stable JSON (sorted map keys via
// encoding/json default). Operators can pipe it to jq / logger
// without further normalisation.
func (r *Report) AsJSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}
