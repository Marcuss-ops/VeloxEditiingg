// Package doctor provides a composable production-readiness validation
// framework for the Velox worker agent. It implements RW-PROD-002 §2-3:
// a Validator interface with 10 sub-validators that replace the
// transport-only --validate-config with a full runtime check.
//
// Usage from main.go:
//
//	validators := doctor.DefaultValidators(cfg)
//	if err := doctor.Run(ctx, cfg, validators, os.Stderr); err != nil {
//	    os.Exit(1)
//	}
//	os.Exit(0)
//
// The doctor runs validators sequentially, collects results, and writes
// a JSON report. Exit code is 0 only when all validators pass.
package doctor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"velox-worker-agent/pkg/config"
)

// Status constants for validator results.
const (
	StatusPass = "PASS"
	StatusWarn = "WARN"
	StatusFail = "FAIL"
)

// Result is the outcome of a single validator.
type Result struct {
	ID     string `json:"id"`
	Status string `json:"status"` // PASS | WARN | FAIL
	Detail string `json:"detail"`
	Remedy string `json:"remedy,omitempty"`
}

// Report is the aggregate output of a doctor run.
type Report struct {
	WorkerID  string   `json:"worker_id"`
	Verdict   string   `json:"verdict"` // READY | NOT_READY
	CheckedAt string   `json:"checked_at"`
	Checks    []Result `json:"checks"`
}

// Verdict constants.
const (
	VerdictReady    = "READY"
	VerdictNotReady = "NOT_READY"
)

// Validator is a single production-readiness check.
// ID must be a stable, machine-readable identifier (e.g. "mtls", "disk.free").
// Run receives a context for timeout control.
type Validator interface {
	ID() string
	Run(ctx context.Context, cfg *config.WorkerConfig) Result
}

// Run executes all validators sequentially, collects results, computes a
// verdict, and writes the JSON report to w. It returns error if at least
// one validator returned FAIL, or nil if all passed.
func Run(ctx context.Context, cfg *config.WorkerConfig, validators []Validator, w io.Writer) error {
	report := Report{
		WorkerID:  cfg.WorkerID,
		CheckedAt: time.Now().UTC().Format(time.RFC3339),
		Checks:    make([]Result, 0, len(validators)),
	}

	anyFail := false
	for _, v := range validators {
		select {
		case <-ctx.Done():
			report.Checks = append(report.Checks, Result{
				ID:     v.ID(),
				Status: StatusFail,
				Detail: fmt.Sprintf("context cancelled: %v", ctx.Err()),
				Remedy: "increase doctor timeout or fix the underlying hang",
			})
			anyFail = true
		default:
			r := v.Run(ctx, cfg)
			report.Checks = append(report.Checks, r)
			if r.Status == StatusFail {
				anyFail = true
			}
		}
	}

	if anyFail {
		report.Verdict = VerdictNotReady
	} else {
		report.Verdict = VerdictReady
	}

	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	_ = enc.Encode(report)

	if anyFail {
		return fmt.Errorf("doctor: %s (see JSON report for details)", VerdictNotReady)
	}
	return nil
}

// DefaultValidators returns the canonical set of 10 production-readiness
// validators in the order prescribed by RW-PROD-002 §2. Callers may
// append additional validators or reorder for custom flows.
//
// The registry validator is omitted when registry is nil (e.g., in
// --validate-config mode before executor wiring). The full doctor
// (RW-PROD-016) will always supply a non-nil registry.
func DefaultValidators() []Validator {
	return []Validator{
		&EnvironmentValidator{},
		&TransportTLSValidator{},
		&CertExpiryValidator{},
		&DNSReachabilityValidator{},
		&DirsValidator{},
		&DiskFreeValidator{},
		&PortsValidator{},
		&EngineBinaryValidator{},
		&FFmpegValidator{},
	}
}

// DefaultValidatorsWithRegistry returns the full canonical set including
// the executor registry check. Use this when the registry has been wired
// (e.g., during the full doctor command, or after executor construction).
func DefaultValidatorsWithRegistry(registry ExecutorRegistryView) []Validator {
	vals := DefaultValidators()
	if registry != nil {
		vals = append(vals, &RegistryValidator{Registry: registry})
	}
	return vals
}

// ExecutorRegistryView is the narrow interface the doctor needs from the
// executor registry. It keeps pkg/doctor decoupled from the executor
// package's concrete types.
type ExecutorRegistryView interface {
	Descriptors() []DescriptorView
}

// DescriptorView mirrors executor.Descriptor for the doctor's purposes.
type DescriptorView struct {
	ID      string
	Version int
}

// fail is a helper to build a FAIL Result with optional remedy.
func fail(id, detail, remedy string) Result {
	return Result{ID: id, Status: StatusFail, Detail: detail, Remedy: remedy}
}

// pass is a helper to build a PASS Result.
func pass(id, detail string) Result {
	return Result{ID: id, Status: StatusPass, Detail: detail}
}

// warn is a helper to build a WARN Result.
func warn(id, detail string) Result {
	return Result{ID: id, Status: StatusWarn, Detail: detail}
}

// trim is used throughout validators to sanitise env-var and path inputs.
func trim(s string) string { return strings.TrimSpace(s) }
