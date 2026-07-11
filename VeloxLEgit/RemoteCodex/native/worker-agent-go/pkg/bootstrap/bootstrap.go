// pkg/bootstrap/bootstrap.go — orchestrator (RW-PROD-003 §2).
//
// Run() is the WORKER-AGENT COMPOSITION-ROOT entry point. It is called
// synchronously between the C++ pipeline wiring (video.NewPipelineRunner
// must succeed) and the executor wiring (executors.NewSceneComposite
// must be constructed with a proven runner). Failure to boot here
// means the worker exits 1 BEFORE sending Hello — the master side
// selector never sees `registered=true`, and costmodel.Score will
// exclude the worker from scheduling despite any heartbeat probe that
// might arrive.
//
// On success Run() flips a package-level atomic gate so any caller
// that wants to gate side-effects on "bootstrap actually approved"
// can consult Ok(). Defence-in-depth use: worker.New() constructed in
// main.go can refuse to enter the session loop unless Ok() is true —
// turning out-of-order composition roots into an os.Exit(1) instead of
// a quietly-broken worker.
package bootstrap

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sync/atomic"
	"time"

	"velox-worker-agent/pkg/config"
)

// okGate is the package-level atomic bool that flips to true at the
// end of Run() iff every Step ended in "OK". The composition root, the
// worker.New constructor, and PlanMode debug surfaces all consult it.
//
// Process-wide because bootstrap is a one-shot per process: a worker
// either booted successfully or it didn't. There is no scenario where
// two concurrent bootstrap calls compete.
var okGate atomic.Bool

// Ok reports whether Run() completed successfully in this process. Any
// pre-Ok() call returns false. This is the defensible gate worker.go
// can rely on to assert "all engine/ffmpeg/output/bundle checks
// passed before session start".
func Ok() bool {
	return okGate.Load()
}

// HardGate returns nil iff Ok() is true. Worker.go Start() calls this
// once at the top so an out-of-order composition root (e.g. someone
// re-arranging main.go and accidentally calling Start() before
// bootstrap.Run) fails closed with a loud, greppable reason.
//
// Returns &errBootstrapNotRun when okGate is false. errBootstrapNotRun
// embeds the stable code "bootstrap_not_run" so dashboards can alert
// on "worker booted without bootstrap" without parsing prose.
func HardGate() error {
	if okGate.Load() {
		return nil
	}
	return fmt.Errorf("bootstrap_not_run: bootstrap.Ok() is false — composition root called w.Start before bootstrap.Run (RW-PROD-003 A6)")
}

// Reset clears the gate. Used by tests that need to re-bootstrap in
// the same process. Production code MUST NOT call this — a reset
// after boot would silently re-admit a previously failed worker.
func Reset() {
	okGate.Store(false)
}

// Run executes every sub-step in deterministic order and returns a
// fully-populated *Report. The runner parameter is the canonical
// pkg/video pipeline.Runner — caller must have already called
// video.NewPipelineRunner and confirmed no error.
//
// opts.WorkDir defaults to cfg.WorkDir; opts.OutputDir defaults to the
// canonical scratch path; opts.FFmpegMinMajor defaults to
// DefaultFFmpegMinMajor; opts.BaselineSHA256Path defaults to
// <WorkDir>/tests/fixtures/engine_selftest_baseline.sha256.
//
// The Run() return value is non-nil even on failure — the *Report
// records which Step failed so the JSON dump on os.Exit(1) gives
// operators an immediate triage handle. The error return is the
// canonical short-form of the failing step for grep-friendly log
// emission.
// Run returns a *Report + error. error is nil iff every Step is OK.
// Report is always non-nil (even on failure) so operators have the
// full step record.
func Run(ctx context.Context, cfg *config.WorkerConfig, runner RunnerView, opts Options) (*Report, error) {
	resolved := applyOptions(opts, cfg)
	report := &Report{
		OutputDir: resolved.OutputDir,
		StartedAt: time.Now().UTC(),
	}
	if cfg != nil {
		report.WorkerID = cfg.WorkerID
		report.BundleHash = cfg.BundleHash
	}

	// step() centralises the timing helper so every sub-step result
	// has a uniform completed-at + dur_ms shape. It does NOT swallow
	// errors — the step itself decides whether its outcome is a FAIL.
	appender := func(s StepResult) {
		report.Steps = append(report.Steps, s)
		// Emit a structured per-step log row regardless of level.
		// Verbose in dev; collapsed by the worker logger in prod via
		// LogLevel filtering.
		if resolved.Logger != nil {
			resolved.Logger.Info(
				"[BOOTSTRAP] step=%s status=%s code=%s dur_ms=%d detail=%s",
				s.Name, s.Status, s.Code, s.DurMs, s.Detail,
			)
		}
		if s.Status == "FAIL" && resolved.Logger != nil {
			resolved.Logger.Error(
				"[BOOTSTRAP_FAIL] %s/%s dur_ms=%d detail=%s",
				s.Name, s.Code, s.DurMs, s.Detail,
			)
		}
	}

	// Order matters: bundle first (cheap; detects operator mistake
	// before we waste 5s on a C++ render), then ffmpeg (cheap
	// LookPath + version parse), then output_dir (1 syscall chain),
	// then engine self-render (expensive; ≤5s bound).
	appender(runBundleHashGate(cfg, resolved.WorkDir))
	appender(runFFmpegSelfTest(ctx, resolved))
	appender(runOutputDirSmokeTest(ctx, resolved.OutputDir))

	if runner == nil {
		// Without a runner we cannot exercise the engine at all. This
		// is a different code from "the engine refused the plan" so
		// operators can distinguish "composition root forgot to pass
		// pipelineRunner" from "the engine is broken".
		failAt := time.Now().UTC()
		appender(StepResult{
			Name:        "engine_self_render",
			Status:      "FAIL",
			Code:        "engine_missing",
			Detail:      "RunnerView param is nil — composition root did not pass pipelineRunner into bootstrap.Run",
			StartedAt:   failAt,
			CompletedAt: failAt,
			DurMs:       failAt.Sub(failAt).Milliseconds(),
		})
	} else {
		appender(runEngineSelfRender(ctx, resolved, runner.RenderClient()))
	}

	report.CompletedAt = time.Now().UTC()
	report.DurMs = report.CompletedAt.Sub(report.StartedAt).Milliseconds()

	// Verdict is the greppable top-level signal dashboards alert on.
	if report.HasFailure() {
		report.Verdict = "FAIL"
		okGate.Store(false)
		// Build a non-nil error that includes the FIRST failing code
		// so a grep for "bootstrap:" in stderr surfaces immediately.
		for _, s := range report.Steps {
			if s.Status == "FAIL" {
				return report, fmt.Errorf("bootstrap: %s/%s (%s)", s.Name, s.Code, s.Detail)
			}
		}
		return report, fmt.Errorf("bootstrap: failed (no per-step code recorded)")
	}

	report.Verdict = "OK"
	okGate.Store(true)
	return report, nil
}

// DumpReport writes the JSON of r to w (typically os.Stderr) so a
// failing-boot os.Exit(1) can leave a parseable trail for ops. The
// function is separate from Run() so callers can decide whether to
// dump a silent boot, a verbose one, or only the failure form.
func DumpReport(r *Report) error {
	if r == nil {
		return nil
	}
	buf, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		// Fall back to a hand-rolled outline so the operator still
		// sees verdict+steps even if Marshalling trips on an exotic
		// field.
		fmt.Fprintf(os.Stderr, "[BOOTSTRAP_REPORT_FALLBACK] verdict=%s dur_ms=%d steps=%d\n",
			r.Verdict, r.DurMs, len(r.Steps))
		for _, s := range r.Steps {
			fmt.Fprintf(os.Stderr, "  step=%s status=%s code=%s dur_ms=%d\n",
				s.Name, s.Status, s.Code, s.DurMs)
		}
		return err
	}
	fmt.Fprintln(os.Stderr, "[BOOTSTRAP_REPORT]")
	_, _ = os.Stderr.Write(buf)
	fmt.Fprintln(os.Stderr)
	return nil
}

// ForceOkForTest is a TEST-ONLY helper that flips the package-level gate
// to true WITHOUT having to drive the full Run() orchestrator. It exists
// so pre-existing worker unit tests in internal/worker that exercise
// w.Start() can opt in without rewriting every test to call bootstrap.Run
// against a fake build pipeline.
//
// Production code MUST NOT call this — the gate is fail-closed for a
// reason: a worker that boots without bootstrap proves nothing about
// its engine, ffmpeg, output dir, or bundle identity.
func ForceOkForTest() {
	okGate.Store(true)
}
