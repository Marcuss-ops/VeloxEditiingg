// Package taskrunner / upload_lifecycle.go
//
// Upload phase — publishes executor outputs.
//
// PR-3.3 minimum: identity publication of outputs into the report; real
// upload arrives when PersistentLocalArtifactCache (PR-3.7) lands.
//
// Today the runner only records the phase marker; the executor's outputs
// are already in the report (success path assigns result.Outputs
// directly into report.Outputs in runner.go).
package taskrunner

import (
	"velox-worker-agent/internal/executor"
)

// runUpload publishes executor outputs. PR-3.3 minimum: identity
// publication of outputs into the report; real upload arrives when
// PersistentLocalArtifactCache (PR-3.7) lands.
func (r *TaskRunner) runUpload(rc *runnerContext, result executor.ExecutionResult, appendPhase func(PhaseMarker)) error {
	start := r.now()
	if len(result.Outputs) == 0 {
		appendPhase(PhaseMarker{Name: PhaseUpload, StartedAt: start, CompletedAt: r.now(), Status: "ok", Notes: "skipped: no outputs"})
		return nil
	}
	// PR-3.7 will publish each output through the ArtifactAccess backend.
	// Today we only record the phase marker.
	appendPhase(PhaseMarker{Name: PhaseUpload, StartedAt: start, CompletedAt: r.now(), Status: "ok", Notes: "stub: PR-3.7 wires real upload"})
	return nil
}
