// Package taskrunner / upload_lifecycle.go
//
// Upload phase — records the hand-off to the worker publication lifecycle.
//
// TaskRunner does not own the transport upload. The worker lifecycle invokes
// uploadTaskOutputs after TaskRunner.Run returns, so this phase must never
// claim that an artifact was uploaded here.
//
// Today the runner only records the phase marker; the executor's outputs
// are already in the report (success path assigns result.Outputs
// directly into report.Outputs in runner.go).
package taskrunner

import (
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
)

// runUpload records the publication hand-off. The actual upload result is
// recorded by worker.uploadTaskOutputs, outside TaskRunner.Run.
func (r *TaskRunner) runUpload(rc *runnerContext, result executor.ExecutionResult, appendPhase func(PhaseMarker), rec *telemetry.EventRecorder) error {
	start := r.now()
	recordUpload := func(status, notes string) {
		end := r.now()
		appendPhase(PhaseMarker{Name: PhaseUpload, StartedAt: start, CompletedAt: end, Status: status, Notes: notes})
		if rec != nil {
			rec.Record(telemetry.EventSpec{
				Origin:    telemetry.OriginWorker,
				Scope:     telemetry.ScopeAttempt,
				Component: "runner",
				Action:    PhaseUpload,
				Phase:     PhaseUpload,
			}, start, end, end.Sub(start).Milliseconds(), status, "", notes)
		}
	}
	if len(result.Outputs) == 0 {
		recordUpload("skipped", "no outputs to publish")
		return nil
	}
	recordUpload("deferred", "publication handled by worker artifact lifecycle")
	return nil
}
