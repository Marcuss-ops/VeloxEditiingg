// Package taskrunner / runner_report.go
//
// Report finalization for TaskRunner: completeError (failure paths) and
// the detailed-phase drain helpers (attachDetailedPhases /
// AppendDetailedPhases). The Run orchestrator itself stays in
// runner.go.
package taskrunner

import (
	"strconv"
	"strings"

	"velox-worker-agent/internal/telemetry"
)

// completeError finalizes the report under the given code and detail,
// then runs the report phase to keep the 5-phase invariant intact.
// PR-3.7: failure paths also surface cache + blob counters so operators
// see real hit/miss/eviction activity on failed-task reports rather
// than a misleading zero-map.
func (r *TaskRunner) completeError(rec *telemetry.EventRecorder, report *TaskExecutionReport, appendPhase func(PhaseMarker), code, detail string) TaskExecutionReport {
	report.Status = "failed"
	report.ErrorCode = code
	report.ErrorDetail = detail
	report.CompletedAt = r.now()
	// Record the failure as the terminal worker-origin event so the
	// detailed phase stream shows how the attempt ended, even when the
	// failure happened before any canonical phase ran.
	if rec != nil {
		now := r.now()
		rec.Record(telemetry.EventSpec{
			Origin:    telemetry.OriginWorker,
			Scope:     telemetry.ScopeAttempt,
			Component: "runner",
			Action:    "run",
		}, now, now, 0, telemetry.StatusFailed, code, detail)
	}
	// Always have at least one marker (the report phase) so consumers
	// that check `len(phaseMarkers) == 0` can rely on truth: failure
	// means a phase WAS run.
	appendPhase(PhaseMarker{Name: PhaseReport, StartedAt: r.now(), CompletedAt: r.now(), Status: "ok", Notes: "failure recorded"})
	// Preserve the typed mirror on failure as well; native phases often
	// explain why the executor failed and must remain wire-visible.
	if report.Metrics == nil {
		report.Metrics = make(map[string]interface{})
	}
	r.mergeStatsInto(report, report.Metrics)
	r.attachDetailedPhases(rec, report)
	return *report
}

// attachDetailedPhases drains the recorder onto the report as the
// ordered DetailedPhases list, numbering events 1..N across the whole
// attempt. Identity fields come from the report's canonical executor
// tuple; lease/snapshot identity is stamped at the submit boundary and
// overridden by the master at ingest.
func (r *TaskRunner) attachDetailedPhases(rec *telemetry.EventRecorder, report *TaskExecutionReport) {
	if rec == nil {
		return
	}
	execID := report.ExecutorID
	execVersion := int32(0)
	if key := report.ExecutorKey; key != "" {
		if i := strings.LastIndexByte(key, '@'); i >= 0 {
			if v, err := strconv.Atoi(key[i+1:]); err == nil {
				execVersion = int32(v)
			}
		}
	}
	phases := rec.Snapshot()
	report.AttemptRecorderOffset = len(phases)
	if len(phases) == 0 {
		return
	}
	// Preserve detailed native phases already attached from the executor;
	// worker lifecycle events are appended in recorder order. Both sources
	// retain their canonical per-origin event_index values.
	start := len(report.DetailedPhases) + 1
	for i, p := range phases {
		report.DetailedPhases = append(report.DetailedPhases, fromRecordedPhase(p, start+i, execID, execVersion, ""))
	}
}

// AppendDetailedPhases drains events recorded after TaskRunner.Run returned
// (for example the worker's output upload and commit lifecycle) onto the
// existing report. Run uses Snapshot so the attempt recorder remains alive
// until the outer worker boundary has completed the entire attempt.
func AppendDetailedPhases(report *TaskExecutionReport, rec *telemetry.EventRecorder) {
	if report == nil || rec == nil {
		return
	}
	phases := rec.DrainFrom(report.AttemptRecorderOffset)
	if len(phases) == 0 {
		return
	}
	report.AttemptRecorderOffset += len(phases)
	execID := report.ExecutorID
	execVersion := int32(0)
	if key := report.ExecutorKey; key != "" {
		if i := strings.LastIndexByte(key, '@'); i >= 0 {
			if v, err := strconv.Atoi(key[i+1:]); err == nil {
				execVersion = int32(v)
			}
		}
	}
	start := len(report.DetailedPhases) + 1
	for i, p := range phases {
		report.DetailedPhases = append(report.DetailedPhases, fromRecordedPhase(p, start+i, execID, execVersion, ""))
	}
}
