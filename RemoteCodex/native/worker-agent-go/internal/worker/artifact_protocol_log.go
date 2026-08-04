package worker

import (
	"encoding/json"
	"time"

	"velox-worker-agent/internal/taskrunner"
)

// logArtifactProtocol emits one JSON object per artifact-publication boundary.
// The identity fields are present on every event, even when a phase has not
// produced an artifact/upload/commit identifier yet, so log ingestion can use
// one stable schema while tracing a task across the complete terminal path.
func (w *Worker) logArtifactProtocol(event string, pte *PendingTaskExecution, startedAt time.Time, commitID, artifactID, uploadID string, fields map[string]interface{}) {
	if w == nil || w.logger == nil {
		return
	}
	entry := make(map[string]interface{}, len(fields)+9)
	for key, value := range fields {
		entry[key] = value
	}
	// Reserved identity/timing fields are applied last so optional phase
	// metadata cannot accidentally corrupt the stable log schema.
	entry["event"] = event
	entry["job_id"] = ""
	entry["task_id"] = ""
	entry["attempt_id"] = ""
	entry["lease_id"] = ""
	entry["commit_id"] = commitID
	entry["artifact_id"] = artifactID
	entry["upload_id"] = uploadID
	entry["elapsed_ms"] = time.Since(startedAt).Milliseconds()
	if pte != nil {
		entry["job_id"] = pte.JobID
		entry["task_id"] = pte.TaskID
		entry["attempt_id"] = pte.AttemptID
		entry["lease_id"] = pte.LeaseID
	}
	payload, err := json.Marshal(entry)
	if err != nil {
		w.logger.Error("ARTIFACT_PROTOCOL event=%s log_marshal_error=%v", event, err)
		return
	}
	w.logger.Info("ARTIFACT_PROTOCOL %s", payload)
}

func artifactReportOutputCount(report *taskrunner.TaskExecutionReport) int {
	if report == nil {
		return 0
	}
	return len(report.Outputs)
}
