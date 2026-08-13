package grpcserver

import (
	"context"
	"encoding/json"
	"time"

	"velox-server/internal/logging"
)

func logArtifactProtocol(event string, startedAt time.Time, fields map[string]interface{}) {
	entry := make(map[string]interface{}, len(fields)+9)
	for key, value := range fields {
		entry[key] = value
	}
	entry["event"] = event
	for _, key := range []string{"job_id", "task_id", "attempt_id", "lease_id", "commit_id", "artifact_id", "upload_id"} {
		if _, ok := entry[key]; !ok {
			entry[key] = ""
		}
	}
	entry["elapsed_ms"] = time.Since(startedAt).Milliseconds()
	payload, err := json.Marshal(entry)
	if err != nil {
		logGRPCf(context.Background(), logging.LevelWarn, logging.CodeGRPCArtifactProtocolLog, "ARTIFACT_PROTOCOL event=%s log_marshal_error=%v", event, err)
		return
	}
	logGRPCf(context.Background(), logging.LevelInfo, logging.CodeGRPCArtifactProtocolLog, "ARTIFACT_PROTOCOL %s", payload)
}
