package worker

// heartbeat_payload.go owns the construction and serialization of a single
// Heartbeat proto message. It deliberately does NOT manage a ticker and
// does NOT implement any retry/backoff logic — those concerns live in
// heartbeat_loop.go (orchestration) and heartbeat_intervals.go (intervals).

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/telemetry"

	"google.golang.org/protobuf/types/known/structpb"
)

// sendHeartbeat sends a single heartbeat to the master via transport.Send().
// Capabilities are derived from Worker.capabilitiesMap() —
// the same single helper buildHello uses. Any wire-shape change
// touches ONE function; hello and heartbeat stay in lock-step.
func (w *Worker) sendHeartbeat(ctx context.Context) error {
	status := w.Status()

	// Build typed Heartbeat proto directly instead of map payload.
	hb := &pb.Heartbeat{
		WorkerName:      w.config.WorkerName,
		WorkerStatus:    string(status),
		Status:          string(status),
		CodeVersion:     w.version,
		BundleVersion:   w.config.BundleVersion,
		BundleHash:      w.config.BundleHash,
		ProtocolVersion: w.config.ProtocolVersion,
		EngineVersion:   w.config.EngineVersion,
		JobsCompleted:   w.tasksCompleted.Load(),
		JobsFailed:      w.tasksFailed.Load(),
	}

	// Collect dynamic extra fields (recent_logs, capabilities, active_jobs,
	// resources, current_job) into Heartbeat.Extra as structpb.Struct.
	extraMap := make(map[string]interface{})

	readySnap := telemetry.GlobalReady().Snapshot()
	readyReasons := make([]interface{}, 0, len(readySnap.NotReadyReasons()))
	for _, reason := range readySnap.NotReadyReasons() {
		readyReasons = append(readyReasons, reason)
	}
	extraMap["readiness"] = map[string]interface{}{
		"status":  map[bool]string{true: "ok", false: "not_ready"}[readySnap.IsReady()],
		"reasons": readyReasons,
		"detail":  readySnap.DetailMap(),
	}

	recentLogs, recentErrors := w.recentLogs.Snapshot(300, 100)
	if len(recentLogs) > 0 {
		extraMap["recent_logs"] = recentLogs
		extraMap["recent_logs_count"] = len(recentLogs)
	}
	if len(recentErrors) > 0 {
		extraMap["recent_errors"] = recentErrors
		extraMap["recent_errors_count"] = len(recentErrors)
	}

	// Attach typed WorkerResourceCounters.
	if w.sampler != nil {
		if snap := w.sampler.Latest(); snap != nil {
			if m := snap.ToWireMap(); m != nil {
				extraMap["resources"] = m
			}
		}
	}

	hostname := ""
	if h, err := os.Hostname(); err == nil {
		hostname = h
	}
	extraMap["capabilities"] = w.capabilitiesMap(hostname)
	extraMap["worker_id"] = w.config.WorkerID

	w.activeTasksMu.RLock()
	activeJobList := make([]map[string]interface{}, 0, len(w.activeTasks))
	var primaryJobID string
	for _, at := range w.activeTasks {
		if at == nil || at.Task == nil {
			continue
		}
		if primaryJobID == "" {
			primaryJobID = at.JobID
		}
		jobInfo := map[string]interface{}{
			"job_id":      at.JobID,
			"task_id":     at.TaskID,
			"attempt_id":  at.AttemptID,
			"job_run_id":  "",
			"job_type":    at.Task.ExecutorID,
			"executor_id": at.Task.ExecutorID,
			"priority":    0,
			"lease_id":    at.LeaseID,
			"attempt":     at.Task.AttemptNumber,
			"status":      "RUNNING",
			"started_at":  at.StartedAt.UTC().Format(time.RFC3339Nano),
		}
		if at.Progress.Percent > 0 {
			jobInfo["progress_percent"] = at.Progress.Percent
			jobInfo["progress_scene"] = at.Progress.Scene
			jobInfo["progress_total"] = at.Progress.TotalScenes
			if at.Progress.Stage != "" {
				jobInfo["progress_stage"] = at.Progress.Stage
			}
		}
		activeJobList = append(activeJobList, jobInfo)
	}
	w.activeTasksMu.RUnlock()

	// Send the complete current list, including an explicit empty list when
	// the worker becomes idle. Omitting the key would make the master retain
	// the previous active_jobs projection indefinitely.
	extraMap["active_jobs"] = activeJobList
	extraMap["active_tasks"] = len(activeJobList)
	if w.concurrencyLimiter != nil {
		extraMap["task_slots"] = w.concurrencyLimiter.MaxActiveJobs()
	}
	hb.CurrentJob = primaryJobID
	hb.ActiveJobsCount = int32(len(activeJobList))

	// Serialize extra map to structpb.Struct.
	if len(extraMap) > 0 {
		// Structpb accepts JSON-shaped values only.  Several heartbeat
		// fields are naturally built as []string or typed maps; passing
		// those directly makes NewStruct fail and silently drops the whole
		// Extra payload (including active_jobs and resources).  Normalize
		// through JSON so the complete heartbeat survives protobuf encoding.
		if normalized, err := jsonCompatibleMap(extraMap); err == nil {
			if extra, err := structpb.NewStruct(normalized); err == nil {
				hb.Extra = extra
			}
		} else {
			w.logger.Warn("[HEARTBEAT] extra payload omitted: %v", err)
		}
	}

	msg := controltransport.NewTypedMessage(
		controltransport.MsgHeartbeat,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		hb,
	)

	if err := w.transport.Send(ctx, msg); err != nil {
		return err
	}
	return nil
}

// jsonCompatibleMap normalises a Go map with mixed value types into one that
// structpb.NewStruct can consume. Implemented as a JSON round-trip and shared
// exclusively by sendHeartbeat (in this file).
func jsonCompatibleMap(input map[string]interface{}) (map[string]interface{}, error) {
	encoded, err := json.Marshal(input)
	if err != nil {
		return nil, err
	}
	var normalized map[string]interface{}
	if err := json.Unmarshal(encoded, &normalized); err != nil {
		return nil, err
	}
	return normalized, nil
}
