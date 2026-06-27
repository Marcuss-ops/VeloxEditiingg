// Package worker — RecoveryReport protocol.
//
// On every restart, the worker reads its persisted JSON state and notifies
// the master via a one-shot enriched heartbeat carrying the heartbeat.extra
// key "recovery_report_v1". The master inspects this on its handleHeartbeat
// path and responds with a ConfigurationUpdate carrying a
// "recovery_action_v1" directive.
//
// The protocol relies ENTIRELY on existing fields (Heartbeat.extra and
// ConfigurationUpdate.configuration, both `google.protobuf.Struct`), so no
// .proto regeneration is required. Both sides upgrade together.
//
// Wire format (worker → master via Heartbeat.extra["recovery_report_v1"]):
//
//	{
//	  "schema_version":       "v1",
//	  "saved_at":             "<RFC3339>",
//	  "seen_commands_count":  N,
//	  "active_jobs_count":    N,
//	  "pending_leases_count": N,
//	  "active_jobs":          [{job_id, job_run_id, job_type, lease_id, started_at}, ...],
//	  "pending_lease_jobs":   [{job_id, job_run_id, job_type, lease_id}, ...]
//	}
//
// Wire format (master → worker via ConfigurationUpdate.configuration["recovery_action_v1"]):
//
//	{
//	  "action":      "CONTINUE" | "CANCEL" | "RESUME_UPLOAD" | "CLEANUP",
//	  "job_actions": {job_id: action, ...}   // optional, scope = per-job
//	}
//
// Worker applies the per-job actions; if only `action` (global) is set, it
// is treated as a hint and logged but no specific action is taken unless
// the worker has open jobs. Initial policy on the master side defaults to
// CONTINUE for all jobs (which matches the lease-expiry fallback behavior
// that existed before this protocol).
package worker

import (
	"context"
	"encoding/json"
	"os"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/types/known/structpb"
)

// RecoveryReportKey is the heartbeat.extra key carrying prior-run state.
const RecoveryReportKey = "recovery_report_v1"

// RecoveryActionKey is the ConfigurationUpdate.configuration key carrying
// the master's directive back to the worker.
const RecoveryActionKey = "recovery_action_v1"

// RecoveryAction* — vocabulary for the master's per-job directive.
// The vocabulary is intentionally small so worker tests pin the surface.
const (
	RecoveryActionContinue     = "CONTINUE"
	RecoveryActionCancel       = "CANCEL"
	RecoveryActionResumeUpload = "RESUME_UPLOAD"
	RecoveryActionCleanup      = "CLEANUP"
)

// BuildRecoveryReport converts the on-disk persistedState into a structpb.Struct
// suitable for embedding in Heartbeat.extra. Returns (struct, activeCount, true)
// when there is signal to report. Returns (nil, 0, false) on empty/nil state.
//
// The "active_jobs" and "pending_lease_jobs" arrays carry the full per-job
// metadata so the master can match them against its own lease table without
// the worker having to allocate from its in-memory state.
// BuildRecoveryReport constructs the heartbeat.extra Struct and returns
// (struct, activeCount, true) when state has signal. Returns (nil, 0, false)
// when state is empty or nil.
//
// We build the *structpb.Struct FIELD-BY-FIELD using structpb.NewXxx helpers
// instead of structpb.NewStruct(reflect.Slice(...)) to avoid rare concrete-slice
// reflection regressions in the protobuf-go runtime (some older versions do
// not auto-convert []map[string]interface{} into a ListValue of StructValues).
func BuildRecoveryReport(state *persistedState) (*structpb.Struct, int, bool) {
	if state == nil {
		return nil, 0, false
	}
	// Empty-state contract: only JOB-level signal (ActiveJobs or PendingLeaseJobs)
	// triggers a recovery report. SeenCommands are restored automatically on
	// loadLocalState and do not require master coordination.
	if len(state.ActiveJobs) == 0 && len(state.PendingLeaseJobs) == 0 {
		return nil, 0, false
	}

	out := &structpb.Struct{Fields: make(map[string]*structpb.Value, 7)}

	out.Fields["schema_version"] = structpb.NewStringValue("v1")

	savedAt := ""
	if !state.SavedAt.IsZero() {
		savedAt = state.SavedAt.UTC().Format(time.RFC3339)
	}
	if savedAt != "" {
		out.Fields["saved_at"] = structpb.NewStringValue(savedAt)
	}

	out.Fields["seen_commands_count"] = structpb.NewNumberValue(float64(len(state.SeenCommands)))
	out.Fields["active_jobs_count"] = structpb.NewNumberValue(float64(len(state.ActiveJobs)))
	out.Fields["pending_leases_count"] = structpb.NewNumberValue(float64(len(state.PendingLeaseJobs)))

	// active_jobs: ListValue of StructValues
	activeVals := make([]*structpb.Value, 0, len(state.ActiveJobs))
	for _, aj := range state.ActiveJobs {
		jmap := map[string]interface{}{
			"job_id":     aj.JobID,
			"job_run_id": aj.JobRunID,
			"job_type":   aj.JobType,
			"lease_id":   aj.LeaseID,
		}
		if aj.StartedAt != "" {
			jmap["started_at"] = aj.StartedAt
		}
		js, err := structpb.NewStruct(jmap)
		if err != nil {
			return nil, 0, false
		}
		activeVals = append(activeVals, structpb.NewStructValue(js))
	}
	out.Fields["active_jobs"] = structpb.NewListValue(&structpb.ListValue{Values: activeVals})

	// pending_lease_jobs: ListValue of StructValues (started_at omitted for pending)
	pendingVals := make([]*structpb.Value, 0, len(state.PendingLeaseJobs))
	for _, aj := range state.PendingLeaseJobs {
		jmap := map[string]interface{}{
			"job_id":     aj.JobID,
			"job_run_id": aj.JobRunID,
			"job_type":   aj.JobType,
			"lease_id":   aj.LeaseID,
		}
		js, err := structpb.NewStruct(jmap)
		if err != nil {
			return nil, 0, false
		}
		pendingVals = append(pendingVals, structpb.NewStructValue(js))
	}
	out.Fields["pending_lease_jobs"] = structpb.NewListValue(&structpb.ListValue{Values: pendingVals})

	return out, len(state.ActiveJobs), true
}

// ReadPersistedState re-reads worker_state.json from disk. Unlike
// loadLocalState on Worker, this helper does NOT mutate the live in-memory
// state — it's a side-effect-free read used by the recovery report path
// so that report generation cannot interfere with deduplication or job maps.
func ReadPersistedState(workDir string) (*persistedState, error) {
	if workDir == "" {
		return nil, os.ErrInvalid
	}
	data, err := os.ReadFile(stateFilePath(workDir))
	if err != nil {
		return nil, err
	}
	var s persistedState
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, err
	}
	return &s, nil
}

// maybeSendRecoveryReport is called by worker.go after HelloAck arrives.
// It sends exactly ONE enriched heartbeat carrying the recovery payload,
// then returns. Subsequent heartbeats from heartbeatLoop carry no recovery
// payload (the Struct field is nil for the regular interval).
//
// Idempotent and safe to skip: if the worker_state.json file is missing or
// empty, the function returns without sending. The master's lease-expiry
// fallback continues to handle any dangling leases — RecoveryReport is an
// OPT-IN optimization, not a replacement.
func (w *Worker) maybeSendRecoveryReport(ctx context.Context) {
	state, err := ReadPersistedState(w.config.WorkDir)
	if err != nil {
		// No state file is the expected first-run case. Don't log at WARN
		// level to avoid noise on every fresh worker boot.
		if !os.IsNotExist(err) {
			w.logger.Warn("[RECOVERY] Failed to read persisted state for report: %v", err)
		}
		return
	}
	report, activeCount, ok := BuildRecoveryReport(state)
	if !ok || report == nil {
		return
	}
	// Wrap the recovery Struct under the canonical key.
	extra := &structpb.Struct{Fields: map[string]*structpb.Value{
		RecoveryReportKey: structpb.NewStructValue(report),
	}}
	// Use TypedPayload with the typed *pb.Heartbeat so Heartbeat.Extra
	// propagates through the transport — the untyped payload path
	// does not always bind nested *structpb.Struct into Heartbeat.Extra.
	hbTyped := &pb.Heartbeat{
		WorkerName:      w.config.WorkerID,
		ActiveJobsCount: int32(activeCount),
		Extra:           extra,
	}
	msg := controltransport.ControlMessage{
		Type:            controltransport.MsgHeartbeat,
		WorkerID:        w.config.WorkerID,
		ProtocolVersion: w.config.ProtocolVersion,
		TypedPayload:    hbTyped,
	}
	if err := w.transport.Send(ctx, msg); err != nil {
		w.logger.Warn("[RECOVERY] Failed to send RecoveryReport: %v", err)
		return
	}
	w.logger.Info("[RECOVERY] RecoveryReport sent: %d active jobs, %d pending leases (saved_at=%s)",
		len(state.ActiveJobs), len(state.PendingLeaseJobs),
		state.SavedAt.UTC().Format(time.RFC3339))
}

// handleRecoveryDirective inspects a ConfigurationUpdate carrying the
// recovery_action_v1 vocabulary and applies per-job actions. Called by
// worker.go's ConfigurationUpdate case via single-line hook.
//
// Vocabulary is stable so future master-side policy changes do not require
// worker updates. The worker treats unknown actions as warnings and never
// crashes.
func (w *Worker) handleRecoveryDirective(cfgUpdate *structpb.Struct) {
	if cfgUpdate == nil {
		return
	}
	raw, ok := cfgUpdate.AsMap()[RecoveryActionKey]
	if !ok {
		return
	}
	recMap, ok := raw.(map[string]interface{})
	if !ok {
		w.logger.Warn("[RECOVERY] Directive payload not a map: %T", raw)
		return
	}
	action, _ := recMap["action"].(string)
	if action != "" {
		w.logger.Info("[RECOVERY] Global directive received: action=%s", action)
	}
	if jobActions, ok := recMap["job_actions"].(map[string]interface{}); ok {
		for jobID, rawAct := range jobActions {
			actStr, _ := rawAct.(string)
			switch actStr {
			case RecoveryActionCancel:
				// cancelJob handles activeTasks + taskIDsByJob cleanup.
				w.cancelJob(jobID)
				w.logger.Info("[RECOVERY] Cancelled job %s", jobID)
			case RecoveryActionContinue:
				w.logger.Info("[RECOVERY] Continue directive for job %s (lease re-acquisition by master)", jobID)
			case RecoveryActionResumeUpload:
				// Future: stageExecutor exposes a Resume(jobID) hook. Today we
				// log and let the master re-issue the JobOffer if state allows.
				w.logger.Info("[RECOVERY] RESUME_UPLOAD directive for job %s (no-op stub)", jobID)
			case RecoveryActionCleanup:
				// cancelJob handles activeTasks + taskIDsByJob cleanup.
				w.cancelJob(jobID)
				w.logger.Info("[RECOVERY] CLEANUP performed for job %s", jobID)
			default:
				w.logger.Warn("[RECOVERY] Unknown per-job directive %q for job %s", actStr, jobID)
			}
		}
	}
}
