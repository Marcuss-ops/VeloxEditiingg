// Package grpcserver — RecoveryReport protocol master side.
//
// The master inspects every Heartbeat envelope for an `extra` map containing
// the "recovery_report_v1" key. When present, it emits a ConfigurationUpdate
// via safeSend carrying the "recovery_action_v1" directive, which the
// worker's receiveLoop already routes through its existing ConfigUpdate case
// (the single new entry point is in worker.go's MsgConfigurationUpdate branch).
//
// Wire shape (analyzed in worker_recovery.go):
//
//   Heartbeat.extra["recovery_report_v1"]: optional Struct describing state
//   ConfigurationUpdate.configuration["recovery_action_v1"]: Struct with action.
//
// No .proto changes: both are typed Struct fields already present in the
// shipped schema. The constants below are the canonical keys.

package grpcserver

import (
	"fmt"
	"log"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// RecoveryReportKey is the heartbeat.extra key that triggers recovery mode
// on the master. Must match worker_recovery.go RecoveryReportKey.
const RecoveryReportKey = "recovery_report_v1"

// RecoveryActionKey is the ConfigurationUpdate.configuration key carrying
// the directive back to the worker. Must match worker_recovery.go
// RecoveryActionKey.
const RecoveryActionKey = "recovery_action_v1"

// Action vocabulary — must match worker_recovery.go constants exactly.
// Kept in sync via cross-package constant pinning in test_helpers.go
// (see TestRecoveryVocabularySync in handler_recovery_test.go).
const (
	RecoveryActionContinue     = "CONTINUE"
	RecoveryActionCancel       = "CANCEL"
	RecoveryActionResumeUpload = "RESUME_UPLOAD"
	RecoveryActionCleanup      = "CLEANUP"
)

// handleRecoveryReport inspects hb.extra for the RecoveryReport payload and,
// when present, queues a ConfigurationUpdate with the chosen directive via
// safeSend on the session writer channel. Returns true if a directive was
// queued, false otherwise.
//
// Initial policy: every job gets CONTINUE. This matches the historical
// lease-expiry fallback behavior — the master continues to be the source of
// truth for what resumes — and gives us a safe baseline while future
// revisions add per-job policy (e.g., CLEANUP for jobs whose persist is
// older than the lease TTL, RESUME_UPLOAD for partial upload sessions).
func (h *Handler) handleRecoveryReport(workerID string, sess *workerSession, hb *pb.Heartbeat) bool {
	if sess == nil || hb == nil || hb.GetExtra() == nil {
		return false
	}
	raw, present := hb.GetExtra().AsMap()[RecoveryReportKey]
	if !present {
		return false
	}
	// Be permissive: protobuf structs nested inside a Struct field can
	// arrive as either a flattened `map[string]interface{}` (the typical
	// AsMap() path) OR as a `*structpb.Struct` (when produced via
	// structpb.NewStruct(...{RecoveryReportKey: recStruct})). Both are
	// legitimate — handle both before falling back to debug logging.
	var recMap map[string]interface{}
	switch v := raw.(type) {
	case map[string]interface{}:
		recMap = v
	case *structpb.Struct:
		recMap = v.AsMap()
	default:
		log.Printf("[GRPC][RECOVERY] worker=%s recovery_report_v1 is %T, expected map or *structpb.Struct", workerID, raw)
		return false
	}

	activeCount, _ := recMap["active_jobs_count"].(float64)
	pendingCount, _ := recMap["pending_leases_count"].(float64)
	seenCount, _ := recMap["seen_commands_count"].(float64)
	savedAt, _ := recMap["saved_at"].(string)
	log.Printf("[GRPC][RECOVERY] worker=%s report=v1 active=%.0f pending=%.0f seen=%.0f saved_at=%s",
		workerID, activeCount, pendingCount, seenCount, savedAt)

	// Build directive: CONTINUE globally + per-job map mirroring active_jobs
	// so the worker gets the full per-job shape for future expansions.
	directive := map[string]interface{}{
		"action": RecoveryActionContinue,
	}
	if jobs, ok := recMap["active_jobs"].([]interface{}); ok {
		perJob := make(map[string]interface{}, len(jobs))
		for _, j := range jobs {
			jobMap, ok := j.(map[string]interface{})
			if !ok {
				continue
			}
			jobID, _ := jobMap["job_id"].(string)
			if jobID == "" {
				continue
			}
			perJob[jobID] = RecoveryActionContinue
		}
		if len(perJob) > 0 {
			directive["job_actions"] = perJob
		}
	}

	payload, err := structpb.NewStruct(map[string]interface{}{
		RecoveryActionKey: directive,
	})
	if err != nil {
		log.Printf("[GRPC][RECOVERY] worker=%s NewStruct failed: %v", workerID, err)
		return false
	}

	env := &pb.MasterToWorkerEnvelope{
		MessageId:       fmt.Sprintf("recovery-action-%s-%d", workerID, time.Now().UnixNano()),
		WorkerId:        workerID,
		SessionId:       sess.sessionID,
		SentAt:          timestamppb.Now(),
		ProtocolVersion: controltransport.ProtocolVersionCurrent,
		Msg: &pb.MasterToWorkerEnvelope_ConfigurationUpdate{
			ConfigurationUpdate: &pb.ConfigurationUpdate{Configuration: payload},
		},
	}
	if !safeSend(sess.sendCh, &outboundMessage{Envelope: env}) {
		log.Printf("[GRPC][RECOVERY] worker=%s sendCh full/closed — directive dropped", workerID)
		return false
	}
	log.Printf("[GRPC][RECOVERY] worker=%s directive queued action=%s", workerID, RecoveryActionContinue)
	return true
}
