package worker

import (
	"context"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/telemetry"
)

func (w *Worker) handleCommandMessage(ctx context.Context, msg controltransport.ControlMessage) {
	cmd := msgToCommand(msg)
	w.logger.Info("[RECEIVE] Command received: %s (id: %s)", cmd.Command, cmd.CommandID)
	w.processCommand(ctx, cmd)
}

func (w *Worker) handleCancelJobMessage(msg controltransport.ControlMessage) {
	cancelJob, ok := msg.TypedPayload.(*pb.CancelJob)
	if !ok || cancelJob == nil {
		return
	}
	jobID := cancelJob.GetJobId()
	w.logger.Info("[RECEIVE] CancelJob received for job %s", jobID)
	if jobID != "" {
		w.cancelJob(jobID)
	}
}

func (w *Worker) handleDrainMessage() {
	w.drainMode.Store(true)
	// RW-PROD-004 A4: update readiness immediately rather than waiting for
	// the next telemetry tick.
	telemetry.MarkDrainMode(true)
	w.logger.Info("[RECEIVE] Drain command received — no new jobs will be accepted")
}

func (w *Worker) handleConfigurationUpdateMessage(msg controltransport.ControlMessage) {
	w.logger.Info("[RECEIVE] ConfigurationUpdate received")
	configUpdate, ok := msg.TypedPayload.(*pb.ConfigurationUpdate)
	if !ok || configUpdate == nil || configUpdate.GetConfiguration() == nil {
		return
	}

	cfgMap := configUpdate.GetConfiguration().AsMap()
	if newMaxJobs, ok := cfgMap["max_parallel_jobs"]; ok {
		switch v := newMaxJobs.(type) {
		case float64:
			w.config.MaxActiveJobs = int(v)
			w.concurrencyLimiter.SetMaxActiveJobs(int(v))
			w.logger.Info("[CONFIG] MaxActiveJobs updated to %d", int(v))
		case int:
			w.config.MaxActiveJobs = v
			w.concurrencyLimiter.SetMaxActiveJobs(v)
			w.logger.Info("[CONFIG] MaxActiveJobs updated to %d", v)
		}
	}
	if v, ok := cfgMap["render_slots"].(float64); ok && v > 0 {
		w.config.RenderSlots = int(v)
		w.logger.Info("[CONFIG] RenderSlots updated to %d", int(v))
	}
	if v, ok := cfgMap["prefetch_slots"].(float64); ok && v > 0 {
		w.config.PrefetchSlots = int(v)
		w.logger.Info("[CONFIG] PrefetchSlots updated to %d", int(v))
	}
	if v, ok := cfgMap["publisher_slots"].(float64); ok && v > 0 {
		w.config.PublisherSlots = int(v)
		w.logger.Info("[CONFIG] PublisherSlots updated to %d", int(v))
	}
	if newLogLevel, ok := cfgMap["log_level"].(string); ok && newLogLevel != "" {
		w.config.LogLevel = newLogLevel
		w.logger.Info("[CONFIG] LogLevel updated to %s", newLogLevel)
	}

	ackMsg := controltransport.NewTypedMessage(
		controltransport.MsgCommandAck,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		&pb.CommandAck{CommandId: msg.MessageID},
	)
	ackCtx, ackCancel := context.WithTimeout(context.Background(), 30*time.Second)
	ackErr := w.transport.Send(ackCtx, ackMsg)
	ackCancel()
	if ackErr != nil {
		w.logger.Warn("[CONFIG] Failed to ack ConfigurationUpdate: %v", ackErr)
	}
}

func (w *Worker) handleLeaseRevokedMessage(msg controltransport.ControlMessage) {
	leaseRevoked, ok := msg.TypedPayload.(*pb.LeaseRevoked)
	if !ok || leaseRevoked == nil {
		return
	}
	jobID := leaseRevoked.GetJobId()
	w.logger.Warn("[RECEIVE] Lease revoked for job %s: %s", jobID, leaseRevoked.GetReason())
	if jobID != "" {
		w.cancelJob(jobID)
	}
}

func (w *Worker) handleTaskResultAckMessage(msg controltransport.ControlMessage) {
	ack, ok := msg.TypedPayload.(*pb.TaskResultAck)
	if !ok || ack == nil {
		w.logger.Warn("[RECEIVE] TaskResultAck without typed payload")
		return
	}
	w.reporter.HandleAck(ack)
}

func (w *Worker) handleArtifactControlMessage(msg controltransport.ControlMessage) {
	if !w.dispatchTypedPlanOrAck(msg) {
		w.logger.Warn("[RECEIVE] %s arrived with no pending pipeline (msg=%s) — dropping", msg.Type, msg.MessageID)
	}
}
