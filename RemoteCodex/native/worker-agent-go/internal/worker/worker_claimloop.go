package worker

// worker_claimloop.go: receive-loop orchestrator and dispatch
// helpers for inbound control messages. The receiveLoop switch is
// the single canonical entry point for master→worker control
// messages (MsgTaskOffer → MsgTaskLeaseGranted → MsgCommand →
// MsgCancelJob → MsgDrain → MsgConfigurationUpdate → MsgLeaseRevoked
// → MsgPing → MsgHelloAck → MsgArtifactUploadPlan / MsgTaskCommitAck).
// Per-message-case extract would split this further — flagged as
// stage-5+ housekeeping, not part of the 4-stage split.
//
// Lifecycle (Start / Stop / runSession), registration metadata
// (buildHello / capabilitiesMap / hostInfo), and the typed
// pending-artifact-ack registry (register/wait/unregister/drain/
// extractTaskIDFromTyped) live in their respective sibling files.
//
// Extracted from worker.go (commit f50f873 → next).

import (
	"context"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api"
)

// receiveLoop processes incoming messages from the transport receive channel.
// Routes MsgTaskOffer to executeTask and MsgCommand to processCommand.
func (w *Worker) receiveLoop(ctx context.Context, recvCh <-chan controltransport.ControlMessage) {
	defer w.wg.Done()

	w.logger.Info("[RECEIVE] Receive loop started — waiting for messages from master")

	for {
		select {
		case <-ctx.Done():
			w.logger.Info("[RECEIVE] Receive loop exiting (context done)")
			return
		case <-w.stopChan:
			w.logger.Info("[RECEIVE] Receive loop exiting (stop signal)")
			return
		case msg, ok := <-recvCh:
			if !ok {
				w.logger.Warn("[RECEIVE] Receive channel closed — transport disconnected")
				return
			}

			switch msg.Type {
			/* PR-protobuf-refactor: legacy JobOffer / JobLeaseGranted / pb.JobOffer
			   removed — superseded by MsgTaskOffer + MsgTaskLeaseGranted + pb.TaskOffer.
			   The old protobuf types no longer exist in the oneof. See grpc_stream.go
			   for the transport-side removal. */
			case controltransport.MsgTaskOffer:
				// PR #5: task-native dispatch — receive pre-compiled TaskSpec from master.
				// PR-2 (canonical-attempt-identity): executeTask dispatch is DEFERRED
				// to MsgTaskLeaseGranted so the master's canonical (attempt_id,
				// attempt_number) tuple + RUNNING TaskAttempt is committed before
				// execution starts. Mirrors the legacy JobOffer → JobLeaseGranted
				// Mirrors the legacy pattern using `pendingTasks` (keyed by
				// task_id) instead of the removed, jobID-keyed legacy map.
				taskOffer, ok := msg.TypedPayload.(*pb.TaskOffer)
				if !ok || taskOffer == nil {
					w.logger.Warn("[RECEIVE] TaskOffer without typed payload")
					continue
				}

				taskID := taskOffer.GetTaskId()
				attemptID := taskOffer.GetAttemptId()
				jobID := taskOffer.GetJobId()
				leaseID := taskOffer.GetLeaseId()
				attemptNumber := taskOffer.GetAttemptNumber()
				revision := taskOffer.GetRevision()
				if taskID == "" || jobID == "" || attemptID == "" || leaseID == "" || attemptNumber <= 0 {
					w.logger.Warn("[RECEIVE] TaskOffer refused — incomplete identity tuple (task=%q job=%q attempt=%q lease=%q attempt_num=%d rev=%d) — dropping",
						taskID, jobID, attemptID, leaseID, attemptNumber, revision)
					continue
				}
				if w.IsStopped() || w.drainMode.Load() {
					if err := w.sendTaskReject(ctx, taskID, jobID, attemptID, leaseID, "draining", attemptNumber, revision); err != nil {
						w.logger.Warn("[RECEIVE] Failed to send TaskRejected: %v", err)
					}
					continue
				}

				w.activeTasksMu.RLock()
				activeCount := len(w.activeTasks)
				w.activeTasksMu.RUnlock()
				// PR-bugfix: also count pendingTasks (offers accepted
				// but waiting for TaskLeaseGranted). The worker must not
				// accept more offers than MaxActiveJobs including tasks
				// that will soon become active but haven't yet started.
				w.pendingTasksMu.Lock()
				pendingCount := len(w.pendingTasks)
				w.pendingTasksMu.Unlock()
				if activeCount+pendingCount >= w.config.MaxActiveJobs {
					if err := w.sendTaskReject(ctx, taskID, jobID, attemptID, leaseID, "capacity_full", attemptNumber, revision); err != nil {
						w.logger.Warn("[RECEIVE] Failed to send TaskRejected (capacity): %v", err)
					}
					continue
				}

				executorID := normalizeOfferedExecutorID(taskOffer.GetExecutorId())
				executorVersion := int(taskOffer.GetExecutorVersion())
				if !w.executorRegistry.Has(executorID, executorVersion) {
					if err := w.sendTaskReject(ctx, taskID, jobID, attemptID, leaseID, "unsupported_executor", attemptNumber, revision); err != nil {
						w.logger.Warn("[RECEIVE] Failed to send TaskRejected (unsupported executor): %v", err)
					}
					continue
				}
				w.logger.Info("[RECEIVE] TaskOffer received: task=%s attempt=%s job=%s executor=%s@%d — deferring executeTask to TaskLeaseGranted",
					taskID, attemptID, jobID, executorID, taskOffer.GetExecutorVersion())

				if err := w.sendTaskAccepted(ctx, taskOffer); err != nil {
					w.logger.Warn("[RECEIVE] Failed to send TaskAccepted: %v", err)
					continue
				}

				// Build PendingTaskExecution from TaskOffer for the deferred
				// executeTask path. TaskSpec travels pre-compiled from master.
				var specPayload map[string]interface{}
				if tsp := taskOffer.GetTaskSpec(); tsp != nil {
					specPayload = tsp.AsMap()
				}
				pte := &PendingTaskExecution{
					TaskID:          taskID,
					JobID:           jobID,
					JobRevision:     int(taskOffer.GetJobRevision()),
					AttemptID:       attemptID,
					AttemptNumber:   int(attemptNumber),
					LeaseID:         leaseID,
					ExecutorID:      executorID,
					ExecutorVersion: int(taskOffer.GetExecutorVersion()),
					Revision:        int(revision),
					Spec: executor.TaskSpec{
						Version:    int(taskOffer.GetExecutorVersion()),
						JobID:      jobID,
						ExecutorID: executorID,
						Payload:    specPayload,
					},
				}

				// PR-2: defer dispatch to MsgTaskLeaseGranted via pendingTasks map.
				w.storePendingTask(taskID, pte)

			case controltransport.MsgTaskLeaseGranted:
				// PR-2 (canonical-attempt-identity): executeTask dispatch is
				// gated on TaskLeaseGranted. The master sends this AFTER
				// accepting the worker's TaskAccepted and committing the
				// TaskAttempt PENDING → RUNNING transition. consume the
				// pending task from storePendingTask; if absent (unknown
				// task_id) log + drop, identical to MsgTaskLeaseGranted's
				// unknown-task behavior.
				taskGrant, ok := msg.TypedPayload.(*pb.TaskLeaseGranted)
				if !ok || taskGrant == nil {
					w.logger.Warn("[RECEIVE] TaskLeaseGranted without typed payload")
					continue
				}
				taskID := taskGrant.GetTaskId()
				if taskID == "" {
					w.logger.Warn("[RECEIVE] TaskLeaseGranted without task_id — dropping")
					continue
				}

				// Validate the full identity tuple on the grant.
				grantJobID := taskGrant.GetJobId()
				grantAttemptID := taskGrant.GetAttemptId()
				grantLeaseID := taskGrant.GetLeaseId()
				grantAttemptNumber := taskGrant.GetAttemptNumber()
				grantRevision := taskGrant.GetRevision()

				if grantJobID == "" || grantAttemptID == "" || grantLeaseID == "" || grantAttemptNumber <= 0 {
					w.logger.Warn("[RECEIVE] TaskLeaseGranted for task %s refused — incomplete identity (job=%q attempt=%q lease=%q attempt_num=%d rev=%d)",
						taskID, grantJobID, grantAttemptID, grantLeaseID, grantAttemptNumber, grantRevision)
					continue
				}

				pte := w.takePendingTask(taskID)
				if pte == nil {
					w.logger.Warn("[RECEIVE] TaskLeaseGranted for unknown task %s — dropping", taskID)
					continue
				}

				// Cross-validate the grant identity against the pending task.
				if grantJobID != pte.JobID || grantAttemptID != pte.AttemptID || grantLeaseID != pte.LeaseID || int(grantAttemptNumber) != pte.AttemptNumber {
					w.logger.Warn("[RECEIVE] TaskLeaseGranted for task %s identity mismatch against pending task (grant: job=%q attempt=%q lease=%q num=%d) vs (pending: job=%q attempt=%q lease=%q num=%d) — dropping",
						taskID, grantJobID, grantAttemptID, grantLeaseID, grantAttemptNumber, pte.JobID, pte.AttemptID, pte.LeaseID, pte.AttemptNumber)
					continue
				}
				if grantJobRevision := int(taskGrant.GetJobRevision()); grantJobRevision > 0 {
					pte.JobRevision = grantJobRevision
				}

				// PR-2 followup: register the full identity tuple so
				// leaseRenewLoop fires MsgTaskLeaseRenewal with all fields.
				w.AddActiveTaskLease(taskID, grantJobID, grantAttemptID, grantLeaseID, int(grantAttemptNumber), int(grantRevision))

				w.logger.Info("[RECEIVE] TaskLeaseGranted for task=%s attempt=%s job=%s lease=%s num=%d rev=%d — starting execution",
					taskID, grantAttemptID, grantJobID, grantLeaseID, grantAttemptNumber, grantRevision)
				// Defer RemoveActiveTaskLease so the lease slot is freed on
				// every terminal exit.
				go func() {
					defer w.RemoveActiveTaskLease(taskID)
					w.executeTask(ctx, pte, taskID, grantAttemptID)
				}()

			case controltransport.MsgCommand:
				cmd := msgToCommand(msg)
				w.logger.Info("[RECEIVE] Command received: %s (id: %s)", cmd.Command, cmd.CommandID)
				w.processCommand(ctx, cmd)

			case controltransport.MsgCancelJob:
				cancelJob, ok := msg.TypedPayload.(*pb.CancelJob)
				if ok && cancelJob != nil {
					jobID := cancelJob.GetJobId()
					w.logger.Info("[RECEIVE] CancelJob received for job %s", jobID)
					if jobID != "" {
						w.cancelJob(jobID)
					}
				}

			case controltransport.MsgDrain:
				w.drainMode.Store(true)
				// RW-PROD-004 A4: MarkDrainMode(true) flips the canonical
				// ReadyState immediately so /health/ready starts reporting
				// reasons=[drain_mode] without waiting for the next tick.
				telemetry.MarkDrainMode(true)
				w.logger.Info("[RECEIVE] Drain command received — no new jobs will be accepted")

			case controltransport.MsgConfigurationUpdate:
				w.logger.Info("[RECEIVE] ConfigurationUpdate received")
				configUpdate, ok := msg.TypedPayload.(*pb.ConfigurationUpdate)
				if ok && configUpdate != nil && configUpdate.GetConfiguration() != nil {
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
					if newLogLevel, ok := cfgMap["log_level"].(string); ok && newLogLevel != "" {
						w.config.LogLevel = newLogLevel
						w.logger.Info("[CONFIG] LogLevel updated to %s", newLogLevel)
					}
					ackMsg := controltransport.NewTypedMessage(
						controltransport.MsgCommandAck,
						w.config.WorkerID,
						w.config.ProtocolVersion,
						&pb.CommandAck{
							CommandId: msg.MessageID,
						},
					)
					ackCtx, ackCancel := context.WithTimeout(context.Background(), 30*time.Second)
					ackErr := w.transport.Send(ackCtx, ackMsg)
					ackCancel()
					if ackErr != nil {
						w.logger.Warn("[CONFIG] Failed to ack ConfigurationUpdate: %v", ackErr)
					}
				}

			case controltransport.MsgLeaseRevoked:
				leaseRevoked, ok := msg.TypedPayload.(*pb.LeaseRevoked)
				if ok && leaseRevoked != nil {
					jobID := leaseRevoked.GetJobId()
					w.logger.Warn("[RECEIVE] Lease revoked for job %s: %s",
						jobID, leaseRevoked.GetReason())
					if jobID != "" {
						// cancelJob handles activeTasks + taskIDsByJob cleanup.
						w.cancelJob(jobID)
					}
				}

			case controltransport.MsgPing:
				w.sendHeartbeat(ctx)

			case controltransport.MsgHelloAck:
				w.logger.Debug("[RECEIVE] HelloAck received — session confirmed")

			case controltransport.MsgArtifactUploadPlan, controltransport.MsgTaskCommitAck:
				// Artifact Commit Protocol v1 (Fase 3.4 / 3.6) —
				// typed master→worker reply on the declare/complete
				// pipeline. The receive loop dispatches into a
				// per-task channel map populated by the executor
				// pipeline; the pipeline blocks waiting for the
				// reply and proceeds with the next stage on receipt.
				if !w.dispatchTypedPlanOrAck(msg) {
					w.logger.Warn("[RECEIVE] %s arrived with no pending pipeline (msg=%s) — dropping",
						msg.Type, msg.MessageID)
				}

			default:
				w.logger.Debug("[RECEIVE] Unhandled message type: %s", msg.Type)
			}
		}
	}
}

/* PR-protobuf-refactor: msgToJob + msgToJobFromProto removed — pb.JobOffer
   no longer exists. TaskOffer is now the canonical dispatch path. */

// msgToCommand converts a ControlMessage (MsgCommand) to an api.WorkerCommand using typed proto fields.
func msgToCommand(msg controltransport.ControlMessage) api.WorkerCommand {
	cmd, ok := msg.TypedPayload.(*pb.Command)
	if !ok || cmd == nil {
		return api.WorkerCommand{}
	}

	ts := ""
	if cmd.GetTimestamp() != nil {
		ts = cmd.GetTimestamp().AsTime().UTC().Format(time.RFC3339)
	}

	wc := api.WorkerCommand{
		CommandID: cmd.GetCommandId(),
		Command:   cmd.GetCommand(),
		Timestamp: ts,
	}
	if p := cmd.GetParams(); p != nil {
		wc.Payload = p.AsMap()
	}
	return wc
}

// getIntParam extracts an int value from a parameters map, returning 0 if missing.
func getIntParam(params map[string]interface{}, key string) int {
	switch v := params[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	}
	return 0
}

func (w *Worker) setConnState(state ConnectionState) {
	w.connStateMu.Lock()
	defer w.connStateMu.Unlock()
	oldState := w.connState
	w.connState = state
	w.logger.Debug("[CONN] State transition: %s -> %s", oldState, state)
}

// ConnState returns the current connection state.
func (w *Worker) ConnState() ConnectionState {
	w.connStateMu.RLock()
	defer w.connStateMu.RUnlock()
	return w.connState
}

// newTransport creates a fresh transport instance for a new session attempt.
func (w *Worker) newTransport() controltransport.ControlTransport {
	return w.transportFactory()
}

/* PR-protobuf-refactor: sendAccept + sendReject removed — legacy
   JobAccepted/JobRejected messages no longer have transport encoding.
   Task-native sendTaskAccepted + sendTaskReject are the canonical path. */

// sendTaskAccepted sends a typed TaskAccepted message via the transport.
func (w *Worker) sendTaskAccepted(ctx context.Context, offer *pb.TaskOffer) error {
	acceptMsg := controltransport.NewTypedMessage(
		controltransport.MsgTaskAccepted,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		&pb.TaskAccepted{
			TaskId:        offer.GetTaskId(),
			JobId:         offer.GetJobId(),
			AttemptId:     offer.GetAttemptId(),
			LeaseId:       offer.GetLeaseId(),
			AttemptNumber: offer.GetAttemptNumber(),
			Revision:      offer.GetRevision(),
		},
	)
	return w.transport.Send(ctx, acceptMsg)
}

// sendTaskReject sends a typed TaskRejected message via the transport.
func (w *Worker) sendTaskReject(ctx context.Context, taskID, jobID, attemptID, leaseID, reason string, attemptNumber, revision int32) error {
	rejectMsg := controltransport.NewTypedMessage(
		controltransport.MsgTaskRejected,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		&pb.TaskRejected{
			TaskId:        taskID,
			JobId:         jobID,
			AttemptId:     attemptID,
			LeaseId:       leaseID,
			Reason:        reason,
			AttemptNumber: attemptNumber,
			Revision:      revision,
		},
	)
	return w.transport.Send(ctx, rejectMsg)
}

// storePendingTask records a TaskOffer-accepted task awaiting
// TaskLeaseGranted before executeTask dispatch (PR-2 canonical-attempt-
// identity). Keyed by task_id via pendingTasks / pendingTasksMu.
func (w *Worker) storePendingTask(taskID string, pte *PendingTaskExecution) {
	w.pendingTasksMu.Lock()
	defer w.pendingTasksMu.Unlock()
	if w.IsStopped() {
		return
	}
	w.pendingTasks[taskID] = pte
}

// takePendingTask retrieves and removes a pending task by task_id.
// Returns nil if the task was not found.
func (w *Worker) takePendingTask(taskID string) *PendingTaskExecution {
	w.pendingTasksMu.Lock()
	defer w.pendingTasksMu.Unlock()
	if w.IsStopped() {
		return nil
	}
	pte := w.pendingTasks[taskID]
	delete(w.pendingTasks, taskID)
	return pte
}
