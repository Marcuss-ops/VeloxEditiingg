// Package worker provides the core worker orchestration logic.
// This file serves as the thin orchestrator that coordinates the worker modules.
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/logger"
)

// Start begins the worker's main loop with automatic re-registration on failure.
// Creates a fresh transport instance per session attempt (reconnect P0 fix).
func (w *Worker) Start(ctx context.Context) error {
	logger.LogStartup(w.config.WorkerID, w.version, w.config.MasterURL)
	w.logger.Debug("Work Directory: %s", w.config.WorkDir)

	w.concurrencyLimiter.Start(ctx)
	w.logger.Info("[CONCURRENCY] Started with max_active_jobs=%d", w.config.MaxActiveJobs)

	// Connection state machine with automatic backoff
	backoff := registrationInitialBackoff
	gRPCConnectFailures := 0 // PR12: track gRPC failures for fallback to HTTP polling
	fellBack := false        // PR12: true once we've fallen back to HTTP polling

	for !w.IsStopped() {
		// P0 reconnect fix: create a fresh transport each session attempt.
		// After Close(), transports are not reusable (channels + sync.Once).
		//
		// PR12 fallback: if gRPC push fails N times in a row and
		// fallback_to_polling is enabled, switch to HTTP polling transport
		// for the remainder of this session.
		if !fellBack && gRPCConnectFailures >= maxGRPCConnectFailuresBeforeFallback && w.config.FallbackToPolling {
			fellBack = true
			w.logger.Warn("[FALLBACK] gRPC push failed %d times — switching to HTTP polling", gRPCConnectFailures)
			w.fallbackToHTTP()
		}
		w.transport = w.newTransport()

		w.setConnState(ConnConnecting)
		w.connFailureCount = 0

		hello := w.buildHello()
		if err := w.transport.Connect(ctx, hello); err != nil {
			w.connFailureCount++

			// PR12: only count gRPC failures for fallback; HTTP failures are
			// always connection-level (reset, refused) and should retry.
			if !fellBack && w.isPrimaryGRPC() {
				gRPCConnectFailures++
			}

			w.logger.Warn("[CONNECT] Registration failed (attempt %d): %v", w.connFailureCount, err)
			w.setConnState(ConnDisconnected)
			_ = w.transport.Close()

			// Use short fixed retry for connection-level errors (reset, refused,
			// transport unavailable) — the server may just be restarting.
			// Exponential backoff is reserved for application-level errors
			// (credential mismatch, protocol version, TLS).
			var sleepDuration time.Duration
		if isConnectionLevelError(err) {
			jitter := time.Duration(rand.Float64() * float64(connectionRetryBackoff) * 0.3)
			sleepDuration = connectionRetryBackoff + jitter
			w.logger.Info("[CONNECT] Connection-level error, retrying in %v", sleepDuration.Round(time.Millisecond))
			} else {
				jitter := time.Duration(rand.Float64() * float64(backoff) * 0.25)
				sleepDuration = backoff + jitter
				w.logger.Info("[CONNECT] Backing off for %v before retry", sleepDuration.Round(time.Millisecond))
			}

			select {
			case <-time.After(sleepDuration):
				// Only grow backoff for non-connection errors
				if !isConnectionLevelError(err) {
					backoff = time.Duration(float64(backoff) * registrationBackoffMult)
					if backoff > registrationMaxBackoff {
						backoff = registrationMaxBackoff
					}
				}
				continue
			case <-w.stopChan:
				w.logger.Info("Worker stopping during backoff")
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}

	// Registration succeeded — reset backoff
	backoff = registrationInitialBackoff
	w.setConnState(ConnReady)
	telemetry.SetHealthRegistered(true)
	w.logger.Info("[CONNECT] Registration successful — running session")

	// ── Shadow mode (PR11): open gRPC observation transport ────────────
	// Shadow runs alongside the primary transport. It receives JobOffer
	// messages but NEVER sends JobAccepted — purely compares timing.
	// The shadow goroutine is managed within runSession so it shares
	// the session context and stops when the session ends.
	var shadowActive bool
	if w.config.ShadowMode && w.shadowTransportFactory != nil {
		shadowActive = true
		w.logger.Info("[SHADOW] Will start shadow gRPC stream in session")
	}

	// Run session: start all loops, manage lifecycle
	sessionEnded := w.runSession(ctx, shadowActive)

		_ = w.transport.Close()

		// Session ended — either through stop or disconnect
		if w.IsStopped() {
			break
		}
		if sessionEnded {
			w.logger.Warn("[SESSION] Session ended — will reconnect")
		} else {
			w.logger.Info("[SESSION] Session ended cleanly")
		}
	}

	w.setConnState(ConnDisconnected)
	w.logger.Info("Worker stopped")
	return nil
}

// buildHello constructs a WorkerHello from the worker configuration.
func (w *Worker) buildHello() controltransport.WorkerHello {
	hostname, _ := os.Hostname()
	maxParallel := detectMaxParallelJobs()
	if w.config.MaxActiveJobs > 1 {
		maxParallel = w.config.MaxActiveJobs
	}

	capabilities := map[string]interface{}{
		"render_scene_image": true,
		"render_clip_stock":  true,
		"upload_drive":       true,
		"ffmpeg":             true,
		"cpp_engine":         true,
		"max_parallel_jobs":  maxParallel,
		"cpu_count":          runtime.NumCPU(),
		"supported_job_types": []string{
			"process_video",
			"render",
			"process_audio",
			"health_check",
		},
	}

	hello := controltransport.WorkerHello{
		WorkerID:        w.config.WorkerID,
		WorkerName:      w.config.WorkerName,
		Hostname:        hostname,
		Version:         w.version,
		BundleVersion:   w.config.BundleVersion,
		BundleHash:      w.config.BundleHash,
		ProtocolVersion: w.config.ProtocolVersion,
		EngineVersion:   w.config.EngineVersion,
		Capabilities:    capabilities,
	}

	// Compute persistent credential hash if worker secret is configured.
	// Credential = SHA-256(workerID + ":" + workerSecret)
	if w.config.WorkerSecret != "" {
		h := sha256.New()
		h.Write([]byte(w.config.WorkerID + ":" + w.config.WorkerSecret))
		hello.CredentialHash = hex.EncodeToString(h.Sum(nil))
		w.logger.Debug("[AUTH] Credential hash computed for registration")
	}

	return hello
}

// runSession starts all communication loops and returns true if the session
// ended due to disconnect (should reconnect), false if stopped gracefully.
// When shadowMode is true, an additional gRPC shadow transport is opened
// for observation-only comparison with the primary transport.
func (w *Worker) runSession(ctx context.Context, shadowMode bool) bool {
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start the receive channel for messages from master (jobs, commands)
	recvCh, errCh, err := w.transport.Receive(sessionCtx)
	if err != nil {
		w.logger.Error("Failed to start receive channel: %v", err)
		return false
	}

	// Start heartbeat goroutine (uses transport.Send)
	w.wg.Add(1)
	go w.heartbeatLoop(sessionCtx)

	// Start dedicated lease renewal goroutine (uses transport.Send)
	w.wg.Add(1)
	go w.leaseRenewLoop(sessionCtx)

	// Start unified receive loop (replaces jobLoop + commandLoop)
	w.wg.Add(1)
	go w.receiveLoop(sessionCtx, recvCh)

	// ── Shadow mode (PR11): launch shadow gRPC transport for observation ────
	if shadowMode {
		w.wg.Add(1)
		go w.shadowSessionLifecycle(sessionCtx)
	}

	// Start local persistence loop (saves seen commands + job metadata)
	w.startPersistenceLoop(sessionCtx)

	// P0 reconnect fix: also watch the error channel for stream failures
	sessionEnded := false
	select {
	case <-w.stopChan:
		w.logger.Info("Worker stopping...")
		w.setStatus(StatusStopped)
		w.setConnState(ConnDraining)
	case <-ctx.Done():
		w.logger.Warn("Parent context cancelled — draining")
		w.setConnState(ConnDraining)
	case streamErr, ok := <-errCh:
		if ok {
			w.logger.Warn("[SESSION] Transport error — session ended: %v", streamErr)
		} else {
			w.logger.Info("[SESSION] Transport closed cleanly")
		}
		w.setConnState(ConnDisconnected)
		sessionEnded = true // Signal caller to reconnect
	}

	// Cancel session context to stop all loops
	cancel()

	// Wait for goroutines to finish with timeout
	done := make(chan struct{})
	go func() {
		w.wg.Wait()
		close(done)
	}()

	select {
	case <-done:
		w.logger.Info("All goroutines stopped cleanly")
	case <-time.After(30 * time.Second):
		w.logger.Warn("Timeout waiting for goroutines, forcing exit")
	}

	return sessionEnded
}

// receiveLoop processes incoming messages from the transport receive channel.
// Routes MsgJobOffer to executeJob and MsgCommand to processCommand.
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

			switch msg.Type {			case controltransport.MsgJobOffer:
				// Parse the offer exactly once — every reject path needs the
				// job_id, and the accepted path also needs the typed payload.
				offer := msgToJob(msg)
				jobID := ""
				if offer != nil {
					jobID = offer.JobID
				}

				// ── Shadow mode (PR11): record primary job claim for comparison ────
				// Compare this job arrival against the shadow gRPC stream to
				// measure push-vs-poll latency and detect mismatches.
				if w.isShadowModeActive() && jobID != "" {
					w.recordPrimaryJobSeen(jobID)
				}

				// P5 cleanup: never silently drop an offer. Send JobRejected so
				// the master can re-route/retry instead of holding a
				// pendingOffer and waiting on the lease expire timer.
				if w.IsStopped() {
					if err := w.sendReject(ctx, jobID, "stopped"); err != nil {
						w.logger.Warn("[RECEIVE] Failed to send JobRejected (stopped): %v", err)
					}
					continue
				}
				if w.drainMode.Load() {
					if err := w.sendReject(ctx, jobID, "draining"); err != nil {
						w.logger.Warn("[RECEIVE] Failed to send JobRejected (draining): %v", err)
					}
					continue
				}

				// Check concurrency capacity
				w.activeJobsMu.RLock()
				activeCount := len(w.activeJobs)
				w.activeJobsMu.RUnlock()
				if activeCount >= w.config.MaxActiveJobs {
					if err := w.sendReject(ctx, jobID, "capacity_full"); err != nil {
						w.logger.Warn("[RECEIVE] Failed to send JobRejected (capacity): %v", err)
					}
					continue
				}

				// Parse job from payload (already parsed above)
				if offer == nil {
					w.logger.Warn("[RECEIVE] Failed to parse job from JobOffer message")
					continue
				}
				job := offer

				// Validate job offer (contract version, render plan, concurrency)
				if err := w.validateJobOffer(job); err != nil {
					w.logger.Warn("[RECEIVE] Job offer validation failed: %v", err)
					_ = w.sendReject(ctx, job.JobID, err.Error())
					continue
				}

				w.logger.Info("[RECEIVE] JobOffer received: %s (type: %s, lease: %s)", job.JobID, job.JobType, job.LeaseID)

				// Send JobAccepted via transport
				if err := w.sendAccept(ctx, job); err != nil {
					w.logger.Warn("[RECEIVE] Failed to send JobAccepted: %v", err)
					continue
				}					// P0 protocol fix: store pending job, wait for JobLeaseGranted before executing.
					// The master confirms the lease atomically in SQLite before sending JobLeaseGranted.
					w.storePendingJob(job)

			case controltransport.MsgJobAvailable:
				// PR12: lightweight notification from master — "there are jobs for you".
				// The worker calls ClaimNext via HTTP to atomically claim a job.
				// This model keeps SQLite as the single source of truth.
				w.handleJobAvailable(ctx, msg)

			case controltransport.MsgJobLeaseGranted:
				// P0 protocol fix: master confirms the lease.
				// Only now can the worker safely execute the job.
				leaseGranted, ok := msg.TypedPayload.(*pb.JobLeaseGranted)
				if !ok || leaseGranted == nil {
					w.logger.Warn("[RECEIVE] JobLeaseGranted without typed payload")
					continue
				}
				jobID := leaseGranted.GetJobId()
				if jobID == "" {
					w.logger.Warn("[RECEIVE] JobLeaseGranted without job_id")
					continue
				}

				job := w.takePendingJob(jobID)
				if job == nil {
					w.logger.Warn("[RECEIVE] JobLeaseGranted for unknown job %s", jobID)
					continue
				}

				w.logger.Info("[RECEIVE] JobLeaseGranted for %s — starting execution", jobID)
				go w.executeJob(ctx, job)

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
				w.logger.Info("[RECEIVE] Drain command received — no new jobs will be accepted")

			case controltransport.MsgConfigurationUpdate:
				w.logger.Info("[RECEIVE] ConfigurationUpdate received")
				configUpdate, ok := msg.TypedPayload.(*pb.ConfigurationUpdate)
				if ok && configUpdate != nil && configUpdate.GetConfiguration() != nil {
					cfgMap := configUpdate.GetConfiguration().AsMap()
					// RecoveryReport protocol: same envelope carries recovery_action_v1.
					// handleRecoveryDirective is internally guarded against the absence of
					// that key, so the call is unconditional.
					w.handleRecoveryDirective(configUpdate.GetConfiguration())
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
					// Gap #5 fix: send ack with the command_id from the
					// envelope MessageId (ConfigurationUpdate proto has no
					// dedicated command_id field). The master can match this
					// via the CommandAck.CommandId field.
					ackPayload := map[string]interface{}{
						"command_id":        msg.MessageID,
						"worker_id":         w.config.WorkerID,
						"max_parallel_jobs": w.config.MaxActiveJobs,
						"log_level":         w.config.LogLevel,
					}
					ackMsg := controltransport.NewMessageWithPayload(
						controltransport.MsgCommandAck,
						w.config.WorkerID,
						w.config.ProtocolVersion,
						ackPayload,
					)
					ackCtx, ackCancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer ackCancel()
					if ackErr := w.transport.Send(ackCtx, ackMsg); ackErr != nil {
						w.logger.Warn("[CONFIG] Failed to ack ConfigurationUpdate: %v", ackErr)
					}
				}

			case controltransport.MsgLeaseRevoked:
				leaseRevoked, ok := msg.TypedPayload.(*pb.LeaseRevoked)
				if ok && leaseRevoked != nil {
					jobID := leaseRevoked.GetJobId()
					w.logger.Warn("[RECEIVE] Lease revoked for job %s: %s",
						jobID, leaseRevoked.GetReason())
					// Cancel the running job and remove from active list.
					if jobID != "" {
						w.cancelJob(jobID)
						w.activeJobsMu.Lock()
						delete(w.activeJobs, jobID)
						w.activeJobsMu.Unlock()
						w.pendingLeaseMu.Lock()
						delete(w.pendingLeaseJobs, jobID)
						w.pendingLeaseMu.Unlock()
					}
				}

			case controltransport.MsgPing:
				// Reply with a heartbeat via Send
				w.sendHeartbeat(ctx)

			case controltransport.MsgHelloAck:
				w.logger.Debug("[RECEIVE] HelloAck received — session confirmed")
				// RecoveryReport protocol: emit one enriched heartbeat carrying the
				// persisted state from the previous run, so the master can issue a
				// targeted CONTINUE / CANCEL / RESUME_UPLOAD / CLEANUP directive.
				w.maybeSendRecoveryReport(ctx)

			default:
				w.logger.Debug("[RECEIVE] Unhandled message type: %s", msg.Type)
			}
		}
	}
}

// msgToJob converts a ControlMessage (MsgJobOffer) to an api.Job.
// Handles both typed proto payload (gRPC transport: TypedPayload=*pb.JobOffer)
// and generic map payload (HTTP polling transport: Payload=map[string]interface{}).
func msgToJob(msg controltransport.ControlMessage) *api.Job {
	// Try typed proto payload first (gRPC transport).
	if offer, ok := msg.TypedPayload.(*pb.JobOffer); ok && offer != nil {
		return msgToJobFromProto(offer)
	}

	// Fall back to generic Payload map (HTTP polling transport).
	if msg.Payload != nil {
		return msgToJobFromPayload(msg.Payload)
	}

	return nil
}

// msgToJobFromProto extracts an api.Job from a typed *pb.JobOffer.
func msgToJobFromProto(offer *pb.JobOffer) *api.Job {
	jobID := offer.GetJobId()
	if jobID == "" {
		return nil
	}

	// Extract dynamic fields from job_payload (job_type, priority, parameters, timeout_secs, contract_version)
	var parameters map[string]interface{}
	if jp := offer.GetJobPayload(); jp != nil {
		parameters = jp.AsMap()
	}

	createdAt := ""
	if offer.GetCreatedAt() != nil {
		createdAt = offer.GetCreatedAt().AsTime().UTC().Format(time.RFC3339)
	}

	leaseExpiry := ""
	if offer.GetLeaseExpiry() != nil {
		leaseExpiry = offer.GetLeaseExpiry().AsTime().UTC().Format(time.RFC3339)
	}

	return &api.Job{
		JobID:           jobID,
		JobRunID:        offer.GetRunId(),
		JobType:         getStrParam(parameters, "job_type"),
		Priority:        getIntParam(parameters, "priority"),
		Parameters:      parameters,
		CreatedAt:       createdAt,
		TimeoutSecs:     getIntParam(parameters, "timeout_secs"),
		ContractVersion: getIntParam(parameters, "contract_version"),
		LeaseID:         offer.GetLeaseId(),
		LeaseExpiry:     leaseExpiry,
		Attempt:         int(offer.GetAttempt()),
	}
}

// msgToJobFromPayload extracts an api.Job from a generic Payload map (HTTP transport).
func msgToJobFromPayload(payload map[string]interface{}) *api.Job {
	jobID := getPayloadStrRaw(payload, "job_id")
	if jobID == "" {
		return nil
	}

	// Collect known fields into parameters, bubbling remaining to the top.
	parameters := make(map[string]interface{})
	for k, v := range payload {
		switch k {
		case "job_id", "run_id", "lease_id", "attempt", "lease_expiry", "created_at":
			// Known top-level fields, not parameters.
		default:
			parameters[k] = v
		}
	}

	return &api.Job{
		JobID:           jobID,
		JobRunID:        getPayloadStrRaw(payload, "run_id"),
		JobType:         getPayloadStrRaw(payload, "job_type"),
		Priority:        getPayloadIntRaw(payload, "priority"),
		Parameters:      parameters,
		CreatedAt:       payload["created_at"],
		TimeoutSecs:     getPayloadIntRaw(payload, "timeout_secs"),
		ContractVersion: getPayloadIntRaw(payload, "contract_version"),
		LeaseID:         getPayloadStrRaw(payload, "lease_id"),
		LeaseExpiry:     getPayloadStrRaw(payload, "lease_expiry"),
		Attempt:         getPayloadIntRaw(payload, "attempt"),
	}
}

func getPayloadStrRaw(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getPayloadIntRaw(m map[string]interface{}, key string) int {
	switch v := m[key].(type) {
	case float64:
		return int(v)
	case int:
		return v
	case int64:
		return int(v)
	}
	return 0
}

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

// getStrParam extracts a string value from a parameters map, returning "" if missing.
func getStrParam(params map[string]interface{}, key string) string {
	if v, ok := params[key].(string); ok {
		return v
	}
	return ""
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

// isPrimaryGRPC returns true when the current primary transport is gRPC
// (i.e., we haven't fallen back to HTTP polling yet).
func (w *Worker) isPrimaryGRPC() bool {
	return w.config.JobDelivery != "polling"
}

// fallbackToHTTP overrides the transport factory to return an HTTP polling
// transport for the remainder of the session (PR12).
func (w *Worker) fallbackToHTTP() {
	w.transportFactory = func() controltransport.ControlTransport {
		return newHTTPPollingTransportUnvalidated(w.config, w.logger)
	}
}

// handleJobAvailable processes a MsgJobAvailable notification (PR12).
// The master sends this lightweight notification when jobs are queued for
// the worker. The worker then claims atomically via HTTP ClaimNext.
// This model keeps SQLite as the single source of truth.
func (w *Worker) handleJobAvailable(ctx context.Context, msg controltransport.ControlMessage) {
	w.logger.Debug("[RECEIVE] JobAvailable received — claiming via HTTP ClaimNext")

	// Check preconditions before spawning a claim goroutine.
	if w.IsStopped() || w.drainMode.Load() {
		w.logger.Debug("[RECEIVE] JobAvailable ignored — worker is stopped or draining")
		return
	}
	w.activeJobsMu.RLock()
	activeCount := len(w.activeJobs)
	w.activeJobsMu.RUnlock()
	if activeCount >= w.config.MaxActiveJobs {
		w.logger.Debug("[RECEIVE] JobAvailable ignored — at capacity (%d/%d)", activeCount, w.config.MaxActiveJobs)
		return
	}

	// Spawn a lightweight goroutine so the receiveLoop is not blocked.
	go func() {
		claimCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		job, err := w.apiClient.GetJobV2(claimCtx, w.config.WorkerID)
		if err != nil {
			w.logger.Warn("[RECEIVE] JobAvailable claim failed: %v", err)
			return
		}
		if job == nil || job.JobID == "" {
			w.logger.Debug("[RECEIVE] JobAvailable claim returned no job (already taken)")
			return
		}

		w.logger.Info("[RECEIVE] JobAvailable claim succeeded: %s (type: %s)", job.JobID, job.JobType)

		// Inject the claimed job into the normal execution flow.
		// Use sendAccept + storePendingJob to keep protocol consistent.
		if err := w.sendAccept(ctx, job); err != nil {
			w.logger.Warn("[RECEIVE] Failed to send JobAccepted after claim: %v", err)
			return
		}
		w.storePendingJob(job)
	}()
}

// newTransport creates a fresh transport instance for a new session attempt.
func (w *Worker) newTransport() controltransport.ControlTransport {
	return w.transportFactory()
}

// sendAccept sends a JobAccepted message via the transport.
func (w *Worker) sendAccept(ctx context.Context, job *api.Job) error {
	acceptPayload := map[string]interface{}{
		"job_id":     job.JobID,
		"job_run_id": job.JobRunID,
		"lease_id":   job.LeaseID,
	}
	acceptMsg := controltransport.NewMessageWithPayload(
		controltransport.MsgJobAccepted,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		acceptPayload,
	)
	return w.transport.Send(ctx, acceptMsg)
}

// sendReject sends a JobRejected message via the transport.
func (w *Worker) sendReject(ctx context.Context, jobID, reason string) error {
	rejectPayload := map[string]interface{}{
		"job_id": jobID,
		"reason": reason,
	}
	rejectMsg := controltransport.NewMessageWithPayload(
		controltransport.MsgJobRejected,
		w.config.WorkerID,
		w.config.ProtocolVersion,
		rejectPayload,
	)
	return w.transport.Send(ctx, rejectMsg)
}

// storePendingJob records a job that has been accepted but is waiting for
// JobLeaseGranted before execution.
func (w *Worker) storePendingJob(job *api.Job) {
	w.pendingLeaseMu.Lock()
	defer w.pendingLeaseMu.Unlock()
	w.pendingLeaseJobs[job.JobID] = job
}

// takePendingJob retrieves and removes a pending job by ID.
// Returns nil if the job was not found.
func (w *Worker) takePendingJob(jobID string) *api.Job {
	w.pendingLeaseMu.Lock()
	defer w.pendingLeaseMu.Unlock()
	job := w.pendingLeaseJobs[jobID]
	delete(w.pendingLeaseJobs, jobID)
	return job
}

// Stop signals the worker to stop gracefully.
// This method is idempotent - calling it multiple times has no additional effect.
func (w *Worker) Stop() {
	w.stopOnce.Do(func() {
		w.logger.Info("Stop requested")
		close(w.stopChan)
		w.stopped.Store(true)
	})
}

// isConnectionLevelError returns true when the error is a transient
// connection-level failure (reset, refused, transport unavailable).
// These typically occur when the server is restarting and will recover
// in seconds. Application-level errors (credential mismatch, protocol
// version, TLS) return false and should use exponential backoff.
func isConnectionLevelError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	lower := strings.ToLower(msg)

	// Connection-level indicators
	connectionPatterns := []string{
		"connection refused",
		"connection reset by peer",
		"no route to host",
		"network is unreachable",
		"transport is closing",
		"broken pipe",
		"use of closed network connection",
		"i/o timeout", // dial timeout, not application timeout
	}
	for _, p := range connectionPatterns {
		if strings.Contains(lower, p) {
			return true
		}
	}

	// gRPC Unavailable status code (transient): the server may be restarting
	// and the TCP listener is open but gRPC isn't serving yet.
	if strings.Contains(lower, "unavailable") {
		return true
	}

	return false
}
