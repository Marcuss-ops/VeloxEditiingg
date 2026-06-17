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
	"time"

	"velox-shared/controltransport"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/logger"
)

// Start begins the worker's main loop with automatic re-registration on failure.
// Uses ControlTransport for all master communication, making the worker
// transport-agnostic (HTTP polling today, gRPC stream tomorrow).
func (w *Worker) Start(ctx context.Context) error {
	logger.LogStartup(w.config.WorkerID, w.version, w.config.MasterURL)
	w.logger.Debug("Work Directory: %s", w.config.WorkDir)

	w.concurrencyLimiter.Start(ctx)
	w.logger.Info("[CONCURRENCY] Started with max_active_jobs=%d", w.config.MaxActiveJobs)

	// Connection state machine with automatic backoff
	backoff := registrationInitialBackoff

	for !w.IsStopped() {
		w.setConnState(ConnConnecting)
		w.connFailureCount = 0

		hello := w.buildHello()
		if err := w.transport.Connect(ctx, hello); err != nil {
			w.connFailureCount++
			w.logger.Warn("[CONNECT] Registration failed (attempt %d): %v", w.connFailureCount, err)
			w.setConnState(ConnDisconnected)

			// Exponential backoff with jitter
			jitter := time.Duration(rand.Float64() * float64(backoff) * 0.25)
			sleepDuration := backoff + jitter
			w.logger.Info("[CONNECT] Backing off for %v before retry", sleepDuration.Round(time.Millisecond))

			select {
			case <-time.After(sleepDuration):
				backoff = time.Duration(float64(backoff) * registrationBackoffMult)
				if backoff > registrationMaxBackoff {
					backoff = registrationMaxBackoff
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

		// Run session: start all loops, manage lifecycle
		w.runSession(ctx)

		// Session ended — either through stop or disconnect
		if w.IsStopped() {
			break
		}
		w.logger.Warn("[SESSION] Session ended — will reconnect")
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

// runSession starts all communication loops and waits for shutdown or disconnect.
func (w *Worker) runSession(ctx context.Context) {
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Start the receive channel for messages from master (jobs, commands)
	recvCh, err := w.transport.Receive(sessionCtx)
	if err != nil {
		w.logger.Error("Failed to start receive channel: %v", err)
		return
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

	// Wait for stop signal or session disconnect
	select {
	case <-w.stopChan:
		w.logger.Info("Worker stopping...")
		w.setStatus(StatusStopped)
		w.setConnState(ConnDraining)
	case <-ctx.Done():
		w.logger.Warn("Parent context cancelled — draining")
		w.setConnState(ConnDraining)
	}

	// Cancel session context to stop all loops
	cancel()

	// Unregister via transport
	_ = w.transport.Close()

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

			switch msg.Type {
			case controltransport.MsgJobOffer:
				if w.IsStopped() || w.drainMode.Load() {
					w.logger.Debug("[RECEIVE] Ignoring job offer — worker stopped/draining")
					continue
				}

				// Check concurrency capacity
				w.activeJobsMu.RLock()
				activeCount := len(w.activeJobs)
				w.activeJobsMu.RUnlock()
				if activeCount >= w.config.MaxActiveJobs {
					w.logger.Debug("[RECEIVE] Ignoring job offer — at max capacity (%d/%d)",
						activeCount, w.config.MaxActiveJobs)
					continue
				}

				// Parse job from payload
				job := msgToJob(msg)
				if job == nil {
					w.logger.Warn("[RECEIVE] Failed to parse job from JobOffer message")
					continue
				}

				// Validate job offer (contract version, render plan, concurrency)
				if err := w.validateJobOffer(job); err != nil {
					w.logger.Warn("[RECEIVE] Job offer validation failed: %v", err)
					// Send JobRejected message
					rejectPayload := map[string]interface{}{
						"job_id": job.JobID,
						"reason": err.Error(),
					}
					rejectMsg := controltransport.NewMessageWithPayload(
						controltransport.MsgJobRejected,
						w.config.WorkerID,
						w.config.ProtocolVersion,
						rejectPayload,
					)
					_ = w.transport.Send(ctx, rejectMsg)
					continue
				}

				w.logger.Info("[RECEIVE] JobOffer received: %s (type: %s)", job.JobID, job.JobType)

				// Send JobAccepted via transport
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
				_ = w.transport.Send(ctx, acceptMsg)

				// Launch job execution in a goroutine
				go w.executeJob(ctx, job)

			case controltransport.MsgCommand:
				cmd := msgToCommand(msg)
				w.logger.Info("[RECEIVE] Command received: %s (id: %s)", cmd.Command, cmd.CommandID)
				w.processCommand(ctx, cmd)

			case controltransport.MsgCancelJob:
				jobID := ""
				if msg.Payload != nil {
					if j, ok := msg.Payload["job_id"].(string); ok {
						jobID = j
					}
				}
				w.logger.Info("[RECEIVE] CancelJob received for job %s", jobID)
				if jobID != "" {
					w.cancelJob(jobID)
				}

			case controltransport.MsgDrain:
				w.drainMode.Store(true)
				w.logger.Info("[RECEIVE] Drain command received — no new jobs will be accepted")

			case controltransport.MsgConfigurationUpdate:
				w.logger.Info("[RECEIVE] ConfigurationUpdate received")

			case controltransport.MsgLeaseRevoked:
				jobID := ""
				if msg.Payload != nil {
					if j, ok := msg.Payload["job_id"].(string); ok {
						jobID = j
					}
				}
				w.logger.Warn("[RECEIVE] Lease revoked for job %s", jobID)

			case controltransport.MsgPing:
				// Reply with a heartbeat via Send
				w.sendHeartbeat(ctx)

			case controltransport.MsgHelloAck:
				w.logger.Debug("[RECEIVE] HelloAck received — session confirmed")

			default:
				w.logger.Debug("[RECEIVE] Unhandled message type: %s", msg.Type)
			}
		}
	}
}

// msgToJob converts a ControlMessage (MsgJobOffer) back to an api.Job.
func msgToJob(msg controltransport.ControlMessage) *api.Job {
	if msg.Payload == nil {
		return nil
	}

	jobID, _ := msg.Payload["job_id"].(string)
	if jobID == "" {
		return nil
	}

	return &api.Job{
		JobID:           jobID,
		JobRunID:        getMsgString(msg.Payload, "job_run_id"),
		JobType:         getMsgString(msg.Payload, "job_type"),
		Priority:        getMsgInt(msg.Payload, "priority"),
		Parameters:      getMsgMap(msg.Payload, "parameters"),
		CreatedAt:       msg.Payload["created_at"],
		TimeoutSecs:     getMsgInt(msg.Payload, "timeout_secs"),
		ContractVersion: getMsgInt(msg.Payload, "contract_version"),
		LeaseID:         getMsgString(msg.Payload, "lease_id"),
		LeaseExpiry:     getMsgString(msg.Payload, "lease_expiry"),
		Attempt:         getMsgInt(msg.Payload, "attempt"),
	}
}

// msgToCommand converts a ControlMessage (MsgCommand) to an api.WorkerCommand.
func msgToCommand(msg controltransport.ControlMessage) api.WorkerCommand {
	cmd := api.WorkerCommand{
		CommandID: getMsgString(msg.Payload, "command_id"),
		Command:   getMsgString(msg.Payload, "command"),
		Timestamp: getMsgString(msg.Payload, "timestamp"),
	}
	if params, ok := msg.Payload["params"].(map[string]interface{}); ok {
		cmd.Payload = params
	}
	return cmd
}

func getMsgString(m map[string]interface{}, key string) string {
	if v, ok := m[key].(string); ok {
		return v
	}
	return ""
}

func getMsgInt(m map[string]interface{}, key string) int {
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

func getMsgMap(m map[string]interface{}, key string) map[string]interface{} {
	if v, ok := m[key].(map[string]interface{}); ok {
		return v
	}
	return nil
}

// setConnState updates the connection state.
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

// Stop signals the worker to stop gracefully.
// This method is idempotent - calling it multiple times has no additional effect.
func (w *Worker) Stop() {
	w.stopOnce.Do(func() {
		w.logger.Info("Stop requested")
		close(w.stopChan)
		w.stopped.Store(true)
	})
}
