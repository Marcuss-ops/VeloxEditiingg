// Package worker provides the core worker orchestration logic.
// This file serves as the thin orchestrator that coordinates the worker modules.
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand"
	"os"
	"runtime"
	"strings"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/bootstrap"
	"velox-worker-agent/pkg/logger"
)

// Start begins the worker's main loop with automatic re-registration on failure.
// Creates a fresh transport instance per session attempt (reconnect P0 fix).
//
// RW-PROD-003 §3 A6 defence-in-depth: the very first thing Start does is
// call bootstrap.HardGate(). The composition root (cmd/velox-worker-agent/main.go)
// already wired bootstrap.Run SYNCHRONOUSLY between the C++ pipeline
// construction and the executor wiring (RW-PROD-003 A5), so by the time
// Start is called in production the gate is already true. This check
// exists to catch FUTURE refactors that might reorder the composition
// root — e.g. someone calls worker.New(...).Start() directly from a
// test harness or a future --doctor entry-point. In that case the gate
// is false and Start returns with a "bootstrap_not_run" error BEFORE
// touching the transport, the registry, or the heartbeat loop. The
// worker is then correctly seen as `registered=false` from the master's
// selector because no Hello message is ever produced.
func (w *Worker) Start(ctx context.Context) error {
	if err := bootstrap.HardGate(); err != nil {
		w.logger.Error("[START_GUARD] refusing to start: %v", err)
		return fmt.Errorf("worker.Start precondition: %w", err)
	}
	logger.LogStartup(w.config.WorkerID, w.version, w.config.MasterURL)
	w.logger.Debug("Work Directory: %s", w.config.WorkDir)

	w.concurrencyLimiter.Start(ctx)
	w.logger.Info("[CONCURRENCY] Started with max_active_jobs=%d", w.config.MaxActiveJobs)

	// PR-3.5: surface empty executor registry early. WithRegistry(empty)
	// is the supported default; operators must see this on the wire
	// before deciding the worker is broken or PR-3.6 hasn't shipped.
	if w.executorRegistry != nil && len(w.executorRegistry.Descriptors()) == 0 {
		w.logger.Warn("[STARTUP] executor registry is empty — ZERO executors will be advertised to master until scene.composite.v1 is registered (PR-3.6)")
	}

	// Connection state machine with automatic backoff
	backoff := registrationInitialBackoff

	for !w.IsStopped() {
		// P0 reconnect fix: create a fresh transport each session attempt.
		// After Close(), transports are not reusable (channels + sync.Once).
		w.transport = w.newTransport()
		if w.transport == nil {
			w.connFailureCount++
			w.logger.Warn("[CONNECT] Transport factory returned nil — backing off")
			w.setConnState(ConnDisconnected)
			telemetry.SetHealthRegistered(false)
			jitter := time.Duration(rand.Float64() * float64(connectionRetryBackoff) * 0.3)
			sleepDuration := connectionRetryBackoff + jitter
			select {
			case <-time.After(sleepDuration):
				continue
			case <-w.stopChan:
				w.logger.Info("Worker stopping during transport-factory backoff")
				return nil
			case <-ctx.Done():
				return ctx.Err()
			}
		}

		w.setConnState(ConnConnecting)
		w.connFailureCount = 0

		hello := w.buildHello()
		if err := w.transport.Connect(ctx, hello); err != nil {
			w.connFailureCount++

			w.logger.Warn("[CONNECT] Registration failed (attempt %d): %v", w.connFailureCount, err)
			w.setConnState(ConnDisconnected)
			// RW-PROD-004 A4: mirror ConnReady on every ConnDisconnected
			// transition so /health/ready drops `not_registered` once the
			// session is re-established. MarkRegistered queues an
			// UpdateReady copy-and-store under the process-global atomic.Pointer.
			telemetry.SetHealthRegistered(false)
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
		// RW-PROD-004 A4: MarkRegistered(true) is already chained via the
		// legacy SetHealthRegistered setter. The explicit call here is
		// defensive: if a future refactor decouples the legacy flag from
		// the canonical ready snapshot, we still want the ready flip to
		// happen at the ConnReady instant.
		w.logger.Info("[CONNECT] Registration successful — running session")

		// Run session: start all loops, manage lifecycle
		sessionEnded := w.runSession(ctx)

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
	// RW-PROD-004 A4: final teardown clears both halves of the readiness
	// taxonomy. MarkRegistered(false) drops the `not_registered` reading
	// (it should already be false at this point, but the explicit call is
	// defensive against future refactors that shift the order). MarkDrainMode
	// is left as-is: drain_mode is sticky across reconnects (operators
	// want to see it stay true until the worker exits), and Stop is the
	// final exit point — clearer to drop the flag explicitly here too so a
	// fresh process after a restart starts from a clean ready slate.
	telemetry.SetHealthRegistered(false)
	telemetry.MarkDrainMode(false)
	w.logger.Info("Worker stopped")
	return nil
}

// buildHello constructs a WorkerHello from the worker configuration.
// PR-3.5: the capability payload is derived EXCLUSIVELY from
// w.capabilitiesMap(hostname) — a single helper also used by
// sendHeartbeat. Any wire-shape change touches one function.
func (w *Worker) buildHello() controltransport.WorkerHello {
	hostname, _ := os.Hostname()

	hello := controltransport.WorkerHello{
		WorkerID:        w.config.WorkerID,
		WorkerName:      w.config.WorkerName,
		Hostname:        hostname,
		Version:         w.version,
		BundleVersion:   w.config.BundleVersion,
		BundleHash:      w.config.BundleHash,
		ProtocolVersion: w.config.ProtocolVersion,
		EngineVersion:   w.config.EngineVersion,
		WorkerClass:     w.config.WorkerClass,
		RolloutGroup:    w.config.RolloutGroup,
		Capabilities:    w.capabilitiesMap(hostname),
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

// capabilitiesMap is the SINGLE source of truth for the worker's
// capability map. Both buildHello and sendHeartbeat call it; any change
// to wire shape touches one function, not two.
//
// Concurrency invariants:
//   - max_parallel_jobs is sourced ONCE from w.concurrencyLimiter (host
//     block). The top-level mirror reads from the SAME host value, so a
//     ConfigurationUpdate flipped via SetMaxActiveJobs is visible in
//     BOTH locations atomically per capabilitiesMap call.
//   - AsMap emits an empty slice (not nil) when the registry is empty so
//     encoding/json never silently drops the executors key.
func (w *Worker) capabilitiesMap(hostname string) map[string]interface{} {
	host := w.hostInfo(hostname, w.concurrencyLimiter.MaxActiveJobs())
	report := executor.BuildCapabilityReport(w.executorRegistry, host)
	m := report.AsMap()
	// Top-level mirror of host.max_parallel_jobs for legacy master
	// decoders that don't walk into the host sub-block. Sourced from
	// the SAME host field — both paths MUST stay byte-identical.
	m["max_parallel_jobs"] = host.MaxParallelJobs
	return m
}

// normalizeOfferedExecutorID strips an accidental "@version" suffix
// from a task offer's executor_id when the master already split the
// version into executor_version. Registry descriptors forbid '@' in
// the base ID, so the last '@' unambiguously identifies the suffix.
func normalizeOfferedExecutorID(id string) string {
	if i := strings.LastIndex(id, "@"); i > 0 {
		return id[:i]
	}
	return id
}

// hostInfo packages the static host-side fields of the capability report.
// All values are pre-shaped so PR-3.6's resource sampler can fill
// RAMBytes / DiskFreeBytes / HasGPU without breaking the wire contract —
// the master will simply start seeing non-zero values.
//
// F4 integration: Host() is consulted lazily on every hostInfo call (cheap
// atomic.Pointer load); the sampler publishes refreshed values from its
// background 5s tick loop. If the sampler hasn't yet booted (pre-tick),
// the related HostInfo fields default to zero — same wire contract the
// master has handled for years (zero == "not yet sampled").
func (w *Worker) hostInfo(hostname string, maxParallel int) api.HostInfo {
	host := api.HostInfo{
		WorkerID:        w.config.WorkerID,
		Hostname:        hostname,
		CPUCount:        runtime.NumCPU(),
		MaxParallelJobs: maxParallel,
	}
	if w.sampler != nil {
		if h := w.sampler.Host(); h != nil {
			host.HasGPU = h.HasGPU
			host.RAMBytes = h.RAMBytes
			host.DiskFreeBytes = h.DiskFreeBytes
		}
	}
	return host
}

// runSession starts all communication loops and returns true if the session
// ended due to disconnect (should reconnect), false if stopped gracefully.
func (w *Worker) runSession(ctx context.Context) bool {
	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	recvCh, errCh, err := w.transport.Receive(sessionCtx)
	if err != nil {
		w.logger.Error("Failed to start receive channel: %v", err)
		return false
	}

	w.wg.Add(1)
	go w.heartbeatLoop(sessionCtx)

	w.wg.Add(1)
	go w.leaseRenewLoop(sessionCtx)

	// PR-3.6 / F4: kick the resource-sampler loop under the session
	// context. Uses NewResourceSampler-registered 5s tick + 3-tick
	// emit cadence (heartbeat.resources is the only consumer of
	// Latest(); without this loop it would stay nil forever). On
	// sessionCtx cancel, the loop exits and Sample() returns any
	// partially-built snapshot would be discarded — acceptable
	// because the next session restarts sampling.
	w.wg.Add(1)
	go func() {
		defer w.wg.Done()
		if err := w.sampler.Run(sessionCtx); err != nil && !errors.Is(err, context.Canceled) {
			w.logger.Warn("[SAMPLER] resource sampler loop exited: %v", err)
		}
	}()

	w.wg.Add(1)
	go w.receiveLoop(sessionCtx, recvCh)

	w.startPersistenceLoop(sessionCtx)

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
		// RW-PROD-004 A4 (BLOCKER round-2 fix): the third ConnDisconnected
		// site (transport-error / clean channel close) flips the readiness
		// registered flag to false so /health/ready reports reasons=[not_registered]
		// throughout the backoff-and-reconnect window. Without this hook the
		// readiness snapshot stayed "true" between sessions even though no
		// Hello+HelloAck roundtrip has been acknowledged yet.
		telemetry.SetHealthRegistered(false)
		sessionEnded = true
	}

	cancel()

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

// Stop signals the worker to stop gracefully.
// This method is idempotent - calling it multiple times has no additional effect.
//
// PR-2 (fix/canonical-attempt-identity): drain pendingTasks
// on Stop so the next session starts with an empty map.
// Without this, an offer->stop cycle would leak entries across restarts and
// the next session would carry orphaned canonical (attempt_id, lease_id)
// tuples whose master-side TaskAttempts are already in a terminal state.
func (w *Worker) Stop() {
	w.stopOnce.Do(func() {
		w.logger.Info("Stop requested")
		close(w.stopChan)
		w.stopped.Store(true)
		// Drain pendingTasks on Stop. Entries here correspond to offers
		// the worker accepted but never received a LeaseGranted for
		// (worker died mid-flight, master restarted, etc). Master's lease
		// reaper handles the stranded PENDING / RUNNING TaskAttempts on
		// the master side; the local map is safe to clear.
		w.pendingTasksMu.Lock()
		clearedTask := len(w.pendingTasks)
		w.pendingTasks = make(map[string]*PendingTaskExecution)
		w.pendingTasksMu.Unlock()
		// PR-2 followup: drain activeTaskLeases on Stop so the next
		// session starts empty and any lease the master has already
		// marked TIMED_OUT cannot keep driving phantom renewals.
		w.activeTaskLeasesMu.Lock()
		clearedTaskLeases := len(w.activeTaskLeases)
		w.activeTaskLeases = make(map[string]*ActiveTaskLease)
		w.activeTaskLeasesMu.Unlock()
		if clearedTask > 0 || clearedTaskLeases > 0 {
			w.logger.Info("[STOP] Drained pending entries: task=%d task_leases=%d",
				clearedTask, clearedTaskLeases)
		}
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

	connectionPatterns := []string{
		"connection refused",
		"connection reset by peer",
		"no route to host",
		"network is unreachable",
		"transport is closing",
		"broken pipe",
		"use of closed network connection",
		"i/o timeout",
	}
	for _, p := range connectionPatterns {
		if strings.Contains(lower, p) {
			return true
		}
	}

	if strings.Contains(lower, "unavailable") {
		return true
	}

	return false
}
