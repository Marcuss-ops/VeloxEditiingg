// Package worker provides communication and heartbeat logic for the worker agent.
package worker

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"math"
	"os"
	"runtime"
	"time"

	"velox-worker-agent/internal/telemetry"
	"velox-worker-agent/pkg/api"
	"velox-worker-agent/pkg/logger"
)

// Heartbeat intervals based on worker status
const (
	heartbeatIntervalIdle      = 60 * time.Second // Idle: less frequent
	heartbeatIntervalBusy      = 15 * time.Second // Busy: more frequent for progress updates
	heartbeatIntervalError     = 10 * time.Second // Error: rapid recovery attempts
	heartbeatMaxBackoff        = 5 * time.Minute  // Maximum backoff interval
	heartbeatBackoffMultiplier = 2.0              // Backoff multiplier
)

// detectMaxParallelJobs calculates the optimal concurrency based on hardware.
// Formula: clamp(NumCPU / 2, min=1, max=8)
func detectMaxParallelJobs() int {
	cpuCount := runtime.NumCPU()
	if cpuCount <= 0 {
		cpuCount = 2
	}
	// Use half the CPUs, minimum 1, maximum 8
	parallel := int(math.Max(1, math.Min(8, float64(cpuCount/2))))
	return parallel
}

// register registers the worker with the master server.
func (w *Worker) register(ctx context.Context) error {
	hostname, _ := os.Hostname()

	maxParallel := detectMaxParallelJobs()
	// Override with config if explicitly set > 0
	if w.config.MaxActiveJobs > 1 {
		maxParallel = w.config.MaxActiveJobs
	}

	info := &api.WorkerInfo{
		WorkerID:   w.config.WorkerID,
		WorkerName: w.config.WorkerName,
		Capabilities: map[string]interface{}{
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
		},
		Hostname:        hostname,
		IP:              "",
		Version:         w.version,
		CodeVersion:     w.version,
		BundleVersion:   w.config.BundleVersion,
		BundleHash:      w.config.BundleHash,
		ProtocolVersion: w.config.ProtocolVersion,
		EngineVersion:   w.config.EngineVersion,
	}

	// Compute persistent credential hash if worker secret is configured.
	// Credential = SHA-256(workerID + ":" + workerSecret)
	if w.config.WorkerSecret != "" {
		h := sha256.New()
		h.Write([]byte(w.config.WorkerID + ":" + w.config.WorkerSecret))
		info.Credential = hex.EncodeToString(h.Sum(nil))
		w.logger.Debug("[AUTH] Credential hash computed for registration")
	}

	w.logger.Debug("Registering with master at %s", w.config.MasterURL)
	if err := w.apiClient.RegisterWorker(ctx, info); err != nil {
		logger.LogRegisterFailed(w.config.WorkerID, w.config.MasterURL, err)
		return err
	}

	logger.LogRegisterSuccess(w.config.WorkerID, w.config.MasterURL)

	// Mark worker as registered for the /health endpoint
	telemetry.SetHealthRegistered(true)

	// Log whether we received an auth token for future requests
	token := w.apiClient.AuthToken()
	if token != "" {
		w.logger.Debug("[AUTH] Auth token received during registration (length: %d)", len(token))
	} else {
		w.logger.Debug("[AUTH] No auth token received — continuing without token (tokenless requests are allowed)")
	}

	return nil
}

// unregister unregisters the worker from the master server.
func (w *Worker) unregister(ctx context.Context) error {
	w.logger.Debug("Unregistering from master...")
	telemetry.SetHealthRegistered(false)
	return w.apiClient.UnregisterWorker(ctx, w.config.WorkerID)
}

// getHeartbeatInterval returns the appropriate heartbeat interval based on worker status.
func (w *Worker) getHeartbeatInterval() time.Duration {
	w.mu.RLock()
	defer w.mu.RUnlock()

	switch w.status {
	case StatusBusy:
		return heartbeatIntervalBusy
	case StatusError:
		return heartbeatIntervalError
	default:
		return heartbeatIntervalIdle
	}
}

// heartbeatLoop sends periodic heartbeats to the master with adaptive intervals.
func (w *Worker) heartbeatLoop(ctx context.Context) {
	defer w.wg.Done()

	consecutiveErrors := 0
	maxConsecutiveErrors := 5
	currentInterval := w.getHeartbeatInterval()

	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	// Send initial heartbeat immediately
	if err := w.sendHeartbeat(ctx); err != nil {
		logger.LogHeartbeatFailed(w.config.WorkerID, err, 1, maxConsecutiveErrors)
	} else {
		logger.LogHeartbeatSuccess(w.config.WorkerID, string(StatusIdle))
	}

	lastStatus := w.getStatus()

	for {
		select {
		case <-ctx.Done():
			w.logger.Debug("Heartbeat loop exiting (context done)")
			return
		case <-w.stopChan:
			w.logger.Debug("Heartbeat loop exiting (stop signal)")
			return
		case <-ticker.C:
			// Check if status changed and adjust interval
			currentStatus := w.getStatus()
			if currentStatus != lastStatus {
				newInterval := w.getHeartbeatInterval()
				if newInterval != currentInterval {
					w.logger.Debug("[HEARTBEAT] Status changed %s->%s, adjusting interval %v->%v",
						lastStatus, currentStatus, currentInterval, newInterval)
					currentInterval = newInterval
					ticker.Reset(currentInterval)
				}
				lastStatus = currentStatus
			}

			err := w.sendHeartbeat(ctx)
			if err != nil {
				consecutiveErrors++
				logger.LogHeartbeatFailed(w.config.WorkerID, err, consecutiveErrors, maxConsecutiveErrors)

				// Apply exponential backoff on errors
				if consecutiveErrors >= maxConsecutiveErrors {
					currentInterval = time.Duration(float64(currentInterval) * heartbeatBackoffMultiplier)
					if currentInterval > heartbeatMaxBackoff {
						currentInterval = heartbeatMaxBackoff
					}
					w.logger.Warn("[HEARTBEAT_BACKOFF] Applying backoff, next heartbeat in %v",
						currentInterval)
					ticker.Reset(currentInterval)
				}
			} else {
				// Reset on success
				if consecutiveErrors > 0 {
					logger.LogHeartbeatRecover(w.config.WorkerID, consecutiveErrors)
				}
				consecutiveErrors = 0

				// Reset to status-based interval
				newInterval := w.getHeartbeatInterval()
				if newInterval != currentInterval {
					currentInterval = newInterval
					ticker.Reset(currentInterval)
				}
			}
		}
	}
}

// getStatus returns the current worker status (thread-safe).
func (w *Worker) getStatus() Status {
	w.mu.RLock()
	defer w.mu.RUnlock()
	return w.status
}

// sendHeartbeat sends a single heartbeat to the master.
func (w *Worker) sendHeartbeat(ctx context.Context) error {
	w.mu.RLock()
	status := w.status
	w.mu.RUnlock()

	recentLogs, recentErrors := w.recentLogs.Snapshot(300, 100)
	extra := map[string]interface{}{}
	if len(recentLogs) > 0 {
		extra["recent_logs"] = recentLogs
		extra["recent_logs_count"] = len(recentLogs)
	}
	if len(recentErrors) > 0 {
		extra["recent_errors"] = recentErrors
		extra["recent_errors_count"] = len(recentErrors)
	}
	extra["worker_status"] = string(status)
	extra["worker_id"] = w.config.WorkerID
	extra["worker_name"] = w.config.WorkerName
	extra["code_version"] = w.version
	extra["bundle_version"] = w.config.BundleVersion
	extra["bundle_hash"] = w.config.BundleHash
	extra["protocol_version"] = w.config.ProtocolVersion
	extra["engine_version"] = w.config.EngineVersion
	extra["capabilities"] = map[string]interface{}{
		"render_scene_image": true,
		"render_clip_stock":  true,
		"upload_drive":       true,
		"ffmpeg":             true,
		"cpp_engine":         true,
		"max_parallel_jobs":  w.concurrencyLimiter.MaxActiveJobs(),
		"cpu_count":          runtime.NumCPU(),
		"supported_job_types": []string{
			"process_video",
			"render",
			"process_audio",
			"health_check",
		},
	}
	extra["jobs_completed"] = w.jobsCompleted.Load()
	extra["jobs_failed"] = w.jobsFailed.Load()

	// Report all active jobs with their individual progress
	w.activeJobsMu.RLock()
	activeJobList := make([]map[string]interface{}, 0, len(w.activeJobs))
	var primaryJobID string
	for _, aj := range w.activeJobs {
		if primaryJobID == "" {
			primaryJobID = aj.Job.JobID
		}
		jobInfo := map[string]interface{}{
			"job_id":     aj.Job.JobID,
			"job_run_id": aj.Job.JobRunID,
			"job_type":   aj.Job.JobType,
			"priority":   aj.Job.Priority,
			"lease_id":   aj.LeaseID,
			"attempt":    resolveJobAttempt(aj.Job),
		}
		if aj.Progress.Percent > 0 {
			jobInfo["progress_percent"] = aj.Progress.Percent
			jobInfo["progress_scene"] = aj.Progress.Scene
			jobInfo["progress_total"] = aj.Progress.TotalScenes
			if aj.Progress.Stage != "" {
				jobInfo["progress_stage"] = aj.Progress.Stage
			}
		}
		activeJobList = append(activeJobList, jobInfo)
	}
	w.activeJobsMu.RUnlock()

	if len(activeJobList) > 0 {
		extra["active_jobs"] = activeJobList
		extra["active_jobs_count"] = len(activeJobList)
	}

	payload := &api.HeartbeatPayload{
		WorkerID:        w.config.WorkerID,
		WorkerName:      w.config.WorkerName,
		Status:          string(status),
		JobID:           primaryJobID,
		CurrentJob:      primaryJobID,
		CodeVersion:     w.version,
		BundleVersion:   w.config.BundleVersion,
		BundleHash:      w.config.BundleHash,
		ProtocolVersion: w.config.ProtocolVersion,
		EngineVersion:   w.config.EngineVersion,
		Extra:           extra,
	}
	if err := w.apiClient.SendHeartbeat(ctx, payload); err != nil {
		return err
	}
	return nil
}

// leaseRenewLoop sends periodic lease renewals for all active jobs, decoupled from heartbeat.
func (w *Worker) leaseRenewLoop(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(15 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Debug("Lease renew loop exiting (context done)")
			return
		case <-w.stopChan:
			w.logger.Debug("Lease renew loop exiting (stop signal)")
			return
		case <-ticker.C:
			w.activeJobsMu.RLock()
			jobs := make([]*ActiveJob, 0, len(w.activeJobs))
			for _, aj := range w.activeJobs {
				jobs = append(jobs, aj)
			}
			w.activeJobsMu.RUnlock()

			for _, aj := range jobs {
				if aj == nil || aj.Job == nil {
					continue
				}

				leaseID := aj.LeaseID
				if leaseID == "" {
					continue
				}

				leaseExpiry := time.Now().UTC().Add(30 * time.Minute).Format(time.RFC3339)
				attempt := resolveJobAttempt(aj.Job)
				var err error
				if w.config.UseV2Endpoints != nil && *w.config.UseV2Endpoints {
					err = w.apiClient.RenewJobLeaseV2(ctx, aj.Job.JobID, w.config.WorkerID, leaseID, attempt, leaseExpiry)
				} else {
					err = w.apiClient.RenewJobLease(ctx, aj.Job.JobID, w.config.WorkerID, leaseID, attempt, leaseExpiry)
				}
				if err != nil {
					w.logger.Warn("[LEASE] Failed to renew lease for job %s: %v", aj.Job.JobID, err)
				} else {
					w.logger.Debug("[LEASE] Renewed lease for job %s (lease_id=%s)", aj.Job.JobID, leaseID)
				}
			}
		}
	}
}

// calculateBackoff returns the next backoff interval capped at heartbeatMaxBackoff.
func (w *Worker) calculateBackoff(current time.Duration) time.Duration {
	next := time.Duration(float64(current) * heartbeatBackoffMultiplier)
	if next > heartbeatMaxBackoff {
		return heartbeatMaxBackoff
	}
	return next
}
