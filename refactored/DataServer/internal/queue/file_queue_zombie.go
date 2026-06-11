package queue

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"
)

// ZombieState represents the current state of a potential zombie job
type ZombieState struct {
	JobID             string        `json:"job_id"`
	DetectedAt        time.Time     `json:"detected_at"`
	LastActivity      time.Time     `json:"last_activity"`
	WorkerID          string        `json:"worker_id"`
	WorkerLastSeen    time.Time     `json:"worker_last_seen,omitempty"`
	Age               time.Duration `json:"age"`
	HasRecentLogs     bool          `json:"has_recent_logs"`
	LogsLastUpdated   time.Time     `json:"logs_last_updated,omitempty"`
	WarningLevel      int           `json:"warning_level"`
	ConsecutiveChecks int           `json:"consecutive_checks"`
	Resolution        string        `json:"resolution,omitempty"`
}

// ZombieHandlerConfig holds configuration for zombie detection
type ZombieHandlerConfig struct {
	WarnThreshold      time.Duration `json:"warn_threshold"`
	CriticalThreshold  time.Duration `json:"critical_threshold"`
	ZombieThreshold    time.Duration `json:"zombie_threshold"`
	MinConsecutiveHits int           `json:"min_consecutive_hits"`
	CheckInterval      time.Duration `json:"check_interval"`
	WorkerOfflineGrace time.Duration `json:"worker_offline_grace"`
	MaxZombieAge       time.Duration `json:"max_zombie_age"`
}

// DefaultZombieHandlerConfig returns sensible defaults
func DefaultZombieHandlerConfig() *ZombieHandlerConfig {
	return &ZombieHandlerConfig{
		WarnThreshold:      10 * time.Minute,
		CriticalThreshold:  20 * time.Minute,
		ZombieThreshold:    30 * time.Minute,
		MinConsecutiveHits: 2,
		CheckInterval:      1 * time.Minute,
		WorkerOfflineGrace: 10 * time.Minute,
		MaxZombieAge:       2 * time.Hour,
	}
}

// ZombieTracker tracks zombie detection state
type ZombieTracker struct {
	mu          sync.RWMutex
	states      map[string]*ZombieState
	config      *ZombieHandlerConfig
	workerState func(workerID string) (lastSeen time.Time, isOnline bool)
	onZombie    func(state *ZombieState, job *Job)
}

// NewZombieTracker creates a new zombie tracker
func NewZombieTracker(cfg *ZombieHandlerConfig) *ZombieTracker {
	if cfg == nil {
		cfg = DefaultZombieHandlerConfig()
	}
	return &ZombieTracker{
		states: make(map[string]*ZombieState),
		config: cfg,
	}
}

// SetWorkerStateProvider sets the callback for checking worker state
func (zt *ZombieTracker) SetWorkerStateProvider(provider func(workerID string) (lastSeen time.Time, isOnline bool)) {
	zt.workerState = provider
}

// SetOnZombieCallback sets the callback when a zombie is confirmed
func (zt *ZombieTracker) SetOnZombieCallback(cb func(state *ZombieState, job *Job)) {
	zt.onZombie = cb
}

// CheckJob evaluates a job for zombie state
func (zt *ZombieTracker) CheckJob(job *Job) *ZombieState {
	zt.mu.Lock()
	defer zt.mu.Unlock()

	if job.Status != StatusProcessing {
		delete(zt.states, job.JobID)
		return nil
	}

	now := time.Now()

	state, exists := zt.states[job.JobID]
	if !exists {
		state = &ZombieState{
			JobID:        job.JobID,
			DetectedAt:   now,
			WorkerID:     job.AssignedTo,
			WarningLevel: 0,
		}
		zt.states[job.JobID] = state
	}

	var lastActivity time.Time
	switch v := job.AssignedAt.(type) {
	case string:
		lastActivity, _ = time.Parse(time.RFC3339, v)
	case float64:
		lastActivity = time.Unix(int64(v), 0)
	}

	var logsUpdated time.Time
	if job.LogsUpdatedAt != "" {
		logsUpdated, _ = time.Parse(time.RFC3339, job.LogsUpdatedAt)
		state.HasRecentLogs = now.Sub(logsUpdated) < zt.config.WarnThreshold
		state.LogsLastUpdated = logsUpdated
		if logsUpdated.After(lastActivity) {
			lastActivity = logsUpdated
		}
	}

	state.LastActivity = lastActivity
	state.Age = now.Sub(lastActivity)

	var workerGrace time.Duration
	if zt.workerState != nil && job.AssignedTo != "" {
		workerLastSeen, isOnline := zt.workerState(job.AssignedTo)
		state.WorkerLastSeen = workerLastSeen
		if isOnline {
			state.WarningLevel = 0
			state.ConsecutiveChecks = 0
			return nil
		}
		if !isOnline {
			workerGrace = zt.config.WorkerOfflineGrace
		}
	}

	effectiveAge := state.Age - workerGrace

	prevLevel := state.WarningLevel
	switch {
	case effectiveAge >= zt.config.ZombieThreshold:
		state.WarningLevel = 3
	case effectiveAge >= zt.config.CriticalThreshold:
		state.WarningLevel = 2
	case effectiveAge >= zt.config.WarnThreshold:
		state.WarningLevel = 1
	default:
		state.WarningLevel = 0
		state.ConsecutiveChecks = 0
	}

	if state.WarningLevel > 0 {
		state.ConsecutiveChecks++
	}

	if state.WarningLevel == 3 && state.ConsecutiveChecks >= zt.config.MinConsecutiveHits {
		return state
	}

	if prevLevel != state.WarningLevel && state.WarningLevel > 0 {
		levelNames := []string{"normal", "warning", "critical", "zombie"}
		log.Printf("🧟 Job %s zombie state: %s (age: %v, consecutive: %d)",
			job.JobID[:8], levelNames[state.WarningLevel], state.Age.Round(time.Second), state.ConsecutiveChecks)
	}

	return nil
}

// ClearState clears zombie tracking state for a job
func (zt *ZombieTracker) ClearState(jobID string) {
	zt.mu.Lock()
	defer zt.mu.Unlock()
	delete(zt.states, jobID)
}

// GetState returns the current zombie state for a job
func (zt *ZombieTracker) GetState(jobID string) *ZombieState {
	zt.mu.RLock()
	defer zt.mu.RUnlock()
	if state, ok := zt.states[jobID]; ok {
		return state
	}
	return nil
}

// GetAllZombieStates returns all current zombie states
func (zt *ZombieTracker) GetAllZombieStates() []*ZombieState {
	zt.mu.RLock()
	defer zt.mu.RUnlock()

	states := make([]*ZombieState, 0, len(zt.states))
	for _, state := range zt.states {
		states = append(states, state)
	}
	return states
}

// Stats returns zombie tracking statistics
func (zt *ZombieTracker) Stats() map[string]interface{} {
	zt.mu.RLock()
	defer zt.mu.RUnlock()

	stats := map[string]interface{}{
		"tracked_jobs": len(zt.states),
		"by_level":     map[string]int{"warning": 0, "critical": 0, "zombie": 0},
	}

	for _, state := range zt.states {
		switch state.WarningLevel {
		case 1:
			stats["by_level"].(map[string]int)["warning"]++
		case 2:
			stats["by_level"].(map[string]int)["critical"]++
		case 3:
			stats["by_level"].(map[string]int)["zombie"]++
		}
	}

	return stats
}

// ZombieAwareFileQueue extends FileQueue with advanced zombie handling
type ZombieAwareFileQueue struct {
	*FileQueue
	zombieTracker *ZombieTracker
	dlq           *DeadLetterQueue
	notifyChan    chan ZombieState
}

// NewZombieAwareFileQueue creates a queue with zombie handling
func NewZombieAwareFileQueue(cfg *FileQueueConfig, zombieCfg *ZombieHandlerConfig, dlq *DeadLetterQueue) (*ZombieAwareFileQueue, error) {
	fq, err := NewFileQueue(cfg)
	if err != nil {
		return nil, err
	}

	if zombieCfg == nil {
		zombieCfg = DefaultZombieHandlerConfig()
	}

	return &ZombieAwareFileQueue{
		FileQueue:     fq,
		zombieTracker: NewZombieTracker(zombieCfg),
		dlq:           dlq,
		notifyChan:    make(chan ZombieState, 50),
	}, nil
}

// SetWorkerStateProvider forwards to zombie tracker
func (zaq *ZombieAwareFileQueue) SetWorkerStateProvider(provider func(workerID string) (lastSeen time.Time, isOnline bool)) {
	zaq.zombieTracker.SetWorkerStateProvider(provider)
}

// ZombieCheckLoop runs periodic zombie detection
func (zaq *ZombieAwareFileQueue) ZombieCheckLoop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			zaq.checkForZombies(ctx)
		case state := <-zaq.notifyChan:
			zaq.handleZombieNotification(ctx, state)
		}
	}
}

// checkForZombies scans all processing jobs for zombie state
func (zaq *ZombieAwareFileQueue) checkForZombies(ctx context.Context) {
	jobs, err := zaq.GetProcessingJobs(ctx)
	if err != nil {
		log.Printf("⚠️ Zombie check failed to load jobs: %v", err)
		return
	}

	for _, job := range jobs {
		if zombieState := zaq.zombieTracker.CheckJob(job); zombieState != nil {
			select {
			case zaq.notifyChan <- *zombieState:
			default:
				zaq.handleZombieNotification(ctx, *zombieState)
			}
		}
	}
}

// handleZombieNotification handles a confirmed zombie job
func (zaq *ZombieAwareFileQueue) handleZombieNotification(ctx context.Context, state ZombieState) {
	job, err := zaq.GetJob(ctx, state.JobID)
	if err != nil {
		zaq.zombieTracker.ClearState(state.JobID)
		return
	}

	if job.Status != StatusProcessing {
		zaq.zombieTracker.ClearState(state.JobID)
		return
	}

	var resolution string
	maxAge := zaq.zombieTracker.config.MaxZombieAge

	if state.Age > maxAge || job.RetryCount >= job.MaxRetries {
		resolution = "dlq"
		if zaq.dlq != nil {
			zaq.dlq.AddJob(ctx, job, "zombie_timeout", fmt.Sprintf("No activity for %v", state.Age))
		}
		zaq.FileQueue.DeleteJob(ctx, state.JobID)
	} else {
		resolution = "requeued"
		zaq.requeueZombieJob(ctx, job, state)
	}

	state.Resolution = resolution
	zaq.zombieTracker.ClearState(state.JobID)

	log.Printf("🧟 Job %s resolved as zombie: %s (age: %v)", state.JobID[:8], resolution, state.Age.Round(time.Second))
}

// requeueZombieJob requeues a zombie job with proper tracking
func (zaq *ZombieAwareFileQueue) requeueZombieJob(ctx context.Context, job *Job, state ZombieState) error {
	zaq.mu.Lock()
	defer zaq.mu.Unlock()

	now := NowUnix()
	nowISOVal := NowISO()

	job.Status = StatusPending
	job.LastError = fmt.Sprintf("Zombie requeue: no activity for %v (worker: %s)", state.Age, state.WorkerID)
	job.LastErrorAt = now
	job.AssignedTo = ""
	job.AssignedAt = nil
	job.ClaimedBy = ""
	job.ClaimedAt = ""
	job.ProcessingAt = nil
	job.RetryCount++
	job.UpdatedAt = now

	job.History = append(job.History, JobHistoryEntry{
		Status:    "PENDING",
		Timestamp: nowISOVal,
		Message:   fmt.Sprintf("Requeued after zombie detection (attempt %d)", job.RetryCount),
	})

	if err := PersistJob(job, zaq.dbStore); err != nil {
		return err
	}

	zaq.logEvent(job.JobID, "zombie_requeued", map[string]interface{}{
		"worker_id":   state.WorkerID,
		"age":         state.Age.String(),
		"retry_count": job.RetryCount,
	})

	return nil
}

// GetZombieStats returns zombie tracking statistics
func (zaq *ZombieAwareFileQueue) GetZombieStats() map[string]interface{} {
	return zaq.zombieTracker.Stats()
}

// GetZombieStates returns all current zombie states
func (zaq *ZombieAwareFileQueue) GetZombieStates() []*ZombieState {
	return zaq.zombieTracker.GetAllZombieStates()
}

// RequeueZombieJobs finds jobs with expired leases and requeues them
func (q *FileQueue) RequeueZombieJobs(ctx context.Context, timeout time.Duration) (int, error) {
	q.mu.Lock()
	defer q.mu.Unlock()

	now := time.Now()
	requeued := 0

	for id, job := range q.activeJobs {
		if job.Status != StatusProcessing {
			continue
		}

		var assignedTime time.Time
		switch v := job.AssignedAt.(type) {
		case string:
			assignedTime, _ = time.Parse(time.RFC3339, v)
		case float64:
			assignedTime = time.Unix(int64(v), 0)
		}

		if assignedTime.IsZero() {
			continue
		}

		if now.Sub(assignedTime) > timeout {
			nowISOVal := NowISO()
			job.Status = StatusPending
			job.LastError = fmt.Sprintf("Zombie: no heartbeat for %v", now.Sub(assignedTime))
			job.LastErrorAt = now.Unix()
			job.AssignedTo = ""
			job.AssignedAt = nil
			job.ClaimedBy = ""
			job.ClaimedAt = ""
			job.RetryCount++

			job.History = append(job.History, JobHistoryEntry{
				Status:    "PENDING",
				Timestamp: nowISOVal,
				Message:   "Requeued after zombie timeout",
			})

			if err := PersistJob(job, q.dbStore); err != nil {
				log.Printf("⚠️ Failed to persist zombie requeue for %s: %v", id, err)
				continue
			}

			requeued++
		}
	}

	return requeued, nil
}
