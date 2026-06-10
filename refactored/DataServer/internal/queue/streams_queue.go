package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
)

// Stream status string constants for Redis (prefixed to avoid conflict with JobStatus in file_queue.go)
const (
	StreamStatusQueued     = "QUEUED"
	StreamStatusLeased     = "LEASED"
	StreamStatusProcessing = "PROCESSING"
	StreamStatusCompleted  = "COMPLETED"
	StreamStatusFailed     = "FAILED"
	StreamStatusDead       = "DEAD"
)

// StreamsJob represents a job in the streams queue
type StreamsJob struct {
	JobID     string                 `json:"job_id"`
	Status    string                 `json:"status"`
	WorkerID  string                 `json:"worker_id,omitempty"`
	CreatedAt time.Time              `json:"created_at"`
	UpdatedAt time.Time              `json:"updated_at"`
	LastError string                 `json:"last_error,omitempty"`
	Payload   map[string]interface{} `json:"payload,omitempty"`
}

// StreamsQueue implements a Redis Streams-based job queue
type StreamsQueue struct {
	client     *redis.Client
	streamKey  string
	groupName  string
	consumerID string
}

// StreamsQueueConfig holds configuration for streams queue
type StreamsQueueConfig struct {
	RedisClient *redis.Client
	StreamKey   string
	GroupName   string
	ConsumerID  string
}

// NewStreamsQueue creates a new Redis Streams queue
func NewStreamsQueue(cfg *StreamsQueueConfig) (*StreamsQueue, error) {
	if cfg.StreamKey == "" {
		cfg.StreamKey = "velox:jobs:stream"
	}
	if cfg.GroupName == "" {
		cfg.GroupName = "velox-workers"
	}
	if cfg.ConsumerID == "" {
		cfg.ConsumerID = fmt.Sprintf("consumer-%d", time.Now().UnixNano())
	}

	sq := &StreamsQueue{
		client:     cfg.RedisClient,
		streamKey:  cfg.StreamKey,
		groupName:  cfg.GroupName,
		consumerID: cfg.ConsumerID,
	}

	// Create consumer group if it doesn't exist
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := sq.client.XGroupCreateMkStream(ctx, sq.streamKey, sq.groupName, "0").Err()
	if err != nil && !isGroupExistsErr(err) {
		return nil, fmt.Errorf("failed to create consumer group: %w", err)
	}

	return sq, nil
}

func isGroupExistsErr(err error) bool {
	return err != nil && (err.Error() == "BUSYGROUP Consumer Group name already exists" ||
		containsStr(err.Error(), "BUSYGROUP"))
}

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsStr(s[1:], substr) || s[:len(substr)] == substr)
}

// SubmitJob adds a job to the stream
func (sq *StreamsQueue) SubmitJob(ctx context.Context, jobID string, payload map[string]interface{}) error {
	now := time.Now().UTC()

	job := &StreamsJob{
		JobID:     jobID,
		Status:    StreamStatusQueued,
		CreatedAt: now,
		UpdatedAt: now,
		Payload:   payload,
	}

	jobJSON, err := json.Marshal(job)
	if err != nil {
		return fmt.Errorf("failed to marshal job: %w", err)
	}

	// Add to stream
	args := &redis.XAddArgs{
		Stream: sq.streamKey,
		Values: map[string]interface{}{
			"job_id": jobID,
			"data":   string(jobJSON),
		},
	}

	_, err = sq.client.XAdd(ctx, args).Result()
	return err
}

// GetNextJob retrieves and claims the next job
func (sq *StreamsQueue) GetNextJob(ctx context.Context) (*StreamsJob, error) {
	// Read from stream
	streams, err := sq.client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    sq.groupName,
		Consumer: sq.consumerID,
		Streams:  []string{sq.streamKey, ">"},
		Count:    1,
		Block:    time.Second * 2,
	}).Result()

	if err == redis.Nil {
		return nil, nil // No jobs available
	}
	if err != nil {
		return nil, err
	}

	if len(streams) == 0 || len(streams[0].Messages) == 0 {
		return nil, nil
	}

	msg := streams[0].Messages[0]
	data, ok := msg.Values["data"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid job data in stream")
	}

	var job StreamsJob
	if err := json.Unmarshal([]byte(data), &job); err != nil {
		return nil, fmt.Errorf("failed to unmarshal job: %w", err)
	}

	// Update status
	job.Status = StreamStatusLeased
	job.WorkerID = sq.consumerID
	job.UpdatedAt = time.Now().UTC()

	// Store updated job
	sq.storeJobMeta(ctx, &job)

	return &job, nil
}

// GetJobByID retrieves a job by ID
func (sq *StreamsQueue) GetJobByID(ctx context.Context, jobID string) (*StreamsJob, error) {
	// Check pending entries list first
	pending, err := sq.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: sq.streamKey,
		Group:  sq.groupName,
		Start:  "-",
		End:    "+",
		Count:  1000,
	}).Result()

	if err != nil && err != redis.Nil {
		return nil, err
	}

	// Search in pending
	for _, p := range pending {
		if p.ID == jobID || fmt.Sprintf("%v", p.ID) == jobID {
			// Get the actual message
			msgs, err := sq.client.XRange(ctx, sq.streamKey, jobID, jobID).Result()
			if err == nil && len(msgs) > 0 {
				return sq.parseJobFromMessage(msgs[0])
			}
		}
	}

	// Search in stream
	msgs, err := sq.client.XRange(ctx, sq.streamKey, "-", "+").Result()
	if err != nil {
		return nil, err
	}

	for _, msg := range msgs {
		job, err := sq.parseJobFromMessage(msg)
		if err == nil && job.JobID == jobID {
			return job, nil
		}
	}

	return nil, fmt.Errorf("job not found: %s", jobID)
}

func (sq *StreamsQueue) parseJobFromMessage(msg redis.XMessage) (*StreamsJob, error) {
	data, ok := msg.Values["data"].(string)
	if !ok {
		return nil, fmt.Errorf("invalid job data")
	}

	var job StreamsJob
	if err := json.Unmarshal([]byte(data), &job); err != nil {
		return nil, err
	}

	return &job, nil
}

// storeJobMeta stores job metadata in a hash for quick lookup
func (sq *StreamsQueue) storeJobMeta(ctx context.Context, job *StreamsJob) error {
	jobJSON, err := json.Marshal(job)
	if err != nil {
		return err
	}

	key := fmt.Sprintf("velox:job:meta:%s", job.JobID)
	return sq.client.HSet(ctx, key, map[string]interface{}{
		"data":      string(jobJSON),
		"status":    job.Status,
		"worker_id": job.WorkerID,
		"updated":   job.UpdatedAt.Format(time.RFC3339),
	}).Err()
}

// CompleteJob marks a job as completed
func (sq *StreamsQueue) CompleteJob(ctx context.Context, jobID string) error {
	job, err := sq.GetJobByID(ctx, jobID)
	if err != nil {
		return err
	}

	job.Status = StreamStatusCompleted
	job.UpdatedAt = time.Now().UTC()

	if err := sq.storeJobMeta(ctx, job); err != nil {
		return err
	}

	// Ack the message
	return sq.client.XAck(ctx, sq.streamKey, sq.groupName, jobID).Err()
}

// FailJob marks a job as failed
func (sq *StreamsQueue) FailJob(ctx context.Context, jobID, errMsg string) error {
	job, err := sq.GetJobByID(ctx, jobID)
	if err != nil {
		return err
	}

	job.Status = StreamStatusFailed
	job.LastError = errMsg
	job.UpdatedAt = time.Now().UTC()

	if err := sq.storeJobMeta(ctx, job); err != nil {
		return err
	}

	// Ack the message
	return sq.client.XAck(ctx, sq.streamKey, sq.groupName, jobID).Err()
}

// ListJobs returns all jobs
func (sq *StreamsQueue) ListJobs(ctx context.Context, limit int) ([]*StreamsJob, error) {
	// Get all messages from stream
	msgs, err := sq.client.XRevRange(ctx, sq.streamKey, "+", "-").Result()
	if err != nil {
		return nil, err
	}

	jobs := make([]*StreamsJob, 0, len(msgs))
	count := 0

	for _, msg := range msgs {
		if limit > 0 && count >= limit {
			break
		}

		job, err := sq.parseJobFromMessage(msg)
		if err != nil {
			continue
		}

		// Check metadata for updated status
		key := fmt.Sprintf("velox:job:meta:%s", job.JobID)
		data, err := sq.client.HGet(ctx, key, "data").Result()
		if err == nil && data != "" {
			var updatedJob StreamsJob
			if json.Unmarshal([]byte(data), &updatedJob) == nil {
				job = &updatedJob
			}
		}

		jobs = append(jobs, job)
		count++
	}

	return jobs, nil
}

// GetStats returns queue statistics
func (sq *StreamsQueue) GetStats(ctx context.Context) (map[string]int64, error) {
	info, err := sq.client.XInfoStream(ctx, sq.streamKey).Result()
	if err != nil {
		return nil, err
	}

	pending, err := sq.client.XPending(ctx, sq.streamKey, sq.groupName).Result()
	if err != nil && err != redis.Nil {
		return nil, err
	}

	stats := map[string]int64{
		"total":     info.Length,
		"pending":   0,
		"processed": 0,
	}

	if pending != nil {
		stats["pending"] = int64(pending.Count)
	}

	return stats, nil
}

// ClaimJob claims a pending job that has been idle too long
func (sq *StreamsQueue) ClaimJob(ctx context.Context, jobID string, minIdle time.Duration) error {
	_, err := sq.client.XClaim(ctx, &redis.XClaimArgs{
		Stream:   sq.streamKey,
		Group:    sq.groupName,
		Consumer: sq.consumerID,
		MinIdle:  minIdle,
		Messages: []string{jobID},
	}).Result()
	return err
}

// AckJob acknowledges a job (removes from pending)
func (sq *StreamsQueue) AckJob(ctx context.Context, jobID string) error {
	return sq.client.XAck(ctx, sq.streamKey, sq.groupName, jobID).Err()
}

// PendingCount returns number of pending messages
func (sq *StreamsQueue) PendingCount(ctx context.Context) (int64, error) {
	pending, err := sq.client.XPending(ctx, sq.streamKey, sq.groupName).Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	return int64(pending.Count), nil
}

// GetStreamLength returns the stream length
func (sq *StreamsQueue) GetStreamLength(ctx context.Context) (int64, error) {
	return sq.client.XLen(ctx, sq.streamKey).Result()
}

// DeleteJob removes a job from the stream
func (sq *StreamsQueue) DeleteJob(ctx context.Context, jobID string) error {
	// Delete from stream
	err := sq.client.XDel(ctx, sq.streamKey, jobID).Err()
	if err != nil {
		return err
	}

	// Delete metadata
	key := fmt.Sprintf("velox:job:meta:%s", jobID)
	return sq.client.Del(ctx, key).Err()
}

// GetPendingJobs returns jobs that are pending (claimed but not acknowledged)
func (sq *StreamsQueue) GetPendingJobs(ctx context.Context) ([]*StreamsJob, error) {
	pending, err := sq.client.XPendingExt(ctx, &redis.XPendingExtArgs{
		Stream: sq.streamKey,
		Group:  sq.groupName,
		Start:  "-",
		End:    "+",
		Count:  100,
	}).Result()

	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	jobs := make([]*StreamsJob, 0, len(pending))
	for _, p := range pending {
		msgs, err := sq.client.XRange(ctx, sq.streamKey, p.ID, p.ID).Result()
		if err != nil || len(msgs) == 0 {
			continue
		}

		job, err := sq.parseJobFromMessage(msgs[0])
		if err != nil {
			continue
		}

		// Update with pending info
		job.Status = StreamStatusProcessing
		job.WorkerID = p.Consumer
		jobs = append(jobs, job)
	}

	return jobs, nil
}

// ReclaimStaleJobs reclaims jobs that have been idle too long
func (sq *StreamsQueue) ReclaimStaleJobs(ctx context.Context, maxIdle time.Duration) (int, error) {
	pending, err := sq.GetPendingJobs(ctx)
	if err != nil {
		return 0, err
	}

	reclaimed := 0
	for _, job := range pending {
		// Check idle time - would need to track this properly
		// For now, just mark as available
		job.Status = StreamStatusQueued
		job.WorkerID = ""
		job.UpdatedAt = time.Now().UTC()

		if err := sq.storeJobMeta(ctx, job); err != nil {
			log.Printf("⚠️ Failed to reclaim job %s: %v", job.JobID[:8], err)
			continue
		}
		reclaimed++
	}

	return reclaimed, nil
}

// ParseInt helper
func ParseInt(s string) int {
	n, _ := strconv.Atoi(s)
	return n
}
