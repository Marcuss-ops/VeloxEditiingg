package queue

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	"github.com/redis/go-redis/v9"
	"velox-server/internal/config"
)

// Same key layout as refactored/modules/redis_queue/config.py
const (
	keyPrefix     = "velox"
	queueReady    = keyPrefix + ":queue:ready"
	queueLeased   = keyPrefix + ":queue:leased"
	queueDead     = keyPrefix + ":queue:dead"
	jobHashPrefix = keyPrefix + ":job:"
	leaseTTL      = 120
)

func JobKey(jobID string) string { return jobHashPrefix + jobID }
func ReadyKey() string           { return queueReady }
func LeasedKey() string          { return queueLeased }
func DeadKey() string            { return queueDead }

type Queue struct {
	client *redis.Client
	prefix string
}

func New(cfg *config.Config) (*Queue, error) {
	client := redis.NewClient(&redis.Options{
		Addr:     cfg.RedisHost + ":" + cfg.RedisPort,
		DB:       cfg.RedisDB,
		Password: cfg.RedisPassword,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}
	return &Queue{client: client, prefix: cfg.QueuePrefix}, nil
}

func (q *Queue) SubmitJob(ctx context.Context, jobID string, payload map[string]interface{}) error {
	payloadJSON, _ := json.Marshal(payload)
	key := JobKey(jobID)
	pipe := q.client.Pipeline()
	pipe.HSet(ctx, key, map[string]interface{}{
		"job_id": jobID, "status": "queued", "attempt": 0,
		"payload": string(payloadJSON), "type": "video_render", "priority": 10,
		"created_at":      time.Now().UTC().Format(time.RFC3339),
		"assigned_worker": "", "started_at": "", "completed_at": "", "error": "",
	})
	pipe.RPush(ctx, queueReady, jobID)
	_, err := pipe.Exec(ctx)
	return err
}

func (q *Queue) GetNextJobID(ctx context.Context) (string, error) {
	id, err := q.client.LPop(ctx, queueReady).Result()
	if err == redis.Nil {
		return "", nil
	}
	return id, err
}

func (q *Queue) GetJobPayload(ctx context.Context, jobID string) (map[string]interface{}, error) {
	key := JobKey(jobID)
	m, err := q.client.HGetAll(ctx, key).Result()
	if err != nil || len(m) == 0 {
		return nil, err
	}
	var payload map[string]interface{}
	if p := m["payload"]; p != "" {
		_ = json.Unmarshal([]byte(p), &payload)
	}
	return payload, nil
}

func (q *Queue) GetJobAttempt(ctx context.Context, jobID string) (int, error) {
	s, err := q.client.HGet(ctx, JobKey(jobID), "attempt").Result()
	if err == redis.Nil {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	var n int
	_, _ = fmt.Sscanf(s, "%d", &n)
	return n, nil
}

func (q *Queue) LeaseJob(ctx context.Context, jobID, workerID string) error {
	key := JobKey(jobID)
	expiry := time.Now().UnixMilli() + leaseTTL*1000

	// Use EVALSHA/EVAL with Lua script for atomic check-and-set
	// Only leases if status is "queued" or "pending" (not already leased)
	leaseScript := `
		local current = redis.call('HGET', KEYS[1], 'status')
		if current == 'queued' or current == 'pending' then
			redis.call('HSET', KEYS[1], 'status', 'leased', 'assigned_worker', ARGV[1],
				'started_at', ARGV[2], 'attempt', redis.call('HINCRBY', KEYS[1], 'attempt', 1))
			redis.call('ZADD', KEYS[2], ARGV[3], ARGV[4])
			return 1
		else
			return 0
		end
	`
	result, err := q.client.Eval(ctx, leaseScript, []string{key, queueLeased},
		workerID,
		time.Now().UTC().Format(time.RFC3339),
		strconv.FormatInt(expiry, 10),
		jobID,
	).Int()
	if err != nil {
		return fmt.Errorf("lease job %s: %w", jobID, err)
	}
	if result == 0 {
		return fmt.Errorf("job %s not available for leasing (already claimed)", jobID)
	}
	return nil
}

func (q *Queue) CompleteJob(ctx context.Context, jobID string) error {
	key := JobKey(jobID)
	pipe := q.client.Pipeline()
	pipe.HSet(ctx, key, "status", "completed", "completed_at", time.Now().UTC().Format(time.RFC3339))
	pipe.ZRem(ctx, queueLeased, jobID)
	_, err := pipe.Exec(ctx)
	return err
}

func (q *Queue) FailJob(ctx context.Context, jobID, errMsg string, requeue bool) error {
	key := JobKey(jobID)
	pipe := q.client.Pipeline()
	pipe.ZRem(ctx, queueLeased, jobID)
	if requeue {
		pipe.HSet(ctx, key, "status", "queued", "assigned_worker", "", "error", errMsg)
		pipe.RPush(ctx, queueReady, jobID)
	} else {
		pipe.HSet(ctx, key, "status", "dead", "error", errMsg, "completed_at", time.Now().UTC().Format(time.RFC3339))
		pipe.LPush(ctx, queueDead, jobID)
	}
	_, err := pipe.Exec(ctx)
	return err
}

func (q *Queue) Client() *redis.Client { return q.client }

// GetAllJobs returns all jobs (for compatibility with FileQueue interface)
func (q *Queue) GetAllJobs(ctx context.Context) (map[string]*Job, error) {
	// Get all job IDs from ready and leased queues
	readyIDs, _ := q.client.LRange(ctx, queueReady, 0, -1).Result()
	leasedIDs, _ := q.client.ZRange(ctx, queueLeased, 0, -1).Result()
	deadIDs, _ := q.client.LRange(ctx, queueDead, 0, -1).Result()

	allIDs := append(readyIDs, leasedIDs...)
	allIDs = append(allIDs, deadIDs...)

	jobs := make(map[string]*Job)
	for _, jobID := range allIDs {
		key := JobKey(jobID)
		m, err := q.client.HGetAll(ctx, key).Result()
		if err != nil || len(m) == 0 {
			continue
		}

		job := &Job{
			JobID:      jobID,
			Status:     JobStatus(m["status"]),
			AssignedTo: m["assigned_worker"],
			Payload:    make(map[string]interface{}),
		}

		if m["payload"] != "" {
			_ = json.Unmarshal([]byte(m["payload"]), &job.Payload)
		}
		if v, ok := m["attempt"]; ok {
			n, _ := strconv.Atoi(v)
			job.RetryCount = n
		}

		jobs[jobID] = job
	}

	return jobs, nil
}

func (q *Queue) ReadyCount(ctx context.Context) (int64, error) {
	return q.client.LLen(ctx, queueReady).Result()
}

func (q *Queue) LeasedCount(ctx context.Context) (int64, error) {
	return q.client.ZCard(ctx, queueLeased).Result()
}

// RequeueExpiredLeases moves jobs with expired lease (score < now) from leased back to ready.
// Returns the number of jobs requeued. Safe to call periodically from a background goroutine.
func (q *Queue) RequeueExpiredLeases(ctx context.Context) (int, error) {
	now := time.Now().UnixMilli()
	max := strconv.FormatInt(now, 10)
	jobIDs, err := q.client.ZRangeByScore(ctx, queueLeased, &redis.ZRangeBy{Min: "0", Max: max}).Result()
	if err != nil {
		return 0, err
	}
	requeued := 0
	for _, jobID := range jobIDs {
		key := JobKey(jobID)
		status, _ := q.client.HGet(ctx, key, "status").Result()
		if status != "leased" {
			q.client.ZRem(ctx, queueLeased, jobID)
			continue
		}
		pipe := q.client.Pipeline()
		pipe.ZRem(ctx, queueLeased, jobID)
		pipe.HSet(ctx, key, "status", "queued", "assigned_worker", "")
		pipe.RPush(ctx, queueReady, jobID)
		if _, err := pipe.Exec(ctx); err == nil {
			requeued++
		}
	}
	return requeued, nil
}
