package dashboard

import (
	"context"
	"fmt"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
	"velox-server/internal/queue"
	"velox-server/internal/workers"
)

// Stats returns job statistics matching Python GET /stats
// Response: { total, pending, assigned, processing, completed, error, active_workers_count, active_workers }
func Stats(q *queue.Queue, reg *workers.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		// Get all jobs from Redis
		jobs, err := getAllJobs(ctx, q.Client())
		if err != nil {
			c.JSON(500, gin.H{"ok": false, "error": err.Error()})
			return
		}

		// Count by status
		stats := gin.H{
			"total":      len(jobs),
			"pending":    0,
			"assigned":   0,
			"processing": 0,
			"completed":  0,
			"error":      0,
		}

		for _, job := range jobs {
			status := job["status"]
			switch status {
			case "queued":
				stats["pending"] = stats["pending"].(int) + 1
			case "leased":
				stats["assigned"] = stats["assigned"].(int) + 1
			case "processing":
				stats["processing"] = stats["processing"].(int) + 1
			case "completed":
				stats["completed"] = stats["completed"].(int) + 1
			case "error", "failed", "dead":
				stats["error"] = stats["error"].(int) + 1
			}
		}

		// Add worker info
		workerList := reg.List(ctx)
		var activeWorkers []gin.H
		for _, w := range workerList {
			activeWorkers = append(activeWorkers, gin.H{
				"worker_id":   w.WorkerID,
				"worker_name": w.WorkerName,
				"ip":          w.WorkerName, // compatibility with Python
			})
		}
		stats["active_workers_count"] = len(activeWorkers)
		stats["active_workers"] = activeWorkers

		c.JSON(200, stats)
	}
}

// Jobs returns list of active jobs matching Python shape
// Query params: status (filter by status), limit (max results, default 100)
func Jobs(q *queue.Queue, reg *workers.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		statusFilter := c.Query("status")
		limit := 100
		if l := c.Query("limit"); l != "" {
			if parsed, err := parseInt(l); err == nil && parsed > 0 {
				limit = parsed
			}
		}

		// Get all jobs from Redis
		jobs, err := getAllJobs(ctx, q.Client())
		if err != nil {
			c.JSON(500, gin.H{"ok": false, "error": err.Error()})
			return
		}

		// Filter and transform
		var result []gin.H
		count := 0
		for jobID, job := range jobs {
			if statusFilter != "" && job["status"] != statusFilter {
				continue
			}
			if count >= limit {
				break
			}

			entry := gin.H{
				"job_id":     jobID,
				"status":     job["status"],
				"created_at": job["created_at"],
				"updated_at": job["updated_at"],
				"priority":   job["priority"],
				"attempt":    job["attempt"],
			}

			// Add optional fields
			if v, ok := job["video_name"]; ok {
				entry["video_name"] = v
			}
			if v, ok := job["assigned_worker"]; ok {
				entry["assigned_worker"] = v
			}
			if v, ok := job["assigned_to"]; ok {
				entry["assigned_to"] = v
			}
			if v, ok := job["error"]; ok {
				entry["error"] = v
			}
			if v, ok := job["type"]; ok {
				entry["type"] = v
			}

			result = append(result, entry)
			count++
		}

		c.JSON(200, gin.H{
			"ok":      true,
			"jobs":    result,
			"count":   len(result),
			"total":   len(jobs),
			"filter":  statusFilter,
			"limited": count >= limit,
		})
	}
}

// JobDetail returns detailed info for a single job including logs
func JobDetail(q *queue.Queue) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()
		jobID := c.Param("job_id")

		if jobID == "" {
			c.JSON(400, gin.H{"ok": false, "error": "job_id required"})
			return
		}

		job, err := getJobByID(ctx, q.Client(), jobID)
		if err != nil {
			c.JSON(500, gin.H{"ok": false, "error": err.Error()})
			return
		}
		if job == nil {
			c.JSON(404, gin.H{"ok": false, "error": "job not found"})
			return
		}

		c.JSON(200, gin.H{
			"ok":  true,
			"job": job,
		})
	}
}

// WorkersClearAll clears all processing jobs - admin utility
func WorkersClearAll(q *queue.Queue, reg *workers.Registry) gin.HandlerFunc {
	return func(c *gin.Context) {
		ctx := c.Request.Context()

		// Get all jobs that are processing/leased
		jobs, err := getAllJobs(ctx, q.Client())
		if err != nil {
			c.JSON(500, gin.H{"ok": false, "error": err.Error()})
			return
		}

		cleared := 0
		for jobID, job := range jobs {
			status, _ := job["status"].(string)
			if status == "leased" || status == "processing" {
				// Reset job to queued
				key := queue.JobKey(jobID)
				pipe := q.Client().Pipeline()
				pipe.HSet(ctx, key, "status", "queued", "assigned_worker", "")
				pipe.ZRem(ctx, queue.LeasedKey(), jobID)
				pipe.RPush(ctx, queue.ReadyKey(), jobID)
				if _, err := pipe.Exec(ctx); err == nil {
					cleared++
				}
			}
		}

		c.JSON(200, gin.H{
			"ok":            true,
			"cleared":       cleared,
			"message":       "All processing jobs reset to queued",
			"jobs_affected": cleared,
		})
	}
}

// Helper: get all jobs from Redis
func getAllJobs(ctx context.Context, client *redis.Client) (map[string]map[string]interface{}, error) {
	// Scan for all job keys
	keys, err := client.Keys(ctx, queue.JobKey("*")).Result()
	if err != nil {
		return nil, err
	}

	jobs := make(map[string]map[string]interface{})
	for _, key := range keys {
		data, err := client.HGetAll(ctx, key).Result()
		if err != nil {
			continue
		}
		if len(data) == 0 {
			continue
		}

		// Extract job_id from key (velox:job:XXX -> XXX)
		jobID := key
		if len(key) > len(queue.JobKey("")) {
			jobID = key[len(queue.JobKey("")):]
		}

		// Convert to map[string]interface{}
		job := make(map[string]interface{})
		for k, v := range data {
			job[k] = v
		}
		jobs[jobID] = job
	}

	return jobs, nil
}

// Helper: get single job by ID
func getJobByID(ctx context.Context, client *redis.Client, jobID string) (map[string]interface{}, error) {
	key := queue.JobKey(jobID)
	data, err := client.HGetAll(ctx, key).Result()
	if err != nil {
		return nil, err
	}
	if len(data) == 0 {
		return nil, nil
	}

	job := make(map[string]interface{})
	for k, v := range data {
		job[k] = v
	}
	job["job_id"] = jobID

	return job, nil
}

// Helper: parse int from string
func parseInt(s string) (int, error) {
	var n int
	_, err := fmt.Sscanf(s, "%d", &n)
	return n, err
}
