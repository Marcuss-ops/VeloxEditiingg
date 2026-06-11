// Package renderplan provides the RenderPlan v1 contract for job validation.
package renderplan

import (
	"crypto/sha256"
	"fmt"
)

// RenderPlan represents the v1 contract for job rendering.
type RenderPlan struct {
	Version        string                 `json:"version"`
	JobID          string                 `json:"job_id"`
	JobType        string                 `json:"job_type"`
	CreatedAt      string                 `json:"created_at"`
	JobRunID       string                 `json:"job_run_id,omitempty"`
	JobName        string                 `json:"job_name,omitempty"`
	Priority       int                    `json:"priority,omitempty"`
	MaxRetries     int                    `json:"max_retries,omitempty"`
	TimeoutSecs    int                    `json:"timeout_secs,omitempty"`
	AssignedWorker string                 `json:"assigned_worker,omitempty"`
	WorkerGroup    string                 `json:"worker_group,omitempty"`
	ParentJob      string                 `json:"parent_job,omitempty"`
	Parameters     map[string]interface{} `json:"parameters,omitempty"`
	Metadata       map[string]interface{} `json:"metadata,omitempty"`
	Tags           []string               `json:"tags,omitempty"`
	ValidatedAt    string                 `json:"validated_at,omitempty"`
}

// ToMap converts the RenderPlan to a map for API compatibility.
func (rp *RenderPlan) ToMap() map[string]interface{} {
	m := map[string]interface{}{
		"version": rp.Version, "job_id": rp.JobID, "job_type": rp.JobType, "created_at": rp.CreatedAt,
	}
	if rp.JobRunID != "" { m["job_run_id"] = rp.JobRunID }
	if rp.JobName != "" { m["job_name"] = rp.JobName }
	if rp.Priority != 0 { m["priority"] = rp.Priority }
	if rp.MaxRetries != 0 { m["max_retries"] = rp.MaxRetries }
	if rp.TimeoutSecs != 0 { m["timeout_secs"] = rp.TimeoutSecs }
	if rp.AssignedWorker != "" { m["assigned_worker"] = rp.AssignedWorker }
	if rp.WorkerGroup != "" { m["worker_group"] = rp.WorkerGroup }
	if rp.ParentJob != "" { m["parent_job"] = rp.ParentJob }
	if len(rp.Parameters) > 0 { m["parameters"] = rp.Parameters }
	if len(rp.Metadata) > 0 { m["metadata"] = rp.Metadata }
	if len(rp.Tags) > 0 { m["tags"] = rp.Tags }
	if rp.ValidatedAt != "" { m["validated_at"] = rp.ValidatedAt }
	return m
}

// FromMap creates a RenderPlan from a map.
func FromMap(m map[string]interface{}) *RenderPlan {
	rp := &RenderPlan{
		Parameters: make(map[string]interface{}),
		Metadata:   make(map[string]interface{}),
		Tags:       make([]string, 0),
	}
	if v, ok := m["version"].(string); ok { rp.Version = v }
	if v, ok := m["job_id"].(string); ok { rp.JobID = v }
	if v, ok := m["job_type"].(string); ok { rp.JobType = v }
	if v, ok := m["created_at"].(string); ok { rp.CreatedAt = v }
	if v, ok := m["job_run_id"].(string); ok { rp.JobRunID = v }
	if v, ok := m["job_name"].(string); ok { rp.JobName = v }
	if v, ok := m["priority"].(float64); ok { rp.Priority = int(v) }
	if v, ok := m["max_retries"].(float64); ok { rp.MaxRetries = int(v) }
	if v, ok := m["timeout_secs"].(float64); ok { rp.TimeoutSecs = int(v) }
	if v, ok := m["assigned_worker"].(string); ok { rp.AssignedWorker = v }
	if v, ok := m["worker_group"].(string); ok { rp.WorkerGroup = v }
	if v, ok := m["parent_job"].(string); ok { rp.ParentJob = v }
	if v, ok := m["parameters"].(map[string]interface{}); ok { rp.Parameters = v }
	if v, ok := m["metadata"].(map[string]interface{}); ok { rp.Metadata = v }
	if v, ok := m["tags"].([]interface{}); ok {
		for _, item := range v {
			if s, ok := item.(string); ok { rp.Tags = append(rp.Tags, s) }
		}
	}
	return rp
}

// GenerateIdempotencyKey generates a deterministic idempotency key.
func GenerateIdempotencyKey(jobID, jobRunID, operation string) string {
	h := sha256.New()
	h.Write([]byte(fmt.Sprintf("%s:%s:%s", jobID, jobRunID, operation)))
	return fmt.Sprintf("%x", h.Sum(nil))[:32]
}
