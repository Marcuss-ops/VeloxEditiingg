// Package jobs view layer.
//
// Phase 1 of Ondata 4 Strategy B: collapse queue.QueryService into the
// canonical jobs package by introducing explicit transport-projection
// helpers. QueueItem (and its sub-types JobHistoryEntry, JobLogEntry)
// move verbatim from internal/queue/file_queue.go to this file as
// type-aliases back so legacy callers continue to compile without churn.
//
// The view methods (ToQueueItem, ToPayloadMap, ToFlatMap, FormatStats)
// are the canonical projections that queue.QueryService is being
// collapsed onto:
//
//   QueryService.GetJob        ↔  jobs.ToQueueItem(j)            [wired]
//   QueryService.GetJobPayload ↔  jobs.ToPayloadMap(j)           [next sweep]
//   QueryService.GetJobAsMap   ↔  jobs.ToFlatMap(j)              [next sweep]
//   QueryService.Stats         ↔  jobs.FormatStats(reader.Counts(ctx))  [next sweep]
//
// ToQueueItem is the only projection QueryService currently routes
// through; the other three rows are still aspirational targets.
//
// Pre-existing wire shapes (HTTP/JSON output) are preserved verbatim:
// - ToQueueItem: WorkerName=WorkerID AND AssignedTo=WorkerID (dual-aliasing)
// - ToPayloadMap: lease_id injected only when non-empty; LeaseExpiry NOT bound
//   (it has no domain source today)
// - ToFlatMap: includes blank keys (claimed_by/claimed_at/last_error/error_message/lease_expiry)
//   even when zero-valued — HTTP consumers expect the keys to exist
// - FormatStats: string-cast of jobs.Status yields uppercase passthrough
// - ParsePayloadJSON: empty string and "{}" yield an empty map; invalid JSON
//   falls back to an empty map (consumer treats the job as having no payload metadata)
package jobs

import "encoding/json"

// QueueItem is the scheduling/transport projection of a Job.
// It carries the full operational state expected by HTTP handlers and legacy consumers.
type QueueItem struct {
	JobID        string      `json:"job_id"`
	Status       Status      `json:"status"`
	VideoName    string      `json:"video_name,omitempty"`
	ProjectID    string      `json:"project_id,omitempty"`
	CreatedAt    interface{} `json:"created_at,omitempty"`
	UpdatedAt    interface{} `json:"updated_at,omitempty"`
	StartedAt    interface{} `json:"started_at,omitempty"`
	CompletedAt  interface{} `json:"completed_at,omitempty"`
	AssignedAt   interface{} `json:"assigned_at,omitempty"`
	LeaseExpiry  interface{} `json:"lease_expiry,omitempty"`
	ProcessingAt interface{} `json:"processing_at,omitempty"`

	AssignedTo       string `json:"assigned_to,omitempty"`
	AssignedWorkerIP string `json:"assigned_worker_ip,omitempty"`
	WorkerName       string `json:"worker_name,omitempty"`
	ClaimedBy        string `json:"claimed_by,omitempty"`
	ClaimedAt        string `json:"claimed_at,omitempty"`
	LeaseID          string `json:"lease_id,omitempty"`

	RetryCount int `json:"retry_count,omitempty"`
	Attempt    int `json:"attempt,omitempty"`
	MaxRetries int `json:"max_retries,omitempty"`

	LastError    string      `json:"last_error,omitempty"`
	LastErrorAt  interface{} `json:"last_error_at,omitempty"`
	ErrorMessage string      `json:"error_message,omitempty"`
	FailedAt     interface{} `json:"failed_at,omitempty"`
	FailedBy     string      `json:"failed_by,omitempty"`

	History []JobHistoryEntry `json:"history,omitempty"`

	Logs          []JobLogEntry `json:"logs,omitempty"`
	LogsUpdatedAt string        `json:"logs_updated_at,omitempty"`

	SlotData map[string]interface{} `json:"slot_data,omitempty"`

	JobFingerprint string `json:"job_fingerprint,omitempty"`

	SubmittedVia string `json:"submitted_via,omitempty"`
	LastActivity string `json:"last_activity,omitempty"`
	RunID        string `json:"run_id,omitempty"`

	Payload map[string]interface{} `json:"-"`
}

// JobHistoryEntry represents a status change in job history.
type JobHistoryEntry struct {
	Status    string      `json:"status"`
	Timestamp interface{} `json:"timestamp"`
	WorkerID  string      `json:"worker_id,omitempty"`
	Message   string      `json:"message,omitempty"`
}

// JobLogEntry represents a log entry from the worker.
type JobLogEntry struct {
	Timestamp string `json:"timestamp,omitempty"`
	Time      string `json:"time,omitempty"`
	Message   string `json:"message,omitempty"`
	Level     string `json:"level,omitempty"`
	IsError   bool   `json:"is_error,omitempty"`
	WorkerID  string `json:"worker_id,omitempty"`
}

// ParsePayloadJSON converts a raw JSON payload string into a map.
// Empty string and "{}" yield an empty map; invalid JSON also yields an
// empty map (the consumer treats the job as having no payload metadata).
// This is the canonical implementation; legacy copies were removed.
func ParsePayloadJSON(raw string) map[string]interface{} {
	if raw == "" || raw == "{}" {
		return make(map[string]interface{})
	}
	var m map[string]interface{}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return make(map[string]interface{})
	}
	return m
}

// ToQueueItem converts a canonical jobs.Job into its transport projection.
// HTTP consumers depend on these legacy dual-aliasing invariants:
//
//   WorkerName = WorkerID  AND  AssignedTo = WorkerID  (legacy dual-aliasing)
//   RetryCount = Attempts   AND  Attempt = Attempts    (legacy split-field aliasing)
//   History/Logs/SlotData/LastError/etc. are zero-valued (no domain source today).
func ToQueueItem(j *Job) *QueueItem {
	if j == nil {
		return nil
	}
	return &QueueItem{
		JobID:       j.ID,
		Status:      j.Status,
		VideoName:   j.VideoName,
		ProjectID:   j.ProjectID,
		WorkerName:  j.WorkerID,
		AssignedTo:  j.WorkerID,
		LeaseID:     j.LeaseID,
		RetryCount:  j.Attempts,
		Attempt:     j.Attempts,
		MaxRetries:  j.MaxRetries,
		RunID:       j.RunID,
		CreatedAt:   j.CreatedAt,
		UpdatedAt:   j.UpdatedAt,
		StartedAt:   j.StartedAt,
		CompletedAt: j.CompletedAt,
		Payload:     ParsePayloadJSON(j.Payload),
	}
}

// ToPayloadMap projects the job into an enriched payload dictionary,
// mirroring queue.QueryService.GetJobPayload's flattening semantics.
//
// Enriches the parsed payload with top-level fields: job_id, job_run_id,
// run_id, status, video_name, project_id, and lease_id (only if non-empty).
// LeaseExpiry is NOT bound (no domain source today).
func ToPayloadMap(j *Job) map[string]interface{} {
	payload := ParsePayloadJSON(j.Payload)
	payload["job_id"] = j.ID
	payload["job_run_id"] = j.RunID
	payload["run_id"] = j.RunID
	payload["status"] = string(j.Status)
	payload["video_name"] = j.VideoName
	payload["project_id"] = j.ProjectID
	if j.LeaseID != "" {
		payload["lease_id"] = j.LeaseID
	}
	return payload
}

// ToFlatMap returns a job as a flattened map for flexible field access,
// mirroring queue.QueryService.GetJobAsMap's exact output shape.
//
// Includes hardcoded scalar fields AND blank keys
// (claimed_by/claimed_at/lease_expiry/last_error/error_message) — HTTP
// consumers expect these keys to exist even when zero-valued.
// Payload JSON keys are merged in only for keys not already in the result.
func ToFlatMap(j *Job) map[string]interface{} {
	result := make(map[string]interface{})
	result["job_id"] = j.ID
	result["status"] = string(j.Status)
	result["video_name"] = j.VideoName
	result["project_id"] = j.ProjectID
	result["created_at"] = j.CreatedAt
	result["updated_at"] = j.UpdatedAt
	result["started_at"] = j.StartedAt
	result["completed_at"] = j.CompletedAt
	result["assigned_to"] = j.WorkerID
	result["worker_name"] = j.WorkerID
	result["lease_id"] = j.LeaseID
	result["retry_count"] = j.Attempts
	result["attempt"] = j.Attempts
	result["max_retries"] = j.MaxRetries
	result["run_id"] = j.RunID
	result["job_run_id"] = j.RunID

	// Emulate zero-values of historical queue.Job map projection for back-compat.
	// HTTP handlers depend on these keys being present (even if zero).
	result["claimed_by"] = ""
	result["claimed_at"] = ""
	result["lease_expiry"] = nil
	result["last_error"] = ""
	result["error_message"] = ""

	if j.Payload != "" {
		pl := ParsePayloadJSON(j.Payload)
		for k, v := range pl {
			if _, exists := result[k]; !exists {
				result[k] = v
			}
		}
	}
	return result
}

// FormatStats converts Reader.Counts into a string-keyed map for HTTP/JSON
// consumers. String-cast of jobs.Status yields uppercase passthrough
// (matches pre-Batch-3 wire format produced by queue.QueryService.Stats).
func FormatStats(c Counts) map[string]int64 {
	res := make(map[string]int64, len(c))
	for k, v := range c {
		res[string(k)] = v
	}
	return res
}
