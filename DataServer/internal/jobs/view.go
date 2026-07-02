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
//	QueryService.GetJob        ↔  jobs.ToQueueItem(j)            [wired]
//	QueryService.GetJobPayload ↔  jobs.ToPayloadMap(j)           [next sweep]
//	QueryService.GetJobAsMap   ↔  jobs.ToFlatMap(j)              [next sweep]
//	QueryService.Stats         ↔  jobs.FormatStats(reader.Counts(ctx))  [next sweep]
//
// ToQueueItem is the only projection QueryService currently routes
// through; the other three rows are still aspirational targets.
//
// Pre-existing wire shapes (HTTP/JSON output) are preserved verbatim:
//   - ToQueueItem: WorkerName=WorkerID AND AssignedTo=WorkerID (dual-aliasing)
//   - ToPayloadMap: lease_id injected only when non-empty; LeaseExpiry NOT bound
//     (it has no domain source today)
//   - ToFlatMap: includes blank keys (claimed_by/claimed_at/last_error/error_message/lease_expiry)
//     even when zero-valued — HTTP consumers expect the keys to exist
//   - FormatStats: string-cast of jobs.Status yields uppercase passthrough
//   - ParsePayloadJSON: empty string and "{}" yield an empty map; invalid JSON
//     falls back to an empty map (consumer treats the job as having no payload metadata)
package jobs

import (
	"encoding/json"

	"velox-server/internal/costmodel"
)

// ToQueueItem is the scheduling/transport projection of a Job.
// It carries the full operational state expected by HTTP handlers and legacy consumers.
//
// PR #7: runtime fields (WorkerName, AssignedTo, ClaimedBy, ClaimedAt,
// LeaseID, LeaseExpiry, RetryCount, Attempt) removed — tasks carry these now.
type QueueItem struct {
	JobID        string      `json:"job_id"`
	Status       Status      `json:"status"`
	VideoName    string      `json:"video_name,omitempty"`
	ProjectID    string      `json:"project_id,omitempty"`
	CreatedAt    interface{} `json:"created_at,omitempty"`
	UpdatedAt    interface{} `json:"updated_at,omitempty"`
	StartedAt    interface{} `json:"started_at,omitempty"`
	CompletedAt  interface{} `json:"completed_at,omitempty"`
	ProcessingAt interface{} `json:"processing_at,omitempty"`

	MaxRetries int `json:"max_retries,omitempty"`

	LastError    string `json:"last_error,omitempty"`
	ErrorMessage string `json:"error_message,omitempty"`

	History []JobHistoryEntry `json:"history,omitempty"`

	Logs          []JobLogEntry `json:"logs,omitempty"`
	LogsUpdatedAt string        `json:"logs_updated_at,omitempty"`

	SlotData map[string]interface{} `json:"slot_data,omitempty"`

	JobFingerprint string `json:"job_fingerprint,omitempty"`

	SubmittedVia string `json:"submitted_via,omitempty"`
	LastActivity string `json:"last_activity,omitempty"`
	RunID        string `json:"run_id,omitempty"`

	Payload map[string]interface{} `json:"-"`

	// Requirements is the per-job placement needs surfaced to the
	// transport layer (PR-04.5). Consumers (gRPC workers, future
	// rank sites, HTTP debug endpoints) read this directly. Empty
	// ⇒ no per-job constraint (permissive default preserved).
	Requirements costmodel.JobRequirements `json:"requirements,omitempty"`
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
//
// PR #7: runtime fields (WorkerName, AssignedTo, LeaseID, RetryCount, Attempt)
// removed — tasks carry the per-execution state now. The projection preserves
// the existing wire keys for HTTP consumers but sets them to zero values.
func ToQueueItem(j *Job) *QueueItem {
	if j == nil {
		return nil
	}
	return &QueueItem{
		JobID:        j.ID,
		Status:       j.Status,
		VideoName:    j.VideoName,
		ProjectID:    j.ProjectID,
		MaxRetries:   j.MaxRetries,
		RunID:        j.RunID,
		CreatedAt:    j.CreatedAt,
		UpdatedAt:    j.UpdatedAt,
		StartedAt:    j.StartedAt,
		CompletedAt:  j.CompletedAt,
		Payload:      ParsePayloadJSON(j.Payload),
		Requirements: j.Requirements,
	}
}

// ToPayloadMap projects the job into an enriched payload dictionary,
// mirroring queue.QueryService.GetJobPayload's flattening semantics.
//
// PR #7: LeaseID injection removed — task carries the per-execution lease now.
func ToPayloadMap(j *Job) map[string]interface{} {
	payload := ParsePayloadJSON(j.Payload)
	payload["job_id"] = j.ID
	payload["job_run_id"] = j.RunID
	payload["run_id"] = j.RunID
	payload["status"] = string(j.Status)
	payload["video_name"] = j.VideoName
	payload["project_id"] = j.ProjectID
	// Forward JSON-only keys that the Job struct does NOT carry on its own
	// columns. calendar_event_id is the lead case: it lets the calendar
	// scheduler reconcile jobs back to the originating CalendarEvent without
	// an extra lookup.
	// PR #6: _requirements no longer stored in JSON blobs; Requirements live
	// in dedicated columns and are surfaced via Job.Requirements.
	if cid, ok := payload["calendar_event_id"].(string); ok && cid != "" {
		payload["calendar_event_id"] = cid
	}
	return payload
}

// ToFlatMap returns a job as a flattened map for flexible field access.
//
// PR #7: runtime fields (assigned_to, worker_name, lease_id, retry_count,
// attempt) set to zero — tasks carry the per-execution state.
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
	result["max_retries"] = j.MaxRetries
	result["run_id"] = j.RunID
	result["job_run_id"] = j.RunID

	// PR #7: zero-valued legacy keys preserved for HTTP consumer compat.
	result["assigned_to"] = ""
	result["worker_name"] = ""
	result["lease_id"] = ""
	result["claimed_by"] = ""
	result["claimed_at"] = ""
	result["lease_expiry"] = nil
	result["retry_count"] = 0
	result["attempt"] = 0
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
// Includes a "total" key aggregating all status counts.
func FormatStats(c Counts) map[string]int64 {
	res := make(map[string]int64, len(c)+1)
	var total int64
	for k, v := range c {
		res[string(k)] = v
		total += v
	}
	res["total"] = total
	return res
}
