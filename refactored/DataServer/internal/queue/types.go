// Package queue internal/domain types.
//
// JobRequest, JobResultSummary, JobView are the clean, sub-structured types
// used by the new domain model. The legacy flat fields (MasterVideoPath,
// DriveURL, VideoUploaded, ArtifactID, OutputSHA256, UploadIdempotencyKey,
// LastUploadResult, LastDriveUploadResult, RemoteStatus, OutputVideoID)
// are NOT exposed here. They are computed on demand by JobViewAssembler.
//
// File intentionally has zero side-effecting code so it can be imported by
// both the worker download path (read-only) and the orchestrator (read-write).
package queue

import "time"

// JobRequest captures the immutable input of a job as submitted by the
// creator + the enqueue validation. This blob is written ONCE into
// jobs.request_json and never mutated afterwards.
//
// Removing status/attempt/lease/history/logs/upload from the request blob
// keeps the persistence layout structured: each transient piece of state
// has its own column or table.
type JobRequest struct {
	VideoName    string                 `json:"video_name,omitempty"`
	ProjectID    string                 `json:"project_id,omitempty"`
	JobType      string                 `json:"job_type"`
	TemplateID   string                 `json:"template_id,omitempty"`
	SlotData     map[string]interface{} `json:"slot_data,omitempty"`
	CustomFields map[string]interface{} `json:"custom_fields,omitempty"`
}

// JobResultSummary is the post-render result summary written at job completion
// time. It is NOT a free-for-all blob: only master-owned, completion-verified
// fields live here. The legacy master_video_path / drive_url artifacts are
// computed by JobViewAssembler from artifacts + job_deliveries.
type JobResultSummary struct {
	PrimaryArtifactID string    `json:"primary_artifact_id,omitempty"`
	ArtifactCount     int       `json:"artifact_count"`
	CompletedAt       time.Time `json:"completed_at"`
	// LatencyMs is the wall-clock duration between started_at and
	// completed_at. The legacy "duration_ms" tooling can still surface this
	// through a JobViewAssembler join.
	LatencyMs int64 `json:"latency_ms,omitempty"`
}

// JobView is the JSON-compatible assembled projection of a job that the
// legacy HTTP endpoints still expect. It is NOT the domain Job — it is the
// pre-aggregated join across (jobs, artifacts, job_deliveries, deliveries,
// delivery_attempts) the JobViewAssembler produces.
//
// Anything not in this struct must NOT be readable through the legacy API.
// New code should use the *Job struct and load what it needs explicitly.
type JobView struct {
	JobID           string           `json:"job_id"`
	Type            string           `json:"job_type,omitempty"`
	Status          JobStatus        `json:"status"`
	Revision        int64            `json:"revision,omitempty"`
	CurrentAttemptID *int64           `json:"current_attempt_id,omitempty"`

	VideoName       string           `json:"video_name,omitempty"`
	ProjectID       string           `json:"project_id,omitempty"`
	CreatedAt       time.Time        `json:"created_at"`
	UpdatedAt       time.Time        `json:"updated_at"`
	StartedAt       *time.Time       `json:"started_at,omitempty"`
	CompletedAt     *time.Time       `json:"completed_at,omitempty"`

	// Legacy-derived fields (computed by JobViewAssembler).
	VideoUploaded   bool             `json:"video_uploaded,omitempty"`
	MasterVideoPath string           `json:"master_video_path,omitempty"`
	DriveURL        string           `json:"drive_url,omitempty"`
	DriveFolderID   string           `json:"drive_folder_id,omitempty"`
	YouTubeVideoID  string           `json:"youtube_video_id,omitempty"`
	YouTubeURL      string           `json:"youtube_url,omitempty"`

	LastErrorCode    string          `json:"last_error_code,omitempty"`
	LastErrorMessage string          `json:"last_error_message,omitempty"`
}
