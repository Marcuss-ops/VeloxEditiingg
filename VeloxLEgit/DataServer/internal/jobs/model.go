package jobs

import (
	"time"

	"velox-server/internal/costmodel"
)

// Costmodel-dependency safety: jobs imports costmodel for the per-job
// Requirements threading. The reverse direction does NOT
// hold — costmodel has no compile dependency on jobs (only on
// "strings"). Adding Requirements to Job.Requirements is therefore a
// forward-only edge and introduces no import cycle.

// Job is the canonical domain model for a render job.
//
// It is the business aggregate — NOT a DB row, NOT an HTTP projection,
// NOT a worker transport. Every field here is owned by the domain;
// persistence details (sql.NullString, raw JSON blobs) live in
// store.JobRecord (the persistence projection, renamed from Job in Ondata 3 PR4).
//
// Workers receive a proto.JobAssignment (future); HTTP handlers
// project into queue.JobView. Neither is a jobs.Job.
type Job struct {
	ID          string    `json:"id"`
	Type        string    `json:"type,omitempty"` // job_type from request payload
	Status      Status    `json:"status"`
	Attempts    int       `json:"attempts"`               // retry_count / current attempt number
	Revision    int       `json:"revision"`               // optimistic-lock counter (Ondata 3 PR3 final)
	VideoName   string    `json:"video_name,omitempty"`   // the asset being rendered
	ProjectID   string    `json:"project_id,omitempty"`   // owning project
	RunID       string    `json:"run_id,omitempty"`       // workflow run identifier
	MaxRetries  int       `json:"max_retries"`            // configured retry budget
	StartedAt   time.Time `json:"started_at,omitempty"`   // when work began
	CompletedAt time.Time `json:"completed_at,omitempty"` // when work finished
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Payload     string    `json:"-"` // opaque JSON blob (Ondata 3 PR3 final)

	// Requirements is the per-job placement needs consumed by the
	// master-side cost model (PR #6). Default zero value
	// (ResourceClass="", TemporalMode="") means "no per-job
	// constraint" — the eligibility layer keeps legacy permissive
	// routing for callers that have not yet migrated to publish
	// explicit requirements.
	//
	// Persisted via dedicated columns (job_required_resource_class,
	// job_required_temporal_mode, job_required_deterministic,
	// job_required_cacheable, job_required_min_bandwidth_mbps).
	// No JSON fallback exists.
	Requirements costmodel.JobRequirements `json:"requirements,omitempty"`
}
