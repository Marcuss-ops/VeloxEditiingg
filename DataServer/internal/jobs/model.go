package jobs

import (
	"time"

	"velox-server/internal/costmodel"
)

// Costmodel-dependency safety: jobs imports costmodel for the per-job
// Requirements threading (PR-04.5). The reverse direction does NOT
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
	Attempts    int       `json:"attempts"` // retry_count / current attempt number
	Revision    int       `json:"revision"` // optimistic-lock counter (Ondata 3 PR3 final)
	WorkerID    string    `json:"worker_id,omitempty"`
	VideoName   string    `json:"video_name,omitempty"`   // the asset being rendered
	ProjectID   string    `json:"project_id,omitempty"`   // owning project
	RunID       string    `json:"run_id,omitempty"`       // workflow run identifier
	MaxRetries  int       `json:"max_retries"`            // configured retry budget
	LeaseID     string    `json:"lease_id,omitempty"`     // active lease (Ondata 3 PR3)
	StartedAt   time.Time `json:"started_at,omitempty"`   // when work began
	CompletedAt time.Time `json:"completed_at,omitempty"` // when work finished
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
	Payload     string    `json:"-"` // opaque JSON blob (Ondata 3 PR3 final)

	// Requirements is the per-job placement needs consumed by the
	// master-side cost model (PR-04.5). Default zero value
	// (ResourceClass="", TemporalMode="") means "no per-job
	// constraint" — the eligibility layer keeps legacy permissive
	// routing for callers that have not yet migrated to publish
	// explicit requirements.
	//
	// Persisted in two places: dedicated columns
	// `job_required_resource_class` / `job_required_temporal_mode`
	// (SQLite migration 039) AND JSON-only on `request_json` under
	// the `_requirements` subobject. Deterministic + Cacheable live
	// JSON-only inside request_json (rank-only fields, see
	// sqlite_jobs_writer.go::CreateJob).
	Requirements costmodel.JobRequirements `json:"requirements,omitempty"`
}
