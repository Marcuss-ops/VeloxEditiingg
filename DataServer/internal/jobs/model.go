package jobs

import "time"

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
	ID         string    `json:"id"`
	Type       string    `json:"type,omitempty"`       // job_type from request payload
	Status     Status    `json:"status"`
	Attempts   int       `json:"attempts"`
	WorkerID   string    `json:"worker_id,omitempty"`
	VideoName  string    `json:"video_name,omitempty"` // the asset being rendered
	ProjectID  string    `json:"project_id,omitempty"` // owning project
	RunID      string    `json:"run_id,omitempty"`     // workflow run identifier
	MaxRetries int       `json:"max_retries"`         // configured retry budget
	CreatedAt  time.Time `json:"created_at"`
	UpdatedAt  time.Time `json:"updated_at"`
}
