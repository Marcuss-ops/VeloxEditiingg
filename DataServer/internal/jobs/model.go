package jobs

import "time"

// Job is the canonical domain model for a render job.
//
// It is the business aggregate — NOT a DB row, NOT an HTTP projection,
// NOT a worker transport. Every field here is owned by the domain;
// persistence details (sql.NullString, raw JSON blobs) live in
// store.JobRecord (future rename of the current store.Job struct).
//
// Workers receive a proto.JobAssignment (future); HTTP handlers
// project into queue.JobView. Neither is a jobs.Job.
type Job struct {
	ID        string    `json:"id"`
	Type      string    `json:"type,omitempty"` // job_type from request payload
	Status    Status    `json:"status"`
	Attempts  int       `json:"attempts"`
	WorkerID  string    `json:"worker_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}
