// Package taskgraph / identity.go
//
// TaskIdentity is the complete master-owned identity used to fence task
// lifecycle mutations. Consumers extract it from a Task (via Identity())
// instead of reaching into the Task struct's fields, so the identity
// concept lives with its data owner.

package taskgraph

// TaskIdentity is the complete task fencing identity. It carries exactly
// the fields the master uses to fence lifecycle mutations: the wire-declared
// identity must match this value before any session/task/attempt/lease
// mutation proceeds.
type TaskIdentity struct {
	TaskID        string
	JobID         string
	AttemptID     string
	LeaseID       string
	AttemptNumber int
	Revision      int
	WorkerID      string
}

// Identity returns the task's fencing identity. A nil receiver yields the
// zero value so callers can safely derive an identity from a missing task.
func (t *Task) Identity() TaskIdentity {
	if t == nil {
		return TaskIdentity{}
	}
	return TaskIdentity{
		TaskID:        t.ID,
		JobID:         t.JobID,
		AttemptID:     t.AttemptID,
		LeaseID:       t.LeaseID,
		AttemptNumber: t.AttemptNumber,
		Revision:      t.Revision,
		WorkerID:      t.WorkerID,
	}
}
