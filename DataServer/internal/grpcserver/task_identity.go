package grpcserver

import (
	"fmt"

	"velox-server/internal/taskgraph"
)

// taskIdentity aliases the canonical taskgraph.TaskIdentity. The grpc
// fencing helpers previously re-declared the same seven fields locally —
// feature envy of the Task's identity data. The identity value now lives
// in taskgraph (its data owner); this alias keeps the local name readable
// without duplicating the struct.
type taskIdentity = taskgraph.TaskIdentity

func taskIdentityFromTask(task *taskgraph.Task) taskIdentity {
	return task.Identity()
}

func taskIdentityFromWire(taskID, jobID, attemptID, leaseID string, attemptNumber, revision int, workerID string) taskIdentity {
	return taskgraph.TaskIdentity{
		TaskID:        taskID,
		JobID:         jobID,
		AttemptID:     attemptID,
		LeaseID:       leaseID,
		AttemptNumber: attemptNumber,
		Revision:      revision,
		WorkerID:      workerID,
	}
}

func validateTaskIdentityShape(identity taskIdentity, owner string) error {
	if identity.TaskID == "" || identity.JobID == "" || identity.AttemptID == "" ||
		identity.LeaseID == "" || identity.WorkerID == "" || identity.AttemptNumber <= 0 || identity.Revision <= 0 {
		if owner == "" {
			owner = "task"
		}
		return fmt.Errorf("%s has incomplete task identity", owner)
	}
	return nil
}

// validateTaskIdentity rejects incomplete or mismatched identities. It is
// intentionally pure: callers must complete this check before mutating a
// session, task, attempt, or lease.
func validateTaskIdentity(wire, master taskIdentity) error {
	if err := validateTaskIdentityShape(wire, "wire"); err != nil {
		return err
	}
	if err := validateTaskIdentityShape(master, "master task"); err != nil {
		return err
	}

	fields := [][3]interface{}{
		{"task_id", wire.TaskID, master.TaskID},
		{"job_id", wire.JobID, master.JobID},
		{"attempt_id", wire.AttemptID, master.AttemptID},
		{"lease_id", wire.LeaseID, master.LeaseID},
		{"attempt_number", wire.AttemptNumber, master.AttemptNumber},
		{"revision", wire.Revision, master.Revision},
		{"worker_id", wire.WorkerID, master.WorkerID},
	}
	for _, field := range fields {
		if field[1] != field[2] {
			return fmt.Errorf("%s mismatch: wire=%v master=%v", field[0], field[1], field[2])
		}
	}
	return nil
}
