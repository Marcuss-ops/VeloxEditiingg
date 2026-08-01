package grpcserver

import (
	"fmt"

	"velox-server/internal/taskgraph"
)

// taskIdentity is the complete master-owned identity used to fence every
// task-lifecycle mutation. workerID is supplied by the authenticated gRPC
// stream because the task messages do not carry a worker_id field.
type taskIdentity struct {
	taskID        string
	jobID         string
	attemptID     string
	leaseID       string
	attemptNumber int
	revision      int
	workerID      string
}

func taskIdentityFromTask(task *taskgraph.Task) taskIdentity {
	if task == nil {
		return taskIdentity{}
	}
	return taskIdentity{
		taskID:        task.ID,
		jobID:         task.JobID,
		attemptID:     task.AttemptID,
		leaseID:       task.LeaseID,
		attemptNumber: task.AttemptNumber,
		revision:      task.Revision,
		workerID:      task.WorkerID,
	}
}

func taskIdentityFromWire(taskID, jobID, attemptID, leaseID string, attemptNumber, revision int, workerID string) taskIdentity {
	return taskIdentity{
		taskID:        taskID,
		jobID:         jobID,
		attemptID:     attemptID,
		leaseID:       leaseID,
		attemptNumber: attemptNumber,
		revision:      revision,
		workerID:      workerID,
	}
}

func validateTaskIdentityShape(identity taskIdentity, owner string) error {
	if identity.taskID == "" || identity.jobID == "" || identity.attemptID == "" ||
		identity.leaseID == "" || identity.workerID == "" || identity.attemptNumber <= 0 || identity.revision <= 0 {
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
		{"task_id", wire.taskID, master.taskID},
		{"job_id", wire.jobID, master.jobID},
		{"attempt_id", wire.attemptID, master.attemptID},
		{"lease_id", wire.leaseID, master.leaseID},
		{"attempt_number", wire.attemptNumber, master.attemptNumber},
		{"revision", wire.revision, master.revision},
		{"worker_id", wire.workerID, master.workerID},
	}
	for _, field := range fields {
		if field[1] != field[2] {
			return fmt.Errorf("%s mismatch: wire=%v master=%v", field[0], field[1], field[2])
		}
	}
	return nil
}
