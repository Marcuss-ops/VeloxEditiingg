package grpcserver

import (
	"strings"
	"testing"

	"velox-server/internal/taskgraph"
)

func TestSecurity_ValidateTaskIdentityRejectsForgedWorkerID(t *testing.T) {
	masterTask := taskgraph.Task{
		ID:            "security-task-1",
		JobID:         "security-job-1",
		AttemptID:     "security-attempt-1",
		AttemptNumber: 1,
		LeaseID:       "security-lease-1",
		WorkerID:      "worker-owner",
		Revision:      4,
	}
	wire := taskIdentityFromTask(&masterTask)
	wire.workerID = "worker-forged"

	err := validateTaskIdentity(wire, taskIdentityFromTask(&masterTask))
	if err == nil || !strings.Contains(err.Error(), "worker_id mismatch") {
		t.Fatalf("forged WorkerID validation error=%v, want worker_id mismatch", err)
	}
}

func TestSecurity_ValidateTaskIdentityRejectsLeaseOwnershipMismatch(t *testing.T) {
	masterTask := taskgraph.Task{
		ID:            "security-task-2",
		JobID:         "security-job-2",
		AttemptID:     "security-attempt-2",
		AttemptNumber: 2,
		LeaseID:       "security-lease-owner",
		WorkerID:      "worker-owner",
		Revision:      9,
	}
	wire := taskIdentityFromTask(&masterTask)
	wire.leaseID = "security-lease-attacker"

	err := validateTaskIdentity(wire, taskIdentityFromTask(&masterTask))
	if err == nil || !strings.Contains(err.Error(), "lease_id mismatch") {
		t.Fatalf("ownership mismatch error=%v, want lease_id mismatch", err)
	}
}
