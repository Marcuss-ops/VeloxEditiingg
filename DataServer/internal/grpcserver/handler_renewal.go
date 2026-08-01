// Package grpcserver / handler_renewal.go
//
// TaskLeaseRenewal handler, sliced out of handler_jobs.go so each
// task-lifecycle message type owns a file.
package grpcserver

import (
	"context"
	"log"
	"time"

	"velox-server/internal/taskgraph"
	pb "velox-shared/controltransport/pb"
)

// handleTaskRenewal processes a typed TaskLeaseRenewal via gRPC stream.
// fix/identity-tuple-mandatory: the worker sends the full 6-field
// identity tuple on every renewal. We validate all fields are present
// then issue the CAS-backed RenewLease against the live DB revision.
func (h *Handler) handleTaskRenewal(workerID string, tr *pb.TaskLeaseRenewal, sess *workerSession) {
	if tr == nil || h.taskRepo == nil || sess == nil || sess.workerID != workerID {
		return
	}
	ctx := context.Background()
	taskID := tr.GetTaskId()
	jobID := tr.GetJobId()
	attemptID := tr.GetAttemptId()
	leaseID := tr.GetLeaseId()
	attemptNumber := tr.GetAttemptNumber()
	renewalRevision := tr.GetRevision()

	t, err := h.taskRepo.Get(ctx, taskID)
	if err != nil || t == nil {
		log.Printf("[GRPC] TaskLeaseRenewal task %s not found: %v", taskID, err)
		return
	}
	if t.Status != taskgraph.StatusLeased && t.Status != taskgraph.StatusRunning {
		log.Printf("[GRPC] TaskLeaseRenewal from worker %s refused — task %s is not leasable (status=%s)", workerID, taskID, t.Status)
		return
	}
	wireIdentity := taskIdentityFromWire(taskID, jobID, attemptID, leaseID, int(attemptNumber), int(renewalRevision), workerID)
	if err := validateTaskIdentity(wireIdentity, taskIdentityFromTask(t)); err != nil {
		log.Printf("[GRPC] TaskLeaseRenewal from worker %s refused — identity validation failed for task %s: %v", workerID, taskID, err)
		return
	}

	// Fence renewal to the authenticated stream session and hold the
	// registry read lock through the repository CAS. This prevents an old
	// stream from renewing a task after a same-worker reconnect.
	h.mu.RLock()
	defer h.mu.RUnlock()
	if !h.isCurrentSessionLocked(workerID, sess) {
		log.Printf("[GRPC] TaskLeaseRenewal from worker %s refused — session was replaced before CAS for task %s", workerID, taskID)
		return
	}

	expiry := time.Now().UTC().Add(30 * time.Minute)
	if tr.GetRequestedExpiry() != nil {
		expiry = tr.GetRequestedExpiry().AsTime()
	}

	if err := h.taskRepo.RenewLease(ctx, taskID, workerID, leaseID, expiry, int(renewalRevision)); err != nil {
		log.Printf("[GRPC] TaskLeaseRenewal failed for %s (worker %s lease %s): %v",
			taskID, workerID, leaseID, err)
		return
	}
	log.Printf("[GRPC] TaskLeaseRenewal extended task %s for worker %s lease=%s", taskID, workerID, leaseID)
}
