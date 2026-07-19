package worker

import (
	"context"
	"time"

	"velox-shared/controltransport"
	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// leaseRenewLoop sends periodic lease renewals for active task-native
// leases via transport.Send(). The renewal targets are read from
// w.SnapshotActiveTaskLeases() (defined in active_lease_registry.go).
//
// PR-2 (canonical-attempt-identity): fires MsgTaskLeaseRenewal for
// every activeTaskLeases entry. Legacy job lease renewal removed.
//
// Intentionally isolated from heartbeat logic — lease renewals and
// heartbeats are independent transports with independent cadences.
func (w *Worker) leaseRenewLoop(ctx context.Context) {
	defer w.wg.Done()

	ticker := time.NewTicker(leaseRenewalInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			w.logger.Debug("Lease renew loop exiting (context done)")
			return
		case <-w.stopChan:
			w.logger.Debug("Lease renew loop exiting (stop signal)")
			return
		case <-ticker.C:
			// PR-2: task-native lease renewals — dispatched via activeTaskLeases.
			// Builds typed pb.TaskLeaseRenewal directly instead of map payload.
			taskLeases := w.SnapshotActiveTaskLeases()
			for _, tl := range taskLeases {
				if tl == nil || tl.TaskID == "" || tl.JobID == "" || tl.AttemptID == "" || tl.LeaseID == "" || tl.AttemptNumber <= 0 {
					continue
				}

				taskExpiry := time.Now().UTC().Add(leaseRenewalExpiry)
				renewal := &pb.TaskLeaseRenewal{
					TaskId:          tl.TaskID,
					JobId:           tl.JobID,
					AttemptId:       tl.AttemptID,
					LeaseId:         tl.LeaseID,
					AttemptNumber:   int32(tl.AttemptNumber),
					Revision:        int32(tl.Revision),
					RequestedExpiry: timestamppb.New(taskExpiry),
				}

				msg := controltransport.NewTypedMessage(
					controltransport.MsgTaskLeaseRenewal,
					w.config.WorkerID,
					w.config.ProtocolVersion,
					renewal,
				)

				if err := w.transport.Send(ctx, msg); err != nil {
					w.logger.Warn("[TASK_LEASE] Failed to renew lease for task %s: %v", tl.TaskID, err)
				} else {
					w.logger.Debug("[TASK_LEASE] Renewed lease for task %s (attempt=%s lease_id=%s)",
						tl.TaskID, tl.AttemptID, tl.LeaseID)
				}
			}
		}
	}
}
