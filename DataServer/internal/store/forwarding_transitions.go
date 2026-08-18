// Package store / forwarding_transitions.go
//
// State transition methods for creator_forwardings. The CAS and lease guards
// are the state-machine boundary and now live in the internal/forwardingstore
// leaf; this file keeps the historical store method names for existing callers.
package store

import (
	"context"
	"time"
)

// RecordCreatorForwardingPoll updates the poll-tracking fields on a
// creator_forwardings row without changing its status. Lease-fenced.
func (s *SQLiteStore) RecordCreatorForwardingPoll(ctx context.Context, forwardingID, runnerID, leaseID, remoteStatus string, nextPollAt time.Time) error {
	return s.forwarding.RecordCreatorForwardingPoll(ctx, forwardingID, runnerID, leaseID, remoteStatus, nextPollAt)
}

// MarkCreatorForwardingReadyToForward transitions a POLLING forwarding to
// READY_TO_FORWARD after the remote creator has completed.
func (s *SQLiteStore) MarkCreatorForwardingReadyToForward(ctx context.Context, forwardingID, runnerID, leaseID, payloadJSON, payloadSHA256 string) error {
	return s.forwarding.MarkCreatorForwardingReadyToForward(ctx, forwardingID, runnerID, leaseID, payloadJSON, payloadSHA256)
}

// MarkCreatorForwardingForwarding transitions a READY_TO_FORWARD forwarding
// to FORWARDING (short-lived enqueue gate).
func (s *SQLiteStore) MarkCreatorForwardingForwarding(ctx context.Context, forwardingID string) error {
	return s.forwarding.MarkCreatorForwardingForwarding(ctx, forwardingID)
}

// MarkCreatorForwardingForwarded marks a FORWARDING record as FORWARDED
// and stamps target_job_id.
func (s *SQLiteStore) MarkCreatorForwardingForwarded(ctx context.Context, forwardingID, targetJobID string) error {
	return s.forwarding.MarkCreatorForwardingForwarded(ctx, forwardingID, targetJobID)
}

// MarkCreatorForwardingRetry moves a POLLING forwarding to RETRY_WAIT with
// the next attempt scheduled after a backoff delay.
func (s *SQLiteStore) MarkCreatorForwardingRetry(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg, errorClass string, nextAttemptAt time.Time) error {
	return s.forwarding.MarkCreatorForwardingRetry(ctx, forwardingID, runnerID, leaseID, errorCode, errorMsg, errorClass, nextAttemptAt)
}

// MarkCreatorForwardingFailed moves a leasable forwarding to FAILED.
func (s *SQLiteStore) MarkCreatorForwardingFailed(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg, errorClass string) error {
	return s.forwarding.MarkCreatorForwardingFailed(ctx, forwardingID, runnerID, leaseID, errorCode, errorMsg, errorClass)
}

// MarkCreatorForwardingCancelled moves a leasable forwarding to CANCELLED.
func (s *SQLiteStore) MarkCreatorForwardingCancelled(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg string) error {
	return s.forwarding.MarkCreatorForwardingCancelled(ctx, forwardingID, runnerID, leaseID, errorCode, errorMsg)
}

// MarkCreatorForwardingBlocked moves a leasable forwarding to BLOCKED.
func (s *SQLiteStore) MarkCreatorForwardingBlocked(ctx context.Context, forwardingID, runnerID, leaseID, errorCode, errorMsg string) error {
	return s.forwarding.MarkCreatorForwardingBlocked(ctx, forwardingID, runnerID, leaseID, errorCode, errorMsg)
}

// EnsureForwarded is the repair-path idempotency primitive. It stamps
// (status='FORWARDED', target_job_id=jobID) on a forwarding row that is in
// any non-terminal state.
func (s *SQLiteStore) EnsureForwarded(ctx context.Context, forwardingID, jobID string) error {
	return s.forwarding.EnsureForwarded(ctx, forwardingID, jobID)
}
