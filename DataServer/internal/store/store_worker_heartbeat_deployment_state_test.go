package store

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// heartbeatWithDigest builds a canonical worker heartbeat snapshot carrying
// an observed image_digest and the worker-side observation time. The
// observation time travels in the payload (last_heartbeat) so a delayed
// heartbeat keeps its ORIGINAL observation time — it cannot be disguised as
// a fresh one.
func heartbeatWithDigest(workerID, imageDigest string, observedAt time.Time) []byte {
	raw, _ := json.Marshal(map[string]any{
		"worker_id":      workerID,
		"status":         "idle",
		"image_digest":   imageDigest,
		"last_heartbeat": observedAt.UTC().Format(time.RFC3339Nano),
	})
	return raw
}

// TestHeartbeatRunningDigestRequiresAuthoritativeSession pins the central
// spec invariant: running_digest is written ONLY by a heartbeat that arrives
// with an authoritative worker session. A sessionless heartbeat — even one
// carrying an image_digest — must neither create a state row nor claim a
// running digest, and can never overwrite a session-observed digest.
func TestHeartbeatRunningDigestRequiresAuthoritativeSession(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	digestB := deploymentTestDigest('b')
	now := time.Now().UTC()

	if err := s.InsertSession(&PersistedSession{
		SessionID: "sess-wicket",
		WorkerID:  "wicket",
		TokenHash: "wicket-token",
		ExpiresAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("InsertSession: %v", err)
	}

	// 1) Sessionless heartbeat carrying a digest must not claim running.
	if err := s.PersistWorkerHeartbeat(ctx, heartbeatWithDigest("wicket", digestB, now), ""); err != nil {
		t.Fatalf("sessionless heartbeat: %v", err)
	}
	if _, err := s.GetWorkerDeploymentState(ctx, "wicket"); !errors.Is(err, ErrWorkerDeploymentStateNotFound) {
		t.Fatalf("GetWorkerDeploymentState after sessionless heartbeat = %v, want ErrWorkerDeploymentStateNotFound (no state row created)", err)
	}

	// 2) The same digest through an authoritative session is recorded.
	if err := s.PersistWorkerHeartbeat(ctx, heartbeatWithDigest("wicket", digestB, now), "sess-wicket"); err != nil {
		t.Fatalf("authoritative-session heartbeat: %v", err)
	}
	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.RunningDigest != digestB {
		t.Fatalf("RunningDigest = %q, want %q (session-observed)", state.RunningDigest, digestB)
	}

	// 3) A later sessionless heartbeat cannot overwrite the observed digest.
	if err := s.PersistWorkerHeartbeat(ctx, heartbeatWithDigest("wicket", deploymentTestDigest('x'), now.Add(time.Minute)), ""); err != nil {
		t.Fatalf("second sessionless heartbeat: %v", err)
	}
	state, err = s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.RunningDigest != digestB {
		t.Fatalf("RunningDigest = %q, want %q (sessionless heartbeat must not overwrite)", state.RunningDigest, digestB)
	}
}

// TestHeartbeatStaleOldSessionCannotOverwriteRunningDigest pins the
// stale-heartbeat protection: after the worker reconnects (a fresh stream
// session S2 replaces S1 with the same credential), a DELAYED heartbeat still
// carrying the old session S1 must be rejected — it can never rewrite
// running_digest back to the old observation.
func TestHeartbeatStaleOldSessionCannotOverwriteRunningDigest(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	digestA := deploymentTestDigest('a')
	digestB := deploymentTestDigest('b')
	now := time.Now().UTC()

	if err := s.InsertSession(&PersistedSession{
		SessionID: "sess-1", WorkerID: "wicket", TokenHash: "wicket-token", ExpiresAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("InsertSession(sess-1): %v", err)
	}
	// Session S1 observes digest A.
	oldObserved := now.Add(-2 * time.Minute)
	if err := s.PersistWorkerHeartbeat(ctx, heartbeatWithDigest("wicket", digestA, oldObserved), "sess-1"); err != nil {
		t.Fatalf("S1 heartbeat: %v", err)
	}
	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.RunningDigest != digestA {
		t.Fatalf("RunningDigest after S1 = %q, want %q", state.RunningDigest, digestA)
	}

	// Worker reconnects: same credential, fresh stream session. InsertSession
	// demotes + revokes sess-1 and admits sess-2.
	if err := s.InsertSession(&PersistedSession{
		SessionID: "sess-2", WorkerID: "wicket", TokenHash: "wicket-token", ExpiresAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("InsertSession(sess-2): %v", err)
	}
	// Session S2 observes digest B.
	if err := s.PersistWorkerHeartbeat(ctx, heartbeatWithDigest("wicket", digestB, now), "sess-2"); err != nil {
		t.Fatalf("S2 heartbeat: %v", err)
	}
	state, err = s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.RunningDigest != digestB {
		t.Fatalf("RunningDigest after S2 = %q, want %q", state.RunningDigest, digestB)
	}

	// Delayed heartbeat from the OLD session arrives late: rejected (sess-1
	// was revoked on reconnect) and running_digest stays B.
	err = s.PersistWorkerHeartbeat(ctx, heartbeatWithDigest("wicket", digestA, oldObserved), "sess-1")
	if !errors.Is(err, ErrTransitionConflict) {
		t.Fatalf("delayed S1 heartbeat error = %v, want ErrTransitionConflict", err)
	}
	state, err = s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.RunningDigest != digestB {
		t.Fatalf("RunningDigest = %q, want %q (stale old-session heartbeat must not overwrite)", state.RunningDigest, digestB)
	}
}

// TestHeartbeatNeverMarksRolloutSuccessful pins the final invariant: a
// heartbeat only records what the worker is RUNNING. It must never advance
// last_successful_digest, never flip last_operation_status, and never
// rewrite desired_digest — those dimensions belong to the rollout state
// machine, not to worker observation.
func TestHeartbeatNeverMarksRolloutSuccessful(t *testing.T) {
	s := newDeploymentTestStore(t)
	ctx := context.Background()
	digestA := deploymentTestDigest('a')
	digestB := deploymentTestDigest('b')
	now := time.Now().UTC()

	if err := s.InsertSession(&PersistedSession{
		SessionID: "sess-wicket", WorkerID: "wicket", TokenHash: "wicket-token", ExpiresAt: now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("InsertSession: %v", err)
	}

	// Rollout history: SUCCEEDED to A (baseline), then a FAILED rollout to B.
	if err := s.InsertBaselineDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID: "deploy-success-a", WorkerID: "wicket",
		TargetDigest: digestA,
		StartedAt:    now.Add(-10 * time.Minute),
		FinishedAt:   deploymentTimePtr(now.Add(-10 * time.Minute)),
		Status:       DeployStatusSucceeded,
		AppliedBy:    "bootstrap",
	}); err != nil {
		t.Fatalf("InsertBaselineDeploymentRecord: %v", err)
	}
	if err := s.InsertDeploymentRecord(ctx, DeploymentRecord{
		DeploymentID:   "deploy-failed-b",
		WorkerID:       "wicket",
		PreviousDigest: digestA,
		TargetDigest:   digestB,
		StartedAt:      now.Add(-5 * time.Minute),
		Status:         DeployStatusPending,
		AppliedBy:      "fleetctl",
	}); err != nil {
		t.Fatalf("InsertDeploymentRecord: %v", err)
	}
	if err := s.UpdateDeploymentStatus(ctx, "deploy-failed-b", DeployStatusFailed, now.Add(-4*time.Minute)); err != nil {
		t.Fatalf("UpdateDeploymentStatus(FAILED): %v", err)
	}

	// Heartbeats observe digest C — drift from the FAILED target B.
	digestC := deploymentTestDigest('c')
	if err := s.PersistWorkerHeartbeat(ctx, heartbeatWithDigest("wicket", digestC, now), "sess-wicket"); err != nil {
		t.Fatalf("heartbeat: %v", err)
	}

	state, err := s.GetWorkerDeploymentState(ctx, "wicket")
	if err != nil {
		t.Fatalf("GetWorkerDeploymentState: %v", err)
	}
	if state.RunningDigest != digestC {
		t.Fatalf("RunningDigest = %q, want %q (heartbeat observes C)", state.RunningDigest, digestC)
	}
	// Rollout dimensions untouched: intent B survives, last-known-good A
	// survives, and the FAILED operation survives. A heartbeat must NEVER be
	// able to mark a rollout SUCCEEDED.
	if state.DesiredDigest != digestB {
		t.Fatalf("DesiredDigest = %q, want %q (heartbeat must not rewrite intent)", state.DesiredDigest, digestB)
	}
	if state.LastSuccessfulDigest != digestA {
		t.Fatalf("LastSuccessfulDigest = %q, want %q (heartbeat must not advance last-known-good)", state.LastSuccessfulDigest, digestA)
	}
	if state.LastOperationID != "deploy-failed-b" || state.LastOperationStatus != DeployStatusFailed {
		t.Fatalf("last operation = %s/%s, want deploy-failed-b/FAILED (heartbeat must not mark SUCCEEDED)", state.LastOperationID, state.LastOperationStatus)
	}
}
