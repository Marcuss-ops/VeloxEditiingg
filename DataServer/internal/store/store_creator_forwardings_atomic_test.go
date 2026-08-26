package store

import (
	"context"
	"testing"
	"time"
	"velox-server/internal/costmodel"
	"velox-server/internal/deliverystore"
	"velox-server/internal/forwardingcontract"
	"velox-server/internal/forwardingstore"
	"velox-server/internal/jobs"
	"velox-server/internal/taskgraph"
)

func TestAtomicForwardAndEnqueue_CreatesJobAndMarksForwarded(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	if err := db.Delivery().InsertDeliveryDestination(&deliverystore.DeliveryDestination{
		DestinationID: "drive",
		Provider:      "drive",
		Name:          "drive",
		Enabled:       true,
	}); err != nil {
		t.Fatalf("seed delivery destination: %v", err)
	}

	payloadJSON := `{"video":"test","delivery_plan":[{"destination_id":"drive","priority":1,"retry_budget":3}]}`
	payload := map[string]any{
		"video": "test",
		"delivery_plan": []any{
			map[string]any{
				"destination_id": "drive",
				"priority":       1,
				"retry_budget":   3,
			},
		},
	}

	// Arrange: insert a READY_TO_FORWARD forwarding with stored payload.
	insertTestForwardingWithPayload(t, db, "cf-atomic", "openai", "creator-atomic",
		"scene.composite.v1", "READY_TO_FORWARD", payloadJSON, "abc")

	// Build a minimal Job and TaskSpec.
	job := &jobs.Job{
		ID:        "job-atomic-001",
		Type:      "process_video",
		Status:    jobs.StatusPending,
		VideoName: "AtomicTest",
		RunID:     "run-atomic",
		Payload:   payloadJSON,
		Requirements: costmodel.JobRequirements{
			ResourceClass: "GPU",
			Deterministic: true,
		},
	}
	spec := &taskgraph.TaskSpec{
		Version:    taskgraph.SpecVersion,
		JobID:      "job-atomic-001",
		ExecutorID: "scene.composite.v1@1",
		Payload:    payload,
	}

	// Act: atomic enqueue + forward.
	err := db.Forwarding().AtomicForwardAndEnqueue(ctx, "cf-atomic", job, spec, 5, "", "")
	if err != nil {
		t.Fatalf("AtomicForwardAndEnqueue: %v", err)
	}

	// Assert: forwarding is FORWARDED with target_job_id.
	cf, err := db.Forwarding().GetCreatorForwarding(ctx, "cf-atomic")
	if err != nil {
		t.Fatalf("GetCreatorForwarding: %v", err)
	}
	if cf.Status != "FORWARDED" {
		t.Errorf("status = %q, want FORWARDED", cf.Status)
	}
	if cf.TargetJobID != "job-atomic-001" {
		t.Errorf("target_job_id = %q, want job-atomic-001", cf.TargetJobID)
	}
	if cf.ForwardedAt == "" {
		t.Error("forwarded_at should be set")
	}

	// Assert: Job row exists.
	jobRepo := NewSQLiteJobRepository(db)
	savedJob, err := jobRepo.Get(ctx, "job-atomic-001")
	if err != nil || savedJob == nil {
		t.Fatalf("Get job: err=%v", err)
	}
	if savedJob.ID != "job-atomic-001" {
		t.Errorf("job ID = %q, want job-atomic-001", savedJob.ID)
	}
	if savedJob.Status != jobs.StatusPending {
		t.Errorf("job status = %q, want PENDING", savedJob.Status)
	}
}

func TestAtomicForwardAndEnqueue_FencesWrongRunnerLease(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()
	insertTestForwardingWithPayload(t, db, "cf-owner-fence", "openai", "creator-owner-fence",
		"scene.composite.v1", "READY_TO_FORWARD", `{"video":"test"}`, "abc")
	if _, err := db.db.ExecContext(ctx, `UPDATE creator_forwardings
		SET locked_by = ?, lease_id = ?, lease_expires_at = ? WHERE forwarding_id = ?`,
		"current-runner", "current-lease", time.Now().UTC().Add(time.Minute).Format(time.RFC3339), "cf-owner-fence"); err != nil {
		t.Fatalf("seed owner lease: %v", err)
	}

	err := db.Forwarding().AtomicForwardAndEnqueue(ctx, "cf-owner-fence", &jobs.Job{ID: "job-owner-fence", Type: "process_video"},
		&taskgraph.TaskSpec{Version: taskgraph.SpecVersion, JobID: "job-owner-fence", ExecutorID: "scene.composite.v1@1"},
		5, "stale-runner", "stale-lease")
	if err != forwardingstore.ErrTransitionConflict {
		t.Fatalf("wrong runner atomic enqueue error = %v, want forwardingstore.ErrTransitionConflict", err)
	}
	cf, err := db.Forwarding().GetCreatorForwarding(ctx, "cf-owner-fence")
	if err != nil {
		t.Fatalf("GetCreatorForwarding: %v", err)
	}
	if cf.Status != "READY_TO_FORWARD" || cf.LockedBy != "current-runner" || cf.LeaseID != "current-lease" {
		t.Fatalf("wrong runner changed forwarding: status=%q owner=(%q,%q)", cf.Status, cf.LockedBy, cf.LeaseID)
	}
}

func TestAtomicForwardAndEnqueue_ConflictWhenAlreadyClaimed(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	// Insert a READY_TO_FORWARD row and claim it (simulating another runner).
	insertTestForwardingWithPayload(t, db, "cf-conflict", "openai", "creator-conflict",
		"scene.composite.v1", "READY_TO_FORWARD", `{"v":"1"}`, "abc")

	// Manually transition to FORWARDING (simulating another runner claimed it).
	_, err := db.db.ExecContext(ctx,
		`UPDATE creator_forwardings SET status = 'FORWARDING' WHERE forwarding_id = ?`,
		"cf-conflict")
	if err != nil {
		t.Fatalf("manual transition: %v", err)
	}

	// Attempt atomic enqueue — should fail because it's no longer READY_TO_FORWARD.
	job := &jobs.Job{ID: "job-conflict", Type: "process_video", Status: jobs.StatusPending,
		VideoName: "Conflict", RunID: "run-conflict"}
	spec := &taskgraph.TaskSpec{Version: taskgraph.SpecVersion, JobID: "job-conflict",
		ExecutorID: "scene.composite.v1@1"}

	err = db.Forwarding().AtomicForwardAndEnqueue(ctx, "cf-conflict", job, spec, 5, "", "")
	if err != forwardingstore.ErrTransitionConflict {
		t.Errorf("expected forwardingstore.ErrTransitionConflict, got %v", err)
	}

	// Verify forwarding is still FORWARDING (not mutated).
	cf, err := db.Forwarding().GetCreatorForwarding(ctx, "cf-conflict")
	if err != nil {
		t.Fatalf("GetCreatorForwarding: %v", err)
	}
	if cf.Status != "FORWARDING" {
		t.Errorf("status = %q, want FORWARDING (unchanged)", cf.Status)
	}
	if cf.TargetJobID != "" {
		t.Error("target_job_id should be empty after failed atomic enqueue")
	}
}

func TestMarkCreatorForwardingEnqueueRetry_FromForwarding(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	// Insert a FORWARDING record (simulating failed atomic enqueue).
	cf := &forwardingcontract.CreatorForwarding{
		ForwardingID:     "cf-enq-retry",
		SourceProvider:   "openai",
		SourceJobID:      "creator-enq-retry",
		TargetExecutorID: "scene.composite.v1",
		Status:           "FORWARDING",
		PayloadJSON:      `{"video":"test"}`,
		PayloadSHA256:    "abc",
		CreatedAt:        time.Now().UTC().Format(time.RFC3339),
		UpdatedAt:        time.Now().UTC().Format(time.RFC3339),
	}
	if _, err := db.Forwarding().InsertCreatorForwarding(ctx, cf); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if _, err := db.db.ExecContext(ctx, `UPDATE creator_forwardings
		SET locked_by = ?, lease_id = ?, lease_expires_at = ? WHERE forwarding_id = ?`,
		"runner-enqueue", "lease-enqueue", time.Now().UTC().Add(time.Minute).Format(time.RFC3339), "cf-enq-retry"); err != nil {
		t.Fatalf("seed enqueue lease: %v", err)
	}

	nextAttempt := time.Now().UTC().Add(2 * time.Minute)
	err := db.Forwarding().MarkCreatorForwardingEnqueueRetry(ctx, "cf-enq-retry",
		"runner-enqueue", "lease-enqueue", "ENQUEUE_FAILED", "atomic write conflict", nextAttempt)
	if err != nil {
		t.Fatalf("MarkCreatorForwardingEnqueueRetry: %v", err)
	}

	saved, err := db.Forwarding().GetCreatorForwarding(ctx, "cf-enq-retry")
	if err != nil {
		t.Fatalf("GetCreatorForwarding: %v", err)
	}
	if saved.Status != "RETRY_WAIT" {
		t.Errorf("status = %q, want RETRY_WAIT", saved.Status)
	}
	if saved.NextAttemptAt == "" {
		t.Error("next_attempt_at should be set")
	}
	if saved.LastErrorCode != "ENQUEUE_FAILED" {
		t.Errorf("last_error_code = %q, want ENQUEUE_FAILED", saved.LastErrorCode)
	}
	if saved.LockedBy != "" || saved.LeaseID != "" {
		t.Error("locked_by/lease_id should be cleared")
	}
}

func TestMarkCreatorForwardingEnqueueRetry_FromReadyToForward(t *testing.T) {
	db := setupForwardingTestDB(t)
	ctx := context.Background()

	insertTestForwardingWithPayload(t, db, "cf-r2f-retry", "openai", "creator-r2f-retry",
		"scene.composite.v1", "READY_TO_FORWARD", `{"v":"1"}`, "abc")
	if _, err := db.db.ExecContext(ctx, `UPDATE creator_forwardings
		SET locked_by = ?, lease_id = ?, lease_expires_at = ? WHERE forwarding_id = ?`,
		"runner-ready", "lease-ready", time.Now().UTC().Add(time.Minute).Format(time.RFC3339), "cf-r2f-retry"); err != nil {
		t.Fatalf("seed enqueue lease: %v", err)
	}

	nextAttempt := time.Now().UTC().Add(30 * time.Second)
	err := db.Forwarding().MarkCreatorForwardingEnqueueRetry(ctx, "cf-r2f-retry",
		"runner-ready", "lease-ready", "PREPARE_FAILED", "invalid payload", nextAttempt)
	if err != nil {
		t.Fatalf("MarkCreatorForwardingEnqueueRetry: %v", err)
	}

	saved, err := db.Forwarding().GetCreatorForwarding(ctx, "cf-r2f-retry")
	if err != nil {
		t.Fatalf("GetCreatorForwarding: %v", err)
	}
	if saved.Status != "RETRY_WAIT" {
		t.Errorf("status = %q, want RETRY_WAIT", saved.Status)
	}
}
