package store

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-server/internal/taskoutput_artifacts"
)

// TestE2E_RunningAttemptLifecycle exercises the canonical worker -> Master
// path as one lifecycle: lease claim, RUNNING promotion, live heartbeat
// projection, terminal artifact/delivery events, and final report ingest.
// It deliberately uses the production repository methods rather than
// inserting a pre-built RUNNING row, so a regression cannot hide a missing
// identity edge between LEASED and RUNNING.
func TestE2E_RunningAttemptLifecycle(t *testing.T) {
	store := openTestDB(t)
	defer store.Close()
	ctx := context.Background()

	const (
		jobID      = "e2e-running-job"
		taskID     = "e2e-running-task"
		workerID   = "e2e-running-worker"
		sessionID  = "e2e-running-session"
		snapshotID = "e2e-running-snapshot"
		leaseID    = "e2e-running-lease"
		executorID = "scene.composite.v1"
		artifactID = "artifact-e2e-video"
		uploadID   = "upload-e2e-video"
		destID     = "destination-e2e"
	)
	const verifiedSHA256 = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
	now := time.Date(2026, 8, 10, 12, 0, 0, 0, time.UTC)
	nowString := now.Format(time.RFC3339)

	// The production claim path requires an admitted worker identity and a
	// task in READY with a matching task spec.
	execQuery(t, store, ctx,
		`INSERT INTO workers(worker_id, worker_name, node_role, raw_json, migrated_at)
		 VALUES (?, ?, 'worker', '{}', ?)`, workerID, workerID, nowString)
	if err := store.InsertSession(&PersistedSession{
		SessionID:   sessionID,
		WorkerID:    workerID,
		SessionType: "control",
		TokenHash:   "e2e-running-token",
		CreatedAt:   now,
		ExpiresAt:   now.Add(time.Hour),
	}); err != nil {
		t.Fatalf("insert worker session: %v", err)
	}
	execQuery(t, store, ctx,
		`INSERT INTO worker_runtime_snapshots(snapshot_id, worker_id, session_id, connected_at)
		 VALUES (?, ?, ?, ?)`, snapshotID, workerID, sessionID, nowString)
	execQuery(t, store, ctx,
		`INSERT INTO jobs(job_id, status, max_retries, revision, created_at, updated_at, migrated_at)
		 VALUES (?, 'PENDING', 3, 0, ?, ?, ?)`, jobID, nowString, nowString, nowString)
	execQuery(t, store, ctx,
		`INSERT INTO tasks
		 (task_id, job_id, project_id, render_plan_id, executor_id, executor_version,
		  status, priority, revision, attempt_count, attempt_number, worker_id, lease_id,
		  created_at, updated_at)
		 VALUES (?, ?, '', '', ?, 3, 'READY', 0, 0, 0, 0, '', '', ?, ?)`,
		taskID, jobID, executorID, nowString, nowString)
	execQuery(t, store, ctx,
		`INSERT INTO task_specs(task_id, spec_version, spec_hash, executor_id, payload_json, created_at)
		 VALUES (?, 1, '', ?, '{}', ?)`, taskID, executorID, nowString)

	taskRepo := NewSQLiteTaskRepository(store)
	tws, attempt, err := taskRepo.ClaimTaskForWorkerAtomic(ctx, taskgraph.ClaimTaskForWorkerCommand{
		TaskID:               taskID,
		ExpectedTaskRevision: 0,
		WorkerID:             workerID,
		SessionID:            sessionID,
		WorkerSnapshotID:     snapshotID,
		LeaseID:              leaseID,
		ExecutorID:           executorID,
		ExecutorVersion:      3,
		CapabilityRevision:   1,
	})
	if err != nil {
		t.Fatalf("ClaimTaskForWorkerAtomic: %v", err)
	}
	if tws == nil || attempt == nil || attempt.ID == "" {
		t.Fatalf("claim result = task=%+v attempt=%+v; want canonical identities", tws, attempt)
	}
	var claimedStatus, claimedWorker, claimedLease, claimedAttempt string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status, worker_id, lease_id, attempt_id FROM tasks WHERE task_id = ?`, taskID).
		Scan(&claimedStatus, &claimedWorker, &claimedLease, &claimedAttempt); err != nil {
		t.Fatalf("read claimed task: %v", err)
	}
	if claimedStatus != "LEASED" || claimedWorker != workerID || claimedLease != leaseID || claimedAttempt != attempt.ID {
		t.Fatalf("claimed task identity/state = status=%q worker=%q lease=%q attempt=%q; want LEASED with worker/lease/attempt", claimedStatus, claimedWorker, claimedLease, claimedAttempt)
	}
	if attempt.Status != taskattempts.AttemptStatusPending {
		t.Fatalf("claimed attempt status = %s; want PENDING", attempt.Status)
	}

	// The worker accepted the offer. Claim increments the task revision, so
	// the returned revision is the exact CAS revision used by acceptance.
	if err := taskRepo.AcceptTaskAtomic(ctx, attempt, tws.Revision); err != nil {
		t.Fatalf("AcceptTaskAtomic: %v", err)
	}
	if attempt.Status != taskattempts.AttemptStatusRunning || attempt.StartedAt == nil {
		t.Fatalf("accepted attempt = %+v; want RUNNING with started_at", attempt)
	}

	var taskStatus, taskWorker, taskLease, taskAttempt, taskStarted string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status, worker_id, lease_id, attempt_id, COALESCE(started_at, '')
		   FROM tasks WHERE task_id = ?`, taskID).Scan(
		&taskStatus, &taskWorker, &taskLease, &taskAttempt, &taskStarted); err != nil {
		t.Fatalf("read accepted task: %v", err)
	}
	if taskStatus != "RUNNING" || taskWorker != workerID || taskLease != leaseID || taskAttempt != attempt.ID || taskStarted == "" {
		t.Fatalf("accepted task read model = status=%q worker=%q lease=%q attempt=%q started=%q; want RUNNING/canonical identity",
			taskStatus, taskWorker, taskLease, taskAttempt, taskStarted)
	}

	// The live read model must be populated at acceptance, before the first
	// detailed render heartbeat arrives.
	live, err := store.GetWorkerTaskRuntimeByJob(ctx, jobID)
	if err != nil {
		t.Fatalf("read runtime immediately after accept: %v", err)
	}
	if live == nil || live.WorkerID != workerID || live.AttemptID != attempt.ID || live.StartedAt == "" || live.RuntimeStatus != "RUNNING" {
		t.Fatalf("immediate live identity = %+v; want worker_id, attempt_id, started_at and RUNNING", live)
	}

	canonicalEvents := []map[string]any{
		{"event_id": "attempt-event-" + attempt.ID + "-worker-0", "event_name": "ATTEMPT_STARTED", "event_index": 0, "phase": "render", "status": "ok"},
		{"event_id": "attempt-event-" + attempt.ID + "-worker-1", "event_name": "PHASE_CHANGED", "event_index": 1, "phase": "building_segments", "status": "ok"},
		{"event_id": "attempt-event-" + attempt.ID + "-worker-2", "event_name": "SEGMENT_STARTED", "event_index": 2, "phase": "building_segments", "segment": 7, "status": "ok"},
		{"event_id": "attempt-event-" + attempt.ID + "-ffmpeg-3", "event_name": "PROGRESS_UPDATED", "event_index": 3, "phase": "building_segments", "segment": 7, "status": "ok"},
		{"event_id": "attempt-event-" + attempt.ID + "-worker-4", "event_name": "SEGMENT_COMPLETED", "event_index": 4, "phase": "building_segments", "segment": 7, "status": "ok"},
		{"event_id": "attempt-event-" + attempt.ID + "-validation-5", "event_name": "ARTIFACT_VERIFY_STARTED", "event_index": 5, "phase": "finalize", "status": "ok"},
		{"event_id": "attempt-event-" + attempt.ID + "-validation-6", "event_name": "ARTIFACT_VERIFIED", "event_index": 6, "phase": "finalize", "status": "ok"},
		{"event_id": "attempt-event-" + attempt.ID + "-upload-7", "event_name": "DELIVERY_STARTED", "event_index": 7, "phase": "upload", "status": "ok"},
		{"event_id": "attempt-event-" + attempt.ID + "-worker-8", "event_name": "ATTEMPT_COMPLETED", "event_index": 8, "phase": "finalize", "status": "ok"},
	}
	heartbeat := map[string]any{
		"worker_id": workerID, "worker_name": workerID, "status": "busy", "current_job": jobID,
		"node_role": "worker", "last_heartbeat": now.Add(2 * time.Second).Format(time.RFC3339Nano),
		"metrics": map[string]any{"active_jobs": []any{map[string]any{
			"job_id": jobID, "task_id": taskID, "attempt_id": attempt.ID, "attempt": attempt.AttemptNumber,
			"lease_id": leaseID, "job_type": executorID, "status": "RUNNING", "started_at": taskStarted,
			"progress_percent": 46, "progress_phase": "building_segments", "progress_scene": 7,
			"progress_total": 13, "progress_segment": 7, "progress_total_segments": 26,
			"frames_encoded": 18432, "frames_decoded": 19000, "frames_composited": 18432,
			"ffmpeg_speed_x": 2.37, "elapsed_ms": 183421,
			"progress_metrics":         map[string]any{"frames_encoded": 18432, "frames_decoded": 19000},
			"canonical_attempt_events": canonicalEvents,
		}}},
	}
	heartbeatJSON, err := json.Marshal(heartbeat)
	if err != nil {
		t.Fatalf("marshal live heartbeat: %v", err)
	}
	if err := store.PersistWorkerHeartbeat(ctx, heartbeatJSON, sessionID); err != nil {
		t.Fatalf("PersistWorkerHeartbeat: %v", err)
	}

	live, err = store.GetWorkerTaskRuntimeByJob(ctx, jobID)
	if err != nil {
		t.Fatalf("read live progress: %v", err)
	}
	if live == nil || live.WorkerID != workerID || live.AttemptID != attempt.ID || live.ProgressPhase != "building_segments" ||
		live.CurrentScene != 7 || live.TotalScenes != 13 || live.CurrentSegment != 7 || live.TotalSegments != 26 ||
		live.FramesEncoded != 18432 || live.FramesDecoded != 19000 || live.FramesComposited != 18432 ||
		live.FFmpegSpeedX != 2.37 || live.ElapsedMS != 183421 || live.LastProgressAt == "" || len(live.CanonicalAttemptEvents) != len(canonicalEvents) {
		t.Fatalf("live progress projection = %+v; want phase, scene/segment, cumulative metrics and canonical events", live)
	}

	phaseTimings := make([]taskattempts.PhaseTimingDetailed, 0, len(canonicalEvents))
	phaseSpecs := []struct {
		origin, scope, component, action, phase, eventName string
		segment                                            int32
	}{
		{"worker", "attempt", "runner", "execute", "render", "ATTEMPT_STARTED", 0},
		{"worker", "attempt", "runner", "execute", "building_segments", "PHASE_CHANGED", 0},
		{"worker", "segment", "worker.parallel", "segment_start", "building_segments", "SEGMENT_STARTED", 7},
		{"ffmpeg", "segment", "ffmpeg", "progress", "building_segments", "PROGRESS_UPDATED", 7},
		{"worker", "segment", "worker.parallel", "segment_finish", "building_segments", "SEGMENT_COMPLETED", 7},
		{"validation", "attempt", "quality", "ffprobe", "finalize", "ARTIFACT_VERIFY_STARTED", 0},
		{"validation", "attempt", "quality", "sha256", "finalize", "ARTIFACT_VERIFIED", 0},
		{"upload", "attempt", "worker", "commit_ack_wait", "upload", "DELIVERY_STARTED", 0},
		{"worker", "attempt", "runner", "report", "finalize", "ATTEMPT_COMPLETED", 0},
	}
	for i, spec := range phaseSpecs {
		phaseTimings = append(phaseTimings, taskattempts.PhaseTimingDetailed{
			AttemptID: attempt.ID, EventID: canonicalEvents[i]["event_id"].(string), EventIndex: int64(i),
			Origin: spec.origin, Scope: spec.scope, Component: spec.component, Action: spec.action,
			Phase: spec.phase, EventName: spec.eventName, EventType: "lifecycle", PhaseOrder: i + 1,
			SegmentIndex: int(spec.segment), Status: "ok", StartedAt: now.Add(time.Duration(i) * time.Second),
			CompletedAt: now.Add(time.Duration(i+1) * time.Second), DurationMS: 1000,
			Frames: int64(60 + i), FramesIn: int64(60 + i), FramesOut: int64(60 + i),
			MetadataJSON: "{}",
		})
	}

	cmd := taskgraph.IngestResultCommand{
		TaskID: taskID, JobID: jobID, WorkerID: workerID, LeaseID: leaseID, AttemptID: attempt.ID,
		TaskStatus: taskgraph.StatusSucceeded, AttemptStatus: taskattempts.AttemptStatusSucceeded,
		RawReportJSON:       `{"task_id":"` + taskID + `","attempt_id":"` + attempt.ID + `","status":"succeeded"}`,
		RawReportReceivedAt: now.Add(10 * time.Second), ReportSchemaVersion: 1, ReportVersion: 1,
		Metrics: taskattempts.AttemptMetrics{
			AttemptID: attempt.ID, FramesDecoded: 19000, FramesComposited: 18432, FramesEncoded: 18432,
			InputBytes: 2097152, OutputBytes: 1048576, FFmpegSpeedRatio: 2.37, WallClockSeconds: 183.421,
		},
		SegmentTimings: []taskattempts.SegmentTiming{{
			AttemptID: attempt.ID, JobID: jobID, TaskID: taskID, WorkerID: workerID,
			SegmentIndex: 7, SceneID: "scene-7", SourceType: "image", DurationMS: 1000,
			FramesEncoded: 18432, FramesDecoded: 19000, FramesComposited: 18432,
			FfmpegSpeedX: 2.37, Status: "ok",
		}},
		PhaseTimings: phaseTimings,
		Artifacts: []taskoutput_artifacts.OutputArtifact{{
			TaskID: taskID, AttemptID: attempt.ID, ArtifactID: artifactID, ArtifactType: "video",
			DeclaredPath: "/out/video.mp4", DeclaredSize: 1048576, DeclaredSHA256: verifiedSHA256, MetadataJSON: "{}",
		}},
	}
	if err := taskRepo.IngestTaskResultAtomic(ctx, cmd); err != nil {
		t.Fatalf("IngestTaskResultAtomic: %v", err)
	}

	var finalTaskStatus string
	var pendingTerminal int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status, COALESCE(winning_attempt_terminal_pending, 0) FROM tasks WHERE task_id = ?`, taskID).
		Scan(&finalTaskStatus, &pendingTerminal); err != nil {
		t.Fatalf("read final task state: %v", err)
	}
	if finalTaskStatus != "RUNNING" || pendingTerminal != 1 {
		t.Fatalf("final task state = status=%q winning_attempt_terminal_pending=%d; want RUNNING/1 until artifact commit", finalTaskStatus, pendingTerminal)
	}
	var finalAttemptStatus, finalWorker, finalLease string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status, worker_id, lease_id FROM task_attempts WHERE id = ?`, attempt.ID).
		Scan(&finalAttemptStatus, &finalWorker, &finalLease); err != nil {
		t.Fatalf("read final attempt: %v", err)
	}
	if finalAttemptStatus != "SUCCEEDED" || finalWorker != workerID || finalLease != leaseID {
		t.Fatalf("final attempt = status=%q worker=%q lease=%q; want SUCCEEDED/canonical identity", finalAttemptStatus, finalWorker, finalLease)
	}

	var artifactCount int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_output_artifacts WHERE task_id = ? AND attempt_id = ? AND artifact_id = ?`,
		taskID, attempt.ID, artifactID).Scan(&artifactCount); err != nil {
		t.Fatalf("read output artifact: %v", err)
	}
	if artifactCount != 1 {
		t.Fatalf("output artifact rows = %d; want 1", artifactCount)
	}
	var workerEventCount, lifecycleEventCount int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_execution_events WHERE attempt_id = ? AND origin <> 'master'`, attempt.ID).Scan(&workerEventCount); err != nil {
		t.Fatalf("count worker lifecycle events: %v", err)
	}
	if err := store.DB().QueryRowContext(ctx,
		`SELECT COUNT(*) FROM task_execution_events WHERE attempt_id = ? AND event_name IN ('ARTIFACT_VERIFY_STARTED','ARTIFACT_VERIFIED','DELIVERY_STARTED')`, attempt.ID).Scan(&lifecycleEventCount); err != nil {
		t.Fatalf("count artifact/delivery lifecycle events: %v", err)
	}
	if workerEventCount != len(phaseTimings) || lifecycleEventCount != 3 {
		t.Fatalf("persisted lifecycle events = worker=%d artifact_delivery=%d; want %d/3", workerEventCount, lifecycleEventCount, len(phaseTimings))
	}

	// Ingest intentionally leaves the successful task pending until the
	// master verifies the durable artifact. Seed the same artifact-upload
	// boundary that the production upload service hands to FinalizeVerified.
	nowString = now.Add(11 * time.Second).Format(time.RFC3339)
	execQuery(t, store, ctx, `
		UPDATE jobs SET status = 'AWAITING_ARTIFACT', updated_at = ? WHERE job_id = ?`, nowString, jobID)
	execQuery(t, store, ctx, `
		INSERT INTO artifacts (id, job_id, attempt_id, type, storage_provider, status, created_at)
		VALUES (?, ?, ?, 'video', 'local', 'STAGING', ?)`, artifactID, jobID, attempt.AttemptNumber, nowString)
	execQuery(t, store, ctx, `
		INSERT INTO artifact_uploads
		 (upload_id, artifact_id, job_id, attempt_number, worker_id, lease_id, status,
		  temporary_storage_key, expected_size_bytes, expected_sha256,
		  received_size_bytes, received_sha256, created_at, expires_at)
		VALUES (?, ?, ?, ?, ?, ?, 'FINALIZING', ?, ?, ?, ?, ?, ?, ?)`,
		uploadID, artifactID, jobID, attempt.AttemptNumber, workerID, leaseID,
		"tmp/e2e-running-video", 1048576, verifiedSHA256, 1048576, verifiedSHA256,
		nowString, now.Add(time.Hour).Format(time.RFC3339))
	execQuery(t, store, ctx, `
		INSERT INTO delivery_destinations
		 (destination_id, provider, name, enabled, configuration_json, created_at, updated_at)
		VALUES (?, 'test', 'E2E destination', 1, '{}', ?, ?)`, destID, nowString, nowString)
	execQuery(t, store, ctx, `
		INSERT INTO job_delivery_plans
		 (job_id, destination_id, enabled, priority, retry_budget, metadata_json, created_at, updated_at)
		VALUES (?, ?, 1, 0, 1, '{}', ?, ?)`, jobID, destID, nowString, nowString)

	finalizer := NewSQLiteArtifactFinalizer(store.DB(), nil)
	if _, err := finalizer.FinalizeVerified(ctx, FinalizeVerifiedParams{
		UploadID: uploadID, ArtifactID: artifactID, JobID: jobID,
		WorkerID: workerID, LeaseID: leaseID, AttemptNumber: attempt.AttemptNumber,
		StorageProvider: "local", StorageKey: "artifacts/" + jobID + "/" + verifiedSHA256,
		SHA256: verifiedSHA256, SizeBytes: 1048576, MIMEType: "video/mp4", VerifiedAt: now.Add(time.Second),
		DestinationID: destID,
	}); err != nil {
		t.Fatalf("FinalizeVerified: %v", err)
	}

	var jobStatus, artifactStatus, uploadStatus string
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM jobs WHERE job_id = ?`, jobID).Scan(&jobStatus); err != nil {
		t.Fatalf("read job after artifact verification: %v", err)
	}
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM artifacts WHERE id = ?`, artifactID).Scan(&artifactStatus); err != nil {
		t.Fatalf("read artifact after verification: %v", err)
	}
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM artifact_uploads WHERE upload_id = ?`, uploadID).Scan(&uploadStatus); err != nil {
		t.Fatalf("read upload after verification: %v", err)
	}
	if jobStatus != "DELIVERING" || artifactStatus != "READY" || uploadStatus != "COMPLETED" {
		t.Fatalf("verified artifact state = job=%q artifact=%q upload=%q; want DELIVERING/READY/COMPLETED", jobStatus, artifactStatus, uploadStatus)
	}

	var committedTaskStatus string
	var committedPending int
	if err := store.DB().QueryRowContext(ctx,
		`SELECT status, COALESCE(winning_attempt_terminal_pending, 0) FROM tasks WHERE task_id = ?`, taskID).
		Scan(&committedTaskStatus, &committedPending); err != nil {
		t.Fatalf("read task after artifact verification: %v", err)
	}
	// IngestTaskResultAtomic owns the AttemptResult boundary and deliberately
	// leaves winning_attempt_terminal_pending=1. The separate completion
	// coordinator clears that flag when its commit protocol is used; the
	// artifact finalizer owns artifact/job/delivery state and must not invent
	// a second task-completion writer.
	if committedTaskStatus != "SUCCEEDED" || committedPending != 1 {
		t.Fatalf("task after artifact verification = status=%q pending=%d; want SUCCEEDED/1 until completion commit", committedTaskStatus, committedPending)
	}

	var deliveryID, deliveryStatus string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT delivery_id, status FROM job_deliveries WHERE artifact_id = ? AND destination_id = ?`, artifactID, destID).
		Scan(&deliveryID, &deliveryStatus); err != nil {
		t.Fatalf("read materialized delivery: %v", err)
	}
	if deliveryID == "" || deliveryStatus != "PENDING" {
		t.Fatalf("materialized delivery = id=%q status=%q; want non-empty/PENDING", deliveryID, deliveryStatus)
	}

	leases, err := store.ClaimDeliveries(ctx, "e2e-delivery-runner", time.Minute, 1)
	if err != nil {
		t.Fatalf("ClaimDeliveries: %v", err)
	}
	if len(leases) != 1 || leases[0].DeliveryID != deliveryID {
		t.Fatalf("delivery leases = %#v; want one lease for %s", leases, deliveryID)
	}
	if err := store.MarkDeliverySucceeded(ctx, deliveryID, leases[0].RunnerID, leases[0].LeaseID, "e2e-remote-id", "https://example.test/e2e"); err != nil {
		t.Fatalf("MarkDeliverySucceeded: %v", err)
	}
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM job_deliveries WHERE delivery_id = ?`, deliveryID).Scan(&deliveryStatus); err != nil {
		t.Fatalf("read completed delivery: %v", err)
	}
	if deliveryStatus != "SUCCEEDED" {
		t.Fatalf("completed delivery status = %q; want SUCCEEDED", deliveryStatus)
	}
	if err := store.DB().QueryRowContext(ctx, `SELECT status FROM jobs WHERE job_id = ?`, jobID).Scan(&jobStatus); err != nil {
		t.Fatalf("read job after delivery: %v", err)
	}
	if jobStatus != "SUCCEEDED" {
		t.Fatalf("job status after delivery = %q; want SUCCEEDED", jobStatus)
	}

	// Persisted execution events must retain the exact canonical order used by
	// the incremental Attempt read model, not merely the same cardinality.
	rows, err := store.DB().QueryContext(ctx,
		`SELECT event_name, event_index FROM task_execution_events
		 WHERE attempt_id = ? AND origin <> 'master' ORDER BY event_index ASC`, attempt.ID)
	if err != nil {
		t.Fatalf("read ordered execution events: %v", err)
	}
	defer rows.Close()
	for i, want := range canonicalEvents {
		var eventName string
		var eventIndex int64
		if !rows.Next() {
			t.Fatalf("missing persisted worker event at index %d", i)
		}
		if err := rows.Scan(&eventName, &eventIndex); err != nil {
			t.Fatalf("scan persisted worker event %d: %v", i, err)
		}
		if eventName != want["event_name"].(string) || eventIndex != int64(i) {
			t.Fatalf("persisted worker event[%d] = name=%q index=%d; want name=%q index=%d", i, eventName, eventIndex, want["event_name"], i)
		}
	}
	if rows.Next() {
		t.Fatal("persisted worker execution events contain unexpected trailing rows")
	}
	var masterEventName, masterAction string
	if err := store.DB().QueryRowContext(ctx,
		`SELECT event_name, action FROM task_execution_events
		 WHERE attempt_id = ? AND origin = 'master' ORDER BY event_index ASC LIMIT 1`, attempt.ID).
		Scan(&masterEventName, &masterAction); err != nil {
		t.Fatalf("read master acceptance event: %v", err)
	}
	if masterEventName != "accept_to_start" || masterAction != "accept_to_start" {
		t.Fatalf("master acceptance event = name=%q action=%q; want accept_to_start/accept_to_start", masterEventName, masterAction)
	}

	// The gRPC handler performs this cleanup immediately after the canonical
	// ingest commits. Exercise the same production cleanup method and ensure
	// a late live view cannot survive Attempt completion.
	if err := store.DeleteWorkerTaskRuntime(taskID, attempt.ID); err != nil {
		t.Fatalf("DeleteWorkerTaskRuntime after completion: %v", err)
	}
	live, err = store.GetWorkerTaskRuntimeByJob(ctx, jobID)
	if err != nil {
		t.Fatalf("read runtime after completion: %v", err)
	}
	if live != nil {
		t.Fatalf("live runtime survived terminal Attempt cleanup: %+v", live)
	}
}
