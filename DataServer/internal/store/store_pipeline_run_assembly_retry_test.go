package store

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"velox-server/internal/pipelineruns"
	"velox-shared/contract"
)

func TestRetryFailedAssemblyReusesSavedPlanAndTask(t *testing.T) {
	ctx := context.Background()
	db := newTestPipelineRunStore(t)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.DB().ExecContext(ctx, `INSERT INTO jobs(job_id,status,created_at,updated_at,error_message,failed_at,failed_by,migrated_at) VALUES('job-retry','FAILED',?,?, 'assembly failed',?,'worker',?)`, now, now, now, now); err != nil {
		t.Fatal(err)
	}
	// A later-created preparation task in the same DAG must never be selected
	// for retry in place of the failed final assembly task.
	later := time.Now().UTC().Add(time.Hour).Format(time.RFC3339Nano)
	if _, err := db.DB().ExecContext(ctx, `INSERT INTO tasks(task_id,job_id,executor_id,status,created_at,updated_at,depends_on) VALUES('task-prepare','job-retry','video.prepare.window.v1','SUCCEEDED',?,?, '[]')`, later, later); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().ExecContext(ctx, `INSERT INTO task_specs(task_id,executor_id,payload_json,created_at) VALUES('task-prepare','video.prepare.window.v1','{}',?)`, later); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().ExecContext(ctx, `INSERT INTO tasks(task_id,job_id,executor_id,status,revision,attempt_count,worker_id,lease_id,created_at,updated_at,completed_at,depends_on) VALUES('task-retry','job-retry','video.assemble.copy.v1','FAILED',4,2,'worker-1','lease-1',?,?,?,'[]')`, now, now, now); err != nil {
		t.Fatal(err)
	}
	planJSON := retryTestPlan(t)
	planHash := sha256.Sum256([]byte(planJSON))
	payload, err := json.Marshal(map[string]interface{}{
		contract.PayloadKeyCompiledRenderPlanJSON: planJSON,
		contract.PayloadKeyCompiledRenderPlanSHA:  hex.EncodeToString(planHash[:]),
		"runtime_assets":                          []interface{}{},
	})
	if err != nil {
		t.Fatal(err)
	}
	specHash := sha256.Sum256(payload)
	if _, err := db.DB().ExecContext(ctx, `INSERT INTO task_specs(task_id,spec_version,spec_hash,executor_id,payload_json,created_at) VALUES('task-retry',1,?,'video.assemble.copy.v1',?,?)`, hex.EncodeToString(specHash[:]), string(payload), now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.InsertPipelineRun(ctx, &pipelineruns.PipelineRun{
		ID: "run-retry", IdempotencyKey: "idem-retry", Status: pipelineruns.StatusFailed,
		VeloxJobID: "job-retry", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}

	got, err := db.RetryFailedAssembly(ctx, "run-retry", "")
	if err != nil {
		t.Fatalf("RetryFailedAssembly: %v", err)
	}
	if got.PipelineRunID != "run-retry" || got.JobID != "job-retry" || got.TaskID != "task-retry" {
		t.Fatalf("unexpected retry result: %+v", got)
	}
	var taskStatus, jobStatus, runStatus, savedPayload string
	var jobErrors [6]string
	var taskRevision int
	if err := db.DB().QueryRowContext(ctx, `SELECT status,revision FROM tasks WHERE task_id='task-retry'`).Scan(&taskStatus, &taskRevision); err != nil {
		t.Fatal(err)
	}
	if err := db.DB().QueryRowContext(ctx, `SELECT status,COALESCE(error_message,''),COALESCE(last_error,''),COALESCE(last_error_at,''),COALESCE(failed_at,''),COALESCE(failed_by,''),COALESCE(completed_at,'') FROM jobs WHERE job_id='job-retry'`).Scan(&jobStatus, &jobErrors[0], &jobErrors[1], &jobErrors[2], &jobErrors[3], &jobErrors[4], &jobErrors[5]); err != nil {
		t.Fatal(err)
	}
	if err := db.DB().QueryRowContext(ctx, `SELECT status FROM pipeline_runs WHERE id='run-retry'`).Scan(&runStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.DB().QueryRowContext(ctx, `SELECT payload_json FROM task_specs WHERE task_id='task-retry'`).Scan(&savedPayload); err != nil {
		t.Fatal(err)
	}
	if taskStatus != "READY" || taskRevision != 5 || jobStatus != "PENDING" || runStatus != string(pipelineruns.StatusWorkerQueued) {
		t.Fatalf("unexpected states after retry: task=%s rev=%d job=%s run=%s", taskStatus, taskRevision, jobStatus, runStatus)
	}
	if savedPayload != string(payload) {
		t.Fatal("retry changed immutable task payload")
	}
	for index, value := range jobErrors {
		if value != "" {
			t.Fatalf("retry left terminal job field %d populated: %q", index, value)
		}
	}
}

func TestRetryFailedAssemblyRejectsPendingPreparation(t *testing.T) {
	ctx := context.Background()
	db := newTestPipelineRunStore(t)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.DB().ExecContext(ctx, `INSERT INTO jobs(job_id,status,created_at,updated_at,migrated_at) VALUES('job-pending','FAILED',?,?,?)`, now, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.DB().ExecContext(ctx, `INSERT INTO tasks(task_id,job_id,executor_id,status,created_at,updated_at,depends_on) VALUES('task-pending','job-pending','video.assemble.copy.v1','FAILED',?,?, '[]')`, now, now); err != nil {
		t.Fatal(err)
	}
	payload := `{"runtime_assets_pending":true}`
	if _, err := db.DB().ExecContext(ctx, `INSERT INTO task_specs(task_id,executor_id,payload_json,created_at) VALUES('task-pending','video.assemble.copy.v1',?,?)`, payload, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.InsertPipelineRun(ctx, &pipelineruns.PipelineRun{
		ID: "run-pending", IdempotencyKey: "idem-pending", Status: pipelineruns.StatusFailed,
		VeloxJobID: "job-pending", CreatedAt: time.Now().UTC(), UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := db.RetryFailedAssembly(ctx, "run-pending", ""); err != ErrAssemblyRetryNotEligible {
		t.Fatalf("expected ineligible retry, got %v", err)
	}
	var taskStatus, jobStatus string
	if err := db.DB().QueryRowContext(ctx, `SELECT status FROM tasks WHERE task_id='task-pending'`).Scan(&taskStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.DB().QueryRowContext(ctx, `SELECT status FROM jobs WHERE job_id='job-pending'`).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if taskStatus != "FAILED" || jobStatus != "FAILED" {
		t.Fatalf("ineligible retry mutated state: task=%s job=%s", taskStatus, jobStatus)
	}
}

func retryTestPlan(t *testing.T) string {
	t.Helper()
	plan := &contract.CompiledRenderPlanV2{
		PlanVersion: contract.CompiledPlanVersionV2, TimelineRevision: 1,
		TimelineSHA256: strings.Repeat("c", 64), DurationUS: 1_000_000,
		Output:      contract.OutputContractV2{Container: "mp4", VideoCodec: "h264", Width: 1920, Height: 1080, FPSNum: 30, FPSDen: 1, PixelFormat: "yuv420p"},
		FinalAudio:  contract.FinalAudioV2{Mode: contract.AudioModeFinalAudioCopy, AssetID: "audio", SHA256: strings.Repeat("a", 64), SizeBytes: 10, Codec: "aac", SampleRateHz: 48000, Channels: 2, DurationUS: 1_000_000, TimelineRevision: 1, TimelineSHA256: strings.Repeat("c", 64)},
		VideoTracks: []contract.VideoTrackV2{{TrackID: "main", Segments: []contract.VideoSegmentV2{{SegmentID: "s1", AssetID: "video", SHA256: strings.Repeat("b", 64), FrameCount: 30, SourceDurationUS: 1_000_000}}}},
		Assets: []contract.AssetRefV2{
			{AssetID: "audio", SHA256: strings.Repeat("a", 64), SizeBytes: 10, Kind: "final_audio", MIME: "audio/mp4", DurationUS: 1_000_000},
			{AssetID: "video", SHA256: strings.Repeat("b", 64), SizeBytes: 10, Kind: "video", MIME: "video/mp4", DurationUS: 1_000_000},
		},
	}
	if err := contract.ValidateCompiledRenderPlanV2(plan); err != nil {
		t.Fatalf("invalid plan fixture: %v", err)
	}
	data, err := plan.CanonicalJSON()
	if err != nil {
		t.Fatal(err)
	}
	return string(data)
}
