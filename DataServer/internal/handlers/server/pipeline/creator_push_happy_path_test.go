// Package pipeline: creator_push_e2e_test.go exercises the full HTTP →
// creatorflow.Resolver → atomic Job+Task write path for the
// POST /api/v1/creator/jobs endpoint.
//
// creator_push_test.go (sibling) covers the pure normalization layer
// (creatorPushRequest → normalizedCreatorPush). This file is the
// integration counterpart: it wires a real SQLite store, a real Enqueuer
// + creatorflow.Resolver, and runs the handler through a real
// httptest.Recorder + gin.New engine mounted via h.RegisterRoutes.
//
// The auth middleware is bypassed via adminAuthFake because the auth
// chain has its own unit coverage in handlers/server/api; this file
// exercises the creator_push contract exclusively.
package pipeline

import (
	"context"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"net/http"
	"testing"
	"velox-server/internal/forwardingcontract"
	"velox-server/internal/jobs"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/routing"
)

// adminAuthFake short-circuits the bearer-token check the production
// router applies to /api/v1/creator/jobs. The auth chain is unit-tested
// separately; this file exercises the creator_push contract exclusively.
func TestCreatorPushJobsE2E_VoiceoverStockClipScene(t *testing.T) {
	h, db, jobRepo := newCreatorPushE2EStack(t)
	gin.SetMode(gin.TestMode)
	r := gin.New()
	h.RegisterRoutes(r, adminAuthFake, m2mJobsAuthFake)

	expectedJobID := enqueue.DeriveForwardingJobID(
		routing.FormatForwardingKey("creator_pc_1", "creator-job-001", "scene.composite.v1").String(),
	)
	body := creatorPushE2EBody("creator_pc_1", "creator-job-001", "scene.composite.v1")

	// First POST — full happy path.
	w := postCreatorPush(t, r, body)
	if w.Code != http.StatusAccepted {
		t.Fatalf("first POST: want 202, got %d body=%s", w.Code, w.Body.String())
	}
	var resp map[string]interface{}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response json: %v", err)
	}
	wantFields := map[string]interface{}{
		"ok":                 true,
		"accepted_from":      "creator_push",
		"source_provider":    "creator_pc_1",
		"source_job_id":      "creator-job-001",
		"target_executor_id": "scene.composite.v1",
		"job_id":             expectedJobID,
		"status":             "PENDING",
		"dispatch_status":    "queued_for_workers",
	}
	for key, want := range wantFields {
		if resp[key] != want {
			t.Fatalf("response[%q] = %v, want %v (full body=%s)", key, resp[key], want, w.Body.String())
		}
	}

	// creator_forwardings row written by Resolve's atomic CAS.
	forwarding, err := db.Forwarding().GetCreatorForwardingBySource(context.Background(),
		"creator_pc_1", "creator-job-001", "scene.composite.v1",
	)
	if err != nil {
		t.Fatalf("get forwarding: %v", err)
	}
	if forwarding == nil {
		t.Fatal("creator_forwardings row not persisted (atomic CAS did not write)")
	}
	if forwarding.Status != string(forwardingcontract.CFStatusForwarded) {
		t.Fatalf("forwarding status = %s, want FORWARDED", forwarding.Status)
	}

	// jobs row exists with status PENDING (the canonical Resolver outcome).
	job, err := jobRepo.Get(context.Background(), expectedJobID)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job == nil {
		t.Fatal("jobs row not persisted")
	}
	if job.Status != jobs.StatusPending {
		t.Fatalf("job status = %v, want PENDING", job.Status)
	}

	// tasks + task_specs row exists for the job_id (AtomicJobTaskCreator
	// triple-INSERT must have produced all three tables).
	var taskCount, specCount int
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM tasks WHERE job_id = ?`, expectedJobID).Scan(&taskCount); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if taskCount != 1 {
		t.Fatalf("exactly 1 tasks row expected, got %d", taskCount)
	}
	// task_specs is keyed by task_id (no job_id column); JOIN via tasks.
	if err := db.DB().QueryRow(
		`SELECT COUNT(*) FROM task_specs WHERE task_id IN (SELECT task_id FROM tasks WHERE job_id = ?)`,
		expectedJobID,
	).Scan(&specCount); err != nil {
		t.Fatalf("count task_specs: %v", err)
	}
	if specCount != 1 {
		t.Fatalf("exactly 1 task_specs row expected, got %d", specCount)
	}

	// Idempotency: second POST with the same body returns the same job_id
	// and does NOT create additional rows. The UNIQUE constraint on
	// (source_provider, source_job_id, target_executor_id) and the
	// Resolver idempotency fast-path both guarantee convergence.
	w2 := postCreatorPush(t, r, body)
	if w2.Code != http.StatusAccepted {
		t.Fatalf("second POST: want 202, got %d body=%s", w2.Code, w2.Body.String())
	}
	var resp2 map[string]interface{}
	if err := json.Unmarshal(w2.Body.Bytes(), &resp2); err != nil {
		t.Fatalf("response2 json: %v", err)
	}
	if resp2["job_id"] != expectedJobID {
		t.Fatalf("idempotent job_id: want %s, got %v", expectedJobID, resp2["job_id"])
	}
	// accepted_from is preserved across replays too.
	if resp2["accepted_from"] != "creator_push" {
		t.Fatalf("idempotent accepted_from: want creator_push, got %v", resp2["accepted_from"])
	}
	// created=false is the canonical signal from buildIdempotentResolveResponse
	// that the second POST hit the Resolver fast-path, not a fresh insert.
	if v, ok := resp2["created"]; !ok || v != false {
		t.Fatalf("idempotent created: want false (fast-path), got %v (present=%v)", v, ok)
	}
	// dispatch_status is stamped on every response (fresh + replay), so the
	// replay must carry it identically. This guards against a future
	// regression that strips overlay fields on the idempotent path.
	if resp2["dispatch_status"] != "queued_for_workers" {
		t.Fatalf("idempotent dispatch_status: want queued_for_workers, got %v", resp2["dispatch_status"])
	}

	var fwdCount, jobCount int
	if err := db.DB().QueryRow(
		`SELECT COUNT(*) FROM creator_forwardings WHERE source_provider = ? AND source_job_id = ? AND target_executor_id = ?`,
		"creator_pc_1", "creator-job-001", "scene.composite.v1",
	).Scan(&fwdCount); err != nil {
		t.Fatalf("count forwardings: %v", err)
	}
	if fwdCount != 1 {
		t.Fatalf("want exactly 1 forwarding row after replay, got %d", fwdCount)
	}
	if err := db.DB().QueryRow(`SELECT COUNT(*) FROM jobs WHERE job_id = ?`, expectedJobID).Scan(&jobCount); err != nil {
		t.Fatalf("count jobs: %v", err)
	}
	if jobCount != 1 {
		t.Fatalf("want exactly 1 jobs row after replay, got %d", jobCount)
	}
}
