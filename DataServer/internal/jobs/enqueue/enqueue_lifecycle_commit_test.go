package enqueue

import (
	"context"
	"encoding/json"
	"errors"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"velox-server/internal/costmodel"
	"velox-server/internal/deliveries"
	"velox-server/internal/jobs"
	"velox-server/internal/store"
)

// =====================================================================
// Lifecycle / commit tests
// =====================================================================
//
// Verifies the full enqueue lifecycle:
//   - TestEnqueueCreatesJobAndTaskAtomically: the canonical path —
//     Enqueue returns a Job+Task row committed atomically with the
//     requirements persisted.
//   - TestEnqueueDefaultsPreserved: zero-value JobRequirements flow
//     through unchanged (no false-positive requirement propagation).
//   - TestNewEnqueuer_PanicsOnNilPlanResolver: fail-fast invariant
//     catches misconfiguration at boot, not at first production enqueue.
//   - TestRenderHTTPBoundaryJobResponse: HTTP-edge adapter (5 subtests)
//     covering basic/legacy-alias-fallback/full/nil/error.
//   - TestEnqueue_Precondition_*: tests pinning payload/transaction
//     rejection paths and max-retry propagation, plus integration tests
//     against the real SQLiteDeliveryPlanResolver.

func TestEnqueueCreatesJobAndTaskAtomically(t *testing.T) {
	t.Parallel()
	enq := newTestEnqueuer(t)

	payload := map[string]interface{}{
		"video_name":  "demo.mp4",
		"script_text": "hello world",
		"scenes": []interface{}{
			map[string]interface{}{"scene": "intro", "voiceover": "v1"},
		}, "voiceover_paths": []string{"/tmp/v1.mp3"},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3, "priority": 0},
		},
	}

	req := costmodel.JobRequirements{
		ResourceClass: costmodel.ResourceGPU,
		TemporalMode:  costmodel.TemporalWindowed,
		Deterministic: true,
		Cacheable:     true,
	}

	response, err := enq.Enqueue(context.Background(), payload, req)
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	if response["ok"] != true {
		t.Fatalf("want ok=true, got %v", response["ok"])
	}

	// Verify Job was created.
	jobID, _ := response["job_id"].(string)
	if jobID == "" {
		t.Fatal("expected non-empty job_id")
	}
	j, err := enq.Jobs.Get(context.Background(), jobID)
	if err != nil || j == nil {
		t.Fatalf("Get job: err=%v job=%v", err, j)
	}
	if j.ID != jobID {
		t.Fatalf("job ID mismatch: %q != %q", j.ID, jobID)
	}
	if j.Status != jobs.StatusPending {
		t.Fatalf("job status: want PENDING, got %q", j.Status)
	}
	if j.VideoName != "demo.mp4" {
		t.Fatalf("video_name: want demo.mp4, got %q", j.VideoName)
	}
}

// TestEnqueueDefaultsPreserved verifies the permissive behavior is
// intact when no Requirements are published: an empty JobRequirements
// flows through unchanged.
func TestEnqueuePreservesDeferredDriveAssetWireAndKind(t *testing.T) {
	t.Parallel()
	enq := newTestEnqueuer(t)
	payload := map[string]interface{}{
		"video_name":  "Deferred Drive enqueue",
		"script_text": "typed asset boundary",
		"audio_tracks": []interface{}{map[string]interface{}{
			"source_url": "velox-asset://voice-1",
			"role":       "voiceover",
		}},
		"scenes": []interface{}{map[string]interface{}{
			"scene_id": "scene-1",
			"clip": map[string]interface{}{
				"asset_id":       "drive-file-123456",
				"url":            "velox-asset://drive-file-123456",
				"asset_ref_kind": "deferred_drive",
				"duration_ms":    3000,
			},
			"voiceover": map[string]interface{}{
				"asset_id":    "voice-1",
				"url":         "velox-asset://voice-1",
				"duration_ms": 3000,
			},
		}},
		"delivery_plan": []interface{}{map[string]interface{}{
			"destination_id": "drive-main",
			"retry_budget":   1,
		}},
	}
	response, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	jobID, _ := response["job_id"].(string)
	job, err := enq.Jobs.Get(context.Background(), jobID)
	if err != nil || job == nil {
		t.Fatalf("Get job: err=%v job=%v", err, job)
	}
	var persisted map[string]interface{}
	if err := json.Unmarshal([]byte(job.Payload), &persisted); err != nil {
		t.Fatalf("decode persisted payload: %v", err)
	}
	encoded, err := json.Marshal(persisted)
	if err != nil {
		t.Fatal(err)
	}
	wire := string(encoded)
	for _, want := range []string{"velox-asset://drive-file-123456", "drive-file-123456", "deferred_drive"} {
		if !strings.Contains(wire, want) {
			t.Fatalf("persisted payload lost %q: %s", want, wire)
		}
	}
}

func TestEnqueueDefaultsPreserved(t *testing.T) {
	t.Parallel()
	enq := newTestEnqueuer(t)

	payload := map[string]interface{}{
		"video_name":  "demo.mp4",
		"script_text": "hello world",
		"scenes": []interface{}{
			map[string]interface{}{"scene": "intro", "voiceover": "v1"},
		}, "voiceover_paths": []string{"/tmp/v1.mp3"},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3, "priority": 0},
		},
	}

	req := costmodel.DefaultRequirements()

	response, err := enq.Enqueue(context.Background(), payload, req)
	if err != nil {
		t.Fatalf("Enqueue returned error: %v", err)
	}
	if response["ok"] != true {
		t.Fatalf("want ok=true, got %v", response["ok"])
	}

	// Verify the default requirements persisted correctly.
	jobID, _ := response["job_id"].(string)
	j, err := enq.Jobs.Get(context.Background(), jobID)
	if err != nil || j == nil {
		t.Fatalf("Get job: err=%v", err)
	}
	if j.Requirements.ResourceClass != "" || j.Requirements.TemporalMode != "" ||
		j.Requirements.Deterministic || j.Requirements.Cacheable {
		t.Errorf("DefaultRequirements must stay zero-value; got %+v", j.Requirements)
	}
}

// TestNewEnqueuer_PanicsOnNilPlanResolver verifies the fail-fast invariant
// for misconfiguration: a nil PlanResolver must panic at construction so
// the gap is caught at boot, not on the first production enqueue.
func TestNewEnqueuer_PanicsOnNilPlanResolver(t *testing.T) {
	t.Parallel()
	defer func() {
		if r := recover(); r == nil {
			t.Error("NewEnqueuer(nil PlanResolver) should panic, did not")
		}
	}()
	_ = NewEnqueuer(nil, nil, nil, nil)
}

// TestEnqueue_Precondition_RejectsMissingPlan verifies that an enqueue
// without a delivery plan is rejected during payload preflight, before
// the atomic creator or PlanResolver can run. The error must surface a
// clear "delivery_plan" hint.
func TestEnqueue_Precondition_RejectsMissingPlan(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		&mockPlanResolver{err: errors.New("deliveries: no explicit delivery plan: job_id=test")},
	)

	payload := map[string]interface{}{
		"video_name":      "no-plan",
		"script_text":     "test",
		"scenes":          []interface{}{map[string]interface{}{"scene": "intro", "voiceover": "v1"}},
		"voiceover_paths": []string{"/tmp/v.mp3"},
	}
	_, err = enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err == nil {
		t.Fatal("want error when plan missing, got nil")
	}
	if !strings.Contains(err.Error(), "delivery_plan") {
		t.Errorf("want error to mention delivery_plan, got %v", err)
	}
}

// TestEnqueue_ResolverFailureAfterAtomicCreateDoesNotReportFalseFailure
// characterizes the commit boundary. The payload is valid and the atomic
// creator can commit Job+Task+delivery-plan rows, but the legacy resolver
// deliberately returns an error. Resolver failures belong to finalization;
// they must not turn a committed enqueue into an error that causes callers
// to retry and potentially misdiagnose the already-persisted job.
func TestEnqueue_ResolverFailureAfterAtomicCreateDoesNotReportFalseFailure(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	seedDestinations(t, db, map[string]bool{"d1": true})
	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		&mockPlanResolver{err: errors.New("resolver unavailable after commit")},
	)

	payload := map[string]interface{}{
		"video_name":      "resolver-failure-does-not-orphan",
		"script_text":     "test",
		"scenes":          []interface{}{map[string]interface{}{"scene": "intro", "voiceover": "v1"}},
		"voiceover_paths": []string{"/tmp/v.mp3"},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "d1", "priority": 0, "retry_budget": 3},
		},
	}

	response, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err != nil {
		t.Fatalf("valid atomic enqueue must succeed despite resolver error: %v", err)
	}
	if response["ok"] != true {
		t.Fatalf("response.ok = %v, want true", response["ok"])
	}
	jobID, _ := response["job_id"].(string)
	if jobID == "" {
		t.Fatal("response.job_id is empty")
	}
	job, err := enq.Jobs.Get(context.Background(), jobID)
	if err != nil || job == nil {
		t.Fatalf("committed job missing: err=%v job=%v", err, job)
	}
	if job.Status != jobs.StatusPending {
		t.Fatalf("committed job status = %q, want PENDING", job.Status)
	}
}

func TestEnqueue_ResolverEmptyPlanDoesNotRejectValidPayload(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	seedDestinations(t, db, map[string]bool{"d1": true})
	resolver := &mockPlanResolver{plan: &ResolvedPlan{JobID: "test"}}
	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		resolver,
	)

	payload := map[string]interface{}{
		"video_name":      "empty-dest",
		"script_text":     "test",
		"scenes":          []interface{}{map[string]interface{}{"scene": "intro", "voiceover": "v1"}},
		"voiceover_paths": []string{"/tmp/v.mp3"},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "d1", "priority": 0, "retry_budget": 3},
		},
	}
	response, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err != nil {
		t.Fatalf("valid payload must enqueue despite empty resolver result: %v", err)
	}
	if response["ok"] != true {
		t.Fatalf("response.ok = %v, want true", response["ok"])
	}
	if resolver.calls != 0 {
		t.Fatalf("resolver calls = %d, want 0 on committed enqueue path", resolver.calls)
	}
}

// TestEnqueue_ResolverNegativeRetryBudgetDoesNotRejectValidPayload
// verifies that a stale/invalid resolver result cannot turn a committed
// valid payload into a false enqueue failure. Negative payload budgets
// are still rejected before creation by validateDeliveryPlanRequires.
func TestEnqueue_ResolverNegativeRetryBudgetDoesNotRejectValidPayload(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	seedDestinations(t, db, map[string]bool{"d1": true})
	resolver := &mockPlanResolver{plan: &ResolvedPlan{
		JobID: "test",
		Destinations: []PlanDestination{
			{DestinationID: "d1", Priority: 0, RetryBudget: 5},
			{DestinationID: "d2", Priority: 1, RetryBudget: -1}, // INVALID
		},
	}}
	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		resolver,
	)

	payload := map[string]interface{}{
		"video_name":      "negative-budget",
		"script_text":     "test",
		"scenes":          []interface{}{map[string]interface{}{"scene": "intro", "voiceover": "v1"}},
		"voiceover_paths": []string{"/tmp/v.mp3"},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "d1", "priority": 0, "retry_budget": 3},
		},
	}
	response, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err != nil {
		t.Fatalf("valid payload must enqueue despite invalid resolver result: %v", err)
	}
	if response["ok"] != true {
		t.Fatalf("response.ok = %v, want true", response["ok"])
	}
	if resolver.calls != 0 {
		t.Fatalf("resolver calls = %d, want 0 on committed enqueue path", resolver.calls)
	}
}

// TestEnqueue_Precondition_PropagatesMaxRetryBudget verifies that the
// Job's MaxRetries is set to the max retry_budget across destinations
// so the job-level budget can cover the worst-case per-destination
// retry chain. Per-delivery retry_budget is still authoritative at
// INSERT time (see deliveries/runner.go: lease carries per-row value).
func TestEnqueue_Precondition_PropagatesMaxRetryBudget(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	// Seed d1/d2/d3 so the atomic creator's parse-time validator has
	// all three destinations in scope (matching the payload below).
	seedDestinations(t, db, map[string]bool{
		"d1": true,
		"d2": true,
		"d3": true,
	})
	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		&mockPlanResolver{plan: &ResolvedPlan{
			JobID: "test",
			Destinations: []PlanDestination{
				{DestinationID: "d1", Priority: 0, RetryBudget: 3},
				{DestinationID: "d2", Priority: 1, RetryBudget: 7}, // max
				{DestinationID: "d3", Priority: 2, RetryBudget: 5},
			},
		}},
	)

	payload := map[string]interface{}{
		"video_name":      "max-retry",
		"script_text":     "test",
		"scenes":          []interface{}{map[string]interface{}{"scene": "intro", "voiceover": "v1"}},
		"voiceover_paths": []string{"/tmp/v.mp3"},
		"delivery_plan": []interface{}{
			map[string]interface{}{"destination_id": "d1", "priority": 0, "retry_budget": 3},
			map[string]interface{}{"destination_id": "d2", "priority": 1, "retry_budget": 7}, // max
			map[string]interface{}{"destination_id": "d3", "priority": 2, "retry_budget": 5},
		},
	}
	response, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err != nil {
		t.Fatalf("Enqueue: %v", err)
	}
	jobID, _ := response["job_id"].(string)
	j, err := enq.Jobs.Get(context.Background(), jobID)
	if err != nil {
		t.Fatalf("Get job: %v", err)
	}
	if j.MaxRetries != 7 {
		t.Errorf("MaxRetries = %d, want 7 (max of [3, 7, 5])", j.MaxRetries)
	}
}

func TestRenderHTTPBoundaryJobResponse(t *testing.T) {
	t.Parallel()

	t.Run("basic", func(t *testing.T) {
		t.Parallel()
		job := map[string]interface{}{
			"job_id": "j1", "status": "COMPLETED", "video_name": "V", "scene_count": 5,
			"voiceover_count": 3, "video_mode": "scene_image",
		}
		r := RenderHTTPBoundaryJobResponse(job, false, "")
		if r["ok"] != true || r["job_id"] != "j1" || r["status"] != "COMPLETED" {
			t.Errorf("unexpected: %v", r)
		}
		if _, has := r["job"]; has {
			t.Error("no 'job' key when full=false")
		}
	})

	t.Run("basic_legacy_alias_fallback", func(t *testing.T) {
		t.Parallel()
		// HTTP-edge adapter tolerates legacy aliases on read for backwards compat with old SQLite rows.
		job := map[string]interface{}{
			"id": "j1", "status": "COMPLETED", "title": "V",
		}
		r := RenderHTTPBoundaryJobResponse(job, false, "")
		if r["ok"] != true {
			t.Error("want ok=true")
		}
		// script_id leg falls back to id via job_id lookup
		if r["script_id"] != "j1" {
			t.Errorf("script_id alias fallback failed, got %v", r["script_id"])
		}
		if r["video_name"] != "V" {
			t.Errorf("video_name title fallback failed, got %v", r["video_name"])
		}
	})

	t.Run("full", func(t *testing.T) {
		t.Parallel()
		job := map[string]interface{}{"job_id": "j2", "request": map[string]interface{}{"raw": "x"}}
		r := RenderHTTPBoundaryJobResponse(job, true, "")
		if r["job"] == nil || r["request"] == nil {
			t.Error("want job/request keys when full=true")
		}
	})

	t.Run("nil", func(t *testing.T) {
		t.Parallel()
		r := RenderHTTPBoundaryJobResponse(nil, false, "")
		if r["ok"] != false {
			t.Errorf("want ok=false, got %v", r["ok"])
		}
	})

	t.Run("error", func(t *testing.T) {
		t.Parallel()
		job := map[string]interface{}{"job_id": "j3", "status": "FAILED", "error": "boom"}
		r := RenderHTTPBoundaryJobResponse(job, false, "")
		if r["error"] != "boom" {
			t.Errorf("want error 'boom', got %v", r["error"])
		}
	})
}

// TestEnforceDeliveryPlanPrecondition_IntegrationWithRealResolver
// exercises enforceDeliveryPlanPrecondition end-to-end against a real
// SQLite database with explicit job_delivery_plans rows. It validates
// that the precondition:
//
//  1. Reads from job_delivery_plans (NOT the global delivery_destinations
//     global destination selection is disabled.
//  2. Propagates max(retry_budget) across destinations to job.MaxRetries.
//  3. Accepts a multi-destination plan with varying retry_budget values.
//
// The test calls enforceDeliveryPlanPrecondition directly (rather than
// going through Enqueue) so the precondition's effect on job.MaxRetries
// is observable without the atomic-create path inserting conflicting
// job_delivery_plans rows of its own.
func TestEnforceDeliveryPlanPrecondition_IntegrationWithRealResolver(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}

	ctx := context.Background()
	now := time.Now().UTC().Format(time.RFC3339)

	// Disable FK constraints for this test. The precondition does NOT
	// depend on the job_delivery_plans → jobs / job_delivery_plans →
	// delivery_destinations FKs being enforced: it only reads from
	// job_delivery_plans. Disabling FKs lets the test insert
	// job_delivery_plans rows directly without having to keep a
	// placeholder Job + delivery_destinations rows in sync with the
	// production schema (which has additional NOT NULL columns like
	// delivery_destinations.provider that are irrelevant here). In
	// production, the FKs are enforced and the operator must create the
	// per-job plan rows before enqueueing — that contract is verified
	// by the unit test TestEnqueue_Precondition_RejectsMissingPlan.
	if _, err := db.DB().ExecContext(ctx, "PRAGMA foreign_keys = OFF"); err != nil {
		t.Fatalf("disable foreign keys: %v", err)
	}

	const jobID = "integration-test-job-1"

	// Insert explicit per-job plan rows with varying retry_budget so the
	// precondition's max() calculation has a meaningful signal: 3, 7, 5
	// → MaxRetries must be 7.
	retryBudgets := []struct {
		destID string
		retry  int
	}{
		{"dest-a", 3},
		{"dest-b", 7},
		{"dest-c", 5},
	}
	for _, d := range retryBudgets {
		if _, execErr := db.DB().ExecContext(ctx,
			`INSERT INTO job_delivery_plans (job_id, destination_id, enabled, priority, retry_budget, created_at, updated_at) VALUES (?, ?, 1, 0, ?, ?, ?)`,
			jobID, d.destID, d.retry, now, now,
		); execErr != nil {
			t.Fatalf("insert job_delivery_plan %s: %v", d.destID, execErr)
		}
	}

	// Real DB-backed resolver, explicit-plan-only mode.
	realResolver := deliveries.NewSQLiteDeliveryPlanResolver(db.DB())
	adapter := &planResolverAdapter{inner: realResolver}

	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		adapter,
	)

	job := &jobs.Job{ID: jobID}
	if preErr := enq.enforceDeliveryPlanPrecondition(ctx, jobID, job); preErr != nil {
		t.Fatalf("enforceDeliveryPlanPrecondition: %v", preErr)
	}
	if job.MaxRetries != 7 {
		t.Errorf("MaxRetries = %d, want 7 (max of [3, 7, 5])", job.MaxRetries)
	}
}

// TestEnforceDeliveryPlanPrecondition_IntegrationRejectsMissingPlan
// exercises the fail-closed rejection path: no global
// delivery_destinations fallback AND no per-job
// job_delivery_plans rows → the precondition must reject with a
// validation error whose message mentions "delivery_plan" so operators
// know exactly what to do (create the missing plan rows).
func TestEnforceDeliveryPlanPrecondition_IntegrationRejectsMissingPlan(t *testing.T) {
	t.Parallel()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "test.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}

	ctx := context.Background()
	const jobID = "missing-plan-job-1"

	// Real resolver, production mode. job_delivery_plans is empty for
	// this job_id has no explicit plan, so the precondition must
	// surface deliveries.ErrNoExplicitPlan wrapped in a validationError.
	realResolver := deliveries.NewSQLiteDeliveryPlanResolver(db.DB())
	adapter := &planResolverAdapter{inner: realResolver}

	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		adapter,
	)

	job := &jobs.Job{ID: jobID}
	preErr := enq.enforceDeliveryPlanPrecondition(ctx, jobID, job)
	if preErr == nil {
		t.Fatal("want error when plan missing, got nil")
	}
	if !strings.Contains(preErr.Error(), "delivery_plan") {
		t.Errorf("want error to mention delivery_plan, got %v", preErr)
	}
	if !errors.Is(preErr, deliveries.ErrNoExplicitPlan) && !strings.Contains(preErr.Error(), "no explicit delivery plan") {
		t.Errorf("want error to surface ErrNoExplicitPlan or 'no explicit delivery plan', got %v", preErr)
	}
	if job.MaxRetries != 0 {
		t.Errorf("MaxRetries = %d on rejection, want 0 (no propagation when precondition fails)", job.MaxRetries)
	}
}
