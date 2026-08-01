package enqueue

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/store"
)

func TestEnqueueFailureInjection_ClosedDatabaseFailsWithoutSuccess(t *testing.T) {
	dbPath := filepath.Join(t.TempDir(), "closed-enqueue.sqlite")
	db, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("close sqlite store: %v", err)
	}

	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		&mockPlanResolver{plan: &ResolvedPlan{JobID: "closed-db"}},
	)

	response, err := enq.Enqueue(context.Background(), failureInjectionPayload("closed-db"), costmodel.DefaultRequirements())
	if err == nil {
		t.Fatalf("closed database must fail closed; got response=%v and nil error", response)
	}
	if response != nil {
		t.Fatalf("closed database returned an ambiguous response: %#v", response)
	}
	if !strings.Contains(strings.ToLower(err.Error()), "closed") && !strings.Contains(strings.ToLower(err.Error()), "conn") {
		t.Fatalf("closed database error is not explicit enough: %v", err)
	}

	verify, err := store.NewSQLiteStore(dbPath)
	if err != nil {
		t.Fatalf("reopen sqlite store: %v", err)
	}
	defer verify.Close()
	assertNoPersistedEnqueueGraph(t, verify, "closed-db")
}

func TestEnqueueFailureInjection_PlanResolverErrorIsTypedAndNotSuccess(t *testing.T) {
	resolverErr := errors.New("injected plan resolver outage")
	resolver := &mockPlanResolver{err: resolverErr}
	enq := NewEnqueuer(nil, nil, nil, resolver)
	job := &jobs.Job{ID: "resolver-failure", MaxRetries: 11}

	err := enq.enforceDeliveryPlanPrecondition(context.Background(), job.ID, job)
	if err == nil {
		t.Fatal("injected PlanResolver error must not return success")
	}
	if !errors.Is(err, resolverErr) {
		t.Fatalf("PlanResolver error chain lost injected error: %v", err)
	}
	if !strings.Contains(err.Error(), "delivery_plan") || !strings.Contains(err.Error(), "resolve failed") {
		t.Fatalf("PlanResolver error is ambiguous: %v", err)
	}
	if got := ValidationErrorField(err); got != "delivery_plan" {
		t.Fatalf("ValidationErrorField = %q, want delivery_plan", got)
	}
	if resolver.calls != 1 {
		t.Fatalf("resolver calls = %d, want 1", resolver.calls)
	}
	if job.MaxRetries != 11 {
		t.Fatalf("failed precondition mutated job.MaxRetries to %d, want unchanged 11", job.MaxRetries)
	}
}

func failureInjectionPayload(videoName string) map[string]interface{} {
	return map[string]interface{}{
		"video_name":     videoName,
		"script_text":    "failure injection",
		"voiceover_path": "/tmp/failure-injection.mp3",
		"scenes":         []interface{}{map[string]interface{}{"text": "scene", "image_link": "https://example.com/scene.png"}},
		"delivery_plan":  []interface{}{map[string]interface{}{"destination_id": "drive-main", "retry_budget": 3}},
	}
}

func assertNoPersistedEnqueueGraph(t *testing.T, db *store.SQLiteStore, jobID string) {
	t.Helper()
	ctx := context.Background()
	queries := []struct {
		name  string
		query string
	}{
		{name: "jobs", query: `SELECT COUNT(*) FROM jobs WHERE job_id = ?`},
		{name: "tasks", query: `SELECT COUNT(*) FROM tasks WHERE job_id = ?`},
		{name: "delivery plans", query: `SELECT COUNT(*) FROM job_delivery_plans WHERE job_id = ?`},
	}
	for _, tc := range queries {
		var count int
		if err := db.DB().QueryRowContext(ctx, tc.query, jobID).Scan(&count); err != nil {
			t.Fatalf("count %s for %q: %v", tc.name, jobID, err)
		}
		if count != 0 {
			t.Fatalf("failure path persisted %d %s rows for job %q", count, tc.name, jobID)
		}
	}
}
