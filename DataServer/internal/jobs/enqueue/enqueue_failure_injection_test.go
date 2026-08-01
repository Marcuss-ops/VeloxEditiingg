package enqueue

import (
	"context"
	"errors"
	"path/filepath"
	"strings"
	"testing"

	"velox-server/internal/costmodel"
	"velox-server/internal/jobs"
	"velox-server/internal/socialclient"
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

func TestEnqueueFailureInjection_SocialValidatorHardErrorBlocksCreate(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "social-hard.sqlite"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	seedDestinations(t, db, map[string]bool{"drive-main": true})

	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		&mockPlanResolver{plan: &ResolvedPlan{JobID: "social-hard"}},
	).WithSocialValidator(&stubValidator{err: errors.Join(errors.New("destination rejected"), socialclient.ErrPermanent)})

	payload := failureInjectionPayload("social-hard")
	plan := payload["delivery_plan"].([]interface{})[0].(map[string]interface{})
	plan["external_destination_id"] = "social-hard-destination"
	response, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err == nil {
		t.Fatalf("hard SocialValidator error must fail closed; got response=%v", response)
	}
	if response != nil {
		t.Fatalf("hard SocialValidator error returned ambiguous response: %#v", response)
	}
	if !errors.Is(err, socialclient.ErrPermanent) {
		t.Fatalf("hard SocialValidator sentinel was not preserved: %v", err)
	}
	if got := ValidationErrorField(err); got != "delivery_plan.0.external_destination_id" {
		t.Fatalf("ValidationErrorField = %q, want delivery_plan.0.external_destination_id", got)
	}
	assertNoPersistedEnqueueGraph(t, db, "")
}

func TestEnqueueFailureInjection_SocialValidatorUnknownErrorFailsClosed(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "social-unknown.sqlite"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	seedDestinations(t, db, map[string]bool{"drive-main": true})
	validatorErr := errors.New("injected validator contract failure")
	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		&mockPlanResolver{plan: &ResolvedPlan{JobID: "social-unknown"}},
	).WithSocialValidator(&stubValidator{err: validatorErr})

	payload := failureInjectionPayload("social-unknown")
	plan := payload["delivery_plan"].([]interface{})[0].(map[string]interface{})
	plan["external_destination_id"] = "social-unknown-destination"
	response, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err == nil {
		t.Fatalf("unknown SocialValidator error must fail closed; got response=%v", response)
	}
	if response != nil {
		t.Fatalf("unknown SocialValidator error returned ambiguous response: %#v", response)
	}
	if !errors.Is(err, validatorErr) {
		t.Fatalf("unknown SocialValidator error was not preserved: %v", err)
	}
	if !strings.Contains(err.Error(), "non-retryable or unclassified error") {
		t.Fatalf("unknown SocialValidator error is ambiguous: %v", err)
	}
	assertNoPersistedEnqueueGraph(t, db, "")
}

func TestEnqueueFailureInjection_SocialValidatorTransientIsExplicitSoftSuccess(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "social-transient.sqlite"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	seedDestinations(t, db, map[string]bool{"drive-main": true})
	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		&mockPlanResolver{plan: &ResolvedPlan{JobID: "social-transient"}},
	).WithSocialValidator(&stubValidator{err: errors.Join(errors.New("social service unavailable"), socialclient.ErrTransient)})

	payload := failureInjectionPayload("social-transient")
	plan := payload["delivery_plan"].([]interface{})[0].(map[string]interface{})
	plan["external_destination_id"] = "social-transient-destination"
	response, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err != nil {
		t.Fatalf("transient SocialValidator error must soft-pass; got %v", err)
	}
	if response == nil || response["ok"] != true {
		t.Fatalf("soft-pass must return explicit ok=true response, got %#v", response)
	}
	jobID, _ := response["job_id"].(string)
	if jobID == "" {
		t.Fatal("soft-pass response has no job_id")
	}
	job, err := enq.Jobs.Get(context.Background(), jobID)
	if err != nil || job == nil {
		t.Fatalf("soft-pass committed job missing: job=%v err=%v", job, err)
	}
}

func TestEnqueueFailureInjection_CreateJobWithTaskRollsBackOnDestinationFailure(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "creator-failure.sqlite"))
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	seedDestinations(t, db, map[string]bool{"drive-main": true})
	enq := NewEnqueuer(
		store.NewAtomicJobTaskCreator(db),
		store.NewSQLiteJobRepository(db),
		nil,
		&mockPlanResolver{plan: &ResolvedPlan{JobID: "creator-failure"}},
	)

	payload := failureInjectionPayload("creator-failure")
	plan := payload["delivery_plan"].([]interface{})[0].(map[string]interface{})
	plan["destination_id"] = "missing-destination"
	response, err := enq.Enqueue(context.Background(), payload, costmodel.DefaultRequirements())
	if err == nil {
		t.Fatalf("CreateJobWithTask destination failure must return an error; got response=%v", response)
	}
	if response != nil {
		t.Fatalf("CreateJobWithTask failure returned ambiguous response: %#v", response)
	}
	if !errors.Is(err, store.ErrDestinationNotFound) {
		t.Fatalf("destination failure sentinel was not preserved: %v", err)
	}
	if !strings.Contains(err.Error(), "missing-destination") {
		t.Fatalf("creator error does not identify the rejected destination: %v", err)
	}
	assertNoPersistedEnqueueGraph(t, db, "")

	var count int
	if err := db.DB().QueryRowContext(context.Background(), `SELECT COUNT(*) FROM jobs WHERE video_name = ?`, "creator-failure").Scan(&count); err != nil {
		t.Fatalf("count creator-failure jobs: %v", err)
	}
	if count != 0 {
		t.Fatalf("CreateJobWithTask rollback leaked %d job rows", count)
	}
}

func assertNoPersistedEnqueueGraph(t *testing.T, db *store.SQLiteStore, jobID string) {
	t.Helper()
	ctx := context.Background()
	queries := []struct {
		name  string
		query string
	}{
		{name: "jobs", query: `SELECT COUNT(*) FROM jobs`},
		{name: "tasks", query: `SELECT COUNT(*) FROM tasks`},
		{name: "delivery plans", query: `SELECT COUNT(*) FROM job_delivery_plans`},
	}
	for _, tc := range queries {
		var count int
		if err := db.DB().QueryRowContext(ctx, tc.query).Scan(&count); err != nil {
			t.Fatalf("count %s for %q: %v", tc.name, jobID, err)
		}
		if count != 0 {
			t.Fatalf("failure path persisted %d %s rows (job=%q)", count, tc.name, jobID)
		}
	}
}
