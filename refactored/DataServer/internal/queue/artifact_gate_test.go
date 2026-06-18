package queue

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"velox-server/internal/store"
)

// testJobRepo is a minimal in-memory JobRepository for artifact gate tests.
type testArtifactGateRepo struct {
	jobs map[string]*store.Job
}

func newTestArtifactGateRepo() *testArtifactGateRepo {
	return &testArtifactGateRepo{jobs: make(map[string]*store.Job)}
}

func (r *testArtifactGateRepo) insertJob(j *store.Job) {
	r.jobs[j.JobID] = j
}

var errTestJobNotFound = errors.New("test: job not found")

func (r *testArtifactGateRepo) GetJob(_ context.Context, jobID string) (*store.Job, error) {
	j, ok := r.jobs[jobID]
	if !ok {
		return nil, errTestJobNotFound
	}
	return j, nil
}

func (r *testArtifactGateRepo) Transition(_ context.Context, p store.TransitionParams) error {
	j, ok := r.jobs[p.JobID]
	if !ok {
		return store.ErrJobNotFound
	}
	if j.Status != p.ExpectedStatus {
		return store.ErrTransitionConflict
	}
	if j.Revision != p.Revision {
		return store.ErrTransitionConflict
	}
	j.Status = p.NewStatus
	j.Revision++
	return nil
}

func (r *testArtifactGateRepo) CreateJob(_ context.Context, _ store.CreateJobParams) error {
	return nil
}
func (r *testArtifactGateRepo) ClaimNext(_ context.Context, _ store.ClaimParams) (*store.ClaimResult, error) {
	return nil, store.ErrNoClaimableJob
}
func (r *testArtifactGateRepo) StartJob(_ context.Context, _ store.StartJobParams) error {
	return nil
}
func (r *testArtifactGateRepo) CompleteJob(_ context.Context, _ store.CompleteJobParams) error {
	return nil
}
func (r *testArtifactGateRepo) ListByStatus(_ context.Context, _ []store.JobStatus, _ int) ([]store.Job, error) {
	return nil, nil
}
func (r *testArtifactGateRepo) RenewLease(_ context.Context, _ store.RenewLeaseParams) error {
	return nil
}
func (r *testArtifactGateRepo) LeaseJob(_ context.Context, _, _ string) error {
	return nil
}
func (r *testArtifactGateRepo) ReleaseClaim(_ context.Context, _ string) error {
	return nil
}
func (r *testArtifactGateRepo) RequeueZombieJobs(_ context.Context, _ time.Duration) (int, error) {
	return 0, nil
}
func (r *testArtifactGateRepo) UpdateJobResult(_ context.Context, _ string, _ []byte) error {
	return nil
}

// testEventStore is a minimal EventStore for artifact gate tests.
type testArtifactGateEventStore struct {
	events []string
}

func (s *testArtifactGateEventStore) LogJobEvent(_, eventType string, _ map[string]interface{}) error {
	s.events = append(s.events, eventType)
	return nil
}
func (s *testArtifactGateEventStore) UpdateJobSupplementary(_ string, _ map[string]interface{}) error {
	return nil
}
func (s *testArtifactGateEventStore) AddJobHistory(_, _, _, _ string, _ map[string]interface{}) error {
	return nil
}
func (s *testArtifactGateEventStore) AddJobLog(_, _, _ string, _ bool) error { return nil }
func (s *testArtifactGateEventStore) SetJobRequest(_ string, _ []byte) error { return nil }
func (s *testArtifactGateEventStore) UpsertJobResult(_ string, _ []byte) error { return nil }
func (s *testArtifactGateEventStore) GetJob(_ context.Context, _ string) (map[string]interface{}, error) {
	return nil, nil
}
func (s *testArtifactGateEventStore) GetActiveJobs() (map[string]map[string]interface{}, error) {
	return nil, nil
}
func (s *testArtifactGateEventStore) JobCounts(_ context.Context) (map[string]int64, error) {
	return nil, nil
}
func (s *testArtifactGateEventStore) ListJobsByStatus(_ []string, _ int) ([]map[string]interface{}, error) {
	return nil, nil
}
func (s *testArtifactGateEventStore) DeleteJob(_ string) error              { return nil }
func (s *testArtifactGateEventStore) ArchiveOldJobs(_ time.Time) (int64, error) { return 0, nil }
func (s *testArtifactGateEventStore) TransitionJobStatus(_ context.Context, _, _, _ string, _ int) (int, error) {
	return 0, nil
}
func (s *testArtifactGateEventStore) UpdateArtifactStatus(_ context.Context, _, _ string) error {
	return nil
}
func (s *testArtifactGateEventStore) CompleteJobTx(_ context.Context, _ string, _ int64, _ string) error {
	return nil
}

var _ store.EventStore = (*testArtifactGateEventStore)(nil)

func newTestLifecycleForArtifactGate() (*LifecycleService, *testArtifactGateRepo, *testArtifactGateEventStore) {
	repo := newTestArtifactGateRepo()
	es := &testArtifactGateEventStore{}
	lc := &LifecycleService{jobRepo: repo, eventStore: es}
	return lc, repo, es
}

func runningJob(jobID, workerID, leaseID string) *store.Job {
	return &store.Job{
		JobID:      jobID,
		Status:     store.JobStatusRunning,
		AssignedTo: workerID,
		LeaseID:    leaseID,
		Revision:   1,
	}
}

// Test 1: success senza artifact — job deve restare RENDER_FINISHED, non SUCCEEDED
func TestArtifactGate_SuccessWithoutArtifact_StaysRenderFinished(t *testing.T) {
	lc, repo, es := newTestLifecycleForArtifactGate()
	ctx := context.Background()

	repo.insertJob(runningJob("job-1", "worker-A", "lease-1"))

	if err := lc.RecordRenderFinished(ctx, "job-1", "worker-A", "lease-1", 0, 1); err != nil {
		t.Fatalf("RecordRenderFinished: %v", err)
	}

	job, _ := repo.GetJob(ctx, "job-1")
	if job.Status != store.JobStatusRenderFinished {
		t.Fatalf("expected RENDER_FINISHED, got %s", job.Status)
	}

	// Attempting CompleteJob should fail — RUNNING→SUCCEEDED is no longer valid
	if err := lc.CompleteJob(ctx, "job-1"); err == nil {
		t.Fatal("CompleteJob should fail from RENDER_FINISHED — artifact gate blocks direct completion")
	}

	// Verify render_finished event was logged
	found := false
	for _, e := range es.events {
		if e == "render_finished" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected render_finished event to be logged")
	}
}

// Test 2: artifact con lease errata — deve essere rifiutato
func TestArtifactGate_ArtifactWithWrongLease_Rejected(t *testing.T) {
	lc, repo, _ := newTestLifecycleForArtifactGate()
	ctx := context.Background()

	repo.insertJob(runningJob("job-2", "worker-A", "correct-lease"))

	// Try with wrong lease
	err := lc.RecordRenderFinished(ctx, "job-2", "worker-A", "wrong-lease", 0, 1)
	if err == nil {
		t.Fatal("RecordRenderFinished should fail with wrong lease")
	}
	if !contains(err.Error(), "lease mismatch") {
		t.Fatalf("expected lease mismatch error, got: %v", err)
	}

	// Job should still be RUNNING
	job, _ := repo.GetJob(ctx, "job-2")
	if job.Status != store.JobStatusRunning {
		t.Fatalf("expected RUNNING after rejected attempt, got %s", job.Status)
	}
}

// Test 3: artifact di un altro worker — deve essere rifiutato
func TestArtifactGate_ArtifactFromWrongWorker_Rejected(t *testing.T) {
	lc, repo, _ := newTestLifecycleForArtifactGate()
	ctx := context.Background()

	repo.insertJob(runningJob("job-3", "worker-A", "lease-3"))

	// Try from different worker
	err := lc.RecordRenderFinished(ctx, "job-3", "worker-B", "lease-3", 0, 1)
	if err == nil {
		t.Fatal("RecordRenderFinished should fail from wrong worker")
	}
	if !contains(err.Error(), "does not own job") {
		t.Fatalf("expected ownership error, got: %v", err)
	}

	job, _ := repo.GetJob(ctx, "job-3")
	if job.Status != store.JobStatusRunning {
		t.Fatalf("expected RUNNING after rejected attempt, got %s", job.Status)
	}
}

// Test 4: doppio JobResult — deve essere idempotente
func TestArtifactGate_DoubleJobResult_Idempotent(t *testing.T) {
	lc, repo, _ := newTestLifecycleForArtifactGate()
	ctx := context.Background()

	repo.insertJob(runningJob("job-4", "worker-A", "lease-4"))

	// First success
	if err := lc.RecordRenderFinished(ctx, "job-4", "worker-A", "lease-4", 0, 1); err != nil {
		t.Fatalf("first RecordRenderFinished: %v", err)
	}

	// Second success — should be idempotent
	if err := lc.RecordRenderFinished(ctx, "job-4", "worker-A", "lease-4", 0, 0); err != nil {
		t.Fatalf("second RecordRenderFinished should be idempotent: %v", err)
	}

	job, _ := repo.GetJob(ctx, "job-4")
	if job.Status != store.JobStatusRenderFinished {
		t.Fatalf("expected RENDER_FINISHED after double call, got %s", job.Status)
	}
}

// Test 5: artifact duplicato — double artifact registration
func TestArtifactGate_DuplicateArtifact_HandledGracefully(t *testing.T) {
	lc, repo, es := newTestLifecycleForArtifactGate()
	ctx := context.Background()

	repo.insertJob(runningJob("job-5", "worker-A", "lease-5"))

	// First success
	if err := lc.RecordRenderFinished(ctx, "job-5", "worker-A", "lease-5", 0, 1); err != nil {
		t.Fatalf("RecordRenderFinished: %v", err)
	}

	// Second success from same worker — idempotent
	if err := lc.RecordRenderFinished(ctx, "job-5", "worker-A", "lease-5", 0, 0); err != nil {
		t.Fatalf("second RecordRenderFinished: %v", err)
	}

	// Verify only one render_finished event
	count := 0
	for _, e := range es.events {
		if e == "render_finished" {
			count++
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 render_finished event, got %d", count)
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > 0 && containsSubstr(s, substr))
}

func containsSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
