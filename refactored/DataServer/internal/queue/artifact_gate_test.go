package queue

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"velox-server/internal/store"
)

// testArtifactGateRepo is a minimal in-memory JobRepository for artifact gate tests.
type testArtifactGateRepo struct {
	jobs   map[string]*store.Job
	events []string
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
		return errTestJobNotFound
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

func (r *testArtifactGateRepo) RecordRenderFinished(_ context.Context, cmd store.RecordRenderFinishedCommand) error {
	j, ok := r.jobs[cmd.JobID]
	if !ok {
		return errTestJobNotFound
	}
	if j.Status != store.JobStatusRunning {
		return fmt.Errorf("cannot record render finished: job %s is in status %s, expected RUNNING", cmd.JobID, j.Status)
	}
	if j.AssignedTo != cmd.WorkerID {
		return fmt.Errorf("worker %s does not own job %s (assigned to %s)", cmd.WorkerID, cmd.JobID, j.AssignedTo)
	}
	if cmd.LeaseID != "" && j.LeaseID != cmd.LeaseID {
		return fmt.Errorf("lease mismatch for job %s: expected %s, got %s", cmd.JobID, j.LeaseID, cmd.LeaseID)
	}
	if cmd.ExpectedRevision != 0 && j.Revision != cmd.ExpectedRevision {
		return fmt.Errorf("revision mismatch for job %s: expected %d, got %d", cmd.JobID, cmd.ExpectedRevision, j.Revision)
	}
	r.events = append(r.events, "render_finished")
	return nil
}

// testArtifactGateEventStore is a minimal EventStore for artifact gate tests.
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
func (s *testArtifactGateEventStore) CompleteJobTx(_ context.Context, _ string, _ int64, _ string, _ string, _ int) error {
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

// Test 1: RecordRenderFinished does not change job status — job stays RUNNING
func TestArtifactGate_RenderFinished_StaysRunning(t *testing.T) {
	lc, repo, _ := newTestLifecycleForArtifactGate()
	ctx := context.Background()

	repo.insertJob(runningJob("job-1", "worker-A", "lease-1"))

	cmd := store.RecordRenderFinishedCommand{
		JobID:            "job-1",
		WorkerID:         "worker-A",
		LeaseID:          "lease-1",
		AttemptNumber:    0,
		ExpectedRevision: 1,
	}
	if err := lc.RecordRenderFinished(ctx, cmd); err != nil {
		t.Fatalf("RecordRenderFinished: %v", err)
	}

	job, _ := repo.GetJob(ctx, "job-1")
	if job.Status != store.JobStatusRunning {
		t.Fatalf("expected job to stay RUNNING, got %s", job.Status)
	}

	// Verify render_finished event was logged
	found := false
	for _, e := range repo.events {
		if e == "render_finished" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected render_finished event to be logged")
	}

	// CompleteJob should still work from RUNNING
	if err := lc.CompleteJob(ctx, "job-1"); err != nil {
		t.Fatalf("CompleteJob should succeed from RUNNING: %v", err)
	}
	if job.Status != store.JobStatusSucceeded {
		t.Fatalf("expected SUCCEEDED after CompleteJob, got %s", job.Status)
	}
}

// Test 2: wrong lease — must be rejected
func TestArtifactGate_RenderFinished_WrongLease_Rejected(t *testing.T) {
	lc, repo, _ := newTestLifecycleForArtifactGate()
	ctx := context.Background()

	repo.insertJob(runningJob("job-2", "worker-A", "correct-lease"))

	cmd := store.RecordRenderFinishedCommand{
		JobID:            "job-2",
		WorkerID:         "worker-A",
		LeaseID:          "wrong-lease",
		AttemptNumber:    0,
		ExpectedRevision: 1,
	}
	err := lc.RecordRenderFinished(ctx, cmd)
	if err == nil {
		t.Fatal("RecordRenderFinished should fail with wrong lease")
	}
	if !contains(err.Error(), "lease mismatch") {
		t.Fatalf("expected lease mismatch error, got: %v", err)
	}

	job, _ := repo.GetJob(ctx, "job-2")
	if job.Status != store.JobStatusRunning {
		t.Fatalf("expected RUNNING after rejected attempt, got %s", job.Status)
	}
}

// Test 3: wrong worker — must be rejected
func TestArtifactGate_RenderFinished_WrongWorker_Rejected(t *testing.T) {
	lc, repo, _ := newTestLifecycleForArtifactGate()
	ctx := context.Background()

	repo.insertJob(runningJob("job-3", "worker-A", "lease-3"))

	cmd := store.RecordRenderFinishedCommand{
		JobID:            "job-3",
		WorkerID:         "worker-B",
		LeaseID:          "lease-3",
		AttemptNumber:    0,
		ExpectedRevision: 1,
	}
	err := lc.RecordRenderFinished(ctx, cmd)
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

// Test 4: non-RUNNING job — must be rejected
func TestArtifactGate_RenderFinished_NonRunning_Rejected(t *testing.T) {
	lc, repo, _ := newTestLifecycleForArtifactGate()
	ctx := context.Background()

	job := runningJob("job-4", "worker-A", "lease-4")
	job.Status = store.JobStatusSucceeded
	repo.insertJob(job)

	cmd := store.RecordRenderFinishedCommand{
		JobID:            "job-4",
		WorkerID:         "worker-A",
		LeaseID:          "lease-4",
		AttemptNumber:    0,
		ExpectedRevision: 1,
	}
	err := lc.RecordRenderFinished(ctx, cmd)
	if err == nil {
		t.Fatal("RecordRenderFinished should fail for non-RUNNING job")
	}
	if !contains(err.Error(), "expected RUNNING") {
		t.Fatalf("expected RUNNING error, got: %v", err)
	}
}

// Test 5: duplicate RecordRenderFinished — idempotent event logging
func TestArtifactGate_RenderFinished_Duplicate_Idempotent(t *testing.T) {
	lc, repo, _ := newTestLifecycleForArtifactGate()
	ctx := context.Background()

	repo.insertJob(runningJob("job-5", "worker-A", "lease-5"))

	cmd := store.RecordRenderFinishedCommand{
		JobID:            "job-5",
		WorkerID:         "worker-A",
		LeaseID:          "lease-5",
		AttemptNumber:    0,
		ExpectedRevision: 1,
	}

	// First call
	if err := lc.RecordRenderFinished(ctx, cmd); err != nil {
		t.Fatalf("first RecordRenderFinished: %v", err)
	}

	// Second call — should succeed (idempotent event)
	cmd.AttemptNumber = 1
	if err := lc.RecordRenderFinished(ctx, cmd); err != nil {
		t.Fatalf("second RecordRenderFinished should be idempotent: %v", err)
	}

	// Job still RUNNING
	job, _ := repo.GetJob(ctx, "job-5")
	if job.Status != store.JobStatusRunning {
		t.Fatalf("expected RUNNING after duplicate call, got %s", job.Status)
	}

	// Two render_finished events logged
	count := 0
	for _, e := range repo.events {
		if e == "render_finished" {
			count++
		}
	}
	if count != 2 {
		t.Fatalf("expected 2 render_finished events, got %d", count)
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
