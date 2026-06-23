// =====================================================================
// PR-2 / fix/canonical-attempt-identity — handler-level spoofing tests.
//
// handleTaskResult (DataServer/internal/grpcserver/handler_jobs.go)
// delegates to TaskReportIngestionService.IngestTaskResult which runs
// ValidateIdentityTuple BEFORE any close-write. The audit invariant
// demands that a TaskResult whose wire tuple does not match the
// canonical attempt in task_attempts MUST be dropped silently (logged,
// no state drift) so the close-write + Job roll-up + artifact-register
// paths cannot be reached.
//
// Audit-closure note: pb.TaskResult (shared/controltransport/pb) does
// NOT carry AttemptNumber today, so handleTaskResult builds
// IngestCommand{AttemptNumber: 0} (default). The validator's cheap
// field-presence check "AttemptNumber must be >0" fires BEFORE the
// lookup-miss / strict-compare paths. The handler-layer wire-spoof
// rejection therefore runs through the cheap-check branch, NOT the
// lookup-miss branch. The canonical sentinel
// taskattempts.ErrIdentityMismatch is still produced (proven by
// TestIngestionService_ValidateIdentityTuple_* in
// ingest/service_test.go) for any spoof where the validator reaches
// the lookup / strict-compare layer.
//
// This file splits the audit contract into TWO sub-tests for clearer
// failure-triage:
//   (A) HandlerLayerDrop — handleTaskResult fires; the close-write
//       (taskRepo.TransitionTaskToTerminalAtomic), Job roll-up
//       (jobsRepo.SetStatus), AND artifact register (outputArts.Register)
//       stay at 0 calls. The handler dropped the spoof — by whatever
//       validator branch fired — short-circuiting the audit-closure
//       path. (Independent of the canonical sentinel.)
//   (B) CanonicalSentinel_WireLeaseIDMismatch — directly probe
//       ValidateIdentityTuple on a fully-populated
//       IngestCommand{LeaseID:"L-attacker", AttemptNumber: 1} to
//       confirm the canonical taskattempts.ErrIdentityMismatch wrap is
//       produced on the lookup-miss path that the handler will route
//       through once pb.TaskResult grows the AttemptNumber field
//       (audit follow-up).
// =====================================================================

package grpcserver

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"velox-server/internal/costmodel"
	"velox-server/internal/ingest"
	"velox-server/internal/jobs"
	"velox-server/internal/store"
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-server/internal/taskoutput_artifacts"

	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/types/known/structpb"
)

// spoofFixture bakes the canonical attempt + the wire-spoof tuple
// constants so both sub-tests share the same setup. Read once at the
// top of each sub-test.
type spoofFixture struct {
	taskID         string
	workerID       string
	canonicalLease string // pre-seeded at the canonical attempt store
	wireLease      string // what the wire sends — deliberately mismatched
	wireAttemptID  string
	wireJobID      string
}

func newSpoofFixture() spoofFixture {
	return spoofFixture{
		taskID:         "T-spoof",
		workerID:       "w-spoof",
		canonicalLease: "L-canonical",
		wireLease:      "L-attacker",
		wireAttemptID:  "A-canonical",
		wireJobID:      "J-canonical",
	}
}

// ---- Stub surface for both sub-tests ----
//
// Each stub UNUSED-method panics so we surface any unexpected code
// path while satisfying the underlying interface. Only the methods
// reachable through handleTaskResult → ingestionSvc.IngestTaskResult →
// validator/close-write are exercised; the rest stay loud.

// spoofStubTaskRepo: taskgraph.Repository. Only TransitionTaskToTerminalAtomic
// + Get + List are reachable via these sub-tests; on the rejection path
// neither transition nor list-driven rollup is expected to fire.
type spoofStubTaskRepo struct {
	mu              sync.Mutex
	transitionCalls int
	transitionErr   error
	nowTask         taskgraph.Task
	listTasks       []taskgraph.Task
}

func (s *spoofStubTaskRepo) Get(_ context.Context, id string) (*taskgraph.Task, error) {
	if s.nowTask.ID == id {
		cp := s.nowTask
		return &cp, nil
	}
	return nil, errors.New("not found (spoof stub)")
}

func (s *spoofStubTaskRepo) List(_ context.Context, _ taskgraph.Filter) ([]taskgraph.Task, error) {
	cp := make([]taskgraph.Task, len(s.listTasks))
	copy(cp, s.listTasks)
	return cp, nil
}

func (s *spoofStubTaskRepo) TransitionTaskToTerminalAtomic(
	_ context.Context, _, _, _ string,
	_ taskgraph.Status, _ taskattempts.AttemptStatus, _, _ string,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.transitionCalls++
	return s.transitionErr
}

// Panics for every other Repository method so any stray code path is loud.
func (s *spoofStubTaskRepo) Create(_ context.Context, _ *taskgraph.Task) error {
	panic("spoofStubTaskRepo.Create: not exercised by IngestTaskResult on rejection path")
}
func (s *spoofStubTaskRepo) ListByJobID(_ context.Context, _ string) *taskgraph.Task {
	panic("spoofStubTaskRepo.ListByJobID: not exercised by IngestTaskResult on rejection path")
}
func (s *spoofStubTaskRepo) SetStatus(_ context.Context, _ string, _, _ taskgraph.Status, _ int) error {
	panic("spoofStubTaskRepo.SetStatus: not exercised by IngestTaskResult on rejection path")
}
func (s *spoofStubTaskRepo) Lease(_ context.Context, _, _, _ string) error {
	panic("spoofStubTaskRepo.Lease: not exercised by IngestTaskResult on rejection path")
}
func (s *spoofStubTaskRepo) GetByJobID(_ context.Context, jobID string) (*taskgraph.Task, error) {
	for i := range s.listTasks {
		if s.listTasks[i].JobID == jobID {
			cp := s.listTasks[i]
			return &cp, nil
		}
	}
	return nil, nil
}
func (s *spoofStubTaskRepo) ClaimNextReadyTask(_ context.Context, _, _ string) (*taskgraph.TaskWithSpec, error) {
	panic("spoofStubTaskRepo.ClaimNextReadyTask: not exercised by IngestTaskResult on rejection path")
}
func (s *spoofStubTaskRepo) ReleaseLease(_ context.Context, _ string) error {
	panic("spoofStubTaskRepo.ReleaseLease: not exercised by IngestTaskResult on rejection path")
}
func (s *spoofStubTaskRepo) Start(_ context.Context, _, _, _ string, _, _ int) error {
	panic("spoofStubTaskRepo.Start: not exercised by IngestTaskResult on rejection path")
}
func (s *spoofStubTaskRepo) Fail(_ context.Context, _, _ string, _ int) error {
	panic("spoofStubTaskRepo.Fail: not exercised by IngestTaskResult on rejection path")
}
func (s *spoofStubTaskRepo) IncrementAttempt(_ context.Context, _ string) error {
	panic("spoofStubTaskRepo.IncrementAttempt: not exercised by IngestTaskResult on rejection path")
}
func (s *spoofStubTaskRepo) AreDependenciesSatisfied(_ context.Context, _ []string) (bool, error) {
	panic("spoofStubTaskRepo.AreDependenciesSatisfied")
}
func (s *spoofStubTaskRepo) AcceptTaskAtomic(_ context.Context, _ *taskattempts.TaskAttempt, _ int) error {
	panic("spoofStubTaskRepo.AcceptTaskAtomic")
}
func (s *spoofStubTaskRepo) RenewLease(_ context.Context, _, _, _ string, _ time.Time, _ int) error {
	panic("spoofStubTaskRepo.RenewLease")
}
func (s *spoofStubTaskRepo) ExpireTaskLeaseAtomic(_ context.Context, _, _, _ string, _ int) (taskgraph.ExpireResult, error) {
	panic("spoofStubTaskRepo.ExpireTaskLeaseAtomic")
}
func (s *spoofStubTaskRepo) RequeueExpiredLeases(_ context.Context, _ string, _ int) ([]taskgraph.RequeueCandidate, error) {
	panic("spoofStubTaskRepo.RequeueExpiredLeases")
}
func (s *spoofStubTaskRepo) Delete(_ context.Context, _ string) error {
	panic("spoofStubTaskRepo.Delete")
}
func (s *spoofStubTaskRepo) ClaimNextWithAttemptAtomic(_ context.Context, _, _ string) (*taskgraph.TaskWithSpec, *taskattempts.TaskAttempt, error) {
	panic("spoofStubTaskRepo.ClaimNextWithAttemptAtomic")
}

var _ taskgraph.Repository = (*spoofStubTaskRepo)(nil)

// spoofStubJobsRepo: jobs.Repository. SetStatus MUST stay zero on the
// rejection path because validator should have failed first.
type spoofStubJobsRepo struct {
	mu             sync.Mutex
	getJob         *jobs.Job
	setStatusCalls int
}

func (s *spoofStubJobsRepo) Get(_ context.Context, _ string) (*jobs.Job, error) {
	if s.getJob == nil {
		return nil, errors.New("job not found (spoof stub)")
	}
	cp := *s.getJob
	return &cp, nil
}
func (s *spoofStubJobsRepo) Counts(_ context.Context) (jobs.Counts, error) {
	return jobs.Counts{}, nil
}
func (s *spoofStubJobsRepo) List(_ context.Context, _ jobs.Filter) ([]jobs.Job, error) {
	if s.getJob == nil {
		return nil, nil
	}
	cp := *s.getJob
	return []jobs.Job{cp}, nil
}
func (s *spoofStubJobsRepo) SetStatus(_ context.Context, _ string, _, _ jobs.Status) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setStatusCalls++
	return nil
}

// All other jobs.Writer methods panic on call as sentinels so any
// accidental write path surfaces loud while satisfying the
// jobs.Repository interface.
func (s *spoofStubJobsRepo) Cancel(_ context.Context, _ string, _ string, _ int) error {
	panic("spoofStubJobsRepo.Cancel")
}
func (s *spoofStubJobsRepo) Lease(_ context.Context, _, _ string) error {
	panic("spoofStubJobsRepo.Lease")
}
func (s *spoofStubJobsRepo) Fail(_ context.Context, _ string, _ string) error {
	panic("spoofStubJobsRepo.Fail")
}
func (s *spoofStubJobsRepo) Start(_ context.Context, _, _, _ string, _, _ int) error {
	panic("spoofStubJobsRepo.Start")
}
func (s *spoofStubJobsRepo) RenewLease(_ context.Context, _, _, _ string, _ time.Time, _ bool, _ int) error {
	panic("spoofStubJobsRepo.RenewLease")
}
func (s *spoofStubJobsRepo) FailWithRetry(_ context.Context, _, _, _ string, _ bool, _ int) error {
	panic("spoofStubJobsRepo.FailWithRetry")
}
func (s *spoofStubJobsRepo) RequeueExpiredLeases(_ context.Context, _ time.Time, _ int) ([]jobs.RequeueResult, error) {
	panic("spoofStubJobsRepo.RequeueExpiredLeases")
}
func (s *spoofStubJobsRepo) ClaimNext(_ context.Context, _ string, _ []string) (*jobs.ClaimNextResult, error) {
	panic("spoofStubJobsRepo.ClaimNext")
}
func (s *spoofStubJobsRepo) ClaimNextForProfile(_ context.Context, _ string, _ []string, _ costmodel.WorkerProfile, _ int) (*jobs.ClaimNextResult, error) {
	panic("spoofStubJobsRepo.ClaimNextForProfile")
}
func (s *spoofStubJobsRepo) ReleaseLease(_ context.Context, _ string) error {
	panic("spoofStubJobsRepo.ReleaseLease")
}
func (s *spoofStubJobsRepo) RecordRenderFinished(_ context.Context, _, _, _ string, _, _ int) error {
	panic("spoofStubJobsRepo.RecordRenderFinished")
}
func (s *spoofStubJobsRepo) Delete(_ context.Context, _ string) error {
	panic("spoofStubJobsRepo.Delete")
}

var _ jobs.Repository = (*spoofStubJobsRepo)(nil)

// spoofStubAttemptRepo: taskattempts.Repository.
// GetByTaskIDAndWorkerAndLease is the only method exercised here.
// On the (A) handler-layer path the validator short-circuits via
// cheap-check so the lookup is never reached; on the (B) canonical-
// sentinel probe the wire lease_id ("L-attacker") deliberately
// differs from the canonical seed ("L-canonical") so the lookup
// returns nil → ErrIdentityMismatch wrap.
type spoofStubAttemptRepo struct {
	mu       sync.Mutex
	attempts map[string]*taskattempts.TaskAttempt

	// Scorecard v1 / F1 — typed metrics persistence counters. Tests
	// inspect these to assert that the typed pb.TaskExecutionMetrics
	// payload was actually persisted through the master ingestion path.
	persistMetricsCalls int
	persistCacheCalls   int
	persistCostCalls    int

	lastMetrics    taskattempts.AttemptMetrics
	lastCacheStats taskattempts.AttemptCacheStats
	lastCostBasis  taskattempts.AttemptCostBasis
}

// seedCanonical inserts an attempt at the canonical (task_id, worker_id, lease_id)
// tuple with identity fields (A-canonical/J-canonical/attempt_number=1)
// matching the wire's expected identity. Variant on the wire-side channels
// (esp. lease_id) produces the spoof.
func (s *spoofStubAttemptRepo) seedCanonical(taskID, workerID, leaseID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.attempts == nil {
		s.attempts = map[string]*taskattempts.TaskAttempt{}
	}
	key := taskID + "|" + workerID + "|" + leaseID
	s.attempts[key] = &taskattempts.TaskAttempt{
		ID:            "A-canonical",
		TaskID:        taskID,
		WorkerID:      workerID,
		LeaseID:       leaseID,
		AttemptNumber: 1,
		JobID:         "J-canonical",
		Status:        taskattempts.AttemptStatusRunning,
	}
}

func (s *spoofStubAttemptRepo) GetByTaskIDAndWorkerAndLease(_ context.Context, taskID, workerID, leaseID string) (*taskattempts.TaskAttempt, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	key := taskID + "|" + workerID + "|" + leaseID
	if att, ok := s.attempts[key]; ok {
		cp := *att
		return &cp, nil
	}
	return nil, nil
}

// All other taskattempts.Repository methods panic-on-call as sentinels.
func (s *spoofStubAttemptRepo) Get(_ context.Context, _ string) (*taskattempts.TaskAttempt, error) {
	panic("spoofStubAttemptRepo.Get: not exercised")
}
func (s *spoofStubAttemptRepo) ListByTaskID(_ context.Context, _ string) ([]taskattempts.TaskAttempt, error) {
	panic("spoofStubAttemptRepo.ListByTaskID: not exercised")
}
func (s *spoofStubAttemptRepo) GetActiveAttempt(_ context.Context, _ string) (*taskattempts.TaskAttempt, error) {
	panic("spoofStubAttemptRepo.GetActiveAttempt: not exercised")
}
func (s *spoofStubAttemptRepo) Create(_ context.Context, _ *taskattempts.TaskAttempt) error {
	panic("spoofStubAttemptRepo.Create: not exercised")
}
func (s *spoofStubAttemptRepo) SetStatus(_ context.Context, _ string, _, _ taskattempts.AttemptStatus, _ int) error {
	panic("spoofStubAttemptRepo.SetStatus: not exercised")
}
func (s *spoofStubAttemptRepo) CompleteFinal(_ context.Context, _, _, _ string, _ taskattempts.AttemptStatus, _, _ string, _ int) error {
	panic("spoofStubAttemptRepo.CompleteFinal: not exercised")
}
func (s *spoofStubAttemptRepo) Delete(_ context.Context, _ string) error {
	panic("spoofStubAttemptRepo.Delete: not exercised")
}

var _ taskattempts.Repository = (*spoofStubAttemptRepo)(nil)

// Scorecard v1 / F1 — counters for typed metrics persistence calls.
// Tests asserting on persistCounts use a fully-typed pb.TaskExecutionMetrics
// and verify each of the 3 helper methods (PersistMetrics/PersistCacheStats/
// PersistCostBasis) was invoked exactly once with the expected values.
func (s *spoofStubAttemptRepo) PersistMetrics(_ context.Context, m taskattempts.AttemptMetrics) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persistMetricsCalls++
	s.lastMetrics = m
	return nil
}
func (s *spoofStubAttemptRepo) PersistCacheStats(_ context.Context, c taskattempts.AttemptCacheStats) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persistCacheCalls++
	s.lastCacheStats = c
	return nil
}
func (s *spoofStubAttemptRepo) PersistCostBasis(_ context.Context, b taskattempts.AttemptCostBasis) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.persistCostCalls++
	s.lastCostBasis = b
	return nil
}
func (s *spoofStubAttemptRepo) GetMetrics(_ context.Context, _ string) (*taskattempts.AttemptMetrics, error) {
	panic("spoofStubAttemptRepo.GetMetrics: not exercised in handler-level tests")
}
func (s *spoofStubAttemptRepo) GetCacheStats(_ context.Context, _ string) (*taskattempts.AttemptCacheStats, error) {
	panic("spoofStubAttemptRepo.GetCacheStats: not exercised in handler-level tests")
}
func (s *spoofStubAttemptRepo) GetCostBasis(_ context.Context, _ string) (*taskattempts.AttemptCostBasis, error) {
	panic("spoofStubAttemptRepo.GetCostBasis: not exercised in handler-level tests")
}

// spoofStubOutputArts: taskoutput_artifacts.Repository.
// Register MUST stay zero on the rejection path.
type spoofStubOutputArts struct {
	mu            sync.Mutex
	registerCalls int
	items         map[string]taskoutput_artifacts.OutputArtifact
}

func newSpoofStubOutputArts() *spoofStubOutputArts {
	return &spoofStubOutputArts{items: map[string]taskoutput_artifacts.OutputArtifact{}}
}

func (s *spoofStubOutputArts) Register(_ context.Context, a taskoutput_artifacts.OutputArtifact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.registerCalls++
	if s.items == nil {
		s.items = map[string]taskoutput_artifacts.OutputArtifact{}
	}
	s.items[a.TaskID+"|"+a.ArtifactID] = a
	return nil
}

func (s *spoofStubOutputArts) ListByTask(_ context.Context, taskID string) ([]taskoutput_artifacts.OutputArtifact, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []taskoutput_artifacts.OutputArtifact{}
	for _, a := range s.items {
		if a.TaskID == taskID {
			out = append(out, a)
		}
	}
	return out, nil
}

var _ taskoutput_artifacts.Repository = (*spoofStubOutputArts)(nil)

// buildSpoofHandler wires a Handler + ingestionSvc + canonical attempt
// store with the spoof fixture baked in. Used by all sub-tests.
//
// The jobsRepo + taskRepo are pre-seeded so the Job roll-up path (step 4
// of IngestTaskResult) has a target — without it the auditor gets
// "job not found (spoof stub)" and SetStatus never fires, which makes
// the F1 typed-metrics tests spuriously fail on the audit-closure
// cross-check. The seeds match the canonical fixture
// (T-spoof / J-canonical) so the audit closure runs end-to-end.
func buildSpoofHandler(t *testing.T) (*Handler, *spoofStubTaskRepo, *spoofStubJobsRepo, *spoofStubOutputArts) {
	t.Helper()
	fx := newSpoofFixture()

	attempts := &spoofStubAttemptRepo{}
	attempts.seedCanonical(fx.taskID, fx.workerID, fx.canonicalLease)

	taskRepo := &spoofStubTaskRepo{
		listTasks: []taskgraph.Task{
			{ID: fx.taskID, JobID: fx.wireJobID, Status: taskgraph.StatusSucceeded},
		},
	}
	jobsRepo := &spoofStubJobsRepo{
		getJob: &jobs.Job{ID: fx.wireJobID, Status: jobs.StatusRunning, MaxRetries: 3, Revision: 0},
	}
	outputArts := newSpoofStubOutputArts()

	svc, err := ingest.NewTaskReportIngestionService(taskRepo, jobsRepo, attempts, outputArts)
	if err != nil {
		t.Fatalf("NewTaskReportIngestionService: %v", err)
	}

	// Registry/cmdMgr/artifactSvc/dbStore are NIL because handleTaskResult
	// does not touch them. PushMode=true mirrors production bootstrap wiring.
	handler := NewHandler(
		nil, // registry
		nil, // cmdMgr
		jobsRepo,
		taskRepo,
		attempts,
		nil, // artifactSvc — not exercised by handleTaskResult on rejection
		nil, // dbStore
		&HandlerConfig{PushMode: true},
	)
	handler.SetIngestionSvc(svc)

	return handler, taskRepo, jobsRepo, outputArts
}

// =====================================================================
// (A) HandlerLayerDrop — handleTaskResult fires on a spoofed wire and
// short-circuits the close-write + Job roll-up + artifact-register
// paths. Independent of the canonical sentinel — today the
// cheap AttemptNumber<=0 check fires first; once pb.TaskResult grows
// the AttemptNumber field (audit follow-up), this assertion will
// route through the lookup-miss or strict-compare sentinel instead.
// =====================================================================

func TestHandleTaskResult_RejectsIdentitySpoofing_HandlerLayerDrop(t *testing.T) {
	handler, taskRepo, jobsRepo, outputArts := buildSpoofHandler(t)
	fx := newSpoofFixture()

	tr := &pb.TaskResult{
		TaskId:    fx.taskID,
		AttemptId: fx.wireAttemptID,
		LeaseId:   fx.wireLease, // SPOOF: canonical is fx.canonicalLease
		JobId:     fx.wireJobID,
		Status:    "succeeded",
	}

	handler.handleTaskResult(fx.workerID, tr)

	// The handler dropped the spoof — close-write + roll-up +
	// artifact register MUST stay zero regardless of which validator
	// branch fired.

	// (1) close-write: validator must reject BEFORE the taskRepo
	// close-write is reached (ValidateIdentityTuple is the FIRST step
	// in IngestTaskResult post-PR-2).
	if got := taskRepo.transitionCalls; got != 0 {
		t.Errorf("taskRepo.TransitionTaskToTerminalAtomic calls = %d; want 0 (handleTaskResult must short-circuit before close-write)", got)
	}

	// (2) Job roll-up: maybeTransitionJob only runs after a
	// successful close-write. With close-write NEVER firing,
	// setStatusCalls must be 0.
	if got := jobsRepo.setStatusCalls; got != 0 {
		t.Errorf("jobsRepo.SetStatus calls = %d; want 0 (close-write never fired ⇒ roll-up never fires)", got)
	}

	// (3) artifact register: step (3) of IngestTaskResult runs AFTER
	// the close-write. A rejection at the validator means artifacts
	// are NEVER registered.
	if got := outputArts.registerCalls; got != 0 {
		t.Errorf("outputArtRepo.Register calls = %d; want 0 (validator rejection blocks step-3)", got)
	}
}

// =====================================================================
// (B) CanonicalSentinel_WireLeaseIDMismatch — directly probe
// ValidateIdentityTuple on a fully-populated IngestCommand whose
// lease_id deliberately differs from the canonical seed. Confirms
// the canonical taskattempts.ErrIdentityMismatch wrap is produced on
// the lookup-miss path the handler will route through once
// pb.TaskResult grows the AttemptNumber field.
//
// This test pins the canonical sentinel: a future refactor that
// drops the %w wrap would fail this test even if the side-effect
// counters in (A) were silently loosened.
// =====================================================================
func TestHandleTaskResult_ValidateIdentityTuple_CanonicalSentinel_WireLeaseIDMismatch(t *testing.T) {
	fx := newSpoofFixture()

	// Bind a fresh svc to a single canonical-seeded attempt for the
	// (task_id, worker_id, canonical_lease_id) tuple. The validator
	// probe below supplies a fully-populated IngestCommand whose
	// LeaseID deliberately differs from fx.canonicalLease so the
	// lookup at (fx.taskID, fx.workerID, fx.wireLease) returns nil →
	// ErrIdentityMismatch wrap.
	attempts := &spoofStubAttemptRepo{}
	attempts.seedCanonical(fx.taskID, fx.workerID, fx.canonicalLease)
	svc, err := ingest.NewTaskReportIngestionService(
		&spoofStubTaskRepo{},
		&spoofStubJobsRepo{},
		attempts,
		newSpoofStubOutputArts(),
	)
	if err != nil {
		t.Fatalf("NewTaskReportIngestionService: %v", err)
	}

	// Drive ValidateIdentityTuple with a fully populated command —
	// AttemptNumber=1 to skip the cheap-attempt-count branch.
	vErr := svc.ValidateIdentityTuple(context.Background(), ingest.IngestCommand{
		TaskID:        fx.taskID,
		AttemptID:     fx.wireAttemptID, // matches canonical
		LeaseID:       fx.wireLease,     // SPOOF: differs from canonical
		WorkerID:      fx.workerID,
		JobID:         fx.wireJobID,
		AttemptNumber: 1,
	})
	if vErr == nil {
		t.Errorf("ValidateIdentityTuple returned nil for spoofed lease_id; want wrapped ErrIdentityMismatch")
	}
	if vErr != nil && !errors.Is(vErr, taskattempts.ErrIdentityMismatch) {
		t.Errorf("ValidateIdentityTuple returned %v; want taskattempts.ErrIdentityMismatch wrapped", vErr)
	}
}

// =====================================================================
// (C) F1 — Scorecard v1 typed metrics ingest. Drives handleTaskResult
// with a fully-populated pb.TaskExecutionMetrics payload + an artifact
// declaration; asserts that the typed Go structs flow through
// IngestTaskResult → persistMetrics/PersistCacheStats/PersistCostBasis.
// =====================================================================

func TestHandleTaskResult_PersistTypedMetrics_F1(t *testing.T) {
	handler, taskRepo, jobsRepo, outputArts := buildSpoofHandler(t)
	fx := newSpoofFixture()

	// A non-zero typed execution metrics envelope, every writable
	// int32/int64/float64/bool populated. The derived CostBasis only
	// depends on CpuTimeMs + the 3 price fields today (TempBytesWritten
	// is NOT yet on the typed proto — derives to 0 in the cost row).
	em := &pb.TaskExecutionMetrics{
		InputBytes:            1048576,   // 1 MiB
		OutputBytes:           524288,    // 512 KiB
		BytesFromDrive:        262144,    // 256 KiB
		BytesFromBlobstore:    262144,    // 256 KiB
		BytesFromLocalCache:   524288,    // 512 KiB
		CpuTimeMs:             12345,     // 12.345 s
		PeakRssBytes:          536870912, // 512 MiB
		FramesDecoded:         1800,
		FramesComposited:      1800,
		FramesEncoded:         1800,
		FfmpegSpeedRatio:      1.42,
		EncodePasses:          1,
		FinalConcatStreamCopy: true,
		ConcatMode:            "stream_copy",
		CpuPricePerSecond:     0.000005,
		StoragePricePerGb:     0.00012,
		NetworkPricePerGb:     0.01,
	}

	artItem, _ := structpb.NewStruct(map[string]interface{}{
		"artifact_id":   "art-1",
		"artifact_type": "video",
		"size_bytes":    524288,
	})
	tr := &pb.TaskResult{
		TaskId:           fx.taskID,
		AttemptId:        fx.wireAttemptID,
		LeaseId:          fx.canonicalLease, // match canonical seed (sub-tests above already proved identity)
		JobId:            fx.wireJobID,
		Status:           "succeeded",
		ExecutionMetrics: em,
		OutputArtifacts:  []*structpb.Struct{artItem},
	}

	// Need to satisfy the canonical-attempt tuple for the happy path
	// (buildSpoofHandler pre-seeds it under canonicalLease).
	handler.handleTaskResult(fx.workerID, tr)

	// 1) The scorecard writes MUT commit first: metrics, cache, cost.
	attempts := handler.taskAttemptRepo.(*spoofStubAttemptRepo)
	if attempts.persistMetricsCalls != 1 {
		t.Errorf("PersistMetrics calls = %d; want 1 (F1 typed-wiring)", attempts.persistMetricsCalls)
	}
	if attempts.persistCacheCalls != 1 {
		t.Errorf("PersistCacheStats calls = %d; want 1 (F1 typed-wiring)", attempts.persistCacheCalls)
	}
	if attempts.persistCostCalls != 1 {
		t.Errorf("PersistCostBasis calls = %d; want 1 (F1 typed-wiring)", attempts.persistCostCalls)
	}

	// 2) Spot-check typed fields made the round-trip unchanged.
	got := attempts.lastMetrics
	if got.AttemptID != fx.wireAttemptID {
		t.Errorf("AttemptMetrics.AttemptID = %q; want %q (handler must bind to wire attempt)", got.AttemptID, fx.wireAttemptID)
	}
	if got.InputBytes != em.GetInputBytes() || got.OutputBytes != em.GetOutputBytes() {
		t.Errorf("AttemptMetrics bytes mismatch: got input=%d output=%d want input=%d output=%d",
			got.InputBytes, got.OutputBytes, em.GetInputBytes(), em.GetOutputBytes())
	}
	if got.FramesEncoded != em.GetFramesEncoded() || got.FramesDecoded != em.GetFramesDecoded() {
		t.Errorf("AttemptMetrics frame counters mismatch: got decoded=%d encoded=%d want decoded=%d encoded=%d",
			got.FramesDecoded, got.FramesEncoded, em.GetFramesDecoded(), em.GetFramesEncoded())
	}
	if got.FFmpegSpeedRatio != em.GetFfmpegSpeedRatio() {
		t.Errorf("AttemptMetrics ffmpeg_speed_ratio = %v; want %v", got.FFmpegSpeedRatio, em.GetFfmpegSpeedRatio())
	}
	if got.FinalConcatStreamCopy != em.GetFinalConcatStreamCopy() || got.ConcatMode != em.GetConcatMode() {
		t.Errorf("AttemptMetrics concat fields wrong: sc=%v mode=%q want sc=%v mode=%q",
			got.FinalConcatStreamCopy, got.ConcatMode, em.GetFinalConcatStreamCopy(), em.GetConcatMode())
	}
	// TempBytesWritten is NOT yet on the typed proto (future worker-side
	// follow-up). Today the persisted value is 0; we still pin the
	// round-trip contract so a future proto bump with this field
	// surfaces immediately here.
	if got.TempBytesWritten != 0 {
		t.Errorf("AttemptMetrics TempBytesWritten = %d; want 0 (not yet on typed proto, derives to 0)", got.TempBytesWritten)
	}

	// 3) Spot-check CacheStats — must be honest about what is derivable
	// (BytesUsed=from_local_cache), zero for the rest.
	gotCache := attempts.lastCacheStats
	if gotCache.AttemptID != fx.wireAttemptID {
		t.Errorf("AttemptCacheStats.AttemptID = %q; want %q", gotCache.AttemptID, fx.wireAttemptID)
	}
	if gotCache.CacheBytesUsed != em.GetBytesFromLocalCache() {
		t.Errorf("AttemptCacheStats.CacheBytesUsed = %d; want BytesFromLocalCache=%d",
			gotCache.CacheBytesUsed, em.GetBytesFromLocalCache())
	}
	if gotCache.CacheHits != 0 || gotCache.CacheMisses != 0 ||
		gotCache.CacheEvictions != 0 || gotCache.CacheCorruptions != 0 {
		t.Errorf("AttemptCacheStats must report 0 for un-derivable counters; got H=%d M=%d E=%d C=%d",
			gotCache.CacheHits, gotCache.CacheMisses, gotCache.CacheEvictions, gotCache.CacheCorruptions)
	}

	// 4) Spot-check CostBasis — master-side derivation from proto scalar
	// fields. CPUTimeSecondsTotal = CPUTimeMS/1000; StorageGBWritten +
	// NetworkGBEgressed + OutputMinutesTotal are all 0 today (not yet on
	// the typed proto) so CostPerOutputMinute is 0 (short-circuit via
	// Compute()).
	gotCost := attempts.lastCostBasis
	if gotCost.CPUTimeSecondsTotal != float64(em.GetCpuTimeMs())/1000.0 {
		t.Errorf("CostBasis.CPUTimeSecondsTotal = %v; want %v", gotCost.CPUTimeSecondsTotal, float64(em.GetCpuTimeMs())/1000.0)
	}
	if gotCost.StorageGBWritten != 0 {
		t.Errorf("CostBasis.StorageGBWritten = %v; want 0 (TempBytesWritten not on typed proto yet)", gotCost.StorageGBWritten)
	}
	if gotCost.NetworkGBEgressed != 0 {
		t.Errorf("CostBasis.NetworkGBEgressed = %v; want 0 (TODO PR-3)", gotCost.NetworkGBEgressed)
	}
	if gotCost.CPUPricePerSecond != em.GetCpuPricePerSecond() || gotCost.StoragePricePerGB != em.GetStoragePricePerGb() {
		t.Errorf("CostBasis prices mismatch: cpu=%v storage=%v want cpu=%v storage=%v",
			gotCost.CPUPricePerSecond, gotCost.StoragePricePerGB, em.GetCpuPricePerSecond(), em.GetStoragePricePerGb())
	}
	if gotCost.CostPerOutputMinute != 0 {
		t.Errorf("CostBasis.CostPerOutputMinute = %v; want 0 (OutputMinutesTotal is 0 today)", gotCost.CostPerOutputMinute)
	}

	// 5) Cross-check: the F1 typed writes must NOT regress the
	// audit-closure path the older tests pin. close-write + Job roll-up
	// + artifact register all run once on success.
	if got := taskRepo.transitionCalls; got != 1 {
		t.Errorf("taskRepo.TransitionTaskToTerminalAtomic calls = %d; want 1 (happy path)", got)
	}
	if got := jobsRepo.setStatusCalls; got != 1 {
		t.Errorf("jobsRepo.SetStatus calls = %d; want 1 (Job roll-up must fire)", got)
	}
	if got := outputArts.registerCalls; got != 1 {
		t.Errorf("outputArtRepo.Register calls = %d; want 1 (artifact declare must fire)", got)
	}
}

// =====================================================================
// (D) F1 legacy-em regression-guard. Older v2 workers send TaskResult
// without execution_metrics (nil). Scorecard v1 must STILL persist a
// zero-row so percentile queries on task_attempt_metrics find something;
// the legacy-em-drop bug (where TypedMetrics.AttemptID=='' short-circuited
// persistence entirely) is regression-tested here.
// =====================================================================

func TestHandleTaskResult_PersistTypedMetrics_NilExecutionMetrics(t *testing.T) {
	handler, taskRepo, jobsRepo, outputArts := buildSpoofHandler(t)
	fx := newSpoofFixture()

	artItem, _ := structpb.NewStruct(map[string]interface{}{
		"artifact_id":   "art-1",
		"artifact_type": "video",
	})
	tr := &pb.TaskResult{
		TaskId:           fx.taskID,
		AttemptId:        fx.wireAttemptID,
		LeaseId:          fx.canonicalLease,
		JobId:            fx.wireJobID,
		Status:           "succeeded",
		ExecutionMetrics: nil, // LEGACY v2 worker path — no typed ExecMetrics
		OutputArtifacts:  []*structpb.Struct{artItem},
	}

	handler.handleTaskResult(fx.workerID, tr)

	// Scorecard writes; nil em MUST still produce a row (zero values
	// on bytes/frames/cost are acceptable baseline).
	attempts := handler.taskAttemptRepo.(*spoofStubAttemptRepo)
	if attempts.persistMetricsCalls != 1 {
		t.Errorf("PersistMetrics calls = %d; want 1 (legacy-em must persist zero-row)", attempts.persistMetricsCalls)
	}
	if attempts.persistCacheCalls != 1 {
		t.Errorf("PersistCacheStats calls = %d; want 1 (legacy-em must persist zero-row)", attempts.persistCacheCalls)
	}
	if attempts.persistCostCalls != 1 {
		t.Errorf("PersistCostBasis calls = %d; want 1 (legacy-em must persist zero-row)", attempts.persistCostCalls)
	}
	// And the row MUST be bound to the wire's canonical attempt ID —
	// not produced with an empty AttemptID (would clobber SQL UNIQUE).
	if attempts.lastMetrics.AttemptID != fx.wireAttemptID {
		t.Errorf("nil-em AttemptMetrics.AttemptID = %q; want %q (handler must still bind wire attempt)",
			attempts.lastMetrics.AttemptID, fx.wireAttemptID)
	}
	if attempts.lastCacheStats.AttemptID != fx.wireAttemptID {
		t.Errorf("nil-em AttemptCacheStats.AttemptID = %q; want %q",
			attempts.lastCacheStats.AttemptID, fx.wireAttemptID)
	}

	// Audit-closure side of the handler must still run on the legacy path.
	if got := taskRepo.transitionCalls; got != 1 {
		t.Errorf("taskRepo.TransitionTaskToTerminalAtomic calls = %d; want 1 (legacy happy path)", got)
	}
	if got := jobsRepo.setStatusCalls; got != 1 {
		t.Errorf("jobsRepo.SetStatus calls = %d; want 1 (legacy Job roll-up)", got)
	}
	if got := outputArts.registerCalls; got != 1 {
		t.Errorf("outputArtRepo.Register calls = %d; want 1 (legacy artifact declare)", got)
	}
}

// =====================================================================
// (E) F1 stale-replay regression-guard. When TransitionTaskToTerminalAtomic
// hits ErrTransitionConflict (someone else already closed the task),
// res.AttemptClosed is false, so step 2.5's metrics persist must be
// SKIPPED. Otherwise we'd silently overwrite the canonical row's
// metrics via INSERT OR REPLACE keyed on attempt_id. This test pins
// the gate; without it a future refactor removing `if res.AttemptClosed`
// would corrupt Prometheus with stale-replay metrics.
// =====================================================================

func TestHandleTaskResult_PersistTypedMetrics_StaleReplaySkipsMetrics(t *testing.T) {
	fx := newSpoofFixture()

	attempts := &spoofStubAttemptRepo{}
	attempts.seedCanonical(fx.taskID, fx.workerID, fx.canonicalLease)

	// The trigger: TransitionTaskToTerminalAtomic returns conflict -> step
	// 2 closes via log+continue (AttemptClosed=false), gate skips persist.
	taskRepo := &spoofStubTaskRepo{
		transitionErr: store.ErrTransitionConflict,
		listTasks: []taskgraph.Task{
			{ID: fx.taskID, JobID: fx.wireJobID, Status: taskgraph.StatusSucceeded},
		},
	}
	jobsRepo := &spoofStubJobsRepo{
		getJob: &jobs.Job{ID: fx.wireJobID, Status: jobs.StatusAwaitingArtifact, Revision: 0},
	}
	outputArts := newSpoofStubOutputArts()

	svc, err := ingest.NewTaskReportIngestionService(taskRepo, jobsRepo, attempts, outputArts)
	if err != nil {
		t.Fatalf("NewTaskReportIngestionService: %v", err)
	}

	handler := NewHandler(
		nil, nil,
		jobsRepo,
		taskRepo,
		attempts,
		nil, nil,
		&HandlerConfig{PushMode: true},
	)
	handler.SetIngestionSvc(svc)

	em := &pb.TaskExecutionMetrics{
		InputBytes: 999, // any non-zero to make stale-replay values distinguishable
	}
	tr := &pb.TaskResult{
		TaskId:           fx.taskID,
		AttemptId:        fx.wireAttemptID,
		LeaseId:          fx.canonicalLease,
		JobId:            fx.wireJobID,
		Status:           "succeeded",
		ExecutionMetrics: em,
	}

	handler.handleTaskResult(fx.workerID, tr)

	// 1) Persist counters MUST stay zero — stale replay must NOT
	// overwrite the canonical row's metrics via INSERT OR REPLACE.
	if got := attempts.persistMetricsCalls; got != 0 {
		t.Errorf("PersistMetrics calls = %d; want 0 (stale replay must skip step 2.5)", got)
	}
	if got := attempts.persistCacheCalls; got != 0 {
		t.Errorf("PersistCacheStats calls = %d; want 0 (stale replay must skip step 2.5)", got)
	}
	if got := attempts.persistCostCalls; got != 0 {
		t.Errorf("PersistCostBasis calls = %d; want 0 (stale replay must skip step 2.5)", got)
	}

	// 2) But the rest of IngestTaskResult kept running — artifact register
	// + Job roll-up use idempotent skip / no-op paths on replay, NOT
	// failure. We just need to confirm artifacts were still considered
	// (nil -> 0 declared artifacts in this test) and that SetStatus
	// fired exactly 0 times (because Job is already AWAITING_ARTIFACT).
	if got := taskRepo.transitionCalls; got != 1 {
		t.Errorf("taskRepo.TransitionTaskToTerminalAtomic calls = %d; want 1 (CAS attempted)", got)
	}
	if got := jobsRepo.setStatusCalls; got != 0 {
		t.Errorf("jobsRepo.SetStatus calls = %d; want 0 (idempotent Job roll-up skip)", got)
	}
}
