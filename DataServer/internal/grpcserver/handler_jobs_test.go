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
	"velox-server/internal/taskattempts"
	"velox-server/internal/taskgraph"
	"velox-server/internal/taskoutput_artifacts"

	pb "velox-shared/controltransport/pb"
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
// store with the spoof fixture baked in. Used by both sub-tests.
func buildSpoofHandler(t *testing.T) (*Handler, *spoofStubTaskRepo, *spoofStubJobsRepo, *spoofStubOutputArts) {
	t.Helper()
	fx := newSpoofFixture()

	attempts := &spoofStubAttemptRepo{}
	attempts.seedCanonical(fx.taskID, fx.workerID, fx.canonicalLease)

	taskRepo := &spoofStubTaskRepo{}
	jobsRepo := &spoofStubJobsRepo{}
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
