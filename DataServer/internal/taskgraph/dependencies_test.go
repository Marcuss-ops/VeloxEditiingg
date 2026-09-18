package taskgraph

import (
	"context"
	"strings"
	"testing"
	"time"

	"velox-server/internal/placement"
	"velox-server/internal/taskattempts"
)

// --- Marshal / Unmarshal round-trip ---------------------------------------

func TestMarshalDependsOn_NilBecomesEmptyArray(t *testing.T) {
	data, err := MarshalDependsOn(nil)
	if err != nil {
		t.Fatalf("MarshalDependsOn: %v", err)
	}
	if data != "[]" {
		t.Fatalf("nil edge list must marshal to [] not %q (null must never be stored)", data)
	}
	var back []string
	if err := UnmarshalDependsOn(data, &back); err != nil {
		t.Fatalf("UnmarshalDependsOn: %v", err)
	}
	if len(back) != 0 {
		t.Fatalf("round-trip of empty list produced %v", back)
	}
}

func TestUnmarshalDependsOn_RejectsCorruptRows(t *testing.T) {
	var edges []string
	if err := UnmarshalDependsOn(`{"not":"an array"}`, &edges); err == nil {
		t.Fatal("object-shaped depends_on must be rejected (data integrity)")
	}
	if err := UnmarshalDependsOn("null", &edges); err == nil {
		t.Fatal("null depends_on must be rejected: MarshalDependsOn never emits it")
	}
	if err := UnmarshalDependsOn("", &edges); err != nil || edges != nil {
		t.Fatalf("empty string must parse as nil edge list, got %v err %v", edges, err)
	}
}

// --- Cycle detection --------------------------------------------------------

func TestFindDependencyCycle_None(t *testing.T) {
	tasks := []Task{
		{ID: "prep-1", DependsOn: []string{}},
		{ID: "prep-2", DependsOn: []string{}},
		{ID: "mix", DependsOn: []string{"prep-1", "prep-2"}},
		{ID: "concat", DependsOn: []string{"mix"}},
	}
	if cycle := FindDependencyCycle(tasks); cycle != nil {
		t.Fatalf("acyclic graph reported cycle %v", cycle)
	}
	if err := ValidateTaskGraph(tasks); err != nil {
		t.Fatalf("ValidateTaskGraph: %v", err)
	}
}

func TestFindDependencyCycle_DetectsTwoNodeCycle(t *testing.T) {
	tasks := []Task{
		{ID: "a", DependsOn: []string{"b"}},
		{ID: "b", DependsOn: []string{"a"}},
	}
	cycle := FindDependencyCycle(tasks)
	if cycle == nil {
		t.Fatal("two-node cycle not detected")
	}
	if len(cycle) != 3 || cycle[0] != cycle[len(cycle)-1] {
		t.Fatalf("cycle must be closed (first == last), got %v", cycle)
	}
}

func TestFindDependencyCycle_DetectsSelfReference(t *testing.T) {
	tasks := []Task{{ID: "self", DependsOn: []string{"self"}}}
	if cycle := FindDependencyCycle(tasks); cycle == nil {
		t.Fatal("self-reference cycle not detected")
	}
}

func TestValidateTaskGraph_RejectsDanglingEdge(t *testing.T) {
	tasks := []Task{
		{ID: "a", DependsOn: []string{"ghost"}},
	}
	err := ValidateTaskGraph(tasks)
	if err == nil || !strings.Contains(err.Error(), "unknown task ghost") {
		t.Fatalf("dangling edge must be rejected, got %v", err)
	}
}

// --- Failure propagation -----------------------------------------------------

func TestDoomedTaskIDs_PropagatesTransitively(t *testing.T) {
	tasks := []Task{
		{ID: "prep", Status: StatusFailed},
		{ID: "mix", DependsOn: []string{"prep"}, Status: StatusPending},
		{ID: "concat", DependsOn: []string{"mix"}, Status: StatusPending},
		{ID: "independent", Status: StatusPending},
	}
	doomed := DoomedTaskIDs(tasks)
	if len(doomed) != 2 || doomed[0] != "mix" || doomed[1] != "concat" {
		t.Fatalf("doomed = %v, want [mix concat] in deterministic order", doomed)
	}
}

func TestDoomedTaskIDs_ExcludesAlreadyTerminal(t *testing.T) {
	tasks := []Task{
		{ID: "prep", Status: StatusFailed},
		{ID: "mix", DependsOn: []string{"prep"}, Status: StatusCancelled},
		{ID: "concat", DependsOn: []string{"mix"}, Status: StatusPending},
	}
	doomed := DoomedTaskIDs(tasks)
	// mix is already terminal: not re-marked, but concat is still doomed
	// through it.
	if len(doomed) != 1 || doomed[0] != "concat" {
		t.Fatalf("doomed = %v, want [concat]", doomed)
	}
}

func TestDoomedTaskIDs_SucceededDependencyIsNotAFailure(t *testing.T) {
	tasks := []Task{
		{ID: "prep", Status: StatusSucceeded},
		{ID: "mix", DependsOn: []string{"prep"}, Status: StatusPending},
	}
	if doomed := DoomedTaskIDs(tasks); len(doomed) != 0 {
		t.Fatalf("SUCCEEDED dependency must not propagate failure, got %v", doomed)
	}
}

// --- LifecycleService.TickReadiness end-to-end over a stub repo -------------

// readinessStubRepo drives TickReadiness with an in-memory task table.
type readinessStubRepo struct {
	tasks     map[string]*Task
	cancelled []string
	readied   []string
}

func newReadinessStubRepo(tasks ...Task) *readinessStubRepo {
	repo := &readinessStubRepo{tasks: map[string]*Task{}}
	for i := range tasks {
		task := tasks[i]
		repo.tasks[task.ID] = &task
	}
	return repo
}

func (r *readinessStubRepo) Get(_ context.Context, id string) (*Task, error) {
	if t, ok := r.tasks[id]; ok {
		copy := *t
		return &copy, nil
	}
	return nil, nil
}

func (r *readinessStubRepo) List(_ context.Context, filter Filter) ([]Task, error) {
	var out []Task
	for _, t := range r.tasks {
		if len(filter.Statuses) > 0 {
			match := false
			for _, s := range filter.Statuses {
				if t.Status == s {
					match = true
					break
				}
			}
			if !match {
				continue
			}
		}
		out = append(out, *t)
	}
	return out, nil
}

func (r *readinessStubRepo) SetStatus(_ context.Context, id string, from, to Status, _ int) error {
	t, ok := r.tasks[id]
	if !ok || t.Status != from {
		return ErrTransitionConflict
	}
	t.Status = to
	switch to {
	case StatusReady:
		r.readied = append(r.readied, id)
	case StatusCancelled:
		r.cancelled = append(r.cancelled, id)
	}
	return nil
}

func (r *readinessStubRepo) AreDependenciesSatisfied(_ context.Context, deps []string) (bool, error) {
	for _, dep := range deps {
		if t, ok := r.tasks[dep]; !ok || t.Status != StatusSucceeded {
			return false, nil
		}
	}
	return true, nil
}

// Remaining Repository surface: unused by TickReadiness tests.
func (r *readinessStubRepo) GetByJobID(context.Context, string) (*Task, error) {
	panic("readinessStubRepo.GetByJobID: not used")
}
func (r *readinessStubRepo) Create(context.Context, *Task) error            { panic("not used") }
func (r *readinessStubRepo) SetDependsOn(context.Context, string, []string) error { panic("not used") }
func (r *readinessStubRepo) Lease(context.Context, string, string, string) error { panic("not used") }
func (r *readinessStubRepo) ClaimNextReadyTask(context.Context, string, string) (*TaskWithSpec, error) {
	panic("not used")
}
func (r *readinessStubRepo) ClaimNextWithAttemptAtomic(context.Context, string, string) (*TaskWithSpec, *taskattempts.TaskAttempt, error) {
	panic("not used")
}
func (r *readinessStubRepo) ReleaseLease(context.Context, string, string, string) error { panic("not used") }
func (r *readinessStubRepo) Start(context.Context, string, string, string, int, int) error { panic("not used") }
func (r *readinessStubRepo) Fail(context.Context, string, string, int) error { panic("not used") }
func (r *readinessStubRepo) IncrementAttempt(context.Context, string) error { panic("not used") }
func (r *readinessStubRepo) Delete(context.Context, string) error { panic("not used") }
func (r *readinessStubRepo) RequeueExpiredLeases(context.Context, string, int) ([]RequeueCandidate, error) {
	panic("not used")
}
func (r *readinessStubRepo) ExpireTaskLeaseAtomic(context.Context, string, string, string, int) (ExpireResult, error) {
	panic("not used")
}
func (r *readinessStubRepo) AcceptTaskAtomic(context.Context, *taskattempts.TaskAttempt, int) error {
	panic("not used")
}
func (r *readinessStubRepo) TransitionTaskToTerminalAtomic(context.Context, string, string, string, Status, taskattempts.AttemptStatus, string, string) error {
	panic("not used")
}
func (r *readinessStubRepo) RenewLease(context.Context, string, string, string, time.Time, int) error {
	panic("not used")
}
func (r *readinessStubRepo) ClaimTaskForWorkerAtomic(context.Context, ClaimTaskForWorkerCommand) (*TaskWithSpec, *taskattempts.TaskAttempt, error) {
	panic("not used")
}
func (r *readinessStubRepo) IngestTaskResultAtomic(context.Context, IngestResultCommand) error {
	panic("not used")
}
func (r *readinessStubRepo) IsAllAttemptCommitsCommittedForTasks(context.Context, []string) (bool, error) {
	panic("not used")
}
func (r *readinessStubRepo) ListReadyCandidates(context.Context, int) ([]placement.TaskCandidate, error) {
	panic("not used")
}

func TestTickReadiness_CancelsDoomedTasksAndReadiesIndependent(t *testing.T) {
	repo := newReadinessStubRepo(
		Task{ID: "prep", Status: StatusFailed},
		Task{ID: "mix", DependsOn: []string{"prep"}, Status: StatusPending, Revision: 1},
		Task{ID: "concat", DependsOn: []string{"mix"}, Status: StatusPending, Revision: 1},
		Task{ID: "solo", Status: StatusPending, Revision: 1},
	)
	svc, err := NewLifecycleService(repo)
	if err != nil {
		t.Fatalf("NewLifecycleService: %v", err)
	}

	// Seed SUCCEEDED deps for the ready pass: mix/concat are cancelled in
	// the same tick before readiness is evaluated; solo has no deps.
	repo.tasks["solo"] = &Task{ID: "solo", Status: StatusPending, Revision: 1}

	transitioned, err := svc.TickReadiness(context.Background(), 100)
	if err != nil {
		t.Fatalf("TickReadiness: %v", err)
	}
	if len(repo.cancelled) != 2 {
		t.Fatalf("doomed tasks not cancelled: %v", repo.cancelled)
	}
	if repo.cancelled[0] != "mix" || repo.cancelled[1] != "concat" {
		t.Fatalf("cancellation must propagate transitively in order, got %v", repo.cancelled)
	}
	if transitioned != 1 || len(repo.readied) != 1 || repo.readied[0] != "solo" {
		t.Fatalf("independent task must still become READY: transitioned=%d readied=%v", transitioned, repo.readied)
	}
}

func TestTickReadiness_DoesNotCancelOnUnknownDependency(t *testing.T) {
	repo := newReadinessStubRepo(
		Task{ID: "waiting", DependsOn: []string{"ghost"}, Status: StatusPending, Revision: 1},
	)
	svc, err := NewLifecycleService(repo)
	if err != nil {
		t.Fatalf("NewLifecycleService: %v", err)
	}
	if _, err := svc.TickReadiness(context.Background(), 100); err != nil {
		t.Fatalf("TickReadiness: %v", err)
	}
	if len(repo.cancelled) != 0 {
		t.Fatalf("unknown dependency must not cancel the task: %v", repo.cancelled)
	}
	if repo.tasks["waiting"].Status != StatusPending {
		t.Fatalf("task must stay PENDING on unknown dependency, got %s", repo.tasks["waiting"].Status)
	}
}
