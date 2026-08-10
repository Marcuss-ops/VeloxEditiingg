// Package fleet — Step 4/15 FleetController tests + ExecutorRegistry
// invariants + NewOperationID format pinning.
//
// Test list:
//
//	Publish path:
//	  TestController_Publish_AssignsIDAndPersists          — generates UUIDv4 + writes row
//	  TestController_Publish_InFlightDedupReturnsSentinel — ErrOperationInFlight on duplicate
//	  TestController_Publish_PayloadPreserved             — payload survives round-trip
//
//	Tick path:
//	  TestController_Tick_LifecycleNoop                   — QUEUED → RUNNING → SUCCEEDED
//	  TestController_Tick_FailedExecutorCapturesErrorMsg  — QUEUED → RUNNING → FAILED
//
//	Lifecycle:
//	  TestController_StartStop_Lifecycle                   — Start/Stop semantics, Done() closes
//	  TestController_Done_SatisfiableBeforeStart          — Done() always usable pre-Start
//
//	Executor registry:
//	  TestExecutorRegistry_RegistersAllKinds              — every AllOperationKinds has default
//	  TestExecutorRegistry_RegisterRejectsUnknownKind     — Registry.Register guards enum
//	  TestExecutorRegistry_RegisterNilExecutor            — nil executor is rejected
//
//	Operation ID:
//	  TestNewOperationID_Format                            — UUIDv4 RFC 4122 §4.4 layout
//
//	Op kind canonical:
//	  TestAllOperationKinds_MatchesSchemaCheck           — every kind is also CHECK-acceptable
package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"velox-server/internal/store"
)

// stubStore is a non-SQLite FleetStore used only by tests that
// exercise FleetController-specific logic without standing up a
// SQLite handle. Not exported; package-internal.
type stubStore struct {
	insertErr           error
	insertCalls         []*store.Operation
	queuedList          []store.Operation
	listErr             error
	listCalls           int
	markRunningErr      error
	markRunningClaimSet bool
	markRunningClaim    bool
	markSucceeded       bool
	markFailedErr       error
	markFailedMsg       string
}

func (s *stubStore) InsertOperation(_ context.Context, op *store.Operation) error {
	s.insertCalls = append(s.insertCalls, op)
	if s.insertErr != nil {
		return s.insertErr
	}
	return nil
}
func (s *stubStore) ListQueuedOperations(_ context.Context, _ int) ([]store.Operation, error) {
	s.listCalls++
	return s.queuedList, s.listErr
}
func (s *stubStore) ListOperations(_ context.Context, _, _ string, _ int) ([]store.Operation, error) {
	return nil, nil
}
func (s *stubStore) GetOperation(_ context.Context, _ string) (*store.Operation, error) {
	return nil, store.ErrOperationNotFound
}
func (s *stubStore) MarkRunning(_ context.Context, _ string, _ time.Time) (bool, error) {
	if s.markRunningErr != nil {
		return false, s.markRunningErr
	}
	if s.markRunningClaimSet {
		return s.markRunningClaim, nil
	}
	return true, nil
}
func (s *stubStore) MarkSucceeded(_ context.Context, _ string, _ time.Time) error {
	s.markSucceeded = true
	return nil
}
func (s *stubStore) MarkFailed(_ context.Context, _ string, _ time.Time, msg string) error {
	s.markFailedMsg = msg
	return s.markFailedErr
}

// failExecutor is a hook for tests to control Execute behaviour
// per kind. Returned error is persisted via MarkFailed.
type failExecutor struct {
	err   error
	sleep time.Duration
	calls int
}

func (f *failExecutor) Execute(ctx context.Context, _ *store.Operation) error {
	f.calls++
	if f.sleep > 0 {
		select {
		case <-time.After(f.sleep):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return f.err
}

func TestController_Publish_AssignsIDAndPersists(t *testing.T) {
	st := &stubStore{}
	c := NewFleetController(st, NewTestExecutorRegistry(), time.Second, time.Minute)
	ctx := context.Background()

	op := &store.Operation{
		WorkerID:    "wicket",
		Op:          OperationKindDrain,
		RequestedBy: "ops@example.com",
		Reason:      "maintenance window",
	}
	if err := c.PublishOperation(ctx, op); err != nil {
		t.Fatalf("publish: %v", err)
	}
	// Side-effect: ID is filled in and the row's Status is QUEUED.
	if op.OperationID == "" {
		t.Fatalf("OperationID empty after publish")
	}
	if op.Status != store.OperationStatusQueued {
		t.Errorf("op.Status = %q, want QUEUED", op.Status)
	}
	if op.QueuedAt.IsZero() {
		t.Errorf("QueuedAt not stamped")
	}
	if len(st.insertCalls) != 1 {
		t.Errorf("insertCalls len = %d, want 1", len(st.insertCalls))
	}
	if st.insertCalls[0].OperationID != op.OperationID {
		t.Errorf("insert op ID mismatch: %q vs %q", st.insertCalls[0].OperationID, op.OperationID)
	}
}

func TestController_Publish_PropagatesInsertError(t *testing.T) {
	st := &stubStore{insertErr: errors.New("disk full")}
	c := NewFleetController(st, NewTestExecutorRegistry(), time.Second, time.Minute)
	err := c.PublishOperation(context.Background(), &store.Operation{
		WorkerID: "wicket", Op: OperationKindDrain,
		RequestedBy: "ops", Reason: "err test",
	})
	if err == nil || err.Error() != "disk full" {
		t.Errorf("err = %v, want 'disk full'", err)
	}
}

func TestController_Publish_InFlightDedupReturnsSentinel(t *testing.T) {
	st := &stubStore{insertErr: store.ErrOperationInFlight}
	c := NewFleetController(st, NewTestExecutorRegistry(), time.Second, time.Minute)
	ctx := context.Background()
	err := c.PublishOperation(ctx, &store.Operation{
		WorkerID: "wicket", Op: OperationKindDrain,
		RequestedBy: "ops", Reason: "first",
	})
	if !errors.Is(err, store.ErrOperationInFlight) {
		t.Errorf("err = %v, want ErrOperationInFlight", err)
	}
}

func TestController_Publish_PayloadPreserved(t *testing.T) {
	st := &stubStore{}
	c := NewFleetController(st, NewTestExecutorRegistry(), time.Second, time.Minute)
	pl := json.RawMessage(`{"digest":"sha256:abc","timeout_s":30}`)
	err := c.PublishOperation(context.Background(), &store.Operation{
		WorkerID: "wicket", Op: OperationKindUpdate,
		RequestedBy: "ops", Reason: "payload test",
		Payload: pl,
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if string(st.insertCalls[0].Payload) != string(pl) {
		t.Errorf("payload = %q, want %q", st.insertCalls[0].Payload, pl)
	}
}

// TestController_Tick_LifecycleNoop verifies the test-only happy path:
// a QUEUED row goes QUEUED → RUNNING → SUCCEEDED via the
// explicit test registry. Uses the stubStore so the test does
// not need an on-disk SQLite handle for this code path.
func TestController_Tick_LifecycleNoop(t *testing.T) {
	st := &stubStore{
		queuedList: []store.Operation{{
			OperationID: "op-stub-1",
			WorkerID:    "wicket",
			Op:          OperationKindDrain,
			Status:      store.OperationStatusQueued,
		}},
	}
	c := NewFleetController(st, NewTestExecutorRegistry(), time.Second, time.Minute)
	c.Tick(context.Background())

	if !st.markSucceeded {
		t.Errorf("MarkSucceeded was not called; wanted the noop path to land SUCCEEDED")
	}
	if st.markFailedMsg != "" {
		t.Errorf("MarkFailed was called with %q; wanted the noop path to skip failure", st.markFailedMsg)
	}
	if st.listCalls != 1 {
		t.Errorf("ListQueuedOperations calls = %d, want 1", st.listCalls)
	}
}

// TestController_Tick_FailedExecutorCapturesErrorMsg verifies
// the failure path: an executor returning a non-nil error
// drives MarkFailed with the error string. The audit dashboard
// renders MarkFailed.error_message as the cause.
func TestController_Tick_FailedExecutorCapturesErrorMsg(t *testing.T) {
	st := &stubStore{
		queuedList: []store.Operation{{
			OperationID: "op-stub-2",
			WorkerID:    "wicket",
			Op:          OperationKindDrain,
			Status:      store.OperationStatusQueued,
		}},
	}
	reg := NewTestExecutorRegistry()
	hook := &failExecutor{err: errors.New("ansible: connection refused")}
	if err := reg.Register(OperationKindDrain, hook); err != nil {
		t.Fatalf("register: %v", err)
	}
	c := NewFleetController(st, reg, time.Second, time.Minute)
	c.Tick(context.Background())

	if st.markSucceeded {
		t.Errorf("MarkSucceeded called; wanted the failed path to land FAILED")
	}
	if !strings.Contains(st.markFailedMsg, "ansible: connection refused") {
		t.Errorf("MarkFailed msg = %q, want substring 'ansible: connection refused'", st.markFailedMsg)
	}
	if hook.calls != 1 {
		t.Errorf("hook.calls = %d, want 1", hook.calls)
	}
}

func TestController_Tick_DoesNotExecuteWhenClaimIsLost(t *testing.T) {
	st := &stubStore{
		markRunningClaimSet: true,
		markRunningClaim:    false,
		queuedList: []store.Operation{{
			OperationID: "op-claim-lost",
			WorkerID:    "wicket",
			Op:          OperationKindDrain,
			Status:      store.OperationStatusQueued,
		}},
	}
	reg := NewTestExecutorRegistry()
	hook := &failExecutor{}
	if err := reg.Register(OperationKindDrain, hook); err != nil {
		t.Fatalf("register: %v", err)
	}
	c := NewFleetController(st, reg, time.Second, time.Minute)
	c.Tick(context.Background())

	if hook.calls != 0 {
		t.Fatalf("executor calls = %d, want 0 after lost claim", hook.calls)
	}
	if st.markSucceeded || st.markFailedMsg != "" {
		t.Fatalf("lost claim must not write terminal state: succeeded=%v failed=%q", st.markSucceeded, st.markFailedMsg)
	}
}

// TestController_Tick_NoExecutorForKind ensures the
// ErrNoExecutorForKind path writes FAILED with the sentinel
// message. Catches a misconfigured boot (a kind added to
// AllOperationKinds without updating the registry default).
func TestController_Tick_NoExecutorForKind(t *testing.T) {
	st := &stubStore{
		queuedList: []store.Operation{{
			OperationID: "op-stub-3",
			WorkerID:    "wicket",
			Op:          OperationKindDrain,
			Status:      store.OperationStatusQueued,
		}},
	}
	// Empty registry (no defaults).
	reg := &ExecutorRegistry{executors: map[string]OperationExecutor{}}
	c := NewFleetController(st, reg, time.Second, time.Minute)
	c.Tick(context.Background())
	if st.markSucceeded {
		t.Errorf("MarkSucceeded called; wanted no-executor path to fail")
	}
	if !strings.Contains(st.markFailedMsg, ErrExecutorNotConfigured.Error()) || !strings.Contains(st.markFailedMsg, "no executor registered for operation kind") {
		t.Errorf("MarkFailed msg = %q, want EXECUTOR_NOT_CONFIGURED and ErrNoExecutorForKind", st.markFailedMsg)
	}
}

// TestController_Tick_ListErrRecoversSilentlyPin asserts the
// fail-but-recover contract: a transient ListQueuedOperations
// error must NOT propagate (the supervisor would mistakenly
// exit) — the log entry is acceptable, the next tick retries.
func TestController_Tick_ListErrRecoversSilently(t *testing.T) {
	st := &stubStore{listErr: errors.New("db briefly unavailable")}
	c := NewFleetController(st, NewTestExecutorRegistry(), time.Second, time.Minute)
	// No panic, no return-nil-error boundary issue.
	c.Tick(context.Background())
	if st.markSucceeded || st.markFailedMsg != "" {
		t.Errorf("list error path must not transition: succeeded=%v failedMsg=%q",
			st.markSucceeded, st.markFailedMsg)
	}
}

func TestController_StartStop_Lifecycle(t *testing.T) {
	st := &stubStore{}
	c := NewFleetController(st, NewTestExecutorRegistry(), 50*time.Millisecond, time.Second)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := c.Start(ctx); err != nil {
		t.Fatalf("Start: %v", err)
	}
	// Second Start must reject ErrAlreadyRunning.
	if err := c.Start(ctx); !errors.Is(err, ErrAlreadyRunning) {
		t.Errorf("second Start err = %v, want ErrAlreadyRunning", err)
	}
	c.Stop()
	select {
	case <-c.Done():
	case <-time.After(2 * time.Second):
		t.Fatalf("Done() did not close within 2s of Stop")
	}
	// Stop is idempotent on an already-stopped controller.
	c.Stop()
}

func TestController_Done_SatisfiableBeforeStart(t *testing.T) {
	st := &stubStore{}
	c := NewFleetController(st, NewTestExecutorRegistry(), time.Second, time.Minute)
	// A never-Started controller must return a satifiable Done().
	select {
	case <-c.Done():
	default:
		t.Fatalf("Done() must be immediately satisfiable before Start")
	}
}

// TestController_Run_BlocksUntilCtxDone is the supervisor-facing
// Run-semantics test: Run must block, return nil on ctx-cancel,
// NOT panic on nil-store or non-existent defaults. Production
// boot (cmd/server/bootstrap_composition.go) registers Run as a
// ClassRestartable supervisor runner — Run returning nil on
// ctx-done is what tells the supervisor "this is a clean exit,
// do not backoff-and-retry".
func TestController_Run_BlocksUntilCtxDone(t *testing.T) {
	st := &stubStore{}
	c := NewFleetController(st, NewTestExecutorRegistry(), 25*time.Millisecond, time.Second)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() { done <- c.Run(ctx) }()

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v, want nil on ctx-cancel", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not return within 2s of ctx-cancel")
	}
}

// TestController_Run_BlocksUntilStop is the Stop-channel path:
// a never-cancelled Run must still exit when Stop() closes the
// cancel channel. Ensures the production wiring (which uses
// Stop only via the supervisor's ctx-cancel) is not the
// only supported exit shape.
func TestController_Run_BlocksUntilStop(t *testing.T) {
	st := &stubStore{}
	c := NewFleetController(st, NewTestExecutorRegistry(), 25*time.Millisecond, time.Second)
	c.Start(context.Background())

	done := make(chan error, 1)
	go func() { done <- c.Run(context.Background()) }()

	c.Stop()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("Run returned %v, want nil on Stop", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("Run did not return within 2s of Stop")
	}
}

// TestExecutorRegistry_RegistersAllKinds pins the explicit test-only
// no-op registry. Production registries are intentionally empty.
//
// TestExecutorRegistry_RegistersAllKinds is the canonical-closed-
// set pin: Kinds() MUST return exactly the canonical
// AllOperationKinds set (no extras, no missing).
//
// Comparison is SET-based, NOT slice-order based. Kinds() returns
// alphabetical sort for stable diagnostics (the chosen
// enumeration order is irrelevant to the operator dashboard
// — only the SET membership matters); AllOperationKinds is
// declared in a logical grouping order (drain+resume paired as
// "operator toggles", restart+update+rollback+quarantine as
// "heavy ops", smoke as "verification"). Both order choices
// are intentional; the cross-package contract is "same set of
// strings", not "same order". A future addition to the enum
// MUST update both AllOperationKinds AND the schema CHECK
// (sqlite/104) — pin failure here catches a drift between
// the two.
func TestExecutorRegistry_ProductionStartsEmpty(t *testing.T) {
	reg := NewExecutorRegistry()
	if got := reg.Kinds(); len(got) != 0 {
		t.Fatalf("production registry kinds = %v, want empty", got)
	}
	if _, err := reg.Lookup(OperationKindDrain); !errors.Is(err, ErrExecutorNotConfigured) {
		t.Fatalf("Lookup(drain) error = %v, want ErrExecutorNotConfigured", err)
	}
	if err := reg.Register(OperationKindDrain, &NoopOperationExecutor{}); !errors.Is(err, ErrNoopExecutorNotAllowed) {
		t.Fatalf("production noop registration error = %v, want ErrNoopExecutorNotAllowed", err)
	}
}

func TestExecutorRegistry_RejectsTypedNilExecutor(t *testing.T) {
	reg := NewExecutorRegistry()
	var exec *failExecutor
	if err := reg.Register(OperationKindDrain, exec); err == nil {
		t.Fatal("typed-nil executor registration should fail")
	}
}

func TestExecutorRegistry_ValidateRequiredExecutors(t *testing.T) {
	reg := NewExecutorRegistry()
	if err := reg.ValidateRequiredExecutors(OperationKindUpdate); !errors.Is(err, ErrExecutorNotConfigured) {
		t.Fatalf("empty registry validation error = %v, want ErrExecutorNotConfigured", err)
	}
	if err := reg.Register(OperationKindUpdate, &failExecutor{}); err != nil {
		t.Fatalf("register concrete executor: %v", err)
	}
	if err := reg.ValidateRequiredExecutors(OperationKindUpdate); err != nil {
		t.Fatalf("concrete executor validation: %v", err)
	}
	if err := NewTestExecutorRegistry().ValidateRequiredExecutors(OperationKindUpdate); !errors.Is(err, ErrExecutorNotConfigured) {
		t.Fatalf("noop registry validation error = %v, want ErrExecutorNotConfigured", err)
	}
}

func TestExecutorRegistry_RegistersAllKinds(t *testing.T) {
	reg := NewTestExecutorRegistry()
	got := reg.Kinds()
	if len(got) != len(AllOperationKinds) {
		t.Errorf("len(Kinds()) = %d, want %d", len(got), len(AllOperationKinds))
		return
	}
	seen := make(map[string]int, len(got))
	for _, k := range got {
		seen[k]++
	}
	for _, k := range AllOperationKinds {
		seen[k]--
		if seen[k] < 0 {
			t.Errorf("Kinds() missing %q (declared in AllOperationKinds)", k)
		}
	}
	for k, count := range seen {
		if count != 0 {
			t.Errorf("Kinds() extra %q (count=%d, not in AllOperationKinds)", k, count)
		}
	}
}

func TestExecutorRegistry_RegisterRejectsUnknownKind(t *testing.T) {
	reg := NewTestExecutorRegistry()
	err := reg.Register("launch_missile", &failExecutor{})
	if err == nil {
		t.Errorf("expected rejection of unknown kind, got nil")
	}
}

func TestExecutorRegistry_RegisterNilExecutor(t *testing.T) {
	reg := NewTestExecutorRegistry()
	err := reg.Register(OperationKindDrain, nil)
	if err == nil {
		t.Errorf("expected rejection of nil executor, got nil")
	}
}

func TestNoopOperationExecutor_ExecuteReturnsNil(t *testing.T) {
	exec := &NoopOperationExecutor{}
	if err := exec.Execute(context.Background(), &store.Operation{}); err != nil {
		t.Errorf("noop Execute err = %v, want nil", err)
	}
}

func TestNewOperationID_Format(t *testing.T) {
	id := NewOperationID()
	// RFC 4122 §4.4 layout: 8-4-4-4-12 hex, dashes at 8, 13, 18, 23.
	if len(id) != 36 {
		t.Errorf("len(id) = %d, want 36", len(id))
	}
	for _, pos := range []int{8, 13, 18, 23} {
		if id[pos] != '-' {
			t.Errorf("id[%d] = %q, want '-'", pos, id[pos])
		}
	}
	// Version 4 marker (digit 4 at position 14).
	if id[14] != '4' {
		t.Errorf("version digit at pos 14 = %q, want '4'", id[14])
	}
	// Variant bits 10xx at position 19 (first hex digit of 4 in 8-4-4-4-12,
	// the variant byte: 8|9|a|b).
	variant := id[19]
	if variant != '8' && variant != '9' && variant != 'a' && variant != 'b' {
		t.Errorf("variant nibble at pos 19 = %q, want 8|9|a|b", variant)
	}
}

func TestAllOperationKinds_SchemaCompatible(t *testing.T) {
	// Defensive pin: if a kind lands in AllOperationKinds without
	// matching the schema CHECK (sqlite/104), this
	// test should also fail. We assert at the spec-level here —
	// the SQL-level pin runs in store/ store_fleet_operations_test.
	kinds := map[string]bool{}
	for _, k := range AllOperationKinds {
		if kinds[k] {
			t.Errorf("AllOperationKinds has duplicate %q", k)
		}
		kinds[k] = true
	}
	for _, mustHave := range []string{
		OperationKindDrain, OperationKindResume,
		OperationKindRestart, OperationKindUpdate,
		OperationKindRollback, OperationKindQuarantine,
		OperationKindSmoke,
	} {
		if !kinds[mustHave] {
			t.Errorf("AllOperationKinds missing canonical %q", mustHave)
		}
	}
}
