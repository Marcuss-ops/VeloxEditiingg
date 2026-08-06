package fleet

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

type resumeSmokeStub struct {
	err      error
	calls    int
	lastOp   *store.Operation
	lastBody SmokePayload
}

func (s *resumeSmokeStub) Execute(_ context.Context, op *store.Operation) error {
	s.calls++
	s.lastOp = op
	_ = json.Unmarshal(op.Payload, &s.lastBody)
	return s.err
}

func resumeTestRegistry(t *testing.T, workerID string) *workersreg.Registry {
	t.Helper()
	reg := workersreg.New(nil)
	if err := reg.RegisterWorker(context.Background(), workerID, "test", "127.0.0.1", nil); err != nil {
		t.Fatal(err)
	}
	return reg
}

func TestResumeExecutor_SmokeFailurePreservesExclusion(t *testing.T) {
	reg := resumeTestRegistry(t, "worker-resume")
	if err := reg.SetWorkerDrain(context.Background(), "worker-resume", true); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetWorkerQuarantine(context.Background(), "worker-resume", true); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetWorkerResumingIfClear(context.Background(), "worker-resume", "op-resume-fail"); err != nil {
		t.Fatal(err)
	}
	smoke := &resumeSmokeStub{err: errors.New("ffmpeg failed")}
	exec := NewResumeExecutor(ResumeBackend{
		Registry:      reg,
		SmokeExecutor: smoke,
	})

	err := exec.Execute(context.Background(), &store.Operation{OperationID: "op-resume-fail", WorkerID: "worker-resume", QueuedAt: time.Now().UTC()})
	if !errors.Is(err, ErrResumeSmokeFailed) {
		t.Fatalf("Execute error=%v, want ErrResumeSmokeFailed", err)
	}
	info := reg.GetWorker(context.Background(), "worker-resume")
	if info == nil || !info.Drain || !info.Quarantined || info.Resuming {
		t.Fatalf("flags after failed smoke: drain=%v quarantine=%v resuming=%v, want drain=true/quarantine=true/resuming=false", info != nil && info.Drain, info != nil && info.Quarantined, info != nil && info.Resuming)
	}
	if !strings.Contains(err.Error(), "ffmpeg failed") {
		t.Fatalf("error=%v, want smoke failure detail", err)
	}
	if smoke.calls != 1 {
		t.Fatalf("smoke executor calls=%d, want 1", smoke.calls)
	}
}

func TestResumeExecutor_GreenSmokeClearsBothExclusionFlags(t *testing.T) {
	reg := resumeTestRegistry(t, "worker-resume")
	if err := reg.SetWorkerDrain(context.Background(), "worker-resume", true); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetWorkerQuarantine(context.Background(), "worker-resume", true); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetWorkerResumingIfClear(context.Background(), "worker-resume", "op-resume-green"); err != nil {
		t.Fatal(err)
	}
	smoke := &resumeSmokeStub{}
	exec := NewResumeExecutor(ResumeBackend{
		Registry:      reg,
		SmokeExecutor: smoke,
	})

	if err := exec.Execute(context.Background(), &store.Operation{OperationID: "op-resume-green", WorkerID: "worker-resume", QueuedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	info := reg.GetWorker(context.Background(), "worker-resume")
	if info == nil || info.Drain || info.Quarantined || info.Resuming {
		t.Fatalf("worker flags after green smoke: drain=%v quarantine=%v resuming=%v, want all false", info != nil && info.Drain, info != nil && info.Quarantined, info != nil && info.Resuming)
	}
	if smoke.calls != 1 {
		t.Fatalf("smoke executor calls=%d, want 1", smoke.calls)
	}
	if smoke.lastOp == nil || smoke.lastOp.Op != OperationKindSmoke {
		t.Fatalf("nested operation=%+v, want fresh %q operation", smoke.lastOp, OperationKindSmoke)
	}
	if smoke.lastBody.AssetID != "asset-canary-001" {
		t.Fatalf("nested smoke asset_id=%q, want asset-canary-001", smoke.lastBody.AssetID)
	}
}

func TestResumeExecutor_StaleCompletionCannotClearNewerGate(t *testing.T) {
	reg := resumeTestRegistry(t, "worker-resume")
	if err := reg.SetWorkerDrain(context.Background(), "worker-resume", true); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetWorkerResumingIfClear(context.Background(), "worker-resume", "op-resume-old"); err != nil {
		t.Fatal(err)
	}
	if err := reg.ClearWorkerResumingIfOwner(context.Background(), "worker-resume", "op-resume-old"); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetWorkerResumingIfClear(context.Background(), "worker-resume", "op-resume-new"); err != nil {
		t.Fatal(err)
	}
	if err := reg.CompleteResume(context.Background(), "worker-resume", "op-resume-old"); err == nil {
		t.Fatal("stale completion unexpectedly cleared newer resume gate")
	}
	info := reg.GetWorker(context.Background(), "worker-resume")
	if info == nil || !info.Drain || !info.Resuming || info.ResumeOperationID != "op-resume-new" {
		t.Fatalf("newer gate was changed by stale completion: drain=%v resuming=%v owner=%q", info != nil && info.Drain, info != nil && info.Resuming, info.ResumeOperationID)
	}
}

func TestResumeExecutor_CompleteResumePersistenceFailureKeepsGate(t *testing.T) {
	db, err := store.NewSQLiteStore(t.TempDir() + "/resume-complete-failure.db")
	if err != nil {
		t.Fatal(err)
	}
	reg := workersreg.New(db)
	ctx := context.Background()
	if err := reg.RegisterWorker(ctx, "worker-resume", "test", "127.0.0.1", nil); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetWorkerDrain(ctx, "worker-resume", true); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetWorkerResumingIfClear(ctx, "worker-resume", "op-resume-persist-fail"); err != nil {
		t.Fatal(err)
	}
	// Force the durable completion to fail after the in-memory transition
	// is attempted. The registry must restore the fail-closed snapshot.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	err = reg.CompleteResume(ctx, "worker-resume", "op-resume-persist-fail")
	if err == nil {
		t.Fatal("CompleteResume unexpectedly succeeded with a closed store")
	}
	info := reg.GetWorker(ctx, "worker-resume")
	if info == nil || !info.Drain || !info.Resuming || info.ResumeOperationID != "op-resume-persist-fail" {
		t.Fatalf("in-memory gate after persistence failure: info=%+v, want original fail-closed snapshot", info)
	}
}

func TestResumeExecutor_SmokeCleanupFailureIsObservable(t *testing.T) {
	db, err := store.NewSQLiteStore(t.TempDir() + "/resume-smoke-cleanup-failure.db")
	if err != nil {
		t.Fatal(err)
	}
	reg := workersreg.New(db)
	ctx := context.Background()
	if err := reg.RegisterWorker(ctx, "worker-resume", "test", "127.0.0.1", nil); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetWorkerDrain(ctx, "worker-resume", true); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetWorkerResumingIfClear(ctx, "worker-resume", "op-resume-cleanup-fail"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	err = NewResumeExecutor(ResumeBackend{
		Registry:      reg,
		SmokeExecutor: &resumeSmokeStub{err: errors.New("smoke failed")},
	}).Execute(ctx, &store.Operation{
		OperationID: "op-resume-cleanup-fail",
		WorkerID:    "worker-resume",
		QueuedAt:    time.Now().UTC(),
	})
	if !errors.Is(err, ErrResumeSmokeFailed) || !strings.Contains(err.Error(), "cleanup failed") {
		t.Fatalf("Execute error=%v, want smoke failure with observable cleanup failure", err)
	}
	info := reg.GetWorker(ctx, "worker-resume")
	if info == nil || !info.Drain || !info.Resuming {
		t.Fatalf("cleanup failure must keep fail-closed gate: info=%+v", info)
	}
}

func TestResumeExecutor_RejectsMissingFreshSmokeExecutor(t *testing.T) {
	reg := resumeTestRegistry(t, "worker-resume")
	if err := reg.SetWorkerQuarantine(context.Background(), "worker-resume", true); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetWorkerResumingIfClear(context.Background(), "worker-resume", "op-resume-missing"); err != nil {
		t.Fatal(err)
	}

	err := NewResumeExecutor(ResumeBackend{Registry: reg}).Execute(
		context.Background(),
		&store.Operation{OperationID: "op-resume-missing", WorkerID: "worker-resume"},
	)
	if !errors.Is(err, ErrResumeSmokeFailed) {
		t.Fatalf("Execute error=%v, want ErrResumeSmokeFailed", err)
	}
	info := reg.GetWorker(context.Background(), "worker-resume")
	if info == nil || !info.Quarantined || info.Resuming {
		t.Fatalf("flags when smoke executor is unavailable: quarantine=%v resuming=%v, want quarantine=true/resuming=false", info != nil && info.Quarantined, info != nil && info.Resuming)
	}
}

func TestResumeExecutor_PreservesPreexistingDrainDuringSmokeCleanup(t *testing.T) {
	reg := resumeTestRegistry(t, "worker-resume")
	lease := &RegistryDrainLease{Reg: reg, previousDrains: make(map[string]bool)}
	if err := reg.SetWorkerDrain(context.Background(), "worker-resume", true); err != nil {
		t.Fatal(err)
	}

	if err := lease.AcquireSmokeLease(context.Background(), "smoke-worker-resume-1", "worker-resume"); err != nil {
		t.Fatal(err)
	}
	if err := lease.ReleaseSmokeLease(context.Background(), "smoke-worker-resume-1"); err != nil {
		t.Fatal(err)
	}
	info := reg.GetWorker(context.Background(), "worker-resume")
	if info == nil || !info.Drain {
		t.Fatalf("Drain=%v, want true after smoke cleanup when drain pre-existed", info != nil && info.Drain)
	}
}
