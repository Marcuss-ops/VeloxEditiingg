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
	if info == nil || !info.Drain {
		t.Fatalf("Drain=%v, want true after failed smoke", info != nil && info.Drain)
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
	smoke := &resumeSmokeStub{}
	exec := NewResumeExecutor(ResumeBackend{
		Registry:      reg,
		SmokeExecutor: smoke,
	})

	if err := exec.Execute(context.Background(), &store.Operation{OperationID: "op-resume-green", WorkerID: "worker-resume", QueuedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	info := reg.GetWorker(context.Background(), "worker-resume")
	if info == nil || info.Drain || info.Quarantined {
		t.Fatalf("worker flags after green smoke: drain=%v quarantine=%v, want both false", info != nil && info.Drain, info != nil && info.Quarantined)
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

func TestResumeExecutor_RejectsMissingFreshSmokeExecutor(t *testing.T) {
	reg := resumeTestRegistry(t, "worker-resume")
	if err := reg.SetWorkerQuarantine(context.Background(), "worker-resume", true); err != nil {
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
	if info == nil || !info.Quarantined {
		t.Fatalf("Quarantined=%v, want true when smoke executor is unavailable", info != nil && info.Quarantined)
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
