package fleet

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

type resumeSmokeStub struct {
	artifactID string
	err        error
}

func (s resumeSmokeStub) RunLevelD(context.Context, string) (string, error) {
	return s.artifactID, s.err
}

func (s resumeSmokeStub) RunLevelDAfter(context.Context, string, time.Time) (string, error) {
	return s.artifactID, s.err
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
	exec := NewResumeExecutor(ResumeBackend{
		Registry: reg,
		Smoke:    resumeSmokeStub{err: errors.New("ffmpeg failed")},
	})

	err := exec.Execute(context.Background(), &store.Operation{WorkerID: "worker-resume", QueuedAt: time.Now().UTC()})
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
}

func TestResumeExecutor_GreenSmokeClearsBothExclusionFlags(t *testing.T) {
	reg := resumeTestRegistry(t, "worker-resume")
	if err := reg.SetWorkerDrain(context.Background(), "worker-resume", true); err != nil {
		t.Fatal(err)
	}
	if err := reg.SetWorkerQuarantine(context.Background(), "worker-resume", true); err != nil {
		t.Fatal(err)
	}
	exec := NewResumeExecutor(ResumeBackend{
		Registry: reg,
		Smoke:    resumeSmokeStub{artifactID: "artifact-green"},
	})

	if err := exec.Execute(context.Background(), &store.Operation{WorkerID: "worker-resume", QueuedAt: time.Now().UTC()}); err != nil {
		t.Fatal(err)
	}
	info := reg.GetWorker(context.Background(), "worker-resume")
	if info == nil || info.Drain || info.Quarantined {
		t.Fatalf("worker flags after green smoke: drain=%v quarantine=%v, want both false", info != nil && info.Drain, info != nil && info.Quarantined)
	}
}
