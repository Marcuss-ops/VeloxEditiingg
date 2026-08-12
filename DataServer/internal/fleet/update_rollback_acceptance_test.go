package fleet

import (
	"context"
	"errors"
	"strings"
	"testing"

	"velox-server/internal/store"
)

// recordingDocker preserves the existing update executor stub behaviour while
// recording the immutable image refs pulled by the forward and rollback paths.
type recordingDocker struct {
	*stubBackendsState
	pulls []string
}

func (d *recordingDocker) ActivateImage(ctx context.Context, workerID, imageRef string) (string, error) {
	d.pulls = append(d.pulls, imageRef)
	return d.stubBackendsState.ActivateImage(ctx, workerID, imageRef)
}

func TestUpdateAcceptance_AtoB_PreservesWorkerIDAndUsesPinnedDigests(t *testing.T) {
	backend, state := stubBackends(t)
	docker := &recordingDocker{stubBackendsState: state}
	backend.Docker = docker
	e := NewUpdateExecutor(backend)

	const workerID = "worker-e2e-a"
	const imageB = "ghcr.io/marcuss-ops/velox-worker@sha256:" + "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	state.runtimeDigest = imageB // the worker reconnects advertising image B
	if err := e.Execute(context.Background(), mkOp(workerID, imageB, "")); err != nil {
		t.Fatalf("A→B update returned error: %v", err)
	}

	if len(state.insertedRows) != 1 {
		t.Fatalf("deployment rows=%d, want one forward row", len(state.insertedRows))
	}
	row := state.insertedRows[0]
	if row.WorkerID != workerID {
		t.Fatalf("forward row WorkerID=%q, want %q", row.WorkerID, workerID)
	}
	if row.TargetDigest != imageB {
		t.Fatalf("target digest=%q, want %q", row.TargetDigest, imageB)
	}
	if row.PreviousDigest != state.prevDigest {
		t.Fatalf("previous digest=%q, want %q", row.PreviousDigest, state.prevDigest)
	}
	if state.markedStatuses[row.DeploymentID] != store.DeployStatusSucceeded {
		t.Fatalf("forward status=%q, want %q", state.markedStatuses[row.DeploymentID], store.DeployStatusSucceeded)
	}
	if len(docker.pulls) != 1 || docker.pulls[0] != imageB {
		t.Fatalf("pulled refs=%v, want [%s]", docker.pulls, imageB)
	}

	worker, err := backend.Registry.GetWorker(context.Background(), workerID)
	if err != nil {
		t.Fatal(err)
	}
	if worker == nil || worker.WorkerID.String() != workerID {
		t.Fatalf("worker identity after update=%v, want %q", worker, workerID)
	}
}

func TestUpdateAcceptance_BadImageAutomaticallyRollsBackToPreviousDigest(t *testing.T) {
	backend, state := stubBackends(t)
	state.prevDigest = "ghcr.io/marcuss-ops/velox-worker@sha256:" + strings.Repeat("a", 64)
	// Smoke failure models IMAGE_BAD: the new image starts, but the
	// Level-D certification gate rejects it. Rollback must pull the
	// exact prior immutable digest, not a mutable tag.
	state.smokeErr = errors.New("IMAGE_BAD: readiness/smoke failed")
	docker := &recordingDocker{stubBackendsState: state}
	backend.Docker = docker
	e := NewUpdateExecutor(backend)

	const workerID = "worker-e2e-a"
	const imageBad = "ghcr.io/marcuss-ops/velox-worker@sha256:" + "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
	// The image starts and the digest verifies — the Level-D smoke is the
	// certifier that rejects IMAGE_BAD.
	state.runtimeDigest = imageBad
	err := e.Execute(context.Background(), mkOp(workerID, imageBad, ""))
	if err == nil || !errors.Is(err, ErrRollbackSucceeded) {
		t.Fatalf("IMAGE_BAD update error=%v, want ErrRollbackSucceeded", err)
	}
	if !strings.Contains(err.Error(), "rollback_ok to "+state.prevDigest) {
		t.Fatalf("rollback error=%v, want previous digest %q", err, state.prevDigest)
	}

	if len(state.insertedRows) != 2 {
		t.Fatalf("deployment rows=%d, want forward+rollback", len(state.insertedRows))
	}
	forward, rollback := state.insertedRows[0], state.insertedRows[1]
	if forward.WorkerID != workerID || rollback.WorkerID != workerID {
		t.Fatalf("row WorkerIDs=%q/%q, want %q/%q", forward.WorkerID, rollback.WorkerID, workerID, workerID)
	}
	if forward.TargetDigest != imageBad {
		t.Fatalf("forward target=%q, want IMAGE_BAD %q", forward.TargetDigest, imageBad)
	}
	if !rollback.IsRollback || rollback.TargetDigest != state.prevDigest {
		t.Fatalf("rollback row=%+v, want is_rollback=true target=%q", rollback, state.prevDigest)
	}
	if !state.rolledBack[rollback.DeploymentID] {
		t.Fatalf("rollback deployment %q was not marked successful", rollback.DeploymentID)
	}
	wantPulls := []string{imageBad, state.prevDigest}
	if len(docker.pulls) != len(wantPulls) || docker.pulls[0] != wantPulls[0] || docker.pulls[1] != wantPulls[1] {
		t.Fatalf("pulled refs=%v, want %v", docker.pulls, wantPulls)
	}

	worker, err := backend.Registry.GetWorker(context.Background(), workerID)
	if err != nil {
		t.Fatal(err)
	}
	if worker == nil || worker.WorkerID.String() != workerID {
		t.Fatalf("worker identity after rollback=%v, want %q", worker, workerID)
	}
	if !worker.SessionActive || worker.LastHB == "" {
		t.Fatalf("worker connection after rollback: session_active=%v last_hb=%q", worker.SessionActive, worker.LastHB)
	}
	if state.drain {
		t.Fatal("worker remained drained after successful automatic rollback")
	}
}
