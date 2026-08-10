package fleet

import (
	"context"
	"strings"
	"testing"
	"time"

	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

func TestWorkerStateExecutor_RequiresConcreteKindAndRegistry(t *testing.T) {
	var nilExecutor *WorkerStateExecutor
	if err := nilExecutor.ValidateProductionBackends(); err == nil {
		t.Fatal("nil executor must fail production validation")
	}
	if err := NewWorkerStateExecutor(nil, OperationKindDrain).ValidateProductionBackends(); err == nil {
		t.Fatal("nil registry must fail production validation")
	}
	if err := NewWorkerStateExecutor(&workersreg.Registry{}, OperationKindSmoke).ValidateProductionBackends(); err == nil {
		t.Fatal("unsupported kind must fail production validation")
	}
}

func TestController_Tick_SmokeBackendMissingFailsAuditRow(t *testing.T) {
	st := &stubStore{
		queuedList: []store.Operation{{
			OperationID: "op-smoke-not-wired",
			WorkerID:    "worker-smoke",
			Op:          OperationKindSmoke,
			Status:      store.OperationStatusQueued,
		}},
	}
	reg := NewExecutorRegistry()
	if err := reg.Register(OperationKindSmoke, NewLevelDSmokeExecutor(LevelDSmokeBackend{})); err != nil {
		t.Fatalf("register smoke executor: %v", err)
	}
	controller := NewFleetController(st, reg, time.Second, time.Minute)
	controller.Tick(context.Background())

	if st.markSucceeded {
		t.Fatal("smoke with missing backends must not succeed")
	}
	if !strings.Contains(st.markFailedMsg, ErrSmokeRunnerNotWired.Error()) {
		t.Fatalf("MarkFailed msg = %q, want %q", st.markFailedMsg, ErrSmokeRunnerNotWired)
	}
}

func TestWorkerStateExecutor_RegistersAndAppliesDrainAndQuarantine(t *testing.T) {
	db, err := store.NewSQLiteStore(t.TempDir() + "/worker-state-executor.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	registry := workersreg.New(db)
	caps := map[string]interface{}{
		"capabilities": map[string]interface{}{
			"executors": []interface{}{
				map[string]interface{}{"id": "scene.composite.v1", "version": float64(1)},
			},
		},
	}
	if err := registry.RegisterWorker(context.Background(), "worker-state", "test", "127.0.0.1", caps); err != nil {
		t.Fatal(err)
	}

	for _, tc := range []struct {
		kind  string
		check func(*workersreg.Worker) bool
	}{
		{kind: OperationKindDrain, check: func(w *workersreg.Worker) bool { return w.Drain }},
		{kind: OperationKindQuarantine, check: func(w *workersreg.Worker) bool { return w.Quarantined }},
	} {
		executor := NewWorkerStateExecutor(registry, tc.kind)
		if err := executor.ValidateProductionBackends(); err != nil {
			t.Fatalf("%s validation: %v", tc.kind, err)
		}
		op := &store.Operation{WorkerID: "worker-state", Op: tc.kind}
		if err := executor.Execute(context.Background(), op); err != nil {
			t.Fatalf("%s execute: %v", tc.kind, err)
		}
		if info := registry.GetWorker(context.Background(), op.WorkerID); info == nil || !tc.check(info) {
			t.Fatalf("%s did not apply worker state: %+v", tc.kind, info)
		}
		if err := executor.Execute(context.Background(), op); err != nil {
			t.Fatalf("%s second idempotent execute: %v", tc.kind, err)
		}
	}
}
