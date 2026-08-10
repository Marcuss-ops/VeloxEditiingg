package api

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/fleet"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

func newOperationalFleetStore(t *testing.T) *store.SQLiteStore {
	t.Helper()
	db, err := store.NewSQLiteStore(t.TempDir() + "/operational-fleet.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	if err := db.CreateFleetOperationsTableIfNotExists(); err != nil {
		t.Fatalf("CreateFleetOperationsTableIfNotExists: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func newOperationalWorkerRegistry(t *testing.T, db *store.SQLiteStore, workerID string) *workersreg.Registry {
	t.Helper()
	reg := workersreg.New(db)
	caps := map[string]interface{}{
		"capabilities": map[string]interface{}{
			"host": map[string]interface{}{"max_parallel_jobs": float64(1)},
			"executors": []interface{}{
				map[string]interface{}{"id": "scene.composite.v1", "version": float64(1)},
			},
		},
	}
	if err := reg.RegisterWorker(context.Background(), workerID, "operational-test", "127.0.0.1", caps); err != nil {
		t.Fatalf("RegisterWorker: %v", err)
	}
	if err := reg.Heartbeat(context.Background(), workerID, "operational-test", "", nil); err != nil {
		t.Fatalf("Heartbeat: %v", err)
	}
	return reg
}

func newOperationalStateExecutorRegistry(reg *workersreg.Registry) *fleet.ExecutorRegistry {
	executors := fleet.NewExecutorRegistry()
	_ = executors.Register(fleet.OperationKindDrain, fleet.NewWorkerStateExecutor(reg, fleet.OperationKindDrain))
	_ = executors.Register(fleet.OperationKindQuarantine, fleet.NewWorkerStateExecutor(reg, fleet.OperationKindQuarantine))
	return executors
}

type operationalSmokeExecutor struct {
	err error
}

func (e *operationalSmokeExecutor) Execute(context.Context, *store.Operation) error {
	return e.err
}

func waitOperationalStatus(t *testing.T, db *store.SQLiteStore, operationID, want string) *store.Operation {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		op, err := db.GetOperation(context.Background(), operationID)
		if err == nil && op.Status == want {
			return op
		}
		time.Sleep(5 * time.Millisecond)
	}
	op, err := db.GetOperation(context.Background(), operationID)
	if err != nil {
		t.Fatalf("get operation %s: %v", operationID, err)
	}
	t.Fatalf("operation %s status=%q want %q", operationID, op.Status, want)
	return nil
}

func operationalRouter(reg *workersreg.Registry, controller *fleet.FleetController) *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	mutations := NewAdminWorkersMutationsHandler(reg, controller)
	smoke := NewAdminWorkersSmokeHandler(reg, controller)
	audit := NewAdminOperationsHandler(controller)
	r.POST("/api/v1/admin/workers/:worker_id/drain", mutations.DrainWorker())
	r.POST("/api/v1/admin/workers/:worker_id/quarantine", mutations.QuarantineWorker())
	r.POST("/api/v1/admin/workers/:worker_id/resume", mutations.ResumeWorker())
	r.POST("/api/v1/admin/workers/:worker_id/smoke", smoke.TriggerSmoke())
	r.GET("/api/v1/admin/operations/:operation_id", audit.GetAdminOperation())
	return r
}

func operationalPost(t *testing.T, r *gin.Engine, path string, body interface{}) *httptest.ResponseRecorder {
	t.Helper()
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		if err != nil {
			t.Fatalf("marshal request: %v", err)
		}
	}
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(payload))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func operationFromResponse(t *testing.T, w *httptest.ResponseRecorder) MutationResponse {
	t.Helper()
	var resp MutationResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode mutation response: %v; body=%s", err, w.Body.String())
	}
	if resp.OperationID == "" {
		t.Fatalf("mutation response has empty operation_id: %s", w.Body.String())
	}
	return resp
}

func TestOperationalDrain_202LedgerDuplicate409AndPlacementBlock(t *testing.T) {
	db := newOperationalFleetStore(t)
	reg := newOperationalWorkerRegistry(t, db, "w-drain-operational")
	controller := fleet.NewFleetController(db, newOperationalStateExecutorRegistry(reg), time.Second, time.Minute)
	r := operationalRouter(reg, controller)

	first := operationalPost(t, r, "/api/v1/admin/workers/w-drain-operational/drain", map[string]string{"reason": "operational drain"})
	if first.Code != http.StatusAccepted {
		t.Fatalf("first drain status=%d want 202: %s", first.Code, first.Body.String())
	}
	resp := operationFromResponse(t, first)
	if resp.Status != store.OperationStatusQueued || resp.Op != fleet.OperationKindDrain {
		t.Fatalf("unexpected drain response: %+v", resp)
	}

	info := reg.GetWorker(context.Background(), "w-drain-operational")
	if info == nil || !info.Drain {
		t.Fatalf("drain flag=%v want true", info != nil && info.Drain)
	}
	if eligible := reg.GetSchedulableWorkers(context.Background()); len(eligible) != 0 {
		t.Fatalf("draining worker remained placement-eligible: %+v", eligible)
	}

	queued, err := db.GetOperation(context.Background(), resp.OperationID)
	if err != nil {
		t.Fatalf("ledger GET before tick: %v", err)
	}
	if queued.Status != store.OperationStatusQueued || queued.WorkerID != "w-drain-operational" {
		t.Fatalf("queued ledger row: %+v", queued)
	}

	duplicate := operationalPost(t, r, "/api/v1/admin/workers/w-drain-operational/drain", nil)
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate drain status=%d want 409: %s", duplicate.Code, duplicate.Body.String())
	}
	ops, err := db.ListOperations(context.Background(), "w-drain-operational", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(ops) != 1 {
		t.Fatalf("duplicate drain created %d ledger rows, want 1", len(ops))
	}

	controller.Tick(context.Background())
	terminal, err := db.GetOperation(context.Background(), resp.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Status != store.OperationStatusSucceeded || terminal.StartedAt == nil || terminal.FinishedAt == nil {
		t.Fatalf("terminal drain ledger row: %+v", terminal)
	}

	get := httptest.NewRecorder()
	r.ServeHTTP(get, httptest.NewRequest(http.MethodGet, "/api/v1/admin/operations/"+resp.OperationID, nil))
	if get.Code != http.StatusOK {
		t.Fatalf("operation GET status=%d want 200: %s", get.Code, get.Body.String())
	}
	var card OperationCard
	if err := json.Unmarshal(get.Body.Bytes(), &card); err != nil {
		t.Fatal(err)
	}
	if card.Status != store.OperationStatusSucceeded {
		t.Fatalf("operation API status=%q want SUCCEEDED", card.Status)
	}
}

func TestOperationalQuarantine_202Duplicate409AndPlacementBlock(t *testing.T) {
	db := newOperationalFleetStore(t)
	reg := newOperationalWorkerRegistry(t, db, "w-quarantine-operational")
	controller := fleet.NewFleetController(db, newOperationalStateExecutorRegistry(reg), time.Second, time.Minute)
	r := operationalRouter(reg, controller)

	first := operationalPost(t, r, "/api/v1/admin/workers/w-quarantine-operational/quarantine", map[string]string{"reason": "quarantine test"})
	if first.Code != http.StatusAccepted {
		t.Fatalf("quarantine status=%d want 202: %s", first.Code, first.Body.String())
	}
	resp := operationFromResponse(t, first)
	info := reg.GetWorker(context.Background(), "w-quarantine-operational")
	if info == nil || !info.Quarantined {
		t.Fatalf("quarantine flag=%v want true", info != nil && info.Quarantined)
	}
	if eligible := reg.GetSchedulableWorkers(context.Background()); len(eligible) != 0 {
		t.Fatalf("quarantined worker remained placement-eligible: %+v", eligible)
	}

	duplicate := operationalPost(t, r, "/api/v1/admin/workers/w-quarantine-operational/quarantine", nil)
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate quarantine status=%d want 409", duplicate.Code)
	}
	controller.Tick(context.Background())
	terminal, err := db.GetOperation(context.Background(), resp.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if terminal.Status != store.OperationStatusSucceeded {
		t.Fatalf("quarantine ledger status=%q want SUCCEEDED", terminal.Status)
	}
}

func TestOperationalResume_ControllerRunsSmokeBeforeFlags(t *testing.T) {
	db := newOperationalFleetStore(t)
	reg := newOperationalWorkerRegistry(t, db, "w-resume-gated")
	if err := reg.SetWorkerDrain(context.Background(), "w-resume-gated", true); err != nil {
		t.Fatal(err)
	}
	executors := fleet.NewExecutorRegistry()
	smoke := &operationalSmokeExecutor{err: errors.New("level d artifact verification failed")}
	if err := executors.Register(fleet.OperationKindResume, fleet.NewResumeExecutor(fleet.ResumeBackend{
		Registry: reg, SmokeExecutor: smoke,
	})); err != nil {
		t.Fatal(err)
	}
	controller := fleet.NewFleetController(db, executors, time.Second, time.Minute)
	r := operationalRouter(reg, controller)

	failed := operationalPost(t, r, "/api/v1/admin/workers/w-resume-gated/resume", map[string]string{"reason": "gate failure"})
	if failed.Code != http.StatusAccepted {
		t.Fatalf("failed resume status=%d want 202: %s", failed.Code, failed.Body.String())
	}
	failedResp := operationFromResponse(t, failed)
	controller.Tick(context.Background())
	failedOp := waitOperationalStatus(t, db, failedResp.OperationID, store.OperationStatusFailed)
	if !strings.Contains(failedOp.ErrorMessage, "level d artifact verification failed") {
		t.Fatalf("failed resume ledger lost smoke error: %+v", failedOp)
	}
	info := reg.GetWorker(context.Background(), "w-resume-gated")
	if info == nil || !info.Drain || info.Resuming {
		t.Fatalf("worker flags after failed smoke: drain=%v resuming=%v, want drain=true/resuming=false", info != nil && info.Drain, info != nil && info.Resuming)
	}
	if eligible := reg.GetSchedulableWorkers(context.Background()); len(eligible) != 0 {
		t.Fatalf("failed resume left worker placement-eligible: %+v", eligible)
	}

	smoke.err = nil
	passed := operationalPost(t, r, "/api/v1/admin/workers/w-resume-gated/resume", map[string]string{"reason": "gate recovery"})
	if passed.Code != http.StatusAccepted {
		t.Fatalf("recovery resume status=%d want 202: %s", passed.Code, passed.Body.String())
	}
	passedResp := operationFromResponse(t, passed)
	controller.Tick(context.Background())
	passedOp := waitOperationalStatus(t, db, passedResp.OperationID, store.OperationStatusSucceeded)
	if passedOp.StartedAt == nil || passedOp.FinishedAt == nil {
		t.Fatalf("successful resume missing ledger lifecycle timestamps: %+v", passedOp)
	}
	info = reg.GetWorker(context.Background(), "w-resume-gated")
	if info == nil || info.Drain || info.Quarantined || info.Resuming {
		t.Fatalf("worker flags after green smoke: drain=%v quarantine=%v resuming=%v, want all false", info != nil && info.Drain, info != nil && info.Quarantined, info != nil && info.Resuming)
	}
}

func TestOperationalResume_202KeepsExclusionUntilSmokeAndInFlight409(t *testing.T) {
	db := newOperationalFleetStore(t)
	reg := newOperationalWorkerRegistry(t, db, "w-resume-operational")
	if err := reg.SetWorkerDrain(context.Background(), "w-resume-operational", true); err != nil {
		t.Fatal(err)
	}
	controller := fleet.NewFleetController(db, fleet.NewExecutorRegistry(), time.Second, time.Minute)
	r := operationalRouter(reg, controller)

	first := operationalPost(t, r, "/api/v1/admin/workers/w-resume-operational/resume", map[string]string{"reason": "resume smoke gate"})
	if first.Code != http.StatusAccepted {
		t.Fatalf("resume status=%d want 202: %s", first.Code, first.Body.String())
	}
	resp := operationFromResponse(t, first)
	info := reg.GetWorker(context.Background(), "w-resume-operational")
	if info == nil || !info.Drain || !info.Resuming {
		t.Fatalf("resume gate before smoke: drain=%v resuming=%v, want both true", info != nil && info.Drain, info != nil && info.Resuming)
	}
	if eligible := reg.GetSchedulableWorkers(context.Background()); len(eligible) != 0 {
		t.Fatalf("worker became placement-eligible before smoke: %+v", eligible)
	}

	duplicate := operationalPost(t, r, "/api/v1/admin/workers/w-resume-operational/resume", nil)
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate in-flight resume status=%d want 409: %s", duplicate.Code, duplicate.Body.String())
	}
	queued, err := db.GetOperation(context.Background(), resp.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if queued.Status != store.OperationStatusQueued || queued.Op != fleet.OperationKindResume {
		t.Fatalf("resume ledger row: %+v", queued)
	}
}

func TestOperationalSmoke_202LedgerAndDuplicate409(t *testing.T) {
	db := newOperationalFleetStore(t)
	reg := newOperationalWorkerRegistry(t, db, "w-smoke-operational")
	executors := fleet.NewExecutorRegistry()
	smoke := &operationalSmokeExecutor{}
	if err := executors.Register(fleet.OperationKindSmoke, smoke); err != nil {
		t.Fatal(err)
	}
	controller := fleet.NewFleetController(db, executors, time.Second, time.Minute)
	r := operationalRouter(reg, controller)
	body := map[string]interface{}{"asset_id": "asset-canary-001", "timeout_sec": 30, "reason": "real smoke request"}

	first := operationalPost(t, r, "/api/v1/admin/workers/w-smoke-operational/smoke", body)
	if first.Code != http.StatusAccepted {
		t.Fatalf("smoke status=%d want 202: %s", first.Code, first.Body.String())
	}
	resp := operationFromResponse(t, first)
	if resp.Op != fleet.OperationKindSmoke || resp.Status != store.OperationStatusQueued {
		t.Fatalf("smoke response: %+v", resp)
	}
	stored, err := db.GetOperation(context.Background(), resp.OperationID)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Op != fleet.OperationKindSmoke || string(stored.Payload) == "" {
		t.Fatalf("smoke ledger payload missing: %+v", stored)
	}
	duplicate := operationalPost(t, r, "/api/v1/admin/workers/w-smoke-operational/smoke", body)
	if duplicate.Code != http.StatusConflict {
		t.Fatalf("duplicate smoke status=%d want 409: %s", duplicate.Code, duplicate.Body.String())
	}
	controller.Tick(context.Background())
	terminal := waitOperationalStatus(t, db, resp.OperationID, store.OperationStatusSucceeded)
	if terminal.StartedAt == nil || terminal.FinishedAt == nil {
		t.Fatalf("smoke terminal ledger missing lifecycle timestamps: %+v", terminal)
	}
	ops, err := db.ListOperations(context.Background(), "w-smoke-operational", "", 0)
	if err != nil {
		t.Fatal(err)
	}
	var smokeOps int
	for _, op := range ops {
		if op.Op == fleet.OperationKindSmoke {
			smokeOps++
		}
	}
	if smokeOps != 1 {
		t.Fatalf("duplicate smoke created %d smoke ledger rows, want 1 (all rows=%d)", smokeOps, len(ops))
	}
}
