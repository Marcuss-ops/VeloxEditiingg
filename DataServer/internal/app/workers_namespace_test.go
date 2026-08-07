package app

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	workersapi "velox-server/internal/handlers/remote/workers"
	"velox-server/internal/handlers/remote/workers/lifecycle"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/protectedasset"
	"velox-server/internal/store"
	"velox-server/internal/workers"
	"velox-shared/dispatchable"
)

// wiredWorkersModule builds a WorkersModule with enough handlers wired to
// mount the canonical agent/fleet/admin namespaces (register, assets,
// protected-assets, fleet metrics, fleet alerts). Mirrors the production
// wiring in cmd/server (bootstrap_modules.go + bootstrap_wiring.go):
// per-worker read handlers come from api.NewSQLDBReader and the fleet
// aggregate handlers take the SQLiteStore directly.
func wiredWorkersModule(t *testing.T) *WorkersModule {
	t.Helper()
	gin.SetMode(gin.TestMode)
	cfg := &config.Config{}
	reg := workers.New(nil)
	updateHandler := workersapi.NewWorkerUpdateHandler(cfg, nil, nil, nil, t.TempDir(), nil)
	workerLifecycle := lifecycle.NewHandler(cfg, nil, nil)
	adminAuth := func(c *gin.Context) { c.Next() }

	sqlStore, err := store.NewSQLiteStore(t.TempDir() + "/workers-namespace.db")
	if err != nil {
		t.Fatal(err)
	}
	reader := api.NewSQLDBReader(sqlStore.DB())

	m := NewWorkersModule(cfg, reg, workerLifecycle, updateHandler, adminAuth, nil, store.NewNopBlobStore(t.TempDir()))
	m.SetProtectedAssetsHandler(api.NewProtectedAssetsHandler(protectedasset.NewService(
		protectedasset.RepoFunc(func(ctx context.Context, limit int) ([]dispatchable.Job, error) { return nil, nil }),
		32,
	)))
	m.SetMetricsAggregatorHandler(api.NewAdminWorkersMetricsAggregatorHandler(sqlStore, 5*time.Minute))
	m.SetAlertsHandler(api.NewAdminWorkersAlertsHandler(sqlStore))
	m.SetMetricsHandler(api.NewMetricsHandler(reader.Metrics))
	m.SetSessionsHandler(api.NewSessionsHandler(reader.Sessions))
	m.SetEventsHandler(api.NewEventsHandler(reader.Events))
	return m
}

// routeSet extracts the registered (method, path) pairs.
func routeSet(r *gin.Engine) map[string]struct{} {
	set := make(map[string]struct{})
	for _, route := range r.Routes() {
		set[route.Method+" "+route.Path] = struct{}{}
	}
	return set
}

// TestWorkersModule_CanonicalNamespacesRegistered pins the Phase 6
// API-surface split: /api/v1/agent/* (worker-auth), /api/v1/fleet/*
// (aggregates), /api/v1/admin/workers/* (operators).
func TestWorkersModule_CanonicalNamespacesRegistered(t *testing.T) {
	m := wiredWorkersModule(t)
	r := gin.New()
	m.RegisterRoutes(r)
	set := routeSet(r)

	want := []string{
		// Canonical agent namespace (worker-authenticated traffic).
		http.MethodPost + " /api/v1/agent/register",
		http.MethodGet + " /api/v1/agent/assets/:asset_id",
		http.MethodGet + " /api/v1/agent/cache/protected-assets",
		// Canonical admin operator namespace (including the migrated
		// /worker/* control actions and the revoked list).
		http.MethodGet + " /api/v1/admin/workers",
		http.MethodGet + " /api/v1/admin/workers/:worker_id",
		http.MethodPost + " /api/v1/admin/workers/:worker_id/revoke",
		http.MethodPost + " /api/v1/admin/workers/:worker_id/unrevoke",
		http.MethodPost + " /api/v1/admin/workers/:worker_id/restart",
		http.MethodGet + " /api/v1/admin/workers/revoked",
		// Canonical fleet aggregate namespace.
		http.MethodGet + " /api/v1/fleet/metrics",
		http.MethodGet + " /api/v1/fleet/alerts/active",
		http.MethodGet + " /api/v1/fleet/alerts/recent",
	}
	for _, wantRoute := range want {
		if _, ok := set[wantRoute]; !ok {
			t.Errorf("canonical route not registered: %s", wantRoute)
		}
	}
}

// TestWorkersModule_RemovedLegacyRoutesGone pins the Phase 6 removal
// decision: the pre-canonical routes whose usage counter showed zero
// sustained traffic are UNMOUNTED (agent register/assets/protected-assets
// under legacy paths, the /worker admin group, and the legacy fleet
// aggregates under /api/v1/admin/*). Only the /api/v1/workers diagnostic
// surface (list/get + per-worker reads) remains, still consumed by
// scripts/cert/master_state.sh and the operator runbook.
func TestWorkersModule_RemovedLegacyRoutesGone(t *testing.T) {
	m := wiredWorkersModule(t)
	r := gin.New()
	m.RegisterRoutes(r)
	set := routeSet(r)

	gone := []string{
		http.MethodPost + " /api/v1/workers/register",
		http.MethodGet + " /api/v1/worker-assets/:asset_id",
		http.MethodGet + " /api/v1/workers/cache/protected-assets",
		http.MethodPost + " /worker/revoke",
		http.MethodPost + " /worker/unrevoke",
		http.MethodGet + " /worker/revoked",
		http.MethodPost + " /worker/drain",
		http.MethodPost + " /worker/restart",
		http.MethodGet + " /api/v1/admin/workers/metrics",
		http.MethodGet + " /api/v1/admin/alerts/active",
		http.MethodGet + " /api/v1/admin/alerts/recent",
	}
	for _, goneRoute := range gone {
		if _, ok := set[goneRoute]; ok {
			t.Errorf("removed legacy route still mounted: %s", goneRoute)
		}
	}

	// The remaining diagnostic surface must stay mounted.
	for _, kept := range []string{
		http.MethodGet + " /api/v1/workers",
		http.MethodGet + " /api/v1/workers/:worker_id",
		http.MethodGet + " /api/v1/workers/:worker_id/metrics",
	} {
		if _, ok := set[kept]; !ok {
			t.Errorf("diagnostic route should still be mounted: %s", kept)
		}
	}
}

// TestWorkersModule_CanonicalAgentRegisterServesRequest proves the
// canonical /api/v1/agent/register path actually dispatches (worker
// registration is reachable under the new namespace, not just listed).
func TestWorkersModule_CanonicalAgentRegisterServesRequest(t *testing.T) {
	m := wiredWorkersModule(t)
	r := gin.New()
	m.RegisterRoutes(r)

	req := httptest.NewRequest(http.MethodPost, "/api/v1/agent/register", nil)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	// Registration requires a worker_id; a 400 (not 404) proves the route
	// exists and dispatches to the register handler.
	if w.Code != http.StatusBadRequest {
		t.Fatalf("POST /api/v1/agent/register = %d, want 400 (route must dispatch)", w.Code)
	}
}
