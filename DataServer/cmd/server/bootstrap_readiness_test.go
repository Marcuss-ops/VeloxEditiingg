package main

// AZIONE 2 — fail-closed update capability readiness probe tests.
//
// registerReadinessChecks must expose the UpdateExecutor verdict to
// /ready: a master with any critical update backend missing reports
// NOT READY (503 + probe-named failure) instead of serving /ready=200
// while POST /update would fail 30s after publish. The green path
// locks the wiring: with every backend wired, the update-capability
// check passes and /ready returns 200.

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/app"
	"velox-server/internal/fleet"
	"velox-server/internal/registry"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

// readinessTestComponents builds the minimal appComponents graph
// registerReadinessChecks needs: real persistence (so the db-ping /
// blobstore / outbox / capability-registry checks pass) + the fleet
// dependency carrying the executor under test.
func readinessTestComponents(t *testing.T, update *fleet.UpdateExecutor) *appComponents {
	t.Helper()
	cfg := newTestConfig(t)
	p, err := buildPersistence(cfg)
	if err != nil {
		t.Fatalf("buildPersistence: %v", err)
	}
	t.Cleanup(func() { _ = p.SQLite.Close() })

	components := &appComponents{
		cfg:                cfg,
		persistence:        p,
		modules:            &moduleDeps{Health: app.NewHealthModule()},
		capabilityRegistry: registry.NewCapabilityRegistry(),
		fleet:              &FleetDep{Update: update},
	}
	components.health = components.modules.Health
	return components
}

// fullyWiredUpdateExecutor mirrors the production wiring from
// buildFleet + AttachRuntimeBackends: every critical backend non-nil.
func fullyWiredUpdateExecutor(t *testing.T, p *persistenceDeps) *fleet.UpdateExecutor {
	t.Helper()
	workerNodeRegistry, err := buildWorkerRegistryFromStore(p)
	if err != nil {
		t.Fatalf("buildWorkerRegistryFromStore: %v", err)
	}
	ssh := fleet.NewSSHClientFromRegistry(workerNodeRegistry)
	return fleet.NewUpdateExecutor(fleet.UpdateBackend{
		SSHCmd: ssh,
		Docker: &fleet.SSHWorkerDockerClient{SSH: ssh},
		// Named adapter: p.SQLite's MarkFailed belongs to fleet_operations,
		// not deployment_records (migration 153 split the signatures).
		Deployments: store.NewDeploymentRecordRepository(p.SQLite),
		Cosign:      newUpdateCosignVerifier(),
		Image:       deployUpdateImageValidator{},
		Registry:    &fleet.RealRegistryUpdateGater{Reg: workersreg.New(nil)},
		Smoke:       readinessSmokeRunner{},
		Drive:       readinessDriveVerifier{},
	})
}

type readinessSmokeRunner struct{}

func (readinessSmokeRunner) RunLevelD(_ context.Context, _ string) (string, error) {
	return "cap-test-artifact", nil
}

type readinessDriveVerifier struct{}

func (readinessDriveVerifier) VerifyDelivery(_ context.Context, _ string, _ int64) error { return nil }

// serveReady runs the registered checks against a mounted HealthModule
// and returns the /ready response recorder.
func serveReady(components *appComponents) *httptest.ResponseRecorder {
	components.health.MarkReady()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	components.health.RegisterRoutes(r)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/ready", nil))
	return w
}

func TestReadiness_UpdateCapabilityProbe_FailClosedWhenBackendsMissing(t *testing.T) {
	// Empty backend = every critical dependency missing (the
	// "half-wired master" failure AZIONE 2 eliminates).
	components := readinessTestComponents(t, fleet.NewUpdateExecutor(fleet.UpdateBackend{}))

	registerReadinessChecks(components, &transportBundle{})
	w := serveReady(components)

	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("/ready = %d, want 503: %s", w.Code, w.Body.String())
	}
	body := w.Body.String()
	if !strings.Contains(body, "update-capability") {
		t.Errorf("/ready body must name the update-capability probe; got: %s", body)
	}
	if !strings.Contains(body, "NOT READY") {
		t.Errorf("/ready body must surface 'NOT READY'; got: %s", body)
	}
	if !strings.Contains(body, "docker") || !strings.Contains(body, "smoke") || !strings.Contains(body, "drive") {
		t.Errorf("/ready body must list the missing backends (docker/smoke/drive); got: %s", body)
	}
}

func TestReadiness_UpdateCapabilityProbe_GreenWhenFullyWired(t *testing.T) {
	cfg := newTestConfig(t)
	p, err := buildPersistence(cfg)
	if err != nil {
		t.Fatalf("buildPersistence: %v", err)
	}
	t.Cleanup(func() { _ = p.SQLite.Close() })

	components := &appComponents{
		cfg:                cfg,
		persistence:        p,
		modules:            &moduleDeps{Health: app.NewHealthModule()},
		capabilityRegistry: registry.NewCapabilityRegistry(),
		fleet:              &FleetDep{Update: fullyWiredUpdateExecutor(t, p)},
	}
	components.health = components.modules.Health

	registerReadinessChecks(components, &transportBundle{})
	w := serveReady(components)

	if w.Code != http.StatusOK {
		t.Fatalf("/ready = %d, want 200 with fully-wired update executor: %s", w.Code, w.Body.String())
	}
}

// TestReadiness_CapabilityExposuresHaveFailClosedGates pins the
// architectural capability contract (AGENTS.md §6): every
// AddReadinessCapability("X") exposure MUST be paired with a fail-closed
// AddReadinessCheck("X-capability") so a MISCONFIGURED dependency flips
// /ready red (readiness rossa su dipendenze mancanti) instead of serving
// a silently-degraded capability. The same invariant is enforced
// statically by scripts/ci/check-capability-contract.sh.
func TestReadiness_CapabilityExposuresHaveFailClosedGates(t *testing.T) {
	components := readinessTestComponents(t, nil)

	registerReadinessChecks(components, &transportBundle{})

	checkSet := make(map[string]struct{})
	for _, name := range components.modules.Health.CheckNames() {
		checkSet[name] = struct{}{}
	}

	for _, capName := range components.modules.Health.CapabilityNames() {
		gate := capName + "-capability"
		if _, ok := checkSet[gate]; !ok {
			t.Errorf("capability %q has no fail-closed readiness gate %q", capName, gate)
		}
	}
}

func TestReadiness_NoUpdateCapabilityProbeWithoutFleetDep(t *testing.T) {
	// A boot without fleet wiring (test / partial composition) must
	// not register the probe and must not crash registerReadinessChecks.
	components := readinessTestComponents(t, nil)
	components.fleet = nil // simulate absence of the fleet dependency bundle

	registerReadinessChecks(components, &transportBundle{})
	w := serveReady(components)

	if w.Code != http.StatusOK {
		t.Fatalf("/ready = %d, want 200 without fleet dep: %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Body.String(), "update-capability") {
		t.Errorf("/ready unexpectedly named update-capability probe: %s", w.Body.String())
	}
}
