package main

// bootstrap_composition.go is the thin orchestrator for the master
// process dependency graph.  It defines appComponents, close, and
// buildAppComponents — the latter delegates to domain-specific helpers:
//   - bootstrap_telemetry.go   — wireMetricsTelemetry, wireCompatibilityMode
//   - bootstrap_publishing.go  — wireResolver, wireInstaeditVerifier
//   - bootstrap_prefetch.go    — wireFleetComposition (SSH + registry + fleet + smoke)
//   - bootstrap_wiring.go      — wirePostBuild, wireFleetOperatorHandlers
//
// The build* helpers for individual domains live in:
//   bootstrap_persistence.go, bootstrap_jobs.go, bootstrap_tasks.go,
//   bootstrap_workers.go, bootstrap_assets.go, bootstrap_modules.go,
//   bootstrap_fleet.go, bootstrap_supervisor.go

import (
	"context"
	"fmt"
	"strings"

	"velox-server/internal/app"
	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/fleet"
	"velox-server/internal/fleet/opsalerts"
	"velox-server/internal/instaeditauth"
	"velox-server/internal/logging"
	velmetrics "velox-server/internal/metrics"
	"velox-server/internal/registry"
	"velox-server/internal/supervisor"
)

// appComponents holds every dependency the master process needs at
// runtime.  Fields are set in buildAppComponents in dependency order;
// DO NOT reorder them without re-reading that function.
type appComponents struct {
	cfg         *config.Config
	persistence *persistenceDeps
	jobs        *jobsDeps
	tasks       *taskDeps
	workers     *workerDeps
	assets      *assetDeps
	modules     *moduleDeps
	fleet       *FleetDep

	resolver            *creatorflow.Resolver
	instaeditVerifier   *instaeditauth.Verifier
	capabilityRegistry  *registry.CapabilityRegistry
	metricsRegistry     *velmetrics.Registry
	metricsCollector    *velmetrics.Collector
	supervisor          *supervisor.Supervisor
	health              *app.HealthModule
	opsAlertsCapability opsalerts.CapabilityStatus
	smokeCapability     fleet.SmokeCapabilityStatus
}

// close releases owned resources.  Called via defer in runServer.
func (c *appComponents) close() error {
	if c == nil || c.persistence == nil || c.persistence.SQLite == nil {
		return nil
	}
	if err := c.persistence.SQLite.Close(); err != nil {
		logServerf(context.Background(), logging.LevelError, logging.CodeServerLifecycleError, "[SERVER] Store close failed: %v", err)
		return err
	}
	return nil
}

// buildAppComponents constructs the master process's full dependency
// graph in the canonical order.  Each step fails fast so the operator
// sees the FIRST misconfiguration.
func buildAppComponents(cfg *config.Config) (*appComponents, error) {
	// ── Stage 1: Alerts (before supervisor so outbox-dispatcher works) ──
	alertDeps, err := buildAlerts(cfg.Runtime.Alerts.WebhookURL, cfg.Runtime.Alerts.WebhookType)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: alerts: %w", err)
	}

	// ── Stage 2: Persistence ────────────────────────────────────────────
	p, err := buildPersistence(cfg)
	if err != nil {
		return nil, err
	}

	// ── Stage 3: Jobs + Tasks + cross-layer wiring ──────────────────────
	j, err := buildJobs(p)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}
	t, err := buildTasks(p)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}
	if err := wirePostBuild(j, t); err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}

	// ── Stage 4: Workers + Assets + Modules ─────────────────────────────
	w, err := buildWorkers(cfg, p)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}
	a, err := buildAssets(cfg, p, j)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}
	m, err := buildModules(cfg, p, j, w, a, t)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}

	logServerf(context.Background(), logging.LevelInfo, logging.CodeServerRoutes,
		"[ROUTES] script dependency state: enqueuer=%t store=%t remote_engine=%t",
		m != nil && m.Enqueuer != nil,
		p != nil && p.SQLite != nil,
		cfg != nil && strings.TrimSpace(cfg.Render.RemoteEngineURL) != "",
	)
	if m == nil || m.Enqueuer == nil {
		_ = p.SQLite.Close()
		return nil, fmt.Errorf("server composition: script API requires a non-nil enqueuer")
	}
	if p == nil || p.SQLite == nil {
		return nil, fmt.Errorf("server composition: script API requires a non-nil sqlite store")
	}

	// ── Stage 5: Publishing (resolver + InstaEdit) ──────────────────────
	resolver := wireResolver(cfg, p, m)
	capabilityRegistry := registry.NewCapabilityRegistry()
	instaeditVerifier, err := wireInstaeditVerifier(cfg, p)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}

	// ── Stage 6: Metrics + telemetry ────────────────────────────────────
	metricsRegistry, metricsCollector := wireMetricsTelemetry(p, a, m)
	wireCompatibilityMode(cfg, metricsCollector)

	// ── Stage 7: Supervisor ─────────────────────────────────────────────
	var opsAlertsCapability opsalerts.CapabilityStatus
	sup, err := buildSupervisor(cfg, a, m, j, p, w, t, metricsCollector, &opsAlertsCapability, alertDeps.Notifier)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}

	// ── Stage 8: Fleet (SSH + registry + executors + smoke + operators) ─
	fleetDep, smokeCapability, err := wireFleetComposition(cfg, p, w, m, sup)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}

	return &appComponents{
		cfg:                 cfg,
		persistence:         p,
		jobs:                j,
		tasks:               t,
		workers:             w,
		assets:              a,
		modules:             m,
		fleet:               fleetDep,
		resolver:            resolver,
		capabilityRegistry:  capabilityRegistry,
		metricsRegistry:     metricsRegistry,
		metricsCollector:    metricsCollector,
		instaeditVerifier:   instaeditVerifier,
		supervisor:          sup,
		health:              m.Health,
		opsAlertsCapability: opsAlertsCapability,
		smokeCapability:     smokeCapability,
	}, nil
}
