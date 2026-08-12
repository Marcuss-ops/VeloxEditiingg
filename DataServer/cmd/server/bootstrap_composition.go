package main

// Composition: domain dependency wiring + supervisor registration.
// Holds appComponents, buildAppComponents, buildSupervisor.
// (wirePostBuild lives in bootstrap_wiring.go.)
//
// Blocco 4 step #2: extracted from bootstrap.go. The split keeps
// runServer linear (≤200 lines) while the build* orchestration +
// supervisor registration live here alongside the typed helper
// structs.

import (
	"fmt"
	"log"
	"strings"

	"velox-server/internal/app"
	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/fleet"
	"velox-server/internal/fleet/opsalerts"
	"velox-server/internal/instaeditauth"
	velmetrics "velox-server/internal/metrics"
	"velox-server/internal/registry"
	"velox-server/internal/supervisor"
	"velox-shared/compatibility"
)

// appComponents holds every dependency the master process needs at
// runtime. The split into per-file helpers means runServer itself
// stays linear (≤200 lines) while the build* calls live in the
// slice of files that already defined them. Fields are set in
// buildAppComponents in dependency order; DO NOT reorder them
// without re-reading that function.
//
// appComponents does NOT duplicate the god-shape of the obsolete
// `*serverDeps` mega-struct: every field is typed; no field exists
// for "future use". New top-level concerns grown from an existing
// field are added with explicit justification in a comment.
type appComponents struct {
	cfg         *config.Config
	persistence *persistenceDeps
	jobs        *jobsDeps
	tasks       *taskDeps
	workers     *workerDeps
	assets      *assetDeps
	modules     *moduleDeps
	fleet       *FleetDep

	// Resolver is the canonical creatorflow.Resolver. The pipeline
	// handler (sync forward path) and the CreatorForwardingRunner
	// (async poll path) share this instance so they converge on the
	// same (job_id, forwarding_id) write path.
	resolver *creatorflow.Resolver

	// instaeditVerifier validates the short-lived JWT the InstaEdit
	// BFF sends when calling the /api/v1/instaedit/* routes. Nil when
	// INSTAEDIT_CONTROL_JWT_SECRET is not configured (dev/test).
	instaeditVerifier *instaeditauth.Verifier

	// CapabilityRegistry wires artifact.commit.v1 dispatch gates.
	// Registered probes (coordinator, spool, transport) surface in
	// /ready via Readyz().
	capabilityRegistry *registry.CapabilityRegistry

	// Metrics: scorecard v1 Prometheus exporter. Nil in tests
	// means /metrics is omitted by registerMetricsRoutes.
	metricsRegistry  *velmetrics.Registry
	metricsCollector *velmetrics.Collector

	// Supervisor owns long-lived background runners. Built in
	// buildAppComponents AFTER every other dependency so a runner
	// hook into a missing dep is a structural composition bug.
	supervisor *supervisor.Supervisor

	// health is a thin alias of modules.Health. Hoisted here so
	// registerReadinessChecks does not need to reach into modules.
	health *app.HealthModule

	// opsAlertsCapability is explicit even when the feature is disabled:
	// readiness and diagnostics must distinguish DISABLED from a missing
	// or silently no-op evaluator.
	opsAlertsCapability opsalerts.CapabilityStatus

	// smokeCapability is explicit even when Level-D is unavailable:
	// readiness and route wiring distinguish DISABLED from a real executor.
	smokeCapability fleet.SmokeCapabilityStatus
}

// close releases owned resources. Called via defer on the returned
// *appComponents in runServer (priority: connection close, since
// every other resource leaks upward through the pool).
func (c *appComponents) close() error {
	if c == nil || c.persistence == nil || c.persistence.SQLite == nil {
		return nil
	}
	if err := c.persistence.SQLite.Close(); err != nil {
		log.Printf("[SERVER] Store close failed: %v", err)
		return err
	}
	return nil
}

// buildAppComponents constructs the master process's full dependency
// graph in the canonical order. Each step fails fast so the operator
// sees the FIRST misconfiguration (rather than a confused mass of
// nil-receiver panics in supervisor startup). The returned
// appComponents is the input to startTransports +
// registerReadinessChecks + runUntilShutdown.
func buildAppComponents(cfg *config.Config) (*appComponents, error) {
	// Wire the alert sink BEFORE buildSupervisor so the
	// outbox-dispatcher's first JOB_FAILED delivery hits the real
	// sink. Without this wiring, JOB_FAILED fails closed and is retried
	// instead of being marked processed without an alert. buildSupervisor registers
	// outbox-dispatcher as a ClassCritical supervisor runner, and
	// we don't want any startup-window alerts silently dropped.
	// The return value of buildAlerts is DISCARDED on purpose: the
	// wiring is a side effect of registering alert handlers with
	// the outbox dispatcher; the resulting *alertsDeps is consumed
	// internally and never read by anyone else. Storing it on
	// appComponents would create a dead field.
	if _, err := buildAlerts(); err != nil {
		return nil, fmt.Errorf("bootstrap: alerts: %w", err)
	}

	p, err := buildPersistence(cfg)
	if err != nil {
		return nil, err
	}

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

	log.Printf(
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

	// PR-taskgraph-wiring: forward the canonical Resolver (built
	// from the build* return values) to the pipeline handler so
	// the handler's sync forward path and the runner's async
	// poll-and-forward path converge on the same (job_id,
	// forwarding_id). The runner picks up the same Resolver via
	// ForwardingRunner.SetResolver below.
	var resolver *creatorflow.Resolver
	if p != nil && p.SQLite != nil && m != nil && m.Enqueuer != nil {
		resolver = creatorflow.NewResolver(cfg, m.Enqueuer, p.SQLite)
	}
	if m != nil && m.ForwardingRunner != nil && resolver != nil {
		m.ForwardingRunner.SetResolver(resolver)
		log.Printf("[BOOTSTRAP] CreatorForwardingRunner wired to canonical Resolver (Blocco 5)")
	}

	// Construct the canonical capability registry here so the
	// gRPC handler's SetCapabilityRegistry call (in startTransports)
	// has a non-nil registry available. Probe registration happens
	// later in registerReadinessChecks.
	capabilityRegistry := registry.NewCapabilityRegistry()

	metricsRegistry := velmetrics.NewRegistry()
	metricsCollector := velmetrics.NewCollector(metricsRegistry)
	if p.SQLite != nil {
		p.SQLite.SetDBTelemetry(metricsCollector.OperationalTelemetry())
	}
	if a != nil && a.CompletionSQLiteStore != nil {
		a.CompletionSQLiteStore.SetDBRetryObserver(metricsCollector.OperationalTelemetry())
	}
	if m != nil && m.DeliveryRunner != nil {
		m.DeliveryRunner.WithTelemetry(metricsCollector.OperationalTelemetry())
	}
	if m != nil && m.AssetService != nil {
		for _, family := range velmetrics.NewInputSecurityFamilies(m.AssetService.SecurityMetrics()) {
			if family != nil {
				metricsRegistry.Register(family)
			}
		}
		for _, family := range velmetrics.NewAssetMediaMetadataFamilies(m.AssetService.MediaMetadataMetrics()) {
			if family != nil {
				metricsRegistry.Register(family)
			}
		}
	}
	compatibility.SetAliasReadObserver(metricsCollector.NewCompatibilityAliasObserver())
	compatibility.SetAliasRejectedObserver(metricsCollector.NewCompatibilityAliasRejectionObserver())
	if cfg.Compatibility.Mode == "strict" {
		compatibility.SetMode(compatibility.ModeStrict)
	} else {
		compatibility.SetMode(compatibility.ModeCompat)
	}

	// Build the InstaEdit control-plane verifier when the shared
	// secret is configured. A nil verifier means the /api/v1/instaedit
	// routes are not mounted, so dev/test deployments keep working.
	var instaeditVerifier *instaeditauth.Verifier
	if secret := strings.TrimSpace(cfg.Auth.InstaeditControlJWTSecret); secret != "" {
		v, err := instaeditauth.New(secret)
		if err != nil {
			_ = p.SQLite.Close()
			return nil, fmt.Errorf("bootstrap: instaedit auth verifier: %w", err)
		}
		instaeditVerifier = v
		log.Printf("[BOOTSTRAP] InstaEdit control JWT verifier configured")
	}

	var opsAlertsCapability opsalerts.CapabilityStatus
	supervisor, err := buildSupervisor(cfg, a, m, j, p, w, t, metricsCollector, &opsAlertsCapability)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}

	workerNodeRegistry, err := buildWorkerRegistryFromStore(p)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, fmt.Errorf("bootstrap: worker node registry: %w", err)
	}
	// Health probes and Level-D smoke still consume the existing shell
	// command adapter; those probes intentionally use a few composed,
	// read-only commands. The update executor gets its own hardened client
	// so remote image activation never bypasses host-key verification or
	// BatchMode. Both clients read the same canonical WorkerRegistry.
	sharedSSH := fleet.NewSSHClientFromRegistry(workerNodeRegistry)
	updateSSH := fleet.NewSecureSSHClient(
		workerNodeRegistry,
		fleet.DefaultSSHKeyPath,
		fleet.DefaultKnownHostsPath,
	)
	fleetDep, err := buildFleet(p, w.Registry, updateSSH)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, fmt.Errorf("bootstrap: fleet: %w", err)
	}

	// Fleet-operator wiring (admin worker mutations / health / smoke /
	// metrics / alerts handlers + the shared SSH client) — extracted to
	// bootstrap_wiring.go so buildAppComponents stays a readable
	// dependency-ordered composition. Nil-tolerant per step.
	var smokeCapability fleet.SmokeCapabilityStatus
	if err := wireFleetOperatorHandlers(cfg, fleetDep, m, p, workerNodeRegistry, sharedSSH, &smokeCapability); err != nil {
		_ = p.SQLite.Close()
		return nil, fmt.Errorf("bootstrap: fleet operator wiring: %w", err)
	}
	if fleetDep != nil && fleetDep.Registry != nil {
		if err := fleetDep.Registry.ValidateRequiredExecutors(); err != nil {
			_ = p.SQLite.Close()
			return nil, fmt.Errorf("bootstrap: fleet executor registry: %w", err)
		}
		log.Printf("[BOOTSTRAP] Fleet executor registry validated: %s", strings.Join(fleetDep.Registry.Kinds(), ", "))
	}
	if fleetDep != nil {
		if err := registerFleetRunner(supervisor, fleetDep); err != nil {
			_ = p.SQLite.Close()
			return nil, fmt.Errorf("bootstrap: fleet supervisor: %w", err)
		}
		fleetDep.tickWiredAtBoot = true
		log.Printf("[BOOTSTRAP] FleetController wired and supervised (operation ledger tick enabled)")
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
		supervisor:          supervisor,
		health:              m.Health,
		opsAlertsCapability: opsAlertsCapability,
		smokeCapability:     smokeCapability,
	}, nil
}

// wirePostBuild lives in bootstrap_wiring.go (extracted with the
// fleet-operator wiring so buildAppComponents stays a readable
// dependency-ordered composition).
