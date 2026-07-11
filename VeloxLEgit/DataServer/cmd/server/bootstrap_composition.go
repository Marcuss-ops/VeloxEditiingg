package main

// Composition: domain dependency wiring + supervisor registration.
// Holds appComponents, buildAppComponents, wirePostBuild, buildSupervisor.
//
// Blocco 4 step #2: extracted from bootstrap.go. The split keeps
// runServer linear (≤200 lines) while the build* orchestration +
// supervisor registration live here alongside the typed helper
// structs.

import (
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"velox-server/internal/alertengine"
	"velox-server/internal/app"
	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	workerhandlersuploads "velox-server/internal/handlers/remote/workers/uploads"
	"velox-server/internal/ingest"
	velmetrics "velox-server/internal/metrics"
	"velox-server/internal/registry"
	"velox-server/internal/supervisor"
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

	// Resolver is the canonical creatorflow.Resolver. The pipeline
	// handler (sync forward path) and the CreatorForwardingRunner
	// (async poll path) share this instance so they converge on the
	// same (job_id, forwarding_id) write path.
	resolver *creatorflow.Resolver

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

// routerBundle assembles the per-route dependency sets from the
// build* return values. Kept as a method on appComponents (rather
// than free-standing) so the next time a new build* helper adds a
// per-route dep, the only place to wire it is in this method.
func (c *appComponents) routerBundle() RouterBundle {
	return RouterBundle{
		Script: ScriptRouteDeps{
			Cfg:         c.cfg,
			SQLiteStore: c.persistence.SQLite,
			Enqueuer:    c.modules.Enqueuer,
		},
		Groups: GroupsRouteDeps{SQLiteStore: c.persistence.SQLite},
		Pipeline: PipelineRouteDeps{
			Cfg:      c.cfg,
			Enqueuer: c.modules.Enqueuer,
			JobsRepo: c.jobs.Repository,
			CmdMgr:   c.workers.CommandManager,
			Resolver: c.resolver,
		},
		Darkeditor: DarkeditorRouteDeps{Cfg: c.cfg, SQLiteStore: c.persistence.SQLite},
		Upload: UploadRouteDeps{
			Cfg:            c.cfg,
			ArtifactSvc:    c.assets.ArtifactSvc,
			ChunkedHandler: workerhandlersuploads.NewChunkedUploadHandler(c.assets.ChunkedUploadSvc),
		},
		Metrics: MetricsRouteDeps{Registry: c.metricsRegistry},
	}
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
	// sink, not the NopNotifier default. buildSupervisor registers
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

	supervisor, err := buildSupervisor(a, m, j, p, w, t, metricsCollector)
	if err != nil {
		_ = p.SQLite.Close()
		return nil, err
	}

	return &appComponents{
		cfg:                cfg,
		persistence:        p,
		jobs:               j,
		tasks:              t,
		workers:            w,
		assets:             a,
		modules:            m,
		resolver:           resolver,
		capabilityRegistry: capabilityRegistry,
		metricsRegistry:    metricsRegistry,
		metricsCollector:   metricsCollector,
		supervisor:         supervisor,
		health:             m.Health,
	}, nil
}

// wirePostBuild connects dependencies that cross build-layer
// boundaries (jobs↔tasks). Called by both buildTestDeps (tests)
// and buildAppComponents (production) so the wiring stays canonical
// in exactly one place.
func wirePostBuild(j *jobsDeps, t *taskDeps) error {
	// fix/remove-job-lease-ops: j.SQLiteRepo (concrete
	// *SQLiteJobRepository) satisfies taskgraph.JobsRetryQuerier
	// via structural typing (Get + FailWithRetry). j.Repository
	// returns jobs.Repository which no longer has FailWithRetry
	// on the canonical interface.
	if j != nil && j.SQLiteRepo != nil && t != nil && t.TaskLifecycle != nil {
		t.TaskLifecycle.SetJobsRepo(j.SQLiteRepo)
	}

	// feat/task-report-ingestion: build the canonical
	// TaskReportIngestionService now that all upstream deps
	// (tasks + attempts + jobs + task_output_artifacts) are
	// constructed.
	if j != nil && j.Repository != nil && t != nil && t.TaskRepository != nil && t.OutputArtifacts != nil {
		ingestionSvc, ingErr := ingest.NewTaskReportIngestionService(
			t.TaskRepository, j.Repository, t.AttemptRepository, t.OutputArtifacts,
		)
		if ingErr != nil {
			return fmt.Errorf("bootstrap: task report ingestion service: %w", ingErr)
		}
		t.IngestionSvc = ingestionSvc
	}
	return nil
}

// buildSupervisor registers the long-lived background runners
// using the SupervisedRunner taxonomy introduced in Blocco 1:
//
//   - ClassCritical:    outbox-dispatcher, delivery-runner,
//                       creator-forwarding-runner, task-lease-reaper.
//                       If any dies the master is dead in the water:
//                       VELOX_CRITICAL_MAX_RETRIES bounds the budget
//                       (0 = infinite; positive = fail-loud after).
//   - ClassRestartable: artifact-reconciler, taskgraph dispatcher,
//                       metrics-supervisor. Bounded retries with
//                       backoff; after exhaustion the runner is
//                       removed and the supervisor logs WARN.
//   - ClassOneShot:     manifest-generator. Run once on startup;
//                       failure is non-fatal (logged WARN).
func buildSupervisor(a *assetDeps, m *moduleDeps, j *jobsDeps, p *persistenceDeps, w *workerDeps, t *taskDeps, metricsCollector *velmetrics.Collector) (*supervisor.Supervisor, error) {
	sup := supervisor.New()

	criticalMaxRetries, criticalFailAfter := criticalRetryConfigFromEnv()
	criticalPolicy := supervisor.RestartPolicy{
		MaxRetries:     criticalMaxRetries,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     30 * time.Second,
		RestartOnPanic: true,
	}
	if criticalMaxRetries > 0 {
		log.Printf("[SUPERVISOR] critical retry budget: max_retries=%d (fail-loud after that many consecutive failures); fail_after=%d (log-WARN threshold)",
			criticalMaxRetries, criticalFailAfter)
	} else {
		log.Printf("[SUPERVISOR] critical retry budget: infinite (legacy 0=infinite); fail_after=%d (log-WARN threshold)",
			criticalFailAfter)
	}
	const restartableMaxRetries = 5
	restartablePolicy := supervisor.RestartPolicy{
		MaxRetries:     restartableMaxRetries,
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     30 * time.Second,
		RestartOnPanic: true,
	}

	// ── ClassCritical ────────────────────────────────────────────────
	if a.OutboxDispatcher != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "outbox-dispatcher",
			Class:  supervisor.ClassCritical,
			Policy: criticalPolicy,
			Run: func(ctx context.Context) error {
				log.Printf("[BOOTSTRAP] Outbox dispatcher started — polling outbox_events")
				return a.OutboxDispatcher.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register outbox-dispatcher: %w", err)
		}
	}
	if m.DeliveryRunner != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "delivery-runner",
			Class:  supervisor.ClassCritical,
			Policy: criticalPolicy,
			Run: func(ctx context.Context) error {
				log.Printf("[BOOTSTRAP] DeliveryRunner started — polling PENDING job_deliveries")
				return m.DeliveryRunner.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register delivery-runner: %w", err)
		}
	}
	if m.ForwardingRunner != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "creator-forwarding-runner",
			Class:  supervisor.ClassCritical,
			Policy: criticalPolicy,
			Run: func(ctx context.Context) error {
				log.Printf("[BOOTSTRAP] CreatorForwardingRunner started — polling creator_forwardings")
				return m.ForwardingRunner.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register creator-forwarding-runner: %w", err)
		}
	}
	if t.TaskLeaseReaper != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "task-lease-reaper",
			Class:  supervisor.ClassCritical,
			Policy: criticalPolicy,
			Run: func(ctx context.Context) error {
				return t.TaskLeaseReaper.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register task-lease-reaper: %w", err)
		}
	}

	// ── Legacy Job-side reaper DEPRECATED log (PR-13 → PR-05 cutover) ─
	if j.Repository != nil {
		// PR-13 → PR-05 cutover: the Job-side reaper is DEPRECATED.
		// TaskLeaseReaper is the canonical master-side lease enforcer.
		// VELOX_DISABLE_JOB_REAPER is preserved for back-compat (the
		// old flag would otherwise silently break operators depending
		// on it); the entry just emits a one-line DEPRECATED log.
		if os.Getenv("VELOX_DISABLE_JOB_REAPER") == "true" {
			log.Printf("[BOOTSTRAP] DEPRECATED env=VELOX_DISABLE_JOB_REAPER=true (PR-13 superseded by PR-05 TaskLeaseReaper; flag is now a no-op, set VELOX_TASK_LEASE_REAPER_DISABLED=true at the TaskLeaseReaper runner if you need to disable on the canonical path)")
		} else {
			log.Printf("[BOOTSTRAP] note=job-side zombie reaper is DEPRECATED; TaskLeaseReaper is the canonical master-side lease enforcer")
		}
	}

	// ── ClassRestartable ─────────────────────────────────────────────
	if a.Reconciler != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "artifact-reconciler",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run: func(ctx context.Context) error {
				log.Printf("[BOOTSTRAP] artifacts.Reconciler started (4 rules: expired-uploads + staging, orphan-final-blobs, READY-no-blob QUARANTINED, stuck-STAGING; 15m tick)")
				a.Reconciler.Run(ctx, 15*time.Minute)
				return nil
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register artifact-reconciler: %w", err)
		}
	}
	if t.TaskLifecycle != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "taskgraph-dispatcher",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run: func(ctx context.Context) error {
				ticker := time.NewTicker(2 * time.Second)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-ticker.C:
						n, err := t.TaskLifecycle.TickReadiness(ctx, 100)
						if err != nil {
							log.Printf("[TASKGRAPH] TickReadiness error: %v", err)
							return err
						}
						if n > 0 {
							log.Printf("[TASKGRAPH] TickReadiness: %d PENDING→READY", n)
						}
					}
				}
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register taskgraph-dispatcher: %w", err)
		}
	}
	// SPEC §14 follow-up: metrics-supervisor is the periodic 15s
	// tick that stamps the 4 cost-per-output-minute gauges and
	// refreshes master-health gauges (RSS, goroutines, outbox
	// pending). Nil-tolerance: collector, attempts, or outbox
	// missing ⇒ runner NOT registered (master still serves
	// /metrics but skips the supervisor projection — pre-PR-3
	// deploys without the metrics surface fall through cleanly).
	// ── Alert Engine (Step 6 / Velox Metrics Center) ────────────────
	// Evaluates 5 rules every 30s: error_rate, p95_wall_ms, worker
	// offline, disk_free, ffmpeg_speed_ratio. Logs structured alerts
	// and optionally calls Slack/Telegram webhook via env vars.
	if t.Observability != nil {
		alertDeps := alertengine.DefaultRuleDeps()
		alertDeps.Obs = t.Observability
		alertDeps.DataDir = os.Getenv("VELOX_DATA_DIR")
		alertDeps.ErrorRatePct = alertengine.EnvFloat("VELOX_ALERT_ERROR_RATE_PCT", 5.0)
		alertDeps.P95WallMs = int64(alertengine.EnvFloat("VELOX_ALERT_P95_WALL_MS", 300_000))
		alertDeps.DiskFreeGB = alertengine.EnvFloat("VELOX_ALERT_DISK_FREE_GB", 10.0)
		alertDeps.FFmpegMin = alertengine.EnvFloat("VELOX_ALERT_FFMPEG_MIN", 1.5)

		engine := alertengine.New(30*time.Second, alertengine.NewNotifierFromEnv())
		for _, r := range alertengine.MakeRules(alertDeps) {
			engine.AddRule(r)
		}
		if err := sup.Register(supervisor.Runner{
			Name:   "alert-engine",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run:    engine.Run,
		}); err != nil {
			return nil, fmt.Errorf("supervisor register alert-engine: %w", err)
		}
	}

	if metricsCollector != nil && p.SQLite != nil && p.Outbox != nil {
		labelRes := velmetrics.NewSQLiteLabelResolver(p.SQLite.DB())
		costFactors := velmetrics.LoadCostFactorsFromEnv()
		if err := sup.Register(supervisor.Runner{
			Name:   "metrics-supervisor",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run: func(ctx context.Context) error {
				supv := velmetrics.NewSupervisor(metricsCollector, labelRes, p.Outbox, costFactors)
				supv.SetTick(15 * time.Second)
				supv.SetLimit(1000)
				return supv.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register metrics-supervisor: %w", err)
		}
	}

	// ── ClassOneShot ─────────────────────────────────────────────────
	// Manifest auto-generation: fire-and-forget on startup. Failure
	// is non-fatal (logged WARN, always returns nil) so no restart
	// loop is needed even if the manifest endpoint is briefly
	// unreachable.
	if w.UpdateHandler != nil {
		if err := sup.Register(supervisor.Runner{
			Name:  "manifest-generator",
			Class: supervisor.ClassOneShot,
			Run: func(_ context.Context) error {
				if err := w.UpdateHandler.GenerateManifestV2(); err != nil {
					log.Printf("[BOOTSTRAP] Manifest auto-generation skipped: %v", err)
				}
				// Always returns nil — manifest failure is never fatal.
				return nil
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register manifest-generator: %w", err)
		}
	}
	return sup, nil
}
