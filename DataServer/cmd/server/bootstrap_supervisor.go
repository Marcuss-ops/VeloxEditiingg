package main

// Long-lived background runner registration for the master bootstrap.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"velox-server/internal/alertengine"
	runtimealerts "velox-server/internal/alerts"
	"velox-server/internal/config"
	"velox-server/internal/fleet"
	"velox-server/internal/fleet/opsalerts"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/logging"
	velmetrics "velox-server/internal/metrics"
	"velox-server/internal/protectedasset"
	"velox-server/internal/reconcile"
	"velox-server/internal/store"
	"velox-server/internal/supervisor"
	"velox-shared/dispatchable"
)

// registerOpsAlertsSupervisor constructs the fleet alert engine before
// registering its runner. A missing datasource is an explicit DISABLED
// capability: no runner is registered and no error is hidden as a healthy
// service. A non-nil but invalid composition remains a hard bootstrap error.
func registerOpsAlertsSupervisor(sup *supervisor.Supervisor, store opsalerts.AlertStore, source opsalerts.WorkerAlertsDataSource, policy supervisor.RestartPolicy, metricsSink opsalerts.WorkerEvaluationErrorSink, runtimeMetrics ...runtimealerts.ErrorMetrics) (opsalerts.CapabilityStatus, error) {
	if !opsalerts.DataSourceConfigured(source) {
		status := opsalerts.DisabledStatus("worker datasource is not wired")
		logServerf(context.Background(), logging.LevelInfo, logging.CodeServerCapability, "[FLEET-ALERTS] capability=%s: %s; alerts-supervisor and alert routes are disabled", status.State, status.Reason)
		return status, nil
	}
	engine, err := opsalerts.NewEngine(store, source)
	if err != nil {
		if errors.Is(err, opsalerts.ErrDataSourceNotConfigured) {
			status := opsalerts.MisconfiguredStatus(err.Error())
			return status, status.ReadinessError()
		}
		return opsalerts.MisconfiguredStatus(err.Error()), fmt.Errorf("construct alerts engine: %w", err)
	}
	engine.SetErrorMetrics(metricsSink)
	if len(runtimeMetrics) > 0 {
		engine.SetRuntimeErrorMetrics(runtimeMetrics[0])
	}
	if err := sup.Register(supervisor.Runner{
		Name:   "alerts-supervisor",
		Class:  supervisor.ClassRestartable,
		Policy: policy,
		Run:    engine.Run,
	}); err != nil {
		return opsalerts.MisconfiguredStatus(err.Error()), fmt.Errorf("supervisor register alerts-supervisor: %w", err)
	}
	return opsalerts.ReadyStatus(), nil
}

// buildSupervisor registers the long-lived background runners
// using the SupervisedRunner taxonomy introduced in Blocco 1:
//
//   - ClassCritical:    outbox-dispatcher, delivery-runner,
//     creator-forwarding-runner, task-lease-reaper.
//     If any dies the master is dead in the water:
//     VELOX_CRITICAL_MAX_RETRIES bounds the budget
//     (0 = infinite; positive = fail-loud after).
//   - ClassRestartable: artifact-reconciler, taskgraph dispatcher,
//     metrics-supervisor. Bounded retries with
//     backoff; after exhaustion the runner is
//     removed and the supervisor logs WARN.
//   - ClassOneShot:     manifest-generator. Run once on startup;
//     failure is non-fatal (logged WARN).
func buildSupervisor(cfg *config.Config, a *assetDeps, m *moduleDeps, j *jobsDeps, p *persistenceDeps, w *workerDeps, t *taskDeps, metricsCollector *velmetrics.Collector, opsAlertsCapability *opsalerts.CapabilityStatus) (*supervisor.Supervisor, error) {
	sup := supervisor.New()
	if opsAlertsCapability != nil {
		*opsAlertsCapability = opsalerts.DisabledStatus("worker datasource is not wired")
	}

	criticalMaxRetries := cfg.Runtime.Supervisor.CriticalMaxRetries
	criticalFailAfter := cfg.Runtime.Supervisor.CriticalFailAfter
	criticalPolicy := supervisor.RestartPolicy{
		MaxRetries:     criticalMaxRetries,
		InitialBackoff: 1 * time.Second,
		MaxBackoff:     30 * time.Second,
		RestartOnPanic: true,
	}
	if criticalMaxRetries > 0 {
		logServerf(context.Background(), logging.LevelInfo, logging.CodeServerSupervisor, "[SUPERVISOR] critical retry budget: max_retries=%d (fail-loud after that many consecutive failures); fail_after=%d (log-WARN threshold)",
			criticalMaxRetries, criticalFailAfter)
	} else {
		logServerf(context.Background(), logging.LevelInfo, logging.CodeServerSupervisor, "[SUPERVISOR] critical retry budget: infinite (legacy 0=infinite); fail_after=%d (log-WARN threshold)",
			criticalFailAfter)
	}
	restartablePolicy := supervisor.RestartPolicy{
		MaxRetries:     cfg.Runtime.Scheduler.RestartableMaxRetries,
		InitialBackoff: 500 * time.Millisecond,
		MaxBackoff:     30 * time.Second,
		RestartOnPanic: true,
	}

	// Worker cache protection: the snapshot must use the same dispatchable
	// query as the scheduler, and must be installed before routes are
	// registered. Refresh once before exposing the endpoint; until that
	// succeeds the worker-side cleaner remains fail-safe and removes nothing.
	if m != nil && m.Workers != nil && p != nil && p.SQLite != nil {
		env := cfg.Runtime.Cache
		svc := protectedasset.NewService(protectedasset.RepoFunc(func(ctx context.Context, limit int) ([]dispatchable.Job, error) {
			return dispatchable.ListNextDispatchableJobs(ctx, p.SQLite.DB(), limit)
		}), env.ProtectedAssetLookaheadJobs).SetVersionSeed(uint64(time.Now().UnixNano())).WithErrorHandler(func(err error) {
			logServerf(context.Background(), logging.LevelError, logging.CodeServerSupervisorError, "[CACHE-SNAPSHOT] refresh failed: %v", err)
		})
		if err := svc.Refresh(context.Background()); err != nil {
			logServerf(context.Background(), logging.LevelWarn, logging.CodeServerSupervisorWarn, "[CACHE-SNAPSHOT] initial refresh unavailable; worker cleanup remains fail-safe: %v", err)
		} else {
			logServerf(context.Background(), logging.LevelInfo, logging.CodeServerSupervisor, "[CACHE-SNAPSHOT] initial snapshot ready: version=%d protected=%d lookahead=%d", svc.Snapshot().Version, len(svc.Snapshot().ProtectedAssetKeys), env.ProtectedAssetLookaheadJobs)
		}
		m.Workers.SetProtectedAssetsHandler(api.NewProtectedAssetsHandler(svc))
		if err := sup.Register(supervisor.Runner{
			Name:   "protected-asset-snapshot",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run: func(ctx context.Context) error {
				return svc.Run(ctx, env.SnapshotInterval)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register protected-asset-snapshot: %w", err)
		}
	}

	// ── ClassCritical ────────────────────────────────────────────────
	if a.OutboxDispatcher != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "outbox-dispatcher",
			Class:  supervisor.ClassCritical,
			Policy: criticalPolicy,
			Run: func(ctx context.Context) error {
				logServerf(ctx, logging.LevelInfo, logging.CodeServerSupervisor, "[BOOTSTRAP] Outbox dispatcher started — polling outbox_events")
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
				logServerf(ctx, logging.LevelInfo, logging.CodeServerSupervisor, "[BOOTSTRAP] DeliveryRunner started — polling PENDING job_deliveries")
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
				logServerf(ctx, logging.LevelInfo, logging.CodeServerSupervisor, "[BOOTSTRAP] CreatorForwardingRunner started — polling creator_forwardings")
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

	// ── ClassRestartable ─────────────────────────────────────────────
	if a.MediaProbeWorker != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "media-probe-worker",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run: func(ctx context.Context) error {
				logServerf(ctx, logging.LevelInfo, logging.CodeServerSupervisor, "[BOOTSTRAP] media probe worker started (dedicated pool)")
				return a.MediaProbeWorker.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register media-probe-worker: %w", err)
		}
	}
	if a.Reconciler != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "artifact-reconciler",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run: func(ctx context.Context) error {
				logServerf(ctx, logging.LevelInfo, logging.CodeServerSupervisor, "[BOOTSTRAP] artifacts.Reconciler started (4 rules; tick=%s)", cfg.Runtime.Scheduler.ArtifactReconcileInterval)
				a.Reconciler.Run(ctx, cfg.Runtime.Scheduler.ArtifactReconcileInterval)
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
				ticker := time.NewTicker(cfg.Runtime.Scheduler.TaskGraphTick)
				defer ticker.Stop()
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-ticker.C:
						n, err := t.TaskLifecycle.TickReadiness(ctx, 100)
						if err != nil {
							logServerf(ctx, logging.LevelError, logging.CodeServerTaskgraph, "[TASKGRAPH] TickReadiness error: %v", err)
							return err
						}
						if n > 0 {
							logServerf(ctx, logging.LevelInfo, logging.CodeServerTaskgraph, "[TASKGRAPH] TickReadiness: %d PENDING→READY", n)
						}
					}
				}
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register taskgraph-dispatcher: %w", err)
		}
	}
	// ── Canonical reconciliation-supervisor (Phase A3) ──────────────────
	// Runs the ReconciliationRegistry on a wall-clock cadence:
	// AWAITING_ARTIFACT (terminalize jobs stuck with no attempt, artifact
	// or transfer), DELIVERY_PENDING (roll up stuck DELIVERING jobs and
	// fail budget-exhausted deliveries) and WORKER_LOST (partition workers
	// whose heartbeat stream stopped). Every transition is CAS-guarded,
	// idempotent, never touches terminal jobs, and stamps the
	// reconciled_at / reconciliation_reason / reconciliation_version
	// traceability columns. ClassRestartable: a failed tick retries with
	// backoff; the individual reconcilers stay idempotent so a partial
	// pass is safe to re-run.
	if p != nil && p.SQLite != nil {
		registry, err := store.BuildReconciliationRegistry(
			p.SQLite,
			cfg.Workers.StaleThresholdSeconds,
			cfg.Workers.PartitionThresholdSeconds,
			cfg.Runtime.Scheduler.StaleExecutionLimit,
			"master-reconciliation",
		)
		if err != nil {
			return nil, fmt.Errorf("reconciliation-supervisor: %w", err)
		}
		logServerf(context.Background(), logging.LevelInfo, logging.CodeServerSupervisor, "[BOOTSTRAP] reconciliation-supervisor started (entries=%v; tick=%s; stale_limit=%d)", registry.Names(), cfg.Runtime.Scheduler.ReconciliationTick, cfg.Runtime.Scheduler.StaleExecutionLimit)
		if err := sup.Register(supervisor.Runner{
			Name:   "reconciliation-supervisor",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run: func(ctx context.Context) error {
				return reconcile.RunPeriodically(ctx, registry, cfg.Runtime.Scheduler.ReconciliationTick)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register reconciliation-supervisor: %w", err)
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
		alertDeps.DataDir = cfg.Runtime.DataDir
		alertDeps.ErrorRatePct = cfg.Runtime.Alerts.ErrorRatePct
		alertDeps.P95WallMs = cfg.Runtime.Alerts.P95WallMS
		alertDeps.DiskFreeGB = cfg.Runtime.Alerts.DiskFreeGB
		alertDeps.FFmpegMin = cfg.Runtime.Alerts.FFmpegMin

		engine := alertengine.New(cfg.Runtime.Alerts.EvaluationInterval, alertengine.NewNotifier(cfg.Runtime.Alerts.WebhookURL, cfg.Runtime.Alerts.WebhookType))
		engine.SetErrorMetrics(metricsCollector)
		engine.Cooldown = cfg.Runtime.Alerts.Cooldown
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
		costFactors := velmetrics.CostFactorsFromConfig(cfg.Runtime.Metrics)
		if err := sup.Register(supervisor.Runner{
			Name:   "metrics-supervisor",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run: func(ctx context.Context) error {
				supv := velmetrics.NewSupervisor(metricsCollector, labelRes, p.Outbox, costFactors)
				supv.SetTick(cfg.Runtime.Metrics.Tick)
				supv.SetLimit(cfg.Runtime.Metrics.AttemptLimit)
				return supv.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register metrics-supervisor: %w", err)
		}
	}

	// Step 13/15 fleet-operator: 5-minute scheduler that runs
	// fleet.ComputeAndPersistSnapshot to refresh the
	// worker_metrics_snapshots table (migration 105). Distinct
	// from the metrics-supervisor above which handles Prometheus
	// op-level gauges; this is the fleet-side 13-metric rollup
	// refresh.
	//
	// Why ClassRestartable: a failed snapshot tick should retry
	// with backoff (per restartablePolicy) so a transient SQLite
	// lock or schema-migration blip doesn't permanently stall
	// the dashboard's freshness. NEVER ClassCritical: stale
	// snapshots degrade UI quality but not fleet functionality.
	if p != nil && p.SQLite != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "worker-resource-maintenance",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run: func(ctx context.Context) error {
				ticker := time.NewTicker(cfg.Runtime.Scheduler.MetricsSnapshotInterval)
				defer ticker.Stop()
				maintain := func() error {
					if err := p.SQLite.MaintainWorkerResourceSamples(ctx, time.Now().UTC()); err != nil {
						return fmt.Errorf("worker resource maintenance failed: %w", err)
					}
					return nil
				}
				if err := maintain(); err != nil {
					return err
				}
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-ticker.C:
						if err := maintain(); err != nil {
							return err
						}
					}
				}
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register worker-resource-maintenance: %w", err)
		}

		if err := sup.Register(supervisor.Runner{
			Name:   "metrics-snapshot-supervisor",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run: func(ctx context.Context) error {
				ticker := time.NewTicker(cfg.Runtime.Scheduler.MetricsSnapshotInterval)
				defer ticker.Stop()
				logServerf(ctx, logging.LevelInfo, logging.CodeServerMetrics, "[FLEET-METRICS] metrics-snapshot-supervisor started (5min tick; computes 13-metric rollup per worker from worker_metric_samples + fleet_operations + smoke_runs + deployment_records)")
				persist := func() {
					tickCtx, cancel := context.WithTimeout(ctx, fleet.WorkerMetricsSnapshotTickBudget)
					defer cancel()
					ds := fleet.WorkerMetricsAggregatorDataSource{Store: p.SQLite}
					n, err := fleet.ComputeAndPersistSnapshot(tickCtx, ds, time.Now().UTC())
					if err != nil {
						logServerf(ctx, logging.LevelWarn, logging.CodeServerMetricsWarn, "[FLEET-METRICS] snapshot tick partial/failed: persisted=%d err=%v", n, err)
						return
					}
					logServerf(ctx, logging.LevelInfo, logging.CodeServerMetrics, "[FETCH-METRICS] ticked: persisted %d worker snapshots", n)
				}
				persist()
				for {
					select {
					case <-ctx.Done():
						return ctx.Err()
					case <-ticker.C:
						persist()
					}
				}
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register metrics-snapshot-supervisor: %w", err)
		}
	}

	// ── Fleet alerts ────────────────────────────────────────────────
	// The read-side adapter is not wired yet. registerOpsAlertsSupervisor
	// constructs first and therefore leaves this capability absent instead
	// of exposing a healthy-looking runner whose Tick does nothing.
	if p != nil && p.SQLite != nil && m != nil && m.Workers != nil {
		status, err := registerOpsAlertsSupervisor(sup, p.SQLite, nil, restartablePolicy, metricsCollector, metricsCollector)
		if err != nil {
			return nil, err
		}
		if opsAlertsCapability != nil {
			*opsAlertsCapability = status
		}
		logServerf(context.Background(), logging.LevelInfo, logging.CodeServerCapability, "[BOOTSTRAP] opsalerts capability=%s", status.State)
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
					logServerf(context.Background(), logging.LevelWarn, logging.CodeServerSupervisorWarn, "[BOOTSTRAP] Manifest auto-generation skipped: %v", err)
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
