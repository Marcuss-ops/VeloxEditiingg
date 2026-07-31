package main

// Long-lived background runner registration for the master bootstrap.

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"velox-server/internal/alertengine"
	"velox-server/internal/fleet"
	"velox-server/internal/fleet/opsalerts"
	velmetrics "velox-server/internal/metrics"
	"velox-server/internal/supervisor"
)

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
		sqlDB := p.SQLite.DB()
		if err := sup.Register(supervisor.Runner{
			Name:   "metrics-snapshot-supervisor",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run: func(ctx context.Context) error {
				ticker := time.NewTicker(5 * time.Minute)
				defer ticker.Stop()
				log.Printf("[FLEET-METRICS] metrics-snapshot-supervisor started (5min tick; computes 13-metric rollup per worker from worker_metric_samples + fleet_operations + smoke_runs + deployment_records)")
				persist := func() {
					ds := fleet.WorkerMetricsAggregatorDataSource{
						Store: p.SQLite,
						WorkerIDsFn: func(ctx context.Context) ([]string, error) {
							return fleet.SQLiteWorkerIDs{DB: sqlDB}.WorkerIDs(ctx)
						},
					}
					n, err := fleet.ComputeAndPersistSnapshot(ctx, ds, sqlDB, time.Now().UTC())
					if err != nil {
						log.Printf("[FLEET-METRICS] snapshot tick failed: %v", err)
						return
					}
					log.Printf("[FETCH-METRICS] ticked: persisted %d worker snapshots", n)
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

	// ── ClassOneShot ─────────────────────────────────────────────────
	// Manifest auto-generation: fire-and-forget on startup. Failure
	// is non-fatal (logged WARN, always returns nil) so no restart
	// loop is needed even if the manifest endpoint is briefly
	// unreachable.
	if p != nil && p.SQLite != nil && m != nil && m.Workers != nil {
		if err := sup.Register(supervisor.Runner{
			Name:   "alerts-supervisor",
			Class:  supervisor.ClassRestartable,
			Policy: restartablePolicy,
			Run: func(ctx context.Context) error {
				// Step 16/15 ships the engine with a nil
				// DataSource — the registry API does not yet
				// expose ListAllWorkerIDs / GetWorkerCard so the
				// real adapter lands in Step 17+ with the
				// workersreg surface update. The supervisor
				// still ticks, dedup state machine is wired,
				// alert_events table is persisted, REST
				// endpoints serve the (currently empty) table.
				engine := opsalerts.NewEngine(p.SQLite, nil)
				log.Printf("[FLEET-ALERTS] alerts-supervisor started (5min tick; 12-rule catalog per the user spec; INFO never persisted, WARNING 5min dedup, CRITICAL fires immediately; data source pending Step 17+ workersreg surface)")
				return engine.Run(ctx)
			},
		}); err != nil {
			return nil, fmt.Errorf("supervisor register alerts-supervisor: %w", err)
		}
	}

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
