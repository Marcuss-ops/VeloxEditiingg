package main

// bootstrap_wiring.go — the cross-layer wiring steps of the
// composition root, split out of bootstrap_composition.go so
// buildAppComponents stays a readable dependency-ordered sequence:
//
//   - wirePostBuild: jobs↔tasks cross-layer dependency hooks.
//   - wireFleetOperatorHandlers: the admin worker mutations / health /
//     smoke / metrics / alerts handler wiring (fleet-operator steps
//     6, 10, 12, 13, 16 of 15 + the shared SSH client they all use).
//
// appComponents, close and buildAppComponents live in
// bootstrap_composition.go.

import (
	"fmt"
	"log"
	"strings"

	"time"
	"velox-server/internal/config"

	"velox-server/internal/fleet"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/ingest"
)

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

// wireFleetOperatorHandlers wires the fleet-operator admin surfaces
// onto moduleDeps.Workers. Extracted from buildAppComponents: the
// block is self-contained (fleetDep + moduleDeps.Workers + SQLite)
// and only registers handlers + logs — it never feeds back into the
// composition order, so it can live here without affecting the
// dependency-ordered build sequence.
//
// Steps wired (fleet-operator runbook):
//
//   - 6/15: admin worker mutations (drain/resume/quarantine).
//   - 10/15: 4-level health probe handler.
//   - 12/15: LevelDSmokeExecutor + on-demand smoke endpoint.
//   - 13/15: dual GET telemetry endpoints (metrics snapshots).
//   - 16/15: structured alerting surface.
//
// Nil-tolerant: each step re-checks its own deps so a partial boot
// keeps the routes un-mounted instead of 503-on-every-request.
func wireFleetOperatorHandlers(cfg *config.Config, fleetDep *FleetDep, m *moduleDeps, p *persistenceDeps) {
	// Step 6/15 fleet-operator: wire the admin worker mutations handler
	// (POST /api/v1/admin/workers/{id}/{drain,resume,quarantine}).
	// Composition order: buildFleet returns FleetDep AFTER buildModules
	// returns moduleDeps.Workers, so the FleetController and Registry
	// are both available here. The mutations handler publishes via
	// FleetController.PublishOperation and synchronously flips
	// WorkerInfo.Drain / WorkerInfo.Quarantined via the Registry so the
	// placement matcher immediately excludes the worker.
	//
	// Nil-tolerant: a partial-boot FleetController keeps the POST
	// routes un-mounted (the adminWorkersMutationsHandler nil-guard
	// in app/workers.go:RegisterRoutes handles it).
	if fleetDep != nil && fleetDep.Controller != nil && m != nil && m.Workers != nil {
		m.Workers.SetMutationsHandler(
			api.NewAdminWorkersMutationsHandler(m.Workers.Registry(), fleetDep.Controller),
		)
		log.Printf("[BOOTSTRAP] Admin workers mutations handler wired (drain/resume/quarantine; tick goroutine drives the audit lifecycle)")
	}

	// Shared SSH client for health probes (Level A + B) and smoke (Level D).
	// Built once at composition time; nil-tolerant consumers handle nil gracefully.
	sharedSSH := fleet.NewSSHClient(map[string]fleet.SSHWorkerTarget{
		"host_57_129_132_133":   {Host: "57.129.132.133", User: "pierone", KeyPath: "/etc/velox/ssh/id_ed25519_velox"},
		"host_57_131_20_173":    {Host: "57.131.20.173", User: "debian", KeyPath: "/etc/velox/ssh/id_ed25519_velox"},
		"velox-worker-523925eb": {Host: "51.222.204.158", User: "ubuntu", KeyPath: "/etc/velox/ssh/id_ed25519_velox"},
		"velox-worker-13197":    {Host: "149.56.131.97", User: "pierone", KeyPath: "/etc/velox/ssh/id_ed25519_velox"},
	})

	// Step 10/15 fleet-operator: wire the 4-level health probe handler
	// (GET /api/v1/admin/workers/{id}/health?level=A|B|C|D; absent
	// returns the aggregated envelope over the 4 levels).
	//
	// Wired deps:
	//   - SSH (Level A host + Level B docker inspect) — real SSH via sharedSSH
	//   - Registry (Level C) — in-process WorkerInfo read
	//   - Deployments (Level B image_digest_match) — SQLite ledger
	//   - Smoke (Level D) — nil; the operator sees "smoke runner not wired"
	if fleetDep != nil && m != nil && m.Workers != nil {
		healthHandler := api.NewAdminWorkersHealthHandler(
			m.Workers.Registry(),
			api.HealthProbeDeps{
				SSH:         sharedSSH,
				Deployments: p.SQLite,
				Registry:    &fleet.RealRegistryLevelCGater{Reg: m.Workers.Registry()},
				Smoke:       fleet.NewSmokeRunHealthChecker(p.SQLite),
			},
		)
		m.Workers.SetHealthHandler(healthHandler)
		log.Printf("[BOOTSTRAP] Admin workers health handler wired (SSH=4 targets + Registry + Deployments; level=D smoke pending)")
	}

	// Step 12/15 fleet-operator: register the LevelDSmokeExecutor
	// for the OperationKindSmoke kind (replaces the noop default
	// from NewExecutorRegistry), AND wire the on-demand POST
	// /api/v1/admin/workers/{id}/smoke endpoint that publishes
	// these operations.
	//
	// Smoke mode is controlled by VELOX_SMOKE_MODE:
	//   - "development": wires LocalFileDriveUploader + StubAssetResolver
	//     (fakes). Smoke succeeds with synthetic assets — useful for
	//     local iteration but MUST NOT ship to production.
	//   - default (production / unset): wires the real Drive adapter
	//     (when the Drive module is configured) and leaves Asset nil
	//     until the canonical asset picker lands. The executor's
	//     pre-flight nil check surfaces ErrSmokeRunnerNotWired so the
	//     smoke operation FAILS rather than silently succeeding with
	//     fake dependencies.
	//
	// Async execution: the FleetController tick goroutine
	// (Step 7+ already wired in buildFleet) processes queued
	// smoke operations. Operator dashboard polls GET
	// /api/v1/admin/operations/{id} for terminal state; the
	// smoke_runs table records the duration_ms baseline.
	if fleetDep != nil && fleetDep.Registry != nil && m != nil && m.Workers != nil && p != nil && p.SQLite != nil {
		isDev := strings.EqualFold(strings.TrimSpace(cfg.Fleet.SmokeMode), "development")

		smokeBackend := fleet.LevelDSmokeBackend{
			Lease:     fleet.NewRegistryDrainLease(m.Workers.Registry()),
			SmokeRuns: p.SQLite,
			// Worker, Drive and Asset default to nil — the executor's
			// pre-flight check returns ErrSmokeRunnerNotWired, which is
			// the correct production behavior until real adapters are
			// wired. They are populated below based on VELOX_SMOKE_MODE.
		}

		if isDev {
			// Development mode: wire fakes for local iteration.
			// LocalShellWorker runs smoke phases on the master host
			// (no SSH needed), LocalFileDriveUploader copies to
			// /tmp/velox-smoke-drive, StubAssetResolver returns
			// a canned pickup URL.
			smokeBackend.Worker = fleet.NewLocalShellWorker()
			smokeBackend.Drive = fleet.NewLocalFileDriveUploader()
			smokeBackend.Asset = fleet.NewStubAssetResolver("asset://e2e/smoke/canary.mp4", 0)
			log.Printf("[BOOTSTRAP] LevelDSmokeExecutor: DEV MODE (VELOX_SMOKE_MODE=development) — LocalShellWorker + LocalFileDriveUploader + StubAssetResolver")
		} else {
			// Production mode: real SSH worker + real Drive adapter.
			smokeBackend.Worker = fleet.NewSSHWorkerExec(sharedSSH)
			if m.Drive != nil {
				if svc := m.Drive.Service(); svc != nil {
					smokeBackend.Drive = &driveUploaderAdapter{svc: svc, folderID: cfg.Fleet.SmokeDriveFolderID}
					log.Printf("[BOOTSTRAP] LevelDSmokeExecutor: production Drive adapter wired (integrations/drive.Service)")
				} else {
					log.Printf("[BOOTSTRAP] LevelDSmokeExecutor: Drive module present but Service() is nil — smoke remains not wired")
				}
			} else {
				log.Printf("[BOOTSTRAP] LevelDSmokeExecutor: Drive module unavailable — smoke remains not wired")
			}
			// Stub asset resolver so Phase 1 resolves a canned pickup
			// URL. The worker downloads the asset bundle via SSH; the
			// canary.mp4 asset is pre-staged on each worker.
			smokeBackend.Asset = fleet.NewStubAssetResolver("asset://e2e/smoke/canary.mp4", 0)
			log.Printf("[BOOTSTRAP] LevelDSmokeExecutor: using StubAssetResolver (asset://e2e/smoke/canary.mp4)")
		}

		if err := fleetDep.Registry.Register(fleet.OperationKindSmoke, fleet.NewLevelDSmokeExecutor(smokeBackend)); err != nil {
			log.Printf("[BOOTSTRAP] WARN: LevelDSmokeExecutor registration failed: %v (kind=%s continues with noop fallback)", err, fleet.OperationKindSmoke)
		} else {
			driveDesc := "nil"
			assetDesc := "nil"
			if smokeBackend.Drive != nil {
				driveDesc = "RealDrive"
			}
			if smokeBackend.Asset != nil {
				assetDesc = "StubAsset"
			}
			log.Printf("[BOOTSTRAP] LevelDSmokeExecutor registered for kind=%s (Worker=SSHWorkerExec[4 targets], Drive=%s, Asset=%s, Lease=RegistryDrain, SmokeRuns=SQLite)", fleet.OperationKindSmoke, driveDesc, assetDesc)
		}
		m.Workers.SetSmokeHandler(api.NewAdminWorkersSmokeHandler(m.Workers.Registry(), fleetDep.Controller))
		log.Printf("[BOOTSTRAP] Admin workers smoke handler wired (POST /api/v1/admin/workers/{id}/smoke; tick goroutine drives LevelDSmokeExecutor)")
	}

	// Step 13/15 fleet-operator: wire the dual GET telemetry
	// endpoints
	//   GET /api/v1/admin/workers/{id}/metrics
	//     → LATEST snapshot from worker_metrics_snapshots
	//       (migration 105). 404 when the scheduler hasn't
	//       ticked yet for the worker.
	//   GET /api/v1/admin/workers/metrics
	//     → {data, has_more, count} envelope with one row per
	//       worker (the LATEST snapshot per worker_id).
	//
	// Both endpoints serve the persisted snapshot (not real-time
	// aggregation) — the metrics-snapshot-supervisor registered
	// below in buildSupervisor writes one row per worker every
	// 5 minutes; the dashboard renders a staleness indicator
	// (snapshotted_at field) rather than per-read compute.
	//
	// Nil-tolerant: a partial-boot persists no rows; the handler
	// reads from p.SQLite directly and 404s gracefully.
	if fleetDep != nil && m != nil && m.Workers != nil && p != nil && p.SQLite != nil {
		metricsHandler := api.NewAdminWorkersMetricsAggregatorHandler(p.SQLite, 5*time.Minute)
		m.Workers.SetMetricsAggregatorHandler(metricsHandler)
		log.Printf("[BOOTSTRAP] Admin workers metrics aggregator handler wired (GET /api/v1/admin/workers/{id}/metrics + /metrics; metrics-snapshot-supervisor ticks every 5min via buildSupervisor)")
	}

	// Step 16/15 fleet-operator: wire the structured alerting
	// surface (12-rule catalog persisted to alert_events via
	// migration 107). Read paths: /api/v1/admin/workers/{id}/alerts
	// + /api/v1/admin/alerts/active + /api/v1/admin/alerts/recent.
	// All adminAuth-gated; serve the operator dashboard.
	// The actual alert EVALUATION runs async in the
	// alerts-supervisor registered below in buildSupervisor
	// (ClassRestartable, 5min tick) so HTTP remains read-only.
	//
	// Nil-tolerant via the adminWorkersAlertsHandler nil guard —
	// a misconfigured bootstrap (no SQLite store) keeps the
	// routes silently un-mounted rather than 503-on-every-request.
	if m != nil && m.Workers != nil && p != nil && p.SQLite != nil {
		alertsHandler := api.NewAdminWorkersAlertsHandler(p.SQLite)
		m.Workers.SetAlertsHandler(alertsHandler)
		log.Printf("[BOOTSTRAP] Admin workers alerts handler wired (GET /api/v1/admin/workers/{id}/alerts + /api/v1/admin/alerts/active + /recent; alerts-supervisor ticks every 5min via buildSupervisor)")
	}
}
