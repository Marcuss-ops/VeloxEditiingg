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
	"context"
	"fmt"
	"net/url"
	"strings"

	"time"
	voiceoverassets "velox-server/internal/assets"
	"velox-server/internal/config"

	"velox-server/internal/fleet"
	"velox-server/internal/handlers/server/api"
	"velox-server/internal/ingest"
	"velox-server/internal/logging"
	"velox-server/internal/store"
	"velox-server/internal/taskgraph"
)

// productionAssetResolver adapts the canonical AssetService read model to
// the Level-D smoke resolver contract. Workers fetch the bytes through the
// existing authenticated /api/v1/agent/assets/:asset_id route; no second
// asset store or synthetic production asset is introduced.
type productionAssetResolver struct {
	service *voiceoverassets.AssetService
	baseURL string
	token   string
}

func newProductionAssetResolver(service *voiceoverassets.AssetService, baseURL, token string) (*productionAssetResolver, error) {
	baseURL = strings.TrimRight(strings.TrimSpace(baseURL), "/")
	if service == nil {
		return nil, fmt.Errorf("asset service is unavailable")
	}
	parsed, err := url.Parse(baseURL)
	if err != nil || parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("invalid asset pickup base URL %q", baseURL)
	}
	if strings.TrimSpace(token) == "" {
		return nil, fmt.Errorf("asset pickup token is unavailable")
	}
	return &productionAssetResolver{service: service, baseURL: baseURL, token: token}, nil
}

func (r *productionAssetResolver) ResolveAsset(ctx context.Context, assetID string) (string, int64, error) {
	if r == nil || r.service == nil {
		return "", 0, fmt.Errorf("asset resolver is unavailable")
	}
	assetID = strings.TrimSpace(assetID)
	if assetID == "" || strings.ContainsAny(assetID, `/\\`) {
		return "", 0, fmt.Errorf("invalid asset id")
	}
	asset, err := r.service.Get(ctx, assetID)
	if err != nil {
		return "", 0, fmt.Errorf("lookup asset %s: %w", assetID, err)
	}
	if asset == nil {
		return "", 0, fmt.Errorf("asset %s not found", assetID)
	}
	if asset.Status != voiceoverassets.AssetStatusReady {
		return "", 0, fmt.Errorf("asset %s is %s, want READY", assetID, asset.Status)
	}
	pickup, err := url.Parse(r.baseURL + "/api/v1/agent/assets/" + url.PathEscape(assetID))
	if err != nil {
		return "", 0, fmt.Errorf("build asset pickup URL: %w", err)
	}
	query := pickup.Query()
	query.Set("token", r.token)
	pickup.RawQuery = query.Encode()
	return pickup.String(), asset.SizeBytes, nil
}

// taskgraphJobsRetryQuerier adapts the concrete *store.SQLiteJobRepository
// onto the narrow taskgraph.JobsRetryQuerier contract. taskgraph must not
// import the jobs package (jobs/enqueue imports taskgraph, so importing jobs
// back would recreate the jobs↔taskgraph directory cycle); this adapter
// lives at the composition root — where both domains are already in scope —
// and projects only the retry-budget fields the lease reaper needs.
type taskgraphJobsRetryQuerier struct {
	jobs *store.SQLiteJobRepository
}

func (a *taskgraphJobsRetryQuerier) Get(ctx context.Context, id string) (*taskgraph.JobRetryView, error) {
	job, err := a.jobs.Get(ctx, id)
	if err != nil {
		return nil, err
	}
	if job == nil {
		return nil, nil
	}
	return &taskgraph.JobRetryView{
		MaxRetries: job.MaxRetries,
		Terminal:   job.Status.IsTerminal(),
	}, nil
}

func (a *taskgraphJobsRetryQuerier) Fail(ctx context.Context, id, reason string) error {
	return a.jobs.Fail(ctx, id, reason)
}

// wirePostBuild connects dependencies that cross build-layer
// boundaries (jobs↔tasks). Called by both buildTestDeps (tests)
// and buildAppComponents (production) so the wiring stays canonical
// in exactly one place.
func wirePostBuild(j *jobsDeps, t *taskDeps) error {
	// fix/remove-job-lease-ops: j.SQLiteRepo (concrete
	// *SQLiteJobRepository) is adapted onto taskgraph.JobsRetryQuerier
	// through taskgraphJobsRetryQuerier, which projects the narrow
	// retry-budget view (MaxRetries + terminal state) without pulling
	// the jobs package into taskgraph.
	if j != nil && j.SQLiteRepo != nil && t != nil && t.TaskLifecycle != nil {
		t.TaskLifecycle.SetJobsRepo(&taskgraphJobsRetryQuerier{jobs: j.SQLiteRepo})
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
func wireFleetOperatorHandlers(cfg *config.Config, fleetDep *FleetDep, m *moduleDeps, p *persistenceDeps, workerNodeRegistry *fleet.WorkerRegistry, sharedSSH fleet.BackendSSHClient, smokeCapability *fleet.SmokeCapabilityStatus) error {
	if smokeCapability != nil {
		*smokeCapability = fleet.DisabledSmokeCapability("real asset resolver is not wired")
	}
	// Step 6/15 fleet-operator: wire the admin worker mutations handler
	// (POST /api/v1/admin/workers/{id}/{drain,resume,quarantine}).
	// Composition order: buildFleet returns FleetDep AFTER buildModules
	// returns moduleDeps.Workers, so the FleetController and Registry
	// are both available here. The mutations handler publishes via
	// FleetController.PublishOperation and synchronously flips
	// Worker.Drain / Worker.Quarantined via the Registry so the
	// placement matcher immediately excludes the worker.
	//
	// Nil-tolerant: a partial-boot FleetController keeps the POST
	// routes un-mounted (the adminWorkersMutationsHandler nil-guard
	// in app/workers.go:RegisterRoutes handles it).
	if fleetDep != nil && fleetDep.Controller != nil && m != nil && m.Workers != nil {
		mutationsHandler := api.NewAdminWorkersMutationsHandler(m.Workers.Registry(), fleetDep.Controller)
		// Fail-closed update gate (AZIONE 2): POST /update refuses 503
		// while any critical UpdateExecutor backend is missing, instead
		// of accepting an operation that fails 30s after the POST. The
		// closure reads the LIVE executor capability at request time,
		// so it automatically reflects backends attached later in this
		// function (fresh Level-D smoke + Drive verifier) once wiring
		// completes. Drain/resume/quarantine are intentionally not
		// gated — they have no UpdateExecutor dependency.
		if smokeCapability != nil {
			mutationsHandler.SetResumeGate(func() error {
				if smokeCapability.State == fleet.SmokeCapabilityReady {
					return nil
				}
				return fmt.Errorf("Level-D smoke capability %s: %s", smokeCapability.State, smokeCapability.Reason)
			})
		}
		if fleetDep.Update != nil {
			mutationsHandler.SetUpdateGate(func() error {
				if fleetDep.Update.Ready() {
					return nil
				}
				capability := fleetDep.Update.Capability()
				return fmt.Errorf("missing: %s", strings.Join(capability.Missing, ", "))
			})
		}
		m.Workers.SetMutationsHandler(mutationsHandler)
		logServerf(context.Background(), logging.LevelInfo, logging.CodeServerBootstrap, "[BOOTSTRAP] Admin workers mutations handler wired (drain/resume/quarantine; update gated on UpdateExecutor capability; tick goroutine drives the audit lifecycle)")
	}

	// Shared SSH client for health probes (Level A + B) and smoke (Level D).
	// Built once at composition time from the canonical WorkerNodeRegistry
	// (persistent ansible_hosts worker-node view — Phase 9). The registry is
	// populated by buildWorkerRegistryFromStore; the SSH client derives its
	// targets from it. There is intentionally NO hardcoded target map here:
	// an unseeded inventory fails per-target at Run time with a clear error.
	if workerNodeRegistry == nil || sharedSSH == nil {
		return fmt.Errorf("canonical WorkerNodeRegistry/sharedSSH missing")
	}

	// Step 17/15 fleet-operator: SSH connectivity diagnostic.
	// GET /api/v1/admin/workers/ssh-check — one row per worker in the
	// canonical WorkerNodeRegistry (ssh / hostkey / sudo -n). Reads host,
	// port, user from the registry (never a hardcoded map) and probes with
	// the canonical /etc/velox/ssh key + known_hosts. Nil-tolerant.
	if workerNodeRegistry != nil && m != nil && m.Workers != nil && workerNodeRegistry.Len() >= 0 {
		sshCheckHandler := api.NewAdminWorkersSSHCheckHandler(workerNodeRegistry, api.SSHCheckDeps{
			ResolveWorkerName: workerNameResolverFromStore(p),
		})
		m.Workers.SetSSHCheckHandler(sshCheckHandler)
		logServerf(context.Background(), logging.LevelInfo, logging.CodeServerBootstrap, "[BOOTSTRAP] Admin workers ssh-check handler wired (GET /api/v1/admin/workers/ssh-check; key=%s known_hosts=%s)", fleet.DefaultSSHKeyPath, fleet.DefaultKnownHostsPath)
	}

	// Step 10/15 fleet-operator: wire the 4-level health probe handler
	// (GET /api/v1/admin/workers/{id}/health?level=A|B|C|D; absent
	// returns the aggregated envelope over the 4 levels).
	//
	// Wired deps:
	//   - SSH (Level A host + Level B docker inspect) — real SSH via sharedSSH
	//   - Registry (Level C) — in-process Worker read
	//   - Deployments (Level B image_digest_match) — SQLite ledger
	//   - Smoke (Level D) — nil; the operator sees "smoke runner not wired"
	if fleetDep != nil && m != nil && m.Workers != nil {
		// Admin card read seams: the current-state digest fields
		// (desired/running/last_successful) come from the durable
		// worker_deployment_state read model; deployment_records remains
		// the operation-history journal (PreviousDigest + last op row).
		m.Workers.SetDeploymentReader(p.SQLite)
		m.Workers.SetOperationLedgerReader(p.SQLite)
		m.Workers.SetWorkerDeploymentStateReader(p.SQLite)
		healthHandler := api.NewAdminWorkersHealthHandler(
			m.Workers.Registry(),
			api.HealthProbeDeps{
				SSH: sharedSSH,
				// The named adapter keeps the deployment_records surface
				// distinct from SQLiteStore's fleet_operations methods.
				Deployments: store.NewDeploymentRecordRepository(p.SQLite),
				Registry:    &fleet.RealRegistryLevelCGater{Reg: m.Workers.Registry()},
				Smoke:       fleet.NewSmokeRunHealthChecker(p.SQLite),
			},
		)
		m.Workers.SetHealthHandler(healthHandler)
		logServerf(context.Background(), logging.LevelInfo, logging.CodeServerBootstrap, "[BOOTSTRAP] Admin workers health handler wired (SSH=%d targets from WorkerNodeRegistry + Registry + Deployments; level=D smoke pending)", workerNodeRegistry.Len())
	}

	// Step 12/15 fleet-operator: register the concrete LevelDSmokeExecutor
	// for the OperationKindSmoke kind (the production registry has no
	// noop fallback), AND wire the on-demand POST
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
		environment := strings.ToLower(strings.TrimSpace(cfg.Runtime.Environment))
		isDev := strings.EqualFold(strings.TrimSpace(cfg.Fleet.SmokeMode), "development") && environment == "development"

		smokeBackend := fleet.LevelDSmokeBackend{
			Lease:     fleet.NewRegistryDrainLease(m.Workers.Registry()),
			SmokeRuns: p.SQLite,
			Verifier:  fleet.NewFFprobeArtifactVerifier(),
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
			logServerf(context.Background(), logging.LevelInfo, logging.CodeServerSmoke, "[BOOTSTRAP] LevelDSmokeExecutor: DEV MODE (VELOX_SMOKE_MODE=development) — LocalShellWorker + LocalFileDriveUploader + StubAssetResolver")
		} else {
			// Production mode: real SSH worker + real Drive adapter.
			smokeBackend.Worker = fleet.NewSSHWorkerExec(sharedSSH)
			if m.Drive != nil {
				if svc := m.Drive.Service(); svc != nil {
					smokeBackend.Drive = &driveUploaderAdapter{svc: svc, folderID: cfg.Fleet.SmokeDriveFolderID}
					logServerf(context.Background(), logging.LevelInfo, logging.CodeServerSmoke, "[BOOTSTRAP] LevelDSmokeExecutor: production Drive adapter wired (integrations/drive.Service)")
				} else {
					logServerf(context.Background(), logging.LevelWarn, logging.CodeServerSmoke, "[BOOTSTRAP] LevelDSmokeExecutor: Drive module present but Service() is nil — smoke remains not wired")
				}
			} else {
				logServerf(context.Background(), logging.LevelWarn, logging.CodeServerSmoke, "[BOOTSTRAP] LevelDSmokeExecutor: Drive module unavailable — smoke remains not wired")
			}
			// Resolve smoke assets through the existing worker-authenticated
			// asset route. Use a real registered worker as the session
			// principal; the route validates the session token and the
			// resolver reads the canonical AssetService projection.
			entries := workerNodeRegistry.ListWorkers()
			if m.AssetService != nil && len(entries) > 0 && m.Workers != nil {
				token := m.Workers.IssueAssetPickupToken(entries[0].WorkerID.String())
				resolver, resolverErr := newProductionAssetResolver(
					m.AssetService,
					string(cfg.ControlPlane.RESTPublic),
					token,
				)
				if resolverErr != nil {
					logServerf(context.Background(), logging.LevelWarn, logging.CodeServerSmoke, "[BOOTSTRAP] LevelDSmokeExecutor: production asset resolver unavailable: %v", resolverErr)
				} else {
					smokeBackend.Asset = resolver
					logServerf(context.Background(), logging.LevelInfo, logging.CodeServerSmoke, "[BOOTSTRAP] LevelDSmokeExecutor: canonical AssetService resolver wired through /api/v1/agent/assets")
				}
			} else {
				logServerf(context.Background(), logging.LevelWarn, logging.CodeServerSmoke, "[BOOTSTRAP] LevelDSmokeExecutor: canonical asset resolver dependencies unavailable (asset service, worker registry, or token manager)")
			}
		}

		levelDSmokeExecutor := fleet.NewLevelDSmokeExecutor(smokeBackend)
		status, err := fleet.ConfigureLevelDSmokeCapability(fleetDep.Registry, levelDSmokeExecutor, isDev)
		if err != nil {
			return err
		}
		if smokeCapability != nil {
			*smokeCapability = status
		}
		if status.State == fleet.SmokeCapabilityReady {
			logServerf(context.Background(), logging.LevelInfo, logging.CodeServerSmoke, "[BOOTSTRAP] LevelDSmokeExecutor capability=READY (Worker=SSHWorkerExec[%d targets], Drive and asset resolver configured)", workerNodeRegistry.Len())
			m.Workers.SetSmokeHandler(api.NewAdminWorkersSmokeHandler(m.Workers.Registry(), fleetDep.Controller))
			if err := fleetDep.Registry.Register(fleet.OperationKindResume, fleet.NewResumeExecutor(fleet.ResumeBackend{
				Registry:      m.Workers.Registry(),
				SmokeExecutor: levelDSmokeExecutor,
				SmokeAssetID:  cfg.Fleet.SmokeAssetID,
			})); err != nil {
				return fmt.Errorf("register ResumeExecutor: %w", err)
			}
			logServerf(context.Background(), logging.LevelInfo, logging.CodeServerCapability, "[BOOTSTRAP] ResumeExecutor registered for kind=%s", fleet.OperationKindResume)
		} else {
			logServerf(context.Background(), logging.LevelWarn, logging.CodeServerSmoke, "[BOOTSTRAP] LevelDSmokeExecutor capability=%s: %s; smoke and resume are not registered", status.State, status.Reason)
		}
		if fleetDep.Update != nil {
			// Worker image rollout is deliberately independent from the
			// socialediting Drive publisher. Its gate performs a real local
			// ffmpeg render over SSH and checks authenticated readiness above.
			if err := fleetDep.Update.AttachRuntimeBackends(fleet.NewWorkerUpdateSmokeRunner(sharedSSH), nil); err != nil {
				return fmt.Errorf("update runtime backends: %w", err)
			}
			logServerf(context.Background(), logging.LevelInfo, logging.CodeServerCapability, "[BOOTSTRAP] UpdateExecutor worker-local smoke wired (Drive-independent)")
		}
		if status.State == fleet.SmokeCapabilityReady {
			logServerf(context.Background(), logging.LevelInfo, logging.CodeServerSmoke, "[BOOTSTRAP] Admin workers smoke handler wired (POST /api/v1/admin/workers/{id}/smoke; tick goroutine drives LevelDSmokeExecutor)")
		}
	}

	// Step 13/15 fleet-operator: wire the per-worker GET telemetry
	// endpoint
	//   GET /api/v1/admin/workers/{id}/metrics
	//     → LATEST snapshot from worker_metrics_snapshots
	//       (migration 105). 404 when the scheduler hasn't
	//       ticked yet for the worker.
	// The fleet-wide aggregate lives at GET /api/v1/fleet/metrics
	// (the canonical Phase 6 fleet namespace); the legacy
	// /api/v1/admin/workers/metrics alias was removed.
	//
	// Both serve the persisted snapshot (not real-time
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
		logServerf(context.Background(), logging.LevelInfo, logging.CodeServerBootstrap, "[BOOTSTRAP] Admin workers metrics aggregator handler wired (GET /api/v1/admin/workers/{id}/metrics; fleet aggregate at GET /api/v1/fleet/metrics; metrics-snapshot-supervisor ticks every 5min via buildSupervisor)")
	}
	// Fleet structured alerting is intentionally DISABLED until a real
	// WorkerAlertsDataSource is composed. Do not mount the persisted-alert
	// read routes while evaluation is absent: an empty alert ledger must not
	// look like a healthy alerting service.
	// AZIONE 2 — fail-closed boot verdict. All critical backends were
	// verified above (SSH/Docker/Cosign/Registry wired in buildFleet;
	// Smoke/Drive attached here). Instead of hard-failing the master
	// (which would hide the failure mode behind a crash-loop), the
	// verdict is EXPOSED: the operator log names the missing deps, the
	// /ready update-capability probe flips red, and POST /update
	// refuses with 503. The failure is closed at the boot boundary —
	// no "docker client not wired" discovered 30s after a POST.
	if fleetDep != nil && fleetDep.Update != nil {
		capability := fleetDep.Update.Capability()
		if capability.Ready {
			logServerf(context.Background(), logging.LevelInfo, logging.CodeServerCapability, "[BOOTSTRAP] Update capability READY: ssh docker deployments cosign image registry smoke drive all wired")
		} else {
			logServerf(context.Background(), logging.LevelWarn, logging.CodeServerCapabilityWarn, "[BOOTSTRAP] Update capability NOT READY: missing %s — POST /api/v1/admin/workers/{id}/update refuses with 503 and the /ready update-capability probe is red (fail-closed at boot instead of surfacing mid-update)", strings.Join(capability.Missing, ", "))
		}
	}
	return nil
}
