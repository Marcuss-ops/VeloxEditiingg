# Architecture ownership

This table is the canonical map of "who owns what" — as of June 2026
(post PR-01..PR-15 cutover, PR-07 protocol removal, PR-15 payload V2
single shape, internal/workflow gone, worker-side costmodel gone).

A change that adds a second writer or a parallel entrypoint for any row
below is a regression: the new code will eventually drift and one of the
two paths will lose. The canonical owner is the only place that may
legitimately write or read this capability from outside that owner.

| Responsibility | Canonical owner | Forbidden |
| --- | --- | --- |
| Job business state (status, runs identity) | `internal/jobs` repository + `LifecycleService` | Direct SQL writes from handlers or background jobs |
| Job finalisation (`SUCCEEDED` flip — exclusive gate) | `internal/artifacts.Service` | A second writer that sets `status = 'SUCCEEDED'` outside the service; `maybeTransitionJob → SUCCEEDED` (PR-02 closed it) |
| Asset registry | `internal/assets.ResolverRegistry` | Switch/case trees that pick a resolver by URL scheme |
| Asset upload / canonicalisation | `internal/assets.Service` | Hand-rolled blob persistence from a job handler |
| Configuration (env / file) | `internal/config` (loader + validator) | Sparse `os.Getenv` calls in handlers |
| Worker allowlist | `ValidateProductionWorkers` in `internal/config/workers_validator.go` | Re-implemented ID checks in bootstrap, ansible, or HTTP middlewares |
| Delivery providers | `internal/deliveries.Runner` (plan resolver) | Per-handler router uploads or media forks |
| Outbox event writers | `internal/outbox.Store` | Side-channel INSERTs into `outbox_events` from job/service packages |
| Outbox dispatcher registry | `internal/outbox.Registry` (registered in `cmd/server/bootstrap.go`) | Direct handler invocation from a worker goroutine |
| Worker command acknowledgement | `internal/workers.CommandManager` (registered in `cmd/server/bootstrap.go`) | HTTP fallback routes parallel to gRPC |
| Persistent state | SQLite via repository layer | JSON files or in-memory maps treated as authoritative |
| Binary storage | Filesystem / blob storage | Blobs persisted inside the DB |
| Versioning | `/VERSION.txt` (single root file) | CI fallback to `git describe`, `dev`, local snapshots |
| Worker ID minting | `internal/workers.Registry` | Random IDs generated from request payloads |
| Audit logging | `internal/audit/data_layer` | Free-form `log.Printf` calls for events that the auditor must observe |
| Migrations | Canonical SQL files + migration registration in `cmd/server/bootstrap.go` | Programmatic `CREATE TABLE IF NOT EXISTS` outside the migration registry |
| Task scheduling state (status, attempts, revision, lease expiry) | `internal/taskgraph` repository + `LifecycleService` | Direct SQL writes from handlers or background jobs; `internal/obs`; re-introducing `internal/obs` as a parallel state owner |
| Task execution attempt (status, reports, metrics, phase timing) | `internal/taskattempts` repository | Direct SQL writes from handlers or background jobs |
| Task execution report ingestion | `internal/taskattempts` repository | Duplicate report writers or side-channel INSERTs (PR-06 verified as no-op — typed `TaskResult` is already fully consumed in `handler_jobs.go`) |
| Task observability / diagnostics | `internal/observability` (read-only aggregation) | Direct SQL aggregation from handlers; mutable state in observability package |
| Task phase metrics | `internal/taskattempts` (`PhaseTiming` + `AttemptMetrics` tables) | Free-form phase identifiers; JSON-only metric storage |
| Atomic Job+Task creation | `internal/store.AtomicJobTaskCreator` | Non-atomic Job or Task creation paths; handlers writing task SQL directly; reintroducing a `JobQueue.SubmitJob` style write path |
| Executor registry (worker-side capability catalog) | `internal/executor.Registry` (composition root registers executors in `cmd/velox-worker-agent/main.go`) | Hardcoded capability booleans on the worker hello; a second worker-side map keyed on executor ID; per-job-type switch arms inside `internal/worker/runJobTask` |
| Worker dispatch (per-task executor lifecycle) | `internal/taskrunner.TaskRunner` (worker only invokes it via `worker.dispatchTaskRunner`) | Direct calls to `pipeline.Runner.Run` / `video.*` from inside `internal/worker`; legacy `executeWorkflowJob`/`runRenderJob`/`runVideoJob`/`runAudioJob` helpers; reverting to a second dispatch map keyed on `job.JobType` |
| Resource sampler (worker-side limits snapshot) | `internal/resource` sampler wired through `taskrunner.WithResources` | Inline `runtime.NumCPU` / `os.Stat` reads in executor implementations; per-task resource lookups that bypass the sampler |
| Persistent local artifact cache (worker-side) | `pkg/cache.PersistedLocalCache` (composition root constructs it in `cmd/velox-worker-agent/main.go`) | Hand-rolled `os.WriteFile` cache management inside executors; duplicated content-addressed lookups outside `PersistedLocalCache.Get` |
| Content-addressed blob artifact store (worker-side) | `pkg/blob.BlobArtifacts` (composition root constructs it in `cmd/velox-worker-agent/main.go`) | Inline `os.Open` for blob reads from executors; parallel upload paths that bypass `BlobArtifacts.Put` |
| Scene/composite render adapter (worker-side) | `internal/taskrunner/executors.SceneComposite` (constructed in `cmd/velox-worker-agent/main.go`, registered under `scene.composite.v1@1`) | Reimplementing the scene-composite logic inside `pkg/video` callers; calling `pipeline.Runner.Run` from worker orchestration |
| Cost-aware worker eligibility + score breakdown (PR-04.4/4.5) | `velox-server/internal/costmodel` — **master-only, single owner**. Worker-side mirror was deleted (CONFIRMED §3.6 in audit). | Hardcoded boolean-AND filters (`Schedulable && !Drain && Status != "offline"` etc.) inside `velox-server/internal/workers`; per-job-type placement switch arms inside that package; per-package string-list allowlists (`supported_job_types`) that bypass the four canonical Descriptor fields (`ResourceClass`, `TemporalMode`, `Deterministic`, `Cacheable`) |
| Per-job `costmodel.JobRequirements` threading (PR-04.5 / closed) | `velox-server/internal/costmodel.JobRequirements` is the single source of truth. Read path: `jobs.Writer.Get → jobs.Job.Requirements → jobs.QueueItem.Requirements`. Write path: `Enqueuer.Enqueue → AtomicJobTaskCreator.CreateJobWithTask` → dedicated columns `job_required_resource_class`, `job_required_temporal_mode`, `job_required_deterministic`, `job_required_cacheable`, `job_required_min_bandwidth_mbps`. `_requirements` JSON sub-object mirror: **REMOVED** (PR-06 / §3.5 audit). `RequestResult` lease fields: **REMOVED** (PR-09 / migration 048). | Hand-decoded Requirements at dispatch sites (`grpcserver`, `workers`, `outbox`); second `JobRequirements` type aliases; bypass of the canonical `costmodel.JobRequirements` shape; re-introducing a `_requirements` JSON sub-object under `request_json` / `result_json`. The default `costmodel.DefaultRequirements()` is the only permissive fallback and MUST stay zero-value so empty rows remain claimable. |
| **Payload V2 single shape** (`contract_version=2` typed envelope, canonical for any `process_video` writer) | `shared/contract.JobPayloadV2` + `NewJobPayloadV2(raw)` + `ToMap()` projection. Used by `internal/jobs/enqueue/{enqueue.go,enqueue_scene_image.go}`; structurally followed by `internal/handlers/server/{calendar/calendar_payload.go, smoke/smoke_clip_stock.go}` for canonical-only top-level keys. | `parameters` sub-map mirror writes (gone since PR-15); legacy alias writes `id`/`run_id`/`title`/`voiceover_path`/`audio_path` from canonical writers; raw-key-leak loops that copy from input without alias stripping (`for k, v := range rawPayload { normalized[k] = v }` must be paired with an explicit `delete(normalized, alias)` set before construction); `map[string]interface{}` for `parameters` in canonical writes — the typed envelope + `ToMap` is canonical |
| `WorkerToMaster` envelope dispatch (master side) | `internal/grpcserver/handler.go` dispatcher switch → typed `WorkerToMasterEnvelope_*` arms. Single source of truth for protocol-side routing. Legacy Job arms (`LeaseRenewal`, `JobAccepted`, `JobRejected`, `JobProgress`, `JobResult`) were declared `reserved` in `proto/velox/control/worker_control.proto` (PR-07) and the handlers removed. | New `case *pb.WorkerToMasterEnvelope_LeaseRenewal:` / `_JobAccepted:` / `_JobRejected:` / `_JobProgress:` / `_JobResult:` arms — they cannot compile (types reserved-but-missing). Reintroducing a separate worker-message router parallel to `handler.go`. |
| Legacy pool (`internal/queue` — deleted) | **REMOVED**: `internal/queue` has been deleted. `LifecycleService` lives at `internal/jobs`. | Reintroducing `internal/queue`, `queue.Job`, `queue.QueueItem`, `queue.JobStatus`, or `*queue.FileQueue` |

## Removed packages (historical, retained for traceability)

These packages existed at some point in the codebase and have been
deleted. The doc retains them in this footer so newcomers don't try to
re-introduce them after finding them in older commits, branches, or
documentation.

| Package | Removed by | Replacement owner |
| --- | --- | --- |
| `DataServer/internal/workflow` (run/step/dependency state, `CreateRun`/`MarkStepRunning`/...) | PR-07 protocol-removal line of cutover + Fase 4a-c drain | `internal/taskgraph` (Task state) + `internal/taskattempts` (Attempt state) |
| `RemoteCodex/native/worker-agent-go/internal/costmodel` (worker-side scoring mirror) | PR-04 cleanup (audit §3.6 — `Duplicata in due` resolved as DELETION not mirroring) | master `velox-server/internal/costmodel` is the single scoring owner; worker advertises `WorkerProfile` only |
| `DataServer/internal/queue` (file-backed JobQueue) | PR-03 collapser into `internal/jobs.LifecycleService` | `internal/jobs.LifecycleService` + `internal/store.AtomicJobTaskCreator` |
| `DataServer/internal/obs` (early observability placeholder) | superseded by `internal/observability` (canonical aggregator) | `internal/observability` |

## The single-writer rule

Every important state must have exactly one writer and exactly one entry
point. The shape we want everywhere is:

```
   HTTP / gRPC API
        ↓
   Application Service
        ↓
   Repository
        ↓
   SQLite
```

What we explicitly forbid:

```
   Handler    ─────────► SQLite
   Service    ─────────► SQLite
   Background ─────────► JSON
   Other      ─────────► RAM
```

If a contributor is tempted to skip the service/repo layer, the answer
is always: extend the canonical owner. Adding a side path is a regression
by definition.

## Compatibility shims

A temporary adapter that lets old callers keep working while they
migrate MUST carry the following block at the top of the file (Go):

```go
// COMPATIBILITY:
// Owner:        issue #NNN
// Remove after: 2026-09-30
// Read-only:    yes
```

Rules:

- Compatibility allowed **only on the read path**.
- Never two write paths.
- Never dual-write.
- Never silent fallback.
- Issue number is mandatory.
- Removal date is mandatory.
- A CI check enforces the deadline: after the date, the build fails.

## How to add a new responsibility

1. Identify the canonical owner in this table (or extend the table with
   a new row before writing the code).
2. Update `.github/CODEOWNERS` so reviewers are auto-assigned.
3. Wire the new path into `cmd/server/bootstrap.go` (the composition
   root) — do not let it self-discover.
4. Add a `check-single-writer.sh` rule: assert that the symbol that
   performs the canonical mutation is the only one.
5. Add an invariant test (not a behaviour test) that fails if anyone
   attempts the forbidden pattern.
