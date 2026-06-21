# PR 1 - Canonical tasks and execution telemetry

## Metadata

Title: `feat(tasks): add canonical task lifecycle and execution reports`

Branch: `codex/task-contracts-observability`

Depends on: current `origin/main` passing the baseline verification gate.

## Current state to reuse

Already present:

- canonical `jobs.Job`, repository, revision and lease fields;
- SQLite repositories and migrations;
- outbox and artifact finalization;
- gRPC-only WorkerControl stream;
- worker job telemetry;
- background supervisor;
- worker RenderPlan/pipeline execution.

Not present:

```text
internal/taskgraph
internal/taskattempts
internal/observability
TaskExecutionReport transport
task/attempt migrations
phase-level metrics
job -> initial task integration
```

Do not create `internal/obs`. Do not copy the stale `internal/obs/models.go` prototype from older branches.

## Goal

Introduce one canonical Task domain and one canonical TaskAttempt domain without changing visual output or worker placement.

At merge:

```text
one current Job
  -> one persistent Task
  -> one persistent TaskAttempt per execution
  -> one typed final TaskExecutionReport
  -> existing verified artifact finalization
```

This PR does not introduce multiple tasks per job, dependency scheduling or temporal sharding.

## Package ownership

```text
DataServer/internal/taskgraph
  Task model, validation, lifecycle, repository interfaces.

DataServer/internal/taskattempts
  Attempt model, report ingestion, repository interfaces.

DataServer/internal/observability
  Read-only aggregation and diagnostics.

DataServer/internal/store
  SQLite implementations and cross-domain transaction coordinator.

proto/velox/control + shared/controltransport
  Versioned worker transport contract.

RemoteCodex/.../internal/telemetry
  Monotonic local phase timing and resource counters.
```

No other package may mutate task or attempt state.

## Canonical Task contract

Minimum semantic fields:

```go
type Task struct {
    ID              string
    JobID           string
    ProjectID       string
    ExecutionPlanID string
    ExecutorID      string
    ExecutorVersion int
    Status          Status
    Priority        int
    Revision        int
    AttemptCount    int
    WorkerID        string
    LeaseID         string
    ReadyAt         *time.Time
    StartedAt       *time.Time
    CompletedAt     *time.Time
    CreatedAt       time.Time
    UpdatedAt       time.Time
}
```

`ExecutionPlanID` may be empty only for the temporary one-task bootstrap created in this PR. PR 3 makes it mandatory when persistent multi-task plans are introduced.

Initial statuses:

```text
PENDING
READY
LEASED
RUNNING
SUCCEEDED
FAILED
CANCELLED
```

Rules:

- identity and executor selection are immutable after publication;
- status changes only through `taskgraph.LifecycleService`;
- every mutation uses optimistic revision checks;
- lease ownership is checked on worker-originated changes;
- the Task never stores arbitrary unvalidated maps as its canonical spec.

A typed versioned TaskSpec may be serialized for transport/storage, but it must be validated before persistence and accompanied by a deterministic `spec_hash`. Query-critical fields remain explicit columns.

## Canonical TaskAttempt contract

Minimum fields:

```go
type TaskAttempt struct {
    ID              string
    TaskID          string
    AttemptNumber   int
    WorkerID        string
    LeaseID         string
    Status          AttemptStatus
    StartedAt       *time.Time
    CompletedAt     *time.Time
    ErrorCode       string
    ErrorMessage    string
    ReportVersion   int
    CreatedAt       time.Time
    UpdatedAt       time.Time
}
```

Attempt status:

```text
PENDING
RUNNING
SUCCEEDED
FAILED
CANCELLED
```

Rules:

- unique `(task_id, attempt_number)`;
- at most one active attempt for a task lease;
- duplicate final reports are idempotent;
- stale worker/lease reports are rejected;
- an attempt report cannot mark the parent Job successful.

## Canonical telemetry

Production phase names are fixed:

```text
queue
asset_wait
cache_lookup
download
decode
compile
simulate
render
composite
encode
upload
finalize
```

Worker durations are measured using a monotonic local clock. Do not calculate phase durations from timestamps produced on different hosts.

Store typed metrics:

```text
phase duration_ms
input_bytes
output_bytes
bytes_from_drive
bytes_from_blobstore
bytes_from_local_cache
cpu_time_ms
gpu_time_ms
peak_rss_bytes
peak_vram_bytes
```

GPU fields remain optional/zero on CPU-only workers.

## Database shape

Use forward-only migrations.

Required tables:

```text
tasks
task_specs
task_attempts
task_phase_timings
task_attempt_metrics
```

`tasks` contains operational/queryable state.

`task_specs` contains validated immutable versioned serialization plus `spec_hash`; it is not an opaque unvalidated parameter dump.

`task_phase_timings` contains one row per canonical phase and attempt.

`task_attempt_metrics` contains explicit typed counters. A JSON blob may be retained only as supplementary/debug data, never as the sole source.

## Operational TODO

### PR 1.0 - Clean baseline

- [ ] `git fetch origin` and start from updated `main`.
- [ ] Run `make verify` before editing.
- [ ] Remove the stale `bootstrap.go` comment claiming `internal/platform/database` is missing.
- [ ] Inspect open PRs that touch bootstrap, artifacts, jobs, database or observability.
- [ ] Close or rebase stale overlapping work; do not merge the old `internal/obs` prototype.
- [ ] Keep any unrelated baseline repair in a separate minimal PR.

### PR 1.1 - Task domain

- [ ] Create `internal/taskgraph/model.go`.
- [ ] Create typed status and legal transition table.
- [ ] Create typed `ArtifactRef`, `ArtifactSpec`, `ResourceRequirements`, `TimeRange` and `CostEstimate` contracts for future PRs.
- [ ] Add TaskSpec version validation and deterministic canonical serialization.
- [ ] Add `taskgraph.Repository` and `taskgraph.LifecycleService` interfaces.
- [ ] Reject empty IDs, invalid executor versions, invalid status, self-reference and mutable identity changes.
- [ ] Add numeric transition/revision tests, not only no-crash tests.

### PR 1.2 - Task persistence

- [ ] Add migrations for `tasks` and `task_specs`.
- [ ] Implement the SQLite repository under the canonical persistence layer.
- [ ] Add CAS revision updates.
- [ ] Add indexes for job, status, executor, worker, lease and readiness timestamps.
- [ ] Add tests for create/get/list, duplicate ID, immutable spec, revision conflict and lease mismatch.
- [ ] Add CI single-writer guard for task tables.

### PR 1.3 - Attempt persistence

- [ ] Add migrations for `task_attempts`, `task_phase_timings` and `task_attempt_metrics`.
- [ ] Implement `taskattempts.Repository`.
- [ ] Enforce attempt uniqueness and active-attempt constraints.
- [ ] Persist final reports transactionally with task attempt completion.
- [ ] Add tests for duplicate reports, stale reports and partial rollback.

### PR 1.4 - Atomic initial task creation

- [ ] Keep `enqueue.Enqueuer` as the application entrypoint.
- [ ] Add one store-level transaction coordinator that creates Job plus exactly one initial Task atomically.
- [ ] Do not let handlers write task SQL.
- [ ] Do not create a second enqueue service.
- [ ] Add recovery/invariant test proving every newly enqueued render Job owns exactly one initial Task.
- [ ] Preserve existing Job payload and visual output.

### PR 1.5 - Worker phase timer

- [ ] Add `TaskSpan` and `PhaseTimer` using an injected monotonic clock.
- [ ] Instrument the current whole-render path without changing renderer behavior.
- [ ] Record every canonical phase, including zero durations where not applicable.
- [ ] Record source bytes and resource usage.
- [ ] Preserve existing Prometheus/job metrics for compatibility.
- [ ] Add deterministic tests with fake clocks; no sleep-based assertions.

### PR 1.6 - Typed transport report

- [ ] Add `TaskExecutionReport` to the canonical `.proto` source using a new unused field number.
- [ ] Include contract version, task ID, attempt ID, worker ID, lease ID, status, phase timings, counters, resource usage and stable error.
- [ ] Add task context fields to the current JobOffer bridge.
- [ ] Mark that bridge with a compatibility owner and removal target in PR 2.
- [ ] Regenerate protobuf outputs through the canonical generation command.
- [ ] Worker emits exactly one final task report per attempt.
- [ ] Master validates contract version, task, attempt, worker and lease before persistence.

### PR 1.7 - Preserve finalization ownership

- [ ] Task success records execution completion only.
- [ ] Artifact verification/finalization remains the sole writer of Job `SUCCEEDED`.
- [ ] Link task output artifact IDs through the canonical artifact repository.
- [ ] Ensure duplicate task report and duplicate artifact upload cannot double-finalize.
- [ ] Add integration test: Job -> Task -> Attempt -> Artifact -> Job success.

### PR 1.8 - Read-only observability

- [ ] Create `internal/observability` aggregation service.
- [ ] Report wall time, worker busy time, phase totals, retries and byte sources.
- [ ] Expose bounded internal diagnostics only; no UI.
- [ ] Use repositories, never direct SQL from handlers.
- [ ] Add fixture-based aggregation tests.

### PR 1.9 - Ownership and guards

- [ ] Extend `OWNERSHIP.md` for Task, TaskAttempt, report ingestion and diagnostics.
- [ ] Update CODEOWNERS when present.
- [ ] Add guard against `internal/obs` and duplicate task domains.
- [ ] Add guard against direct task/attempt SQL outside repositories.
- [ ] Add guard against free-form production phase identifiers.

## Required verification

```bash
cd DataServer
gofmt -w internal/taskgraph internal/taskattempts internal/observability
go test -race -count=1 ./internal/taskgraph/...
go test -race -count=1 ./internal/taskattempts/...
go test -race -count=1 ./internal/observability/...
go test -race -count=1 ./internal/jobs/...
go test -race -count=1 ./internal/artifacts/...
go test -race -count=1 ./internal/grpcserver/...
go test -race -count=1 ./cmd/server/...

cd ../RemoteCodex/native/worker-agent-go
gofmt -w internal/telemetry internal/worker
go test -race -count=1 ./internal/telemetry/...
go test -race -count=1 ./internal/worker/...

cd ../../..
SKIP_HEAVY=1 make verify
```

## Acceptance criteria

- [ ] Current rendering output is unchanged.
- [ ] Every new render Job owns exactly one persistent Task.
- [ ] Every execution owns one persistent TaskAttempt.
- [ ] Task/attempt state uses repositories and revision/lease checks.
- [ ] Worker sends typed canonical phase metrics.
- [ ] Duplicate/stale reports are harmless and rejected or idempotent.
- [ ] Job success still has one artifact-finalization writer.
- [ ] No `internal/obs`, second task queue or second lifecycle service exists.
- [ ] `SKIP_HEAVY=1 make verify` passes after rebase on `origin/main`.

## Out of scope

Multiple tasks per Job, dependency edges, late composition, executor registry migration, local cache scheduling, temporal sharding and cost-based placement.
