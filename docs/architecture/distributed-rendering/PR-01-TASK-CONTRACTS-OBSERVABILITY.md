# PR 1 - Task contracts and observability

## PR metadata

Title:

```text
feat(tasks): add task contracts and execution telemetry
```

Branch:

```text
codex/task-contracts-observability
```

Depends on: current `main` compiling and `make verify` passing.

If `main` still fails because `internal/platform/database` is missing, do not hide that repair inside this PR. Restore the canonical database package first, verify `main`, then start this branch.

## Goal

Introduce the domain contracts and measurements required for distributed execution without changing current rendering behavior.

At the end of this PR:

- one existing render job still executes as before;
- that execution is represented as one canonical task;
- each attempt reports phase timings and resource/byte counters;
- the master persists task and attempt state through repositories;
- no scheduling or sharding behavior changes yet.

## Allowed scope

Create or modify only:

```text
DataServer/internal/taskgraph/**
DataServer/internal/taskattempts/**
DataServer/internal/observability/**
DataServer/internal/store/migrations/**
DataServer/cmd/server/bootstrap*.go
DataServer/internal/jobs/** only for task creation integration
RemoteCodex/native/worker-agent-go/internal/telemetry/**
RemoteCodex/native/worker-agent-go/internal/worker/** only for report wiring
shared/** only for versioned task/report contracts
scripts/ci/** only for architecture guards
README/docs ownership files when required
```

Do not modify renderer output, FFmpeg behavior, node graph semantics, asset downloading, or worker assignment policy.

## Required model

### Task

Create a canonical task model owned by `DataServer/internal/taskgraph`.

Minimum fields:

```go
type Task struct {
    ID              string
    JobID           string
    ProjectID       string
    RenderPlanID    string
    Type            string
    Version         int
    Status          Status
    Dependencies    []string
    InputArtifacts  []ArtifactRef
    OutputSpecs     []ArtifactSpec
    Requirements    ResourceRequirements
    EstimatedCost   CostEstimate
    Revision        int
    CreatedAt       time.Time
    UpdatedAt       time.Time
}
```

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

Do not reuse job status constants when task semantics differ.

### TaskAttempt

Create a separate attempt model owned by `DataServer/internal/taskattempts`.

Minimum fields:

```go
type TaskAttempt struct {
    ID              string
    TaskID          string
    AttemptNumber   int
    WorkerID        string
    LeaseID         string
    Status          string
    StartedAt       time.Time
    CompletedAt     time.Time
    Metrics         ExecutionMetrics
    ErrorCode       string
    ErrorMessage    string
}
```

### Canonical execution phases

Use an enum/shared constants for:

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

No arbitrary strings outside this list in production code.

### TaskExecutionReport

Add a versioned shared transport contract.

Minimum payload:

```go
type TaskExecutionReport struct {
    ContractVersion int
    TaskID          string
    AttemptID       string
    WorkerID        string
    Status          string
    PhaseTimings    []PhaseTiming
    Counters        ExecutionCounters
    Resources       ResourceUsage
    Error           *ExecutionError
}
```

The master validates the contract version and owns persistence.

## Operational TODO

### PR 1.0 - Baseline gate

- [ ] Run `git fetch origin`.
- [ ] Start from updated `origin/main`.
- [ ] Run `make verify` before changing files.
- [ ] Confirm no open PR already owns task contracts or telemetry.
- [ ] Record the current job execution path and current worker report path.
- [ ] Stop if baseline is red for unrelated reasons.

### PR 1.1 - Add task graph contracts

- [ ] Create `internal/taskgraph/model.go`.
- [ ] Create task status constants and legal transition table.
- [ ] Create typed `ArtifactRef`, `ArtifactSpec`, `ResourceRequirements`, and `CostEstimate`.
- [ ] Add validation for required IDs, positive versions, known statuses, and duplicate dependencies.
- [ ] Reject self-dependencies.
- [ ] Add unit tests for model validation and transitions.

### PR 1.2 - Add task repository

- [ ] Add canonical SQL migration for `tasks`.
- [ ] Add canonical SQL migration for `task_dependencies` if dependencies are normalized.
- [ ] Implement repository interfaces under `internal/taskgraph`.
- [ ] Keep SQL implementation in the canonical persistence layer.
- [ ] Use optimistic revision checks for task mutation.
- [ ] Add repository tests for create, read, status transition, duplicate ID, and revision conflict.
- [ ] Add one single-writer guard preventing direct task SQL updates from handlers/workers.

### PR 1.3 - Add task attempts

- [ ] Add migration for `task_attempts`.
- [ ] Store phase durations in explicit columns or a validated normalized table; do not store an unvalidated opaque metrics blob as the only source.
- [ ] Implement attempt repository.
- [ ] Enforce unique `(task_id, attempt_number)`.
- [ ] Enforce one active attempt/lease per task according to lifecycle rules.
- [ ] Add tests for idempotent report handling.

### PR 1.4 - Add worker-side phase timer

- [ ] Replace whole-job-only measurement with a reusable `TaskSpan`/`PhaseTimer` abstraction.
- [ ] Keep existing operational counters for backward-compatible monitoring.
- [ ] Record monotonic durations, not wall-clock subtraction across hosts.
- [ ] Record input/output bytes and local/network source bytes.
- [ ] Record CPU time and peak RSS where available.
- [ ] Keep GPU fields optional and zero when unsupported.
- [ ] Add tests using an injected clock; do not use sleep-based assertions.

### PR 1.5 - Add report transport and master ingestion

- [ ] Add versioned `TaskExecutionReport` to the shared transport contract.
- [ ] Add worker emission at task completion/failure.
- [ ] Add master validation and repository persistence.
- [ ] Make duplicate report delivery idempotent.
- [ ] Reject reports for mismatched worker, attempt, or lease.
- [ ] Do not let the worker directly mark the parent job successful.

### PR 1.6 - Wrap current render as one task

- [ ] When a job is enqueued, create one initial task representing current behavior.
- [ ] Preserve existing job lifecycle and output.
- [ ] Link the task output to the existing artifact finalization path.
- [ ] Ensure job success still occurs only through the canonical artifact finalization service.
- [ ] Add an integration test proving one job -> one task -> one attempt -> existing final artifact.

### PR 1.7 - Add project timing aggregation

- [ ] Add a read-only aggregation service under `internal/observability`.
- [ ] Calculate wall-clock duration, total worker busy time, phase totals, retries, and byte sources.
- [ ] Expose data through an internal/read-only endpoint or existing diagnostics module.
- [ ] Do not add frontend work.
- [ ] Add tests against fixed task-attempt fixtures.

### PR 1.8 - Architecture documentation and guards

- [ ] Extend `docs/architecture/OWNERSHIP.md` with TaskGraph and TaskAttempt owners.
- [ ] Extend CODEOWNERS if present.
- [ ] Add CI guard against direct writes to task tables outside the canonical repository.
- [ ] Add CI guard against free-form production phase names.

## Required tests

```bash
cd DataServer
gofmt -w internal/taskgraph internal/taskattempts internal/observability
go test -race -count=1 ./internal/taskgraph/...
go test -race -count=1 ./internal/taskattempts/...
go test -race -count=1 ./internal/observability/...
go test -race -count=1 ./internal/jobs/...
go test -race -count=1 ./cmd/server/...

cd ../RemoteCodex/native/worker-agent-go
gofmt -w internal/telemetry internal/worker
go test -race -count=1 ./internal/telemetry/...
go test -race -count=1 ./internal/worker/...

cd ../../..
SKIP_HEAVY=1 make verify
```

## Acceptance criteria

- [ ] Existing render output is unchanged.
- [ ] One current job creates exactly one task.
- [ ] Task state and attempt state are persisted in SQLite.
- [ ] Worker reports all canonical phase fields, even when some durations are zero.
- [ ] Duplicate reports do not create duplicate attempts or double-finalize work.
- [ ] Parent job success still has one canonical writer.
- [ ] No handler writes directly to task tables.
- [ ] No JSON file or in-memory map becomes authoritative.
- [ ] `SKIP_HEAVY=1 make verify` passes.

## Explicitly out of scope

- Multiple tasks per job.
- Dependency scheduling.
- Overlay prerendering.
- Executor registry migration.
- Cache-local scheduling.
- Temporal sharding.
- Cost-based worker selection.
- UI/dashboard implementation.
