# PR 2 - Executor registry and modular worker runtime

## Metadata

Title: `refactor(worker): route task execution through one executor registry`

Branch: `codex/executor-registry-workers`

Depends on: PR 1 merged into `main` with task-level transport and telemetry passing.

## Why this PR comes before late composition

Overlay, precomposition and composite task types must not be introduced through temporary worker switches. This PR creates the canonical runtime first; PR 3 then adds new executors directly to that runtime.

## Current state to reuse

Already present:

- gRPC-only WorkerControl stream;
- worker pipeline `Registry` and runner;
- C++/FFmpeg RenderPlan execution path;
- current job concurrency and lease handling;
- `AssetCacheDir` configuration field;
- artifact upload path;
- task contracts/reports from PR 1.

Still impure/missing:

- worker capabilities are hard-coded in `buildHello`;
- orchestration routes work through job-specific branches;
- no canonical executor descriptor/registry;
- no generic TaskRunner;
- no typed resource sampler;
- no persistent content-addressed local cache;
- no master-side versioned capability validation;
- current pipeline detection still permits heuristic inference.

## Goal

After this PR the complete worker execution path is:

```text
TaskOffer
  -> executor.Registry.Resolve(id, version)
  -> Executor.Validate
  -> TaskRunner
  -> current renderer/pipeline adapter
  -> artifact publication
  -> TaskExecutionReport
```

No second dispatch table or fallback renderer is allowed.

## Canonical package boundaries

```text
RemoteCodex/.../internal/executor
  Descriptor, Executor, Registry and explicit registration.

RemoteCodex/.../internal/taskrunner
  Generic task lifecycle, cancellation, lease loss and reporting.

RemoteCodex/.../internal/localcache
  Persistent content-addressed worker cache.

RemoteCodex/.../internal/resource
  CPU/RAM/disk/network/GPU sampling.

DataServer/internal/workers
  Validated persisted WorkerProfile and capability declarations.

DataServer/internal/taskgraph
  Typed task requirements only; no worker runtime code.
```

## Executor contract

```go
type Descriptor struct {
    ID              string
    Version         int
    InputTypes      []string
    OutputTypes     []string
    ResourceClass   ResourceClass
    Deterministic   bool
    Cacheable       bool
    TemporalMode    TemporalMode
    SupportsAlpha   bool
}

type Executor interface {
    Descriptor() Descriptor
    Validate(task TaskSpec) error
    Execute(ctx context.Context, exec ExecutionContext, task TaskSpec) (ExecutionResult, error)
}
```

Resource classes:

```text
io
cpu
gpu
mixed
```

Temporal modes:

```text
frame_local
windowed
stateful
global
```

Registration is explicit in worker bootstrap. No package `init`, reflection, plugin discovery or filesystem scanning.

## Initial executor set

Register only real adapters:

```text
video.render.current.v1
asset.prepare.v1        only when backed by the existing asset path
```

PR 3 adds text/precomp/composite executors. Do not advertise placeholders.

## Task transport cutover

PR 1 may temporarily carry task context inside the current JobOffer bridge. This PR removes that bridge and adds typed task control messages:

```text
TaskOffer
TaskAccepted
TaskRejected
TaskProgress
TaskExecutionReport
CancelTask
LeaseRevoked
```

Parent Job identity remains present for correlation, but assignment and attempt lifecycle are task-based.

Do not run JobOffer and TaskOffer as two permanent write paths. The cutover must be one-directional and the old bridge must be deleted before merge.

## Local cache contract

Cache layout is content-addressed:

```text
<cache-root>/sha256/<prefix>/<full-hash>
```

Rules:

- artifact hash is identity;
- hash verification is mandatory before hit;
- active inputs are pinned/leased;
- cache index survives restart but remains reconstructible;
- size-bounded LRU eviction;
- partial/corrupt files are quarantined and never hits;
- cache is never authoritative;
- task output publication still goes through the canonical artifact service.

## Operational TODO

### PR 2.0 - Baseline and inventory

- [ ] Start from updated `origin/main` after PR 1.
- [ ] Run `SKIP_HEAVY=1 make verify`.
- [ ] Inventory every worker job-type switch and direct pipeline/renderer invocation.
- [ ] Inventory hard-coded capabilities and host-resource fields.
- [ ] Confirm the current pipeline registry remains the sole low-level RenderPlan compiler registry.

### PR 2.1 - Executor registry

- [ ] Create `internal/executor/descriptor.go`, `executor.go` and `registry.go`.
- [ ] Resolve by `(executor_id, version)`.
- [ ] Reject empty IDs, invalid versions, duplicate registrations and inconsistent descriptors.
- [ ] Return sorted descriptors for deterministic hello payloads.
- [ ] Add registry completeness, duplicate and deterministic-order tests.
- [ ] Add a CI guard against another executor registry/dispatch map.

### PR 2.2 - Typed ExecutionContext

- [ ] Create `ExecutionContext` containing artifact reader/writer adapters, local cache, TaskSpan, resource limits, clock, logger and cancellation.
- [ ] Do not expose global mutable worker state.
- [ ] Do not expose master lifecycle mutation methods to executors.
- [ ] Route outputs through one artifact publication adapter.
- [ ] Make temporary files task-scoped and cleaned on success/failure/cancel.

### PR 2.3 - Generic TaskRunner

- [ ] Resolve and validate executor before acquiring expensive resources.
- [ ] Run canonical phases: cache lookup, prefetch, execute, upload, final report.
- [ ] Stop on context cancellation, lease revocation or worker drain.
- [ ] Recover panics into stable execution errors.
- [ ] Emit exactly one terminal TaskExecutionReport.
- [ ] Add tests for success, validation failure, cancellation, lease loss, panic and report-send retry.

### PR 2.4 - Current renderer adapter

- [ ] Wrap the existing pipeline runner/C++ engine path as `video.render.current.v1`.
- [ ] Reuse existing renderer and RenderPlan V1; do not duplicate FFmpeg commands.
- [ ] Require explicit `pipeline_id` in the task spec.
- [ ] Remove heuristic pipeline selection from the canonical execution path.
- [ ] Delete migrated direct job dispatch branches.
- [ ] Add parity test proving adapter output matches the pre-registry path.

### PR 2.5 - Task transport cutover

- [ ] Add typed task messages to the canonical proto source with unused field numbers.
- [ ] Master assigns Task/Attempt/Lease, not whole-job execution state.
- [ ] Worker accepts/rejects task offers through the task runner.
- [ ] Preserve Job ID only as parent correlation.
- [ ] Remove the temporary PR 1 JobOffer task-context bridge.
- [ ] Remove dead job-execution transport fields only after all callers migrate.
- [ ] Add protocol compatibility tests and generated-code verification.

### PR 2.6 - Registry-derived worker hello

- [ ] Replace hard-coded capability booleans with executor descriptors.
- [ ] Advertise executor ID/version, resource class, temporal mode, cacheability and alpha support.
- [ ] Advertise host CPU, RAM, disk, network and optional GPU/VRAM separately from executor capabilities.
- [ ] Add a versioned WorkerProfile contract.
- [ ] Add snapshot test: hello executors exactly equal registry descriptors.
- [ ] Add guard against hard-coded `supported_job_types`/executor lists.

### PR 2.7 - Resource sampler

- [ ] Create one sampler interface with explicit registration.
- [ ] Sample CPU load, available RAM, disk pressure, active tasks and network throughput.
- [ ] Sample GPU/VRAM only through an optional supported backend.
- [ ] CPU workers must start without GPU libraries or devices.
- [ ] Use bounded intervals; never sample per frame.
- [ ] Add fake-sampler tests and error isolation.

### PR 2.8 - Persistent local cache

- [ ] Implement content-addressed cache under `internal/localcache`.
- [ ] Persist/rebuild the index across restart.
- [ ] Add hash verification, atomic temp-to-final rename and corruption quarantine.
- [ ] Add pin/lease handling for active task inputs.
- [ ] Add size-bounded LRU eviction.
- [ ] Record local hit/miss bytes, eviction and corruption metrics.
- [ ] Integrate `AssetCacheDir` as the configured root; remove ad-hoc per-feature caches.

### PR 2.9 - Master WorkerProfile validation

- [ ] Persist the latest versioned WorkerProfile through the canonical workers repository.
- [ ] Validate descriptor IDs/versions and host-resource values.
- [ ] Reject task offers to workers without the required executor/version.
- [ ] Reject hard requirement mismatches.
- [ ] Keep selection policy simple: capability plus availability only.
- [ ] Do not add cost/locality ranking before PR 4.

### PR 2.10 - Ownership and deletion pass

- [ ] Extend `OWNERSHIP.md` for executor registry, task runner, resource sampler, WorkerProfile and local cache.
- [ ] Remove migrated switches, hard-coded capability maps and temporary transport adapters.
- [ ] Verify `git grep` has no old dispatch symbols.
- [ ] Add single-registry and no-hardcoded-capability CI guards.
- [ ] Document the only valid procedure for adding an executor.

## Required verification

```bash
cd RemoteCodex/native/worker-agent-go
gofmt -w internal/executor internal/taskrunner internal/localcache internal/resource internal/worker
go test -race -count=1 ./internal/executor/...
go test -race -count=1 ./internal/taskrunner/...
go test -race -count=1 ./internal/localcache/...
go test -race -count=1 ./internal/resource/...
go test -race -count=1 ./internal/worker/...
go test -race -count=1 ./pkg/video/...

cd ../../../DataServer
gofmt -w internal/workers internal/taskgraph
go test -race -count=1 ./internal/workers/...
go test -race -count=1 ./internal/taskgraph/...
go test -race -count=1 ./internal/grpcserver/...
go test -race -count=1 ./cmd/server/...

cd ..
SKIP_HEAVY=1 make verify
make pilot
```

## Acceptance criteria

- [ ] Every task execution resolves through one registry.
- [ ] Current renderer output remains parity-equivalent.
- [ ] Worker hello is generated from registry descriptors.
- [ ] Task transport replaces the temporary JobOffer bridge.
- [ ] CPU-only workers remain first-class.
- [ ] Local cache survives restart and verifies content hashes.
- [ ] No executor can mutate task/job lifecycle directly.
- [ ] No duplicate dispatcher, capability map, cache owner or renderer path exists.
- [ ] `SKIP_HEAVY=1 make verify` and `make pilot` pass after rebase.

## Out of scope

Multi-task DAG publication, late composition, cost-based ranking, data-local placement, temporal sharding and speculative execution.
