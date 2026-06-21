# PR 3 - Executor registry and modular worker runtime

## Metadata

Title: `refactor(worker): add canonical executor registry and capability contracts`

Branch: `codex/executor-registry-workers`

Depends on: PR 2 merged into `main`.

## Goal

Replace hard-coded worker capabilities and scattered execution routing with one canonical executor registry.

At the end of this PR:

- every executable task type resolves through the registry;
- worker hello capabilities are derived from registered executors;
- CPU/GPU/resource requirements are typed;
- the task runner executes one generic lifecycle around any executor;
- no central switch statement becomes a second registry.

## Allowed scope

```text
RemoteCodex/native/worker-agent-go/internal/executor/**
RemoteCodex/native/worker-agent-go/internal/taskrunner/**
RemoteCodex/native/worker-agent-go/internal/localcache/**
RemoteCodex/native/worker-agent-go/internal/resource/**
RemoteCodex/native/worker-agent-go/internal/worker/**
RemoteCodex/native/worker-agent-go/internal/telemetry/**
RemoteCodex/native/worker-agent-go/pkg/video/** only for adapters
DataServer/internal/workers/**
DataServer/internal/taskgraph/** only for typed requirements
DataServer/internal/scheduler/** only for capability matching primitives
shared/** for contracts
scripts/ci/** for guards
```

Do not add cost-based ranking, temporal splitting, or speculative execution in this PR.

## Canonical executor contract

```go
type Descriptor struct {
    ID             string
    Version        int
    InputTypes     []string
    OutputTypes    []string
    ResourceClass  ResourceClass
    Deterministic  bool
    Cacheable      bool
    TemporalMode   TemporalMode
    SupportsAlpha  bool
}

type Executor interface {
    Descriptor() Descriptor
    Validate(TaskSpec) error
    Execute(context.Context, ExecutionContext, TaskSpec) (ExecutionResult, error)
}
```

Initial temporal modes:

```text
frame_local
windowed
stateful
global
```

Initial resource classes:

```text
cpu
gpu
mixed
io
```

## Initial registered executors

```text
asset.prepare.v1
text.compile.v1
text.render.v1
precomp.render.v1
scene.composite.v1
audio.mix.v1
video.concat.v1
video.encode-h264.v1
```

Only register executors that already have a real implementation or adapter in this PR.

## Operational TODO

### PR 3.0 - Baseline

- [ ] Start from updated `origin/main` after PR 2 merge.
- [ ] Run `SKIP_HEAVY=1 make verify`.
- [ ] Inventory all current job-type switches and capability maps.
- [ ] Inventory every direct renderer/FFmpeg invocation from worker orchestration code.
- [ ] Identify the existing canonical pipeline registry and reuse it where appropriate.

### PR 3.1 - Add executor registry

- [ ] Create `internal/executor/registry.go`.
- [ ] Reject empty IDs, non-positive versions, and duplicates.
- [ ] Resolve by `(executor_id, version)`.
- [ ] Return sorted descriptors for deterministic worker hello.
- [ ] Add registry completeness and duplicate tests.
- [ ] Keep registration explicit in worker bootstrap; no self-discovery.

### PR 3.2 - Add typed execution context

- [ ] Create `ExecutionContext` with artifact reader/writer, local cache, telemetry span, resource limits, clock, logger, and cancellation.
- [ ] Do not expose global mutable state to executors.
- [ ] Do not let executors mutate task/job lifecycle directly.
- [ ] Make artifact output publication go through one canonical adapter.

### PR 3.3 - Add generic task runner

- [ ] Resolve executor from task ID/version.
- [ ] Validate task before resource acquisition.
- [ ] Run canonical phases: cache lookup, prefetch, execute, upload, report.
- [ ] Enforce cancellation and lease loss.
- [ ] Always emit one final TaskExecutionReport.
- [ ] Convert executor errors to stable error codes.
- [ ] Add tests for success, validation failure, cancellation, lease loss, and panic containment.

### PR 3.4 - Migrate existing implementations through adapters

- [ ] Wrap existing asset preparation path as `asset.prepare.v1`.
- [ ] Wrap text/precomp path from PR 2.
- [ ] Wrap existing scene composite/render path.
- [ ] Wrap audio mix and final concat only if current code already supports them.
- [ ] Keep one underlying renderer path; adapters must not duplicate rendering logic.
- [ ] Remove migrated direct dispatch branches.

### PR 3.5 - Generate worker capabilities from registry

- [ ] Remove hard-coded capability booleans for migrated executors.
- [ ] Worker hello must include executor IDs, versions, resource classes, and supported temporal modes.
- [ ] Keep host information: CPU count, RAM, GPU presence/VRAM when available, disk, and maximum active tasks.
- [ ] Add a versioned capability contract.
- [ ] Add tests proving hello output matches registry contents exactly.

### PR 3.6 - Add resource sampler

- [ ] Create one sampler interface and registry-owned implementation.
- [ ] Report CPU load, available RAM, disk pressure, active tasks, and network activity.
- [ ] Report GPU utilization/VRAM only when a supported backend exists.
- [ ] Keep unsupported GPU fields absent or zero without failing CPU workers.
- [ ] Use sampling intervals; do not collect expensive metrics per frame.
- [ ] Add tests with fake samplers.

### PR 3.7 - Add persistent local artifact cache

- [ ] Create content-addressed local cache keyed by artifact hash.
- [ ] Persist cache index across worker restarts.
- [ ] Verify hash before declaring a hit.
- [ ] Add leases/pins so active artifacts are not evicted.
- [ ] Implement size-bounded LRU eviction.
- [ ] Record local hit bytes, miss bytes, evictions, and corruption events.
- [ ] Do not make local cache authoritative.

### PR 3.8 - Master-side capability validation

- [ ] Parse and validate versioned worker capability declarations.
- [ ] Persist the latest worker profile through the canonical worker repository.
- [ ] Reject assignment when executor/version is unsupported.
- [ ] Reject assignment when hard requirements exceed worker resources.
- [ ] Keep final worker selection simple in this PR: capability plus availability only.

### PR 3.9 - Remove duplicate routing

- [ ] Search for all switches on job/task type.
- [ ] Remove switches replaced by registry resolution.
- [ ] Keep domain-level branching only when it represents business behavior, not executor lookup.
- [ ] Add CI guard against reintroducing a second executor map/switch.
- [ ] Add CI guard against hard-coded worker capability lists.

### PR 3.10 - Ownership and documentation

- [ ] Extend `OWNERSHIP.md` with executor registry, resource sampler, task runner, and local cache ownership.
- [ ] Document how to add an executor: descriptor, implementation, registration, tests, capability advertisement.
- [ ] Document deterministic/cacheable requirements and seed handling.

## Required tests

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
```

## Acceptance criteria

- [ ] All migrated task types resolve through one executor registry.
- [ ] Worker capabilities are generated from registry descriptors.
- [ ] CPU-only workers remain fully supported.
- [ ] Unsupported GPU functionality does not create a fallback renderer path.
- [ ] Local cache survives worker restart and verifies hashes.
- [ ] Worker orchestration contains no duplicated renderer dispatch table.
- [ ] Task lifecycle remains master-owned.
- [ ] `SKIP_HEAVY=1 make verify` passes.

## Out of scope

Cost-based ranking, cache-local scheduling, adaptive sharding, speculative execution, ML prediction, Kubernetes, and frontend work.
