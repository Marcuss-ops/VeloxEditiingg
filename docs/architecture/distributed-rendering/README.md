# Velox distributed rendering implementation roadmap

Status snapshot: 2026-06-21

Owner: master rendering architecture

This directory is the execution contract for converting Velox from whole-job rendering into a deterministic distributed composition runtime. It describes only the work still missing from `main`.

## Current repository state

Already present and mandatory to reuse:

- canonical `jobs` domain with repository, revision, lease and lifecycle service;
- SQLite-first persistence through repositories;
- `internal/platform/database` with SQLite/Postgres connection abstraction;
- `buildPersistence` with mandatory outbox and fail-fast BlobStore;
- canonical artifact upload/finalization path;
- asset resolver registry;
- explicit background supervisor;
- gRPC-only worker control plane;
- worker pipeline compiler registry;
- worker-side RenderPlan V1 consumed by the C++/FFmpeg engine;
- queue facade removed from the canonical architecture.

Still missing from `main`:

```text
DataServer/internal/taskgraph
DataServer/internal/taskattempts
DataServer/internal/observability
DataServer/internal/renderplan
DataServer/internal/scheduler
DataServer/internal/costmodel        ← LANDED on `codex/pr-04-scheduler-cost-sharding` (commit `52479f6d`, PR-04.5): per-job cost-aware eligibility + cost model mirrored in worker-agent-go. Rank call site (sendPushJobOffer) flips in PR-04.6.
RemoteCodex/native/worker-agent-go/internal/executor
RemoteCodex/native/worker-agent-go/internal/taskrunner
RemoteCodex/native/worker-agent-go/internal/localcache
RemoteCodex/native/worker-agent-go/internal/resource
```

Also still missing:

- typed task-level worker reports;
- immutable master-owned RenderPlan publication;
- persistent task dependencies;
- late composition and reusable RGBA precompositions;
- registry-derived worker capabilities;
- persistent content-addressed worker cache;
- cost/locality-aware scheduling;
- temporal sharding and compatible shard finalization;
- critical-path and parallel-efficiency reporting.

## Immediate repository hygiene

Before implementation PR 1:

1. start from current `origin/main`;
2. run `make verify` and record the result;
3. remove the stale comment in `DataServer/cmd/server/bootstrap.go` claiming `internal/platform/database` is absent;
4. resolve, close or completely rebase any older open PR touching bootstrap, artifacts, database, jobs or observability;
5. do not merge or copy `DataServer/internal/obs/models.go` from stale branches: the canonical packages are `taskgraph`, `taskattempts` and `observability`;
6. do not carry forward free-form phase names or opaque task parameter blobs as the canonical model.

A baseline repair unrelated to distributed rendering must be a separate minimal PR. Do not hide unrelated repairs inside PR 1.

## Target architecture

```text
Editor/project input
        |
        v
Canonical compiler registry
        |
        v
Immutable RenderPlan
        |
        v
Persistent TaskGraph
        |
        +---- verified artifact cache hit
        |
        v
Ready-task lifecycle
        |
        v
Scheduler: capability + load + cost + locality
        |
        v
gRPC task contract
        |
        v
Executor registry on worker
        |
        v
Artifact outputs
        |
        v
Final composition / concat / mux
```

## Domain ownership

```text
Job
  User-visible objective and overall lifecycle.

RenderPlan
  Immutable compiled description of what must be produced.

TaskGraph
  Persistent dependency graph derived from one RenderPlan.

Task
  One schedulable unit with explicit executor, inputs, outputs and requirements.

TaskAttempt
  One execution of one Task on one Worker.

Artifact
  Immutable verified output identified by content hash.

WorkerProfile
  Advertised executor capabilities plus current resource state.
```

## Purity invariants

1. The master is the only planner, dependency resolver, scheduler and task-lifecycle authority.
2. Workers execute assigned contracts; they never reinterpret the project or invent graph edges.
3. Every mutable state has one writer and one repository.
4. Every executor, compiler, resolver, estimator and sampler enters one canonical registry.
5. No switch/case map may duplicate registry dispatch.
6. RenderPlan, TaskSpec and Artifact identity are immutable after publication.
7. Cache keys are deterministic functions of semantic inputs, executor version and engine version.
8. SQLite stores state and metadata; BlobStore stores binary payloads.
9. Memory queues and caches are reconstructible and never authoritative.
10. No silent fallback, dual-write, hidden retry path or second renderer path.
11. CPU-only workers remain valid. GPU support is explicit capability matching, never an implicit fallback.
12. Drive is an ingestion source, not the normal rendering filesystem.
13. Workers do not exchange authoritative state directly with other workers.
14. Final job success remains owned by verified artifact finalization.
15. Velox remains headless, deterministic and server-side.

## Existing components that must not be recreated

Do not add replacements for:

```text
jobs.Repository / jobs.LifecycleService
artifacts.Service and canonical artifact repositories
assets.ResolverRegistry
outbox.Store / outbox.Registry
BackgroundSupervisor
platform/database
worker pipeline.Registry
worker RenderPlan V1 engine path
gRPC WorkerControl stream
```

Extend these owners or add adapters around them. Do not create parallel domains with names such as `task_queue_v2`, `artifact_cache_v2`, `new_scheduler`, `legacy_executor` or `distributed_jobs`.

## Required PR sequence

The implementation remains four sequential PRs:

1. [PR 1 - Canonical tasks and execution telemetry](PR-01-TASK-CONTRACTS-OBSERVABILITY.md)
2. [PR 2 - Persistent DAG and late composition](PR-02-TASK-DAG-LATE-COMPOSITION.md)
3. [PR 3 - Executor registry and worker runtime](PR-03-EXECUTOR-REGISTRY-WORKERS.md)
4. [PR 4 - Scheduler, cost model and temporal sharding](PR-04-SCHEDULER-COST-SHARDING.md)

A later PR must not begin before the previous PR is merged into `main` and its acceptance tests pass on the rebased branch.

## Canonical task phases

Task telemetry may use only these production phase identifiers:

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

Additional phases require an architecture update and registry change. Free-form names are forbidden.

## Canonical executor IDs

Initial IDs:

```text
asset.prepare.v1
text.compile.v1
text.render.v1
precomp.render.v1
scene.composite.v1
scene.render-shard.v1
audio.mix.v1
video.concat.v1
video.encode-h264.v1
```

Only executors backed by real implementations may be advertised.

## Required measurements

Task-attempt measurements:

```text
queue_ms
asset_wait_ms
cache_lookup_ms
download_ms
decode_ms
compile_ms
simulate_ms
render_ms
composite_ms
encode_ms
upload_ms
finalize_ms
total_ms
input_bytes
output_bytes
bytes_from_drive
bytes_from_blobstore
bytes_from_local_cache
cpu_time_ms
gpu_time_ms
peak_rss_bytes
peak_vram_bytes
estimated_ms
estimation_error_ratio
```

Project measurements:

```text
wall_clock_ms
planned_critical_path_ms
actual_critical_path_ms
total_worker_busy_ms
parallel_efficiency
workers_allocated
workers_peak_active
cache_byte_hit_ratio
retry_count
straggler_count
```

Measurements are stored as validated typed fields. An opaque JSON metrics blob cannot be the sole source of truth.

## Git workflow

For every implementation PR:

```bash
git fetch origin
git checkout main
git pull --ff-only origin main
git checkout -b codex/<focused-name>
```

Before push:

```bash
git fetch origin
git rebase origin/main
git status -sb
git diff origin/main...HEAD --stat
```

After push:

```bash
git log -n 5 --oneline
git fetch origin
git status -sb
```

Rules:

- one coherent responsibility per PR;
- no generated binaries, output folders or local caches;
- no broad formatting/refactor mixed with the feature;
- verify remote diffs before touching files changed by another branch;
- run targeted tests plus the documented verification gate;
- compatibility may be read-only only, with owner and removal date;
- delete temporary bridges in the PR explicitly assigned to remove them.

## Roadmap completion criteria

The roadmap is complete only when:

- one Job expands into a persistent multi-task DAG;
- independent overlays render while stock ingestion is pending;
- verified precompositions are reused by content hash;
- worker capabilities are produced from the executor registry;
- task placement uses capability, resource state, measured cost and data locality;
- frame-local/windowed work can be safely sharded;
- finalization assembles compatible shards without full-project re-render;
- actual critical path and parallel efficiency are available per project;
- master restart reconstructs all scheduling state from repositories;
- no duplicate owner, registry, scheduler, cache or renderer exists.

## Non-goals

This roadmap does not add:

- editor/frontend work;
- browser rendering;
- Kubernetes;
- peer-to-peer authoritative state;
- machine-learning prediction;
- PostgreSQL cutover;
- a second C++/FFmpeg renderer path;
- automatic feature discovery;
- an After Effects or Blender clone.
