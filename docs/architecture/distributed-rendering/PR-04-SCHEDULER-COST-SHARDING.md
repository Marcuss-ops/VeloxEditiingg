# PR 4 - Cost-aware scheduler and temporal sharding

## Metadata

Title: `feat(scheduler): add explainable placement and deterministic render shards`

Branch: `codex/adaptive-scheduler-sharding`

Depends on: PR 3 merged into `main` with persistent DAG, registered executors and verified reusable artifacts.

## Goal

Use measured TaskAttempt data to:

- select the best eligible worker;
- reserve resources without double assignment;
- prefer workers that already cache large inputs;
- split safe rendering work into balanced temporal shards;
- assemble compatible shards without a full-project re-render;
- report planned and actual critical paths;
- detect tail-latency stragglers.

This PR must scale from a few workers to a fleet of roughly 50 VPS without turning SQLite, heartbeat traffic or cache inventories into unbounded bottlenecks.

## Ownership boundaries

```text
internal/scheduler
  Candidate filtering, scoring, placement explanation and dispatch loop.

internal/costmodel
  Estimator registry, worker-class history and calibration.

internal/taskgraph
  READY state, lease/CAS transitions and dependency truth.

internal/workers
  WorkerProfile, liveness, resource state and soft cache digest.

internal/observability
  Critical path, efficiency and project reports.

internal/artifacts
  Verified shard outputs and final artifact identity.

worker executor/taskrunner
  Time-range execution and exact output reporting.
```

The scheduler chooses; `taskgraph.LifecycleService` performs the canonical lease/status mutation. The scheduler never writes task SQL directly.

## Event-driven rule

Scheduling is triggered by:

```text
task became READY
worker became available
worker resource profile changed
lease expired/revoked
task completed/failed
```

Do not continuously scan all tasks or workers. Any memory ready queue is reconstructible from SQLite and outbox state.

## Placement formula

Initial score minimizes estimated finish time:

```text
estimated_finish_ms =
    worker_reserved_queue_ms
  + transfer_ms(missing_input_bytes, source_tier, measured_bandwidth)
  + estimated_execution_ms
  + estimated_output_upload_ms
  + resource_pressure_penalty_ms
  + deadline/fairness_penalty_ms
```

Hard eligibility is evaluated before scoring:

```text
executor ID/version
CPU/RAM/disk requirements
GPU vendor/capability/VRAM when required
worker connected and not draining
concurrency/resource reservation available
```

Every placement stores an explanation with candidate rejection reasons and winning score components.

## Worker classes

Performance history is keyed by stable capability/resource classes, never hostnames.

Example dimensions:

```text
executor_id + executor_version
cpu architecture/core bucket
gpu model/capability bucket
resolution/frame-count bucket
codec/output profile
layer/effect feature bucket
engine version
```

Start with medians, p90 and bounded EWMA. Do not add ML dependencies.

## Cache locality at fleet scale

Local worker cache is soft state.

Workers advertise a bounded cache digest:

- exact hashes for pinned/recent/hot artifacts;
- optional compact probabilistic digest for the remaining cache;
- total/free cache bytes and generation number;
- deltas instead of full inventory on every heartbeat.

False-positive locality is acceptable because the worker verifies the file hash and falls back to canonical BlobStore. False-negative locality only loses an optimization.

Do not persist millions of cache rows as authoritative state. Persist only bounded/recent profile data when needed for diagnostics.

## Resource reservations

Do not schedule solely from `max_active_jobs`.

Each task declares requested resources:

```text
cpu_threads
ram_bytes
gpu_slots
vram_bytes
disk_scratch_bytes
network_class
```

The master maintains reconstructible reservations per worker and uses task lifecycle CAS to prevent double lease. Worker-reported usage corrects estimates but never becomes task-state authority.

## Temporal sharding contract

Use integer frame ranges as canonical boundaries:

```text
start_frame inclusive
end_frame exclusive
pre_roll_frames
post_roll_frames
project_frame_offset
```

Milliseconds may be derived for APIs, but frame identity controls rendering and concatenation.

Temporal modes:

```text
frame_local
  split freely at valid frame boundaries.

windowed
  split with declared pre/post-roll, publish only requested frames.

stateful
  split only from verified checkpoint/state artifacts.

global
  never split.
```

All animation/camera evaluation uses absolute project time/frame. Randomness uses deterministic seeds derived from ExecutionPlan and Task identity.

## Shard sizing

Use predicted compute duration, not fixed video duration:

```text
< 1 second predicted compute
  fuse when semantic/cache boundaries allow.

5-20 seconds predicted compute
  preferred shard target.

20-60 seconds
  acceptable while data is sparse.

> 60 seconds
  split when executor temporal mode allows.
```

Generate more READY shards than available workers for dynamic balancing, but apply a bounded maximum per plan to avoid task explosion.

## Compatible shard finalization

`video.concat.v1` may avoid full re-encode only when every shard agrees on:

```text
codec and codec parameters
resolution
frame rate and timebase
pixel format
color profile
GOP/keyframe policy
alpha policy
audio policy
engine/output profile version
```

Finalizer validates:

```text
ordered frame ranges
no gaps
no overlaps after trim
exact frame counts
checksums
duration
profile equality
```

Global audio mix remains a separate task when per-shard audio would create discontinuity.

## Operational TODO

### PR 4.0 - Baseline dataset

- [ ] Start from updated `origin/main` after PR 3.
- [ ] Run `SKIP_HEAVY=1 make verify` and `make pilot`.
- [ ] Collect representative successful TaskAttempt fixtures.
- [ ] Confirm canonical phase and byte metrics are populated.
- [ ] Define initial worker classes from capabilities/resources.
- [ ] Define bounded limits for ready tasks, candidates and diagnostic payloads.

### PR 4.1 - Cost model registry

- [ ] Create `internal/costmodel/registry.go` and estimator interface.
- [ ] Require one estimator per executor/version, with conservative default.
- [ ] Build features from frames, resolution, layers, effects, codec, temporal mode and bytes.
- [ ] Store median, p90, sample count and EWMA per worker class.
- [ ] Ignore cancelled/incomplete measurements and classify failures separately.
- [ ] Add deterministic estimate, cold-start, outlier and version-isolation tests.

### PR 4.2 - Ready queue and recovery

- [ ] Build event-driven ready queue from task/outbox events.
- [ ] Make enqueue/dequeue idempotent.
- [ ] Support priority, deadline, age and estimated cost.
- [ ] Bound memory growth and batch sizes.
- [ ] Reconstruct READY work from SQLite after restart.
- [ ] Do not make the in-memory queue authoritative.
- [ ] Add restart and duplicate-event tests.

### PR 4.3 - Worker eligibility

- [ ] Filter by executor/version and hard resource requirements.
- [ ] Exclude disconnected, draining, quarantined or overloaded workers.
- [ ] Track reconstructible CPU/RAM/GPU/VRAM/disk reservations.
- [ ] Respect worker concurrency and executor-specific limits.
- [ ] Add CPU-only, GPU-required, RAM/VRAM shortage and drain tests.

### PR 4.4 - Locality and transfer estimation

- [ ] Add bounded WorkerCacheDigest contract with generation/deltas.
- [ ] Calculate exact/likely local input bytes.
- [ ] Estimate missing bytes by Drive/BlobStore/local source tier.
- [ ] Maintain bounded bandwidth EWMA per worker/source tier.
- [ ] Verify locality on worker before use.
- [ ] Add test where cached slower worker wins over uncached faster worker.
- [ ] Add test where false-positive digest safely falls back to BlobStore.

### PR 4.5 - Explainable scoring

- [ ] Implement pure scoring function with no side effects.
- [ ] Return per-candidate rejection/score breakdown.
- [ ] Add deadline and fairness penalties without hidden magic constants.
- [ ] Store selected explanation in bounded diagnostics.
- [ ] Add deterministic tie-breaking by stable worker/task IDs.
- [ ] Add snapshot tests for scoring and tie behavior.

### PR 4.6 - Atomic placement

- [ ] Scheduler proposes a worker; Task lifecycle atomically leases via revision/CAS.
- [ ] Reserve resources only after successful lease.
- [ ] Roll back reservation if offer delivery fails or is rejected.
- [ ] Release reservations on completion, failure, cancellation, lease expiry and disconnect.
- [ ] Add concurrent-scheduler test proving one task receives one active lease.

### PR 4.7 - Temporal shard planner

- [ ] Create one shard planner under scheduler/taskgraph ownership.
- [ ] Split only registered executor temporal modes.
- [ ] Use integer frame boundaries and absolute project frame offsets.
- [ ] Add pre/post-roll for windowed effects.
- [ ] Require checkpoint artifacts for stateful splits.
- [ ] Reject global/unsafe split.
- [ ] Bound shard count and target predicted 5-20 seconds compute.
- [ ] Add frame-local, windowed, stateful and global tests.

### PR 4.8 - Shard execution

- [ ] Extend TaskSpec with requested/padded frame ranges.
- [ ] Render padded context but publish only requested frames.
- [ ] Report exact first/last frame, frame count, timestamps and output profile.
- [ ] Derive deterministic random seed from plan/task/shard identity.
- [ ] Add boundary parity test against unsplit rendering.
- [ ] Add camera/keyframe absolute-time parity tests.

### PR 4.9 - Shard finalizer

- [ ] Register `video.concat.v1` through executor registry.
- [ ] Validate profile equality, order, gaps, overlaps, frames and checksums.
- [ ] Concat/mux without full re-encode when compatible.
- [ ] Fail explicitly when profiles differ; do not silently re-encode.
- [ ] Keep global audio mix separate when necessary.
- [ ] Compare sharded and non-sharded output in integration tests.

### PR 4.10 - Critical path and efficiency

- [ ] Calculate planned longest path from estimates.
- [ ] Calculate actual longest path from completed attempt durations.
- [ ] Report estimation error per task and critical path.
- [ ] Calculate total worker busy time and parallel efficiency.
- [ ] Separate queue, asset wait, execution, upload and finalization time.
- [ ] Add fixed-DAG fixture tests.

### PR 4.11 - Straggler handling

- [ ] Compare running attempt duration to p90/median for similar work.
- [ ] Mark stragglers without automatically duplicating all slow work.
- [ ] Permit speculative duplicate only at graph tail/deadline risk and when task is deterministic/idempotent.
- [ ] First verified valid output wins.
- [ ] Cancel loser and release resources.
- [ ] Ensure duplicates cannot double-publish or double-finalize.

### PR 4.12 - Backpressure and fairness

- [ ] Bound active offers per master and worker.
- [ ] Bound ready-queue processing batches.
- [ ] Prevent one large project from starving others.
- [ ] Prefer useful parallelism over assigning every worker to one plan.
- [ ] Keep worker allocation dynamic as graph parallelism changes.
- [ ] Add multi-project fairness/load tests.

### PR 4.13 - Diagnostics and recovery

- [ ] Add bounded read-only project report: wall time, critical paths, busy time, efficiency, cache ratio, bytes, retries and stragglers.
- [ ] Add placement explanations and DAG waterfall JSON.
- [ ] Paginate large graphs/tasks.
- [ ] Register scheduler explicitly in BackgroundSupervisor.
- [ ] Recover READY/LEASED/reservations after restart according to canonical lease state.
- [ ] Stop new offers during drain while allowing configured in-flight completion/cancellation.

### PR 4.14 - Ownership and deletion pass

- [ ] Extend `OWNERSHIP.md` for scheduler, estimator registry, reservations, shard planner, finalizer and critical path.
- [ ] Add guard against worker selection outside scheduler.
- [ ] Add guard against duplicate estimator/shard logic.
- [ ] Add guard against shard concatenation outside registered finalizer.
- [ ] Remove temporary simple matcher paths from PR 2.
- [ ] Verify no busy full-table scheduling loop exists.

## Required verification

```bash
cd DataServer
gofmt -w internal/scheduler internal/costmodel internal/taskgraph internal/observability internal/workers
go test -race -count=1 ./internal/scheduler/...
go test -race -count=1 ./internal/costmodel/...
go test -race -count=1 ./internal/taskgraph/...
go test -race -count=1 ./internal/observability/...
go test -race -count=1 ./internal/workers/...
go test -race -count=1 ./cmd/server/...

cd ../RemoteCodex/native/worker-agent-go
gofmt -w internal/executor internal/taskrunner pkg/video
go test -race -count=1 ./internal/executor/...
go test -race -count=1 ./internal/taskrunner/...
go test -race -count=1 ./pkg/video/...

cd ../../..
SKIP_HEAVY=1 make verify
make pilot
```

## Required integration scenarios

- [ ] Cached slower worker wins when estimated finish time is lower.
- [ ] GPU-required task never reaches CPU-only worker.
- [ ] Concurrent schedulers cannot double-lease a task.
- [ ] Frame-local task splits into balanced shards.
- [ ] Windowed shard trims padded frames correctly.
- [ ] Stateful/global work is not split without valid checkpoint policy.
- [ ] Adjacent shards contain no missing/duplicate frames.
- [ ] Compatible shards finalize without full re-render.
- [ ] Restart reconstructs queue, leases and reservations.
- [ ] Speculative duplicate cannot double-finalize.
- [ ] Multiple projects receive fair progress.
- [ ] Critical-path report matches known fixture.

## Acceptance criteria

- [ ] Placement is capability-, resource-, cost- and locality-aware.
- [ ] Every assignment is explainable and deterministically tie-broken.
- [ ] Scheduling is event-driven and restart-reconstructible.
- [ ] Safe work shards by integer frame ranges with deterministic output.
- [ ] Unsafe work is never split implicitly.
- [ ] Finalizer validates and assembles without hidden re-encode.
- [ ] Critical path and parallel efficiency are available per project.
- [ ] Fleet cache metadata remains bounded and non-authoritative.
- [ ] No second scheduler, estimator, reservation or shard path exists.
- [ ] `SKIP_HEAVY=1 make verify` and `make pilot` pass after rebase.

## Out of scope

Kubernetes, infrastructure autoscaling, peer-to-peer authoritative state, browser UI, ML prediction and PostgreSQL migration.
