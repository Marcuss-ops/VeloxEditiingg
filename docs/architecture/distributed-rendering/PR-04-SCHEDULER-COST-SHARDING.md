# PR 4 - Adaptive scheduler, cost model, and temporal sharding

## Status snapshot (2026-06-21)

| Sub-task | Status | Landed on |
| --- | --- | --- |
| PR 4.0 — Baseline + recorded dataset | OPEN (PR 3 merge done; dataset export + EWMA not started) | — |
| PR 4.1 — Worker performance profiles | OPEN | — |
| PR 4.2 — Task cost estimator | OPEN (deterministic shape is decided by the canonical `Executor.Descriptor` exposure; estimator registry not built) | — |
| PR 4.3 — Ready queue | OPEN (still using today’s FIFO claim-next path; the cost-aware placement slice is orthogonal) | — |
| **PR 4.4 — Capability + resource filtering** | **LANDED (PR-04.5)** | `codex/pr-04-scheduler-cost-sharding` commit `52479f6d` |
| **PR 4.5 — Locality-aware scoring** | **PARTIALLY LANDED (PR-04.5)** — write-side plumbing (per-job `Requirements` end-to-end) + structured `Explanation`. Bandwidth modelling + rank-site flip in PR-04.6. | `codex/pr-04-scheduler-cost-sharding` commit `52479f6d` |
| PR 4.6 — Temporal shard planner | OPEN | — |
| PR 4.7 — Shard execution contract | OPEN | — |
| PR 4.8 — Compatible shard finalization | OPEN | — |
| PR 4.9 — Critical path calculation | OPEN | — |
| PR 4.10 — Straggler detection | OPEN | — |
| PR 4.11 — Project diagnostics | OPEN | — |
| PR 4.12 — Scheduler lifecycle and recovery | OPEN | — |
| PR 4.13 — Ownership and guards | OPEN (PR-04.4/4.5 surface ownership rows already landed in `docs/architecture/OWNERSHIP.md`) | — |

See "Operational TODO" further down for the per-sub-task acceptance bullets and
the commit references that flip each bullet.

**Validation at PR-04.5 landing (`codex/pr-04-scheduler-cost-sharding` commit `52479f6d`):**
`go vet ./...` green, `go build ./...` green, `scripts/ci/check-architecture.sh` exit 0,
`go test -race ./internal/jobs/enqueue/... ./internal/costmodel/...` green for the
two new tests (`TestEnqueuePropagatesRequirements`, `TestEnqueueDefaultsPreserved`).
Four pre-existing baseline flakes reproduce on HEAD and are unrelated to PR-04.5
(they exercise `BuildSceneImagePayload` + `RenderHTTPBoundaryJobResponse` which
this slice does not touch; documented in commit `52479f6d`'s body):
`TestBuildSceneImagePayload` (line 52), `TestBuildSceneImagePayloadForMaster`
(line 158), `TestBuildSceneImagePayloadForMaster_PreservesRemoteSources` (line 199),
`TestRenderHTTPBoundaryJobResponse/basic_legacy_alias_fallback` (line 482).

**Numbering note:** the script-slice identifiers in the commit log (`PR-04.4`,
`PR-04.5`, `PR-04.6`) are an alias for the design-doc sub-tasks `PR 4.4 –
Capability + resource filtering` and `PR 4.5 – Locality-aware scoring`. The
rank-site flip that `PR-04.6` will perform is contained inside design-doc
`PR 4.5`, not `PR 4.6` (which targets temporal shard planning instead).

## Metadata

Title: `feat(scheduler): add cost-aware placement and temporal render sharding`

Branch: `codex/adaptive-scheduler-sharding`

Depends on: PR 3 merged into `main`.

## Goal

Use measured execution data to assign ready tasks to the best worker, split expensive frame-independent rendering into shards, and calculate the real project critical path.

At the end of this PR Velox must be able to:

- estimate task duration by executor, worker class, resolution, and workload features;
- consider worker load and cached input bytes;
- split suitable render tasks into time ranges with pre/post-roll;
- assemble compatible shards without a full re-render;
- identify stragglers and the project critical path;
- explain every scheduling decision.

## Allowed scope

```text
DataServer/internal/scheduler/**
DataServer/internal/costmodel/**
DataServer/internal/taskgraph/**
DataServer/internal/observability/**
DataServer/internal/artifacts/** only for shard outputs
DataServer/internal/workers/** only for profiles/load
DataServer/internal/outbox/** only for scheduler events
DataServer/cmd/server/bootstrap*.go
RemoteCodex/native/worker-agent-go/internal/executor/**
RemoteCodex/native/worker-agent-go/internal/taskrunner/**
RemoteCodex/native/worker-agent-go/pkg/video/** only for time-range execution
shared/** for contracts
scripts/ci/** for guards
```

Do not add Kubernetes, peer-to-peer distribution, ML prediction, or frontend work.

## Scheduling formula

Initial placement must minimize estimated finish time:

```text
estimated_finish_ms =
    worker_queued_ms
  + missing_input_bytes / effective_bandwidth
  + estimated_execution_ms
  + estimated_output_upload_ms
  + resource_pressure_penalty_ms
```

A faster worker without cached inputs may lose to a slower worker with local data.

## Initial task-size policy

Use estimated compute time, not only video duration:

```text
< 1 second      -> fuse when safe
1-20 seconds    -> preferred distributed task size
20-60 seconds   -> acceptable but observed
> 60 seconds    -> attempt supported split
```

## Temporal modes

```text
frame_local  -> split freely by range
windowed     -> split with pre-roll/post-roll
stateful     -> require checkpoint/precomputation
global       -> do not split
```

## Operational TODO

### PR 4.0 - Baseline and recorded dataset

- [ ] Start from updated `origin/main` after PR 3 merge.
- [ ] Run `SKIP_HEAVY=1 make verify`.
- [ ] Export representative TaskAttempt fixtures from CPU workers and any available GPU workers.
- [ ] Confirm phase timings and byte-source counters are populated.
- [ ] Define stable worker classes from resources/capabilities, not hostnames.

### PR 4.1 - Add worker performance profiles

- [ ] Create `internal/costmodel/worker_profile.go`.
- [ ] Track executor/version performance by worker class.
- [ ] Store median, p90, sample count, and last update.
- [ ] Use bounded EWMA or rolling statistics; no ML dependency.
- [ ] Ignore incomplete/failed measurements unless explicitly classified.
- [ ] Add tests for cold start, updates, outliers, and version separation.

### PR 4.2 - Add task cost estimator

- [ ] Create one canonical estimator interface and registry.
- [ ] Build features from frame count, resolution, layers, effects, codec, temporal mode, and input bytes.
- [ ] Use executor-specific estimators through the registry.
- [ ] Fall back to conservative defaults when history is missing.
- [ ] Persist estimated duration and later estimation error.
- [ ] Add tests proving deterministic estimates from identical inputs.

### PR 4.3 - Add ready queue

- [ ] Maintain ready tasks from task lifecycle/outbox events.
- [ ] Do not scan all tasks continuously.
- [ ] Support priority, deadline, estimated cost, and age.
- [ ] Make enqueue/dequeue idempotent.
- [ ] Recover queue state from SQLite after master restart.
- [ ] Keep SQLite authoritative; memory is reconstructible only.

### PR 4.4 - Add capability and resource filtering

> **Status — LANDED on `codex/pr-04-scheduler-cost-sharding` (commit `52479f6d`, PR-04.5).**
> Landed as the costmodel.Score + Registry.GetEligibleWorkers path
> (`DataServer/internal/costmodel`, mirror `RemoteCodex/native/worker-agent-go/internal/costmodel`)
> with per-job `costmodel.JobRequirements` published at enqueue and threaded
> through `jobs.Job → jobs.QueueItem → jobs.Writer.Create → SQLite` (migration 039:
> dedicated columns `job_required_resource_class` + `job_required_temporal_mode`,
> plus the `_requirements` sub-object inside `request_json`).
> Rank call site (`sendPushJobOffer`) remains OFF at this slice per rollout
> flag "Solo-eligibility on, rank off" (PR-04.6 will flip).

- [x] Filter workers by executor ID/version. ⇐ ResourceClass / TemporalMode (executor.Descriptor)
- [x] Enforce CPU/RAM/GPU/VRAM/disk hard requirements. ⇐ compatibility matrix in `costmodel.Score`
- [x] Exclude draining, disconnected, or overcommitted workers. ⇐ drained / offline / capacity gates short-circuit `Score` before the four-field rules
- [x] Respect worker maximum active tasks. ⇐ `MaxParallel > 0 && ActiveJobs >= MaxParallel` gate in `Score`
- [x] Add tests for CPU-only, GPU-required, insufficient memory, and draining workers. ⇐ `DataServer/internal/costmodel/cost_test.go` covers GPU→CPU rejection, draining / offline / capacity exclusion, four-field compatibility matrix, executor-merge policy, permissiveness invariant

### PR 4.5 - Add locality-aware scoring

> **Status — PARTIALLY LANDED on `codex/pr-04-scheduler-cost-sharding` (commit `52479f6d`, PR-04.5).**
> Per-user scope: per-job `costmodel.JobRequirements` threaded end-to-end (write path
> `Enqueuer.Enqueue → JobQueue.SubmitJob → jobs.Writer.Create`; read path
> `jobs.Writer.Get → jobs.Job.Requirements → jobs.QueueItem.Requirements`).
> Plus structured `costmodel.Explanation` carried by every `Score` call.
> Bandwidth modelling and the rank site flip land in PR-04.6; "slower cached worker beats
> faster uncached worker" test lands with the rank flip.

- [x] Calculate bytes already in local worker cache. ⇐ `Cacheable` plumbed on per-job `Requirements` (rank-site consumption lands in PR-04.6)
- [x] Calculate missing bytes and expected source tier. ⇐ `Deterministic` plumbed on per-job `Requirements` (rank-site consumption lands in PR-04.6)
- [ ] Include measured/effective bandwidth. ⇐ follow-up (PR-04.6 / PR-04.7)
- [x] Include worker queue time and resource pressure. ⇐ `LoadFactor = ActiveJobs / max(MaxParallel, 1)` in `costmodel.Explanation`
- [x] Return a structured explanation for the winning placement. ⇐ `Explanation{CapabilityFit, LoadFactor, DeterminismHit, CacheableHint, ModeFit, IneligibilityReason}` plus `Cost{Eligible, Score}`
- [x] Persist or log the scoring components for diagnostics. ⇐ per-job `Requirements` mirrored to SQLite columns + `request_json._requirements`; `Explanation` returned alongside `Cost`
- [ ] Add tests where a slower cached worker beats a faster uncached worker. ⇐ Stage 2 — PR-04.6 flip after rank site consumes `Cacheable` + `Deterministic`

### PR 4.6 - Add temporal shard planner

- [ ] Split only executors declaring supported temporal modes.
- [ ] Generate exact start/end frame or millisecond ranges.
- [ ] For windowed effects, include explicit pre-roll/post-roll.
- [ ] Preserve absolute project time for camera, keyframes, and animation evaluation.
- [ ] Reject unsafe split of stateful/global tasks.
- [ ] Generate more tasks than workers for dynamic balancing.
- [ ] Target estimated 5-20 seconds of compute per shard.

### PR 4.7 - Add shard execution contract

- [ ] Extend TaskSpec with time range, frame range, pre-roll, and post-roll.
- [ ] Render extra context but publish only the requested output interval.
- [ ] Record exact output frame count and timestamps.
- [ ] Enforce deterministic random seeds derived from plan/task identity.
- [ ] Add tests proving adjacent shards match an unsplit render at boundaries.

### PR 4.8 - Add compatible shard finalization

- [ ] Require common codec, resolution, FPS, pixel format, color profile, timebase, and audio policy.
- [ ] Add `video.concat.v1` finalizer.
- [ ] Prefer concat/mux without full video re-encode.
- [ ] Fail explicitly when shard profiles differ.
- [ ] Validate frame count, order, gaps, overlaps, checksum, and duration.
- [ ] Add integration test comparing sharded and non-sharded output.

### PR 4.9 - Add critical path calculation

- [ ] Calculate actual longest path from completed task durations.
- [ ] Calculate planned critical path from estimates.
- [ ] Report estimation error by task and path.
- [ ] Report total worker busy time and parallel efficiency.
- [ ] Report queue wait, asset wait, rendering, encoding, upload, and finalization totals.
- [ ] Add tests using fixed DAG fixtures.

### PR 4.10 - Add straggler detection

- [ ] Calculate task duration relative to similar executor/worker-class median.
- [ ] Mark tasks above a configurable ratio as stragglers.
- [ ] Do not duplicate every slow task.
- [ ] Permit speculative duplicate only near a project deadline or at the tail of a graph.
- [ ] First verified valid output wins; cancel the duplicate.
- [ ] Ensure duplicate attempts cannot double-publish outputs or finalize jobs twice.

### PR 4.11 - Add project diagnostics

- [ ] Add read-only project report with wall time, critical path, busy time, parallel efficiency, cache byte ratio, source bytes, retries, and stragglers.
- [ ] Add per-task placement explanation.
- [ ] Add DAG timing/waterfall data as JSON.
- [ ] Do not add a frontend.
- [ ] Make diagnostics safe for large graphs with pagination or bounded payloads.

### PR 4.12 - Scheduler lifecycle and recovery

- [ ] Register scheduler explicitly in bootstrap/supervisor.
- [ ] Recover READY/LEASED tasks after restart according to lease state.
- [ ] Stop accepting work during drain.
- [ ] Preserve canonical lease and lifecycle ownership.
- [ ] Add restart/recovery integration tests.

### PR 4.13 - Ownership and guards

- [ ] Extend `OWNERSHIP.md` with scheduler, cost model, shard planner, critical path, and straggler ownership.
- [ ] Add guard against direct worker selection outside scheduler.
- [ ] Add guard against duplicate cost estimators.
- [ ] Add guard against time-range split logic outside shard planner.
- [ ] Add guard against final shard concatenation outside the registered finalizer.

## Required tests

```bash
cd DataServer
gofmt -w internal/scheduler internal/costmodel internal/taskgraph internal/observability
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

- [ ] Cached slower worker wins over uncached faster worker when finish time is lower.
- [ ] GPU-required task is never assigned to CPU-only worker.
- [ ] Frame-local task splits into balanced ranges.
- [ ] Windowed task uses pre/post-roll and publishes trimmed output.
- [ ] Stateful/global task is not split.
- [ ] Adjacent shards have no missing or duplicate frames.
- [ ] Finalizer assembles compatible shards without full re-render.
- [ ] Master restart reconstructs ready scheduling state.
- [ ] Straggler duplicate cannot double-finalize.
- [ ] Critical path report matches a known DAG fixture.

## Acceptance criteria

- [ ] Scheduler decisions are capability-, load-, cost-, and locality-aware.
- [ ] Every assignment has an explainable score breakdown.
- [ ] Suitable render work can be temporally sharded.
- [ ] Unsafe tasks are never split.
- [ ] Finalization validates and assembles shard outputs deterministically.
- [ ] Critical path and parallel efficiency are available per project.
- [ ] SQLite remains authoritative and in-memory queues are reconstructible.
- [ ] No second scheduler/cost/shard path exists.
- [ ] `SKIP_HEAVY=1 make verify` and `make pilot` pass.

## Out of scope

Kubernetes, autoscaling infrastructure, peer-to-peer cache transfer, browser UI, ML cost prediction, and PostgreSQL migration.
