# PR 3 - Persistent execution DAG and late composition

## Metadata

Title: `feat(taskgraph): publish execution DAGs and reusable precompositions`

Branch: `codex/task-dag-late-composition`

Depends on: PR 2 merged into `main` with canonical executor registry and task transport.

## Naming rule: ExecutionPlan vs RenderPlan

Velox already has a worker-engine `RenderPlan V1` consumed by the C++/FFmpeg renderer. Do not create a second incompatible type with the same name.

This PR introduces:

```text
ExecutionPlan
  master-owned immutable description of tasks and dependencies.

RenderPlan V1/V2
  worker-engine contract produced for one rendering/compositing executor.
```

An ExecutionPlan may reference multiple task-specific RenderPlan artifacts.

## Current state to reuse

From earlier PRs and current `main`:

- persistent Task and TaskAttempt domains;
- typed task transport and execution reports;
- canonical executor registry and TaskRunner;
- persistent local worker cache;
- artifact service and BlobStore;
- asset resolver registry;
- outbox and background supervisor;
- current worker pipeline registry and RenderPlan engine path.

Still missing:

- immutable master-owned ExecutionPlan;
- multi-task dependency persistence;
- atomic graph publication;
- readiness propagation;
- deterministic artifact cache keys;
- cache-hit task completion;
- independent overlay/precomp executors;
- scene compositing from verified artifacts;
- blocked-reason and graph diagnostics.

## Goal

Support this deterministic flow:

```text
asset.prepare.v1 ------------------------------┐
                                              |
text.compile.v1 -> text.render.v1 ------------+-> scene.composite.v1 -> final artifact
                                              |
optional tracking/mask dependency ------------┘
```

Independent text/overlay work runs while stock ingestion is pending. Composite work starts only after every required verified artifact exists.

## Canonical package boundaries

```text
DataServer/internal/executionplan
  Immutable plan model, planner registry, validation and repository.

DataServer/internal/taskgraph
  Tasks, dependency edges, readiness and lifecycle.

DataServer/internal/artifacts
  Verified artifact identity and canonical cache-key builder.

DataServer/internal/assets
  Source resolution and ingestion coordination.

RemoteCodex/.../internal/executor
  Registered asset/text/precomp/composite executors.

RemoteCodex/.../pkg/video/plan
  Existing low-level engine RenderPlan contract.
```

Do not create `internal/renderplan` on the master. Use `executionplan` to avoid semantic duplication.

## ExecutionPlan contract

```go
type ExecutionPlan struct {
    ID              string
    ContractVersion int
    JobID           string
    ProjectID       string
    PipelineID      string
    PipelineVersion int
    Tasks           []TaskSpec
    Edges           []DependencyEdge
    FinalTaskID     string
    ContentHash     string
    CreatedAt       time.Time
}
```

Rules:

- immutable after publication;
- deterministic canonical serialization;
- content hash covers semantic fields, tasks, edges and final output;
- task and edge order is normalized before hashing;
- final task must exist;
- all executor IDs/versions must be known contracts;
- all graphs must be acyclic;
- any semantic change creates a new plan ID/hash.

## Planner registry

Add one master-side planner registry:

```text
pipeline_id + pipeline_version -> Planner
```

This registry owns high-level Job input -> ExecutionPlan compilation.

It is not a duplicate of the worker pipeline registry:

- master planner creates tasks and edges;
- worker pipeline compiler creates low-level RenderPlan for one executor.

Use explicit names and package boundaries so the two responsibilities cannot drift.

## Dependency modes

```text
independent
requires_metadata
requires_tracking
requires_mask
requires_pixels
```

## Execution policies

```text
compile_only
prerender_rgba
render_at_composite
```

Initial planner decisions:

```text
independent + cheap
  -> compile_only or render in final composite

independent + expensive/reusable
  -> prerender_rgba

requires tracking/mask/pixels
  -> explicit dependency edge
```

## Artifact cache identity

One canonical cache-key builder under the artifact owner:

```text
hash(
  executor_id
  executor_version
  normalized_parameters
  ordered_input_artifact_hashes
  requested_time_range
  output_profile
  engine_version
  deterministic_seed
)
```

Rules:

- no feature package creates its own cache-key algorithm;
- nondeterministic work is not cacheable without an explicit seed;
- only verified artifacts are cache hits;
- cache hit completes the Task through canonical lifecycle;
- artifact metadata records alpha mode, color space, resolution, frame rate and bounds.

## Late-composition artifact types

```text
normalized_asset
text_layout
tracking_data
mask
overlay_rgba
precomp_rgba
scene_segment
audio_stem
final_video
```

Use premultiplied alpha internally for RGBA artifacts.

## Operational TODO

### PR 3.0 - Baseline

- [ ] Start from updated `origin/main` after PR 2.
- [ ] Run `SKIP_HEAVY=1 make verify` and `make pilot`.
- [ ] Confirm every future executor enters the canonical registry.
- [ ] Confirm no second artifact/cache repository exists.
- [ ] Inventory current compiler input and RenderPlan V1 generation.

### PR 3.1 - ExecutionPlan domain

- [ ] Create `internal/executionplan/model.go`, validation and deterministic serializer.
- [ ] Use `ExecutionPlanID` in Task identity; keep worker RenderPlan references as input artifacts.
- [ ] Add pipeline/planner ID and version.
- [ ] Add normalized content hashing.
- [ ] Add tests for stable hash, semantic invalidation, invalid final task and unknown executor.

### PR 3.2 - Planner registry

- [ ] Create one explicit master planner registry.
- [ ] Reject duplicate pipeline/version registration.
- [ ] Require explicit `pipeline_id` and version at ingress.
- [ ] Remove heuristic pipeline detection from canonical planning.
- [ ] Keep registration explicit in bootstrap.
- [ ] Add completeness and deterministic-order tests.

### PR 3.3 - Plan and graph persistence

- [ ] Add forward-only migrations for `execution_plans` and `task_dependencies`.
- [ ] Persist plan, tasks, immutable specs and edges in one transaction.
- [ ] Reject partial publication.
- [ ] Add unique edge constraints and dependency indexes.
- [ ] Add cycle detection before transaction commit.
- [ ] Add tests for diamond graph, missing node, duplicate edge, cycle and rollback.

### PR 3.4 - Readiness lifecycle

- [ ] Extend Task lifecycle with dependency-aware PENDING -> READY transition.
- [ ] READY only when all required dependencies succeeded or resolved from verified cache.
- [ ] Use outbox events; do not poll the entire task table in a tight loop.
- [ ] Make repeated dependency events idempotent.
- [ ] Define failure and cancellation propagation.
- [ ] Reconstruct readiness after master restart from repositories.
- [ ] Add fan-out, fan-in, retry, cancellation and restart tests.

### PR 3.5 - Canonical artifact cache key

- [ ] Implement the single cache-key builder under `internal/artifacts`.
- [ ] Normalize parameter serialization and ordered inputs.
- [ ] Include engine/executor versions and deterministic seed.
- [ ] Add verified cache lookup before task lease.
- [ ] Complete cache-hit tasks through Task lifecycle and link output artifact IDs.
- [ ] Add concurrency test proving duplicate requests do not duplicate final artifacts.

### PR 3.6 - `asset.prepare.v1`

- [ ] Register a real asset preparation executor.
- [ ] Resolve Drive/source references through the existing asset resolver/service.
- [ ] Download a source once per content/version identity.
- [ ] Produce verified normalized/original artifact metadata.
- [ ] Record source bytes and cache metrics.
- [ ] Do not let rendering workers access Drive through ad-hoc handlers.

### PR 3.7 - `text.compile.v1`

- [ ] Register pure text layout/shaping compilation.
- [ ] Resolve fonts by verified artifact/hash.
- [ ] Produce typed layout artifact, not rendered full-frame video.
- [ ] Make output independent from stock pixels.
- [ ] Include renderer/font/layout version in cache identity.
- [ ] Add deterministic layout tests.

### PR 3.8 - `text.render.v1` and `precomp.render.v1`

- [ ] Render transparent premultiplied RGBA artifacts.
- [ ] Support bounded output rectangles instead of unconditional full-frame output.
- [ ] Calculate padding for blur, shadow, glow, rotation and motion bounds.
- [ ] Record bounds/origin in artifact metadata.
- [ ] Use the existing engine path or one adapter; do not add a second renderer.
- [ ] Add alpha-edge and cache-reuse golden tests.

### PR 3.9 - `scene.composite.v1`

- [ ] Consume verified stock/normalized asset plus zero or more RGBA/precomp artifacts.
- [ ] Validate frame rate, timebase, dimensions, color space and alpha mode.
- [ ] Composite all layers in one pass where possible.
- [ ] Encode once per scene output, not once per overlay.
- [ ] Produce verified `scene_segment` artifact.
- [ ] Add golden test for Layer 0 stock plus Layer 1 animated text.

### PR 3.10 - Dependent overlays

- [ ] Represent metadata, tracking, mask and pixel requirements as graph edges.
- [ ] Permit independent compile/layout work before those dependencies are ready.
- [ ] Add fixture: text shaping succeeds while stock downloads.
- [ ] Add fixture: tracked label waits only for tracking artifact.
- [ ] Add fixture: text-behind-person waits only for mask artifact.
- [ ] Ensure changing stock reuses unchanged text/precomp artifacts.

### PR 3.11 - Final-output ownership

- [ ] ExecutionPlan identifies exactly one final task.
- [ ] Final task output flows through canonical artifact verification.
- [ ] Job `SUCCEEDED` remains owned by artifact finalization.
- [ ] Graph/task success alone cannot finalize the Job.
- [ ] Add duplicate completion and stale-attempt tests.

### PR 3.12 - Diagnostics and guards

- [ ] Add read-only graph diagnostics: plan hash, nodes, edges, status, blocked reason, cache state and outputs.
- [ ] Add pagination/bounds for large graphs.
- [ ] Extend `OWNERSHIP.md` for ExecutionPlan, planner registry, graph publication, readiness and cache key.
- [ ] Add guards against `internal/renderplan`, duplicate planner registries and direct READY writes.
- [ ] Add guard against artifact writes outside canonical owners.

## Required verification

```bash
cd DataServer
gofmt -w internal/executionplan internal/taskgraph internal/artifacts internal/assets
go test -race -count=1 ./internal/executionplan/...
go test -race -count=1 ./internal/taskgraph/...
go test -race -count=1 ./internal/artifacts/...
go test -race -count=1 ./internal/assets/...
go test -race -count=1 ./internal/outbox/...
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

- [ ] Overlay completes before delayed stock preparation.
- [ ] Composite remains blocked until all verified inputs exist.
- [ ] Verified overlay cache hit skips worker execution.
- [ ] Text change invalidates only text/precomp and downstream composite.
- [ ] Stock change reuses unchanged text/precomp.
- [ ] Restart reconstructs graph readiness.
- [ ] Duplicate dependency events do not duplicate execution/finalization.

## Acceptance criteria

- [ ] One Job can own an immutable multi-task ExecutionPlan.
- [ ] Plan/tasks/edges publish atomically.
- [ ] Independent overlays execute before stock readiness.
- [ ] All new work enters the executor registry from PR 2.
- [ ] Cache keys are deterministic and centrally owned.
- [ ] Alpha/color/time contracts are explicit.
- [ ] Existing RenderPlan engine path is reused, not duplicated.
- [ ] Job finalization remains single-writer.
- [ ] `SKIP_HEAVY=1 make verify` and `make pilot` pass after rebase.

## Out of scope

Cost-based worker ranking, temporal sharding, speculative execution, autoscaling and frontend work.
