# PR 2 - Task DAG and late composition

## Metadata

Title: `feat(taskgraph): execute DAGs and prerender independent layers`

Branch: `codex/task-dag-late-composition`

Depends on: PR 1 merged into `main`.

## Goal

Expand one job into multiple dependent tasks and process independent overlays while stock assets are still being prepared.

```text
stock prepare --------------------┐
                                 ├-> scene composite -> final artifact
text compile -> overlay RGBA -----┘
```

The overlay branch must not wait for stock unless it requires stock metadata, tracking data, masks, or pixels.

## Allowed scope

```text
DataServer/internal/renderplan/**
DataServer/internal/taskgraph/**
DataServer/internal/artifacts/**
DataServer/internal/assets/**
DataServer/internal/jobs/** only for plan/task creation
DataServer/internal/outbox/** only for readiness events
DataServer/cmd/server/bootstrap*.go
RemoteCodex/native/worker-agent-go/** only for initial executors
shared/** for versioned contracts
scripts/ci/** for guards
```

Do not add adaptive scheduling, temporal sharding, GPU policy, or frontend work.

## Required contracts

### RenderPlan

Create an immutable, versioned plan with:

```text
id, contract_version, project_id, job_id
width, height, fps, color_space, alpha_mode
duration_ms, nodes, content_hash, created_at
```

Any semantic change creates a new plan and hash.

### Dependency modes

```text
independent
requires_metadata
requires_tracking
requires_pixels
```

### Execution policies

```text
compile_only
prerender_rgba
render_at_composite
```

### Reusable artifact types

```text
render_plan
normalized_asset
overlay_rgba
precomp_rgba
tracking_data
mask
scene_segment
audio_stem
final_video
```

## Operational TODO

### PR 2.0 - Baseline

- [ ] Start from updated `origin/main` after PR 1 merge.
- [ ] Run `SKIP_HEAVY=1 make verify`.
- [ ] Inventory current job-to-renderer and artifact-finalization paths.
- [ ] Confirm there is no second queue, cache database, or artifact registry.

### PR 2.1 - Immutable RenderPlan

- [ ] Create `internal/renderplan/model.go` and validation.
- [ ] Add deterministic serialization and content hashing.
- [ ] Normalize map/list ordering before hashing.
- [ ] Persist plan metadata through a repository.
- [ ] Store plan bytes through the canonical artifact/blob path.
- [ ] Reject mutation of a published plan.
- [ ] Add deterministic hash and validation tests.

### PR 2.2 - Compiler entry point

- [ ] Add one explicit compiler interface from job input to RenderPlan plus tasks.
- [ ] Register compilers in one canonical registry.
- [ ] Require explicit pipeline and contract versions.
- [ ] Remove new reliance on heuristic payload detection.
- [ ] Validate the full result before persistence.

### PR 2.3 - Dependency persistence

- [ ] Persist task edges atomically with plan and tasks.
- [ ] Add cycle detection.
- [ ] Reject missing nodes, self-edges, and duplicate edges.
- [ ] Add graph read model and status inspection.
- [ ] Test diamond DAG, cycle rejection, and transaction rollback.

### PR 2.4 - Readiness propagation

- [ ] Mark a task READY only when all dependencies succeeded or resolved from cache.
- [ ] Use the existing lifecycle/outbox path.
- [ ] Make repeated dependency events idempotent.
- [ ] Define cancellation/failure propagation.
- [ ] Test fan-out, fan-in, retry, failure, and duplicate events.

### PR 2.5 - Canonical artifact cache key

- [ ] Add one cache-key builder under the canonical artifact owner.
- [ ] Include executor ID/version, parameters, input hashes, range, output profile, engine version, and seed.
- [ ] Reject cacheable nondeterministic work without an explicit seed.
- [ ] Prove map ordering does not alter keys.
- [ ] Prove semantic changes invalidate keys.

### PR 2.6 - Cache lookup before execution

- [ ] Resolve expected output key before leasing a cacheable task.
- [ ] Reuse only verified artifacts.
- [ ] Complete cache-hit tasks through the canonical task lifecycle.
- [ ] Link reused artifacts to task outputs.
- [ ] Record cache-hit timing and byte metrics.
- [ ] Test concurrent requests for the same cached output.

### PR 2.7 - Independent overlay path

- [ ] Add `text.compile.v1`.
- [ ] Add `text.render.v1` or `precomp.render.v1` producing transparent RGBA.
- [ ] Use and record premultiplied alpha.
- [ ] Support bounded overlay output plus effect padding.
- [ ] Make font/layout work independent from stock availability.
- [ ] Persist output through the existing artifact service.

### PR 2.8 - Scene composite

- [ ] Add `scene.composite.v1` consuming stock plus overlay artifacts.
- [ ] Validate resolution, FPS, timebase, color space, and alpha mode.
- [ ] Composite all layers in one pass where possible.
- [ ] Avoid one encode per overlay.
- [ ] Preserve the existing final artifact authority.
- [ ] Add a golden test for stock Layer 0 plus text Layer 1.

### PR 2.9 - Partially dependent overlays

- [ ] Model metadata, tracking, mask, and pixel dependencies explicitly.
- [ ] Permit text/layout compilation before final dependency availability.
- [ ] Add a fixture where layout completes before tracking data.
- [ ] Add a fixture where final overlay waits for a mask artifact.

### PR 2.10 - Diagnostics and guards

- [ ] Add read-only graph diagnostics: nodes, edges, status, cache state, blocked reason, outputs.
- [ ] Extend `OWNERSHIP.md` for RenderPlan, graph publication, readiness, and cache-key ownership.
- [ ] Add guards against a second cache-key implementation.
- [ ] Add guards against direct READY writes outside lifecycle service.
- [ ] Add guards against artifact persistence outside canonical owners.

## Required tests

```bash
cd DataServer
gofmt -w internal/renderplan internal/taskgraph internal/artifacts
go test -race -count=1 ./internal/renderplan/...
go test -race -count=1 ./internal/taskgraph/...
go test -race -count=1 ./internal/artifacts/...
go test -race -count=1 ./internal/assets/...
go test -race -count=1 ./internal/outbox/...
go test -race -count=1 ./cmd/server/...

cd ../RemoteCodex/native/worker-agent-go
gofmt -w ./internal ./pkg/video
go test -race -count=1 ./internal/...
go test -race -count=1 ./pkg/video/...

cd ../../..
SKIP_HEAVY=1 make verify
```

## Required integration scenarios

- [ ] Stock preparation is slower than overlay rendering.
- [ ] Independent overlay succeeds before stock is ready.
- [ ] Composite remains blocked until both verified inputs exist.
- [ ] Cached overlay skips worker execution.
- [ ] Changing text invalidates only overlay and downstream composite.
- [ ] Changing stock reuses unchanged overlay.

## Acceptance criteria

- [ ] One job owns a multi-task DAG.
- [ ] DAG publication is atomic.
- [ ] Independent overlay work starts before stock readiness.
- [ ] Composite starts only after verified dependencies exist.
- [ ] Cache hits obey lifecycle and ownership rules.
- [ ] Alpha mode is explicit and consistent.
- [ ] No duplicate artifact/cache subsystem exists.
- [ ] `SKIP_HEAVY=1 make verify` passes.

## Out of scope

Dynamic CPU/GPU matching, worker registry redesign, temporal sharding, cost prediction, speculative execution, and frontend work.
