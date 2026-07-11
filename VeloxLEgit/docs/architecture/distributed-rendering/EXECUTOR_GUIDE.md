# How to add a new executor

This guide is the canonical "copy-paste-me" recipe for shipping a new
executable task type on the Velox worker agent. It complements
`docs/architecture/OWNERSHIP.md` (which row of the table owns the
runtime) and the PR-03 design document (which row of the plan your
PR falls under).

The single rule, repeated everywhere:

> **Every executable task type on the worker resolves through one
> place: `internal/executor.Registry`. A second map, switch, or
> handler that routes by `job.JobType` is a regression and is caught
> by `scripts/ci/check-architecture.sh`.**

## When you need a new executor

You need a new executor when:

- A new job type shows up in the master-side scheduler that the
  worker cannot yet resolve.
- An existing implementation (e.g. a `pkg/video` adapter) duplicates
  rendering logic that should be reusable under a stable contract.
- A capability (audio mix, video concat, scene composite, …) is
  stable enough to advertise in the worker hello so the master can
  schedule against it.

You do NOT need a new executor when:

- You are adding a new schema field to an existing executor (bump the
  version in `Descriptor` and emit the new `(ID, version)` key).
- You are adding a new pipeline compiler underneath an existing
  executor (e.g. a new `clips.v2` compiler feeding the existing
  `video.concat.v1` executor).
- You are changing transport/serialisation, not work content.

## Files you will touch

| Layer | File | Why |
| --- | --- | --- |
| Worker (mandatory) | `cmd/velox-worker-agent/main.go` | Composition root — instantiate + register the executor |
| Adapter (mandatory) | `internal/taskrunner/executors/<your>.go` | Concrete `executor.Executor` impl wrapping the underlying engine |
| Adapter tests (mandatory) | `internal/taskrunner/executors/<your>_test.go` | Table-driven `Validate`/`Execute`/panic tests against the contract |
| `OWNERSHIP.md` (mandatory) | `docs/architecture/OWNERSHIP.md` | Add the row that claims your executor + the row(s) it replaces |
| CI guard (almost always) | `scripts/ci/check-architecture.sh` | Already enforces "no second dispatch map"; you rarely need to extend it |

The worker package (`internal/worker/**`) MUST NOT import your
executor; the composition root does the wiring (`worker.WithRegistry`
already accepts an externally-populated registry — see PR-3.5).

## Step 1 — Define the `Descriptor`

Pick an ID and version. The ID format is `<domain>.<verb>.v<N>`. Use
the canonical enums in `internal/executor/types.go` (`ResourceClass`,
`TemporalMode`). Avoid `@` in the ID (registry key format).

```go
// Descriptor — the static, immutable description advertised in worker hello.
return executor.Descriptor{
    ID:            "audio.mix.v1",
    Version:       1,
    InputTypes:    []string{"track.input"},
    OutputTypes:   []string{"audio.output"},
    ResourceClass: executor.ResourceCPU,
    TemporalMode:  executor.TemporalWindowed,
    Deterministic: false,  // true ONLY if the executor can produce
                           // byte-identical output for byte-identical input
    Cacheable:     true,   // true ONLY if the master may reuse outputs by content hash
    SupportsAlpha: false,
}
```

Hard rule on `Deterministic` and `Cacheable`:

| Truth | Set |
| --- | --- |
| Same `payload` + same `seed` ⇒ byte-identical output | `Deterministic: true`, `Cacheable: true` |
| Same `payload` + same `seed` ⇒ byte-identical output MAY vary across engines | `Deterministic: false`, `Cacheable: true` |
| Output depends on wall time / random | `Deterministic: false`, `Cacheable: false` |

If you set `Deterministic: true`, your `Validate` MUST reject any
payload that bypasses the seed. If you set `Cacheable: true`, your
`Execute` MUST call `execCtx.Spec()` to produce a stable
`SpecHash` (use `executor.ComputeSpecHash(payload)`) so cached
outputs are keyed correctly.

## Step 2 — Implement the contract

```go
package executors

import (
    "context"
    "fmt"
    "velox-worker-agent/internal/executor"
)

type AudioMix struct {
    /* backends go here — pipeline.Runner, audio.Encoder, … */
}

func NewAudioMix(/* deps */ *AudioMix) *AudioMix { /* validate + return */ }

func (a *AudioMix) Descriptor() executor.Descriptor { /* copy of the field above */ }

func (a *AudioMix) Validate(spec executor.TaskSpec) error {
    if spec.Payload == nil {
        return fmt.Errorf("%w: payload is required", executor.ErrInvalidDescriptor)
    }
    // strict schema validation here. TaskRunner calls Validate BEFORE
    // resource acquisition, so a missing field can't bleed through
    // into a 30-minute render.
    return nil
}

func (a *AudioMix) Execute(
    ctx context.Context,
    execCtx executor.ExecutionContext,
    spec executor.TaskSpec,
) (executor.ExecutionResult, error) {
    // Respect ctx: check execCtx.Done() in any inner loop. The C++
    // engine can't preempt mid-run; admDoc that on Descriptor.TemporalMode.
    // NEVER call lifecycle APIs from here (TaskRunner.Run owns reports).
    // EVERY ArtifactRef MUST carry a hash that matches the file. The
    // local cache + blob store use the hash for content addressing.
    //
    // Wrap errors into ExecutionResult.Status="failed" + ErrorCode +
    // ErrorDetail; returned (result, err) non-nil err is reserved for
    // panics / catastrophic failures (TaskRunner.Run code-maps them).
    return executor.ExecutionResult{
        Status:      "succeeded",
        Outputs:     []executor.ArtifactRef{{Type: "audio.output", Hash: hash, URI: path}},
        StartedAt:   /* now */,
        CompletedAt: /* now */,
    }, nil
}
```

## Step 3 — Registration (composition root ONLY)

In `cmd/velox-worker-agent/main.go`, after `executor.NewRegistry()`:

```go
registry := executor.NewRegistry()
registry.MustRegister(executors.NewAudioMix(/* deps */))

w, workerErr := worker.New(cfg, resolvedVersion,
    worker.WithRegistry(registry),
    worker.WithCache(localCache),
    worker.WithBlobs(blobs),
)
```

The worker now advertises `audio.mix@1` in hello + heartbeat. The
master scheduler sees it and starts dispatching matching jobs.
Do NOT register executors anywhere else; the composition root is the
single point that decides "which executors does this binary ship".

## Step 4 — Tests

`internal/taskrunner/executors/<your>_test.go` MUST cover:

1. `Descriptor` returns the same fields every time and passes
   `Descriptor.Validate`.
2. `Validate` rejects nil payload AND rejects payloads missing
   required fields.
3. `Execute` succeeds against a fake backend, returns
   `Status="succeeded"` and the canonical `ArtifactRef` shape.
4. `Execute` with a backend error maps to `Status="failed"` +
   `ErrorCode` set, NOT to a second return value.
5. `Execute` honours `ctx` cancellation when the backend is
   obviously cooperative.
6. `New<Your>(nil)` panics loudly (mirrors `NewSceneComposite`'s
   contract).

Plus, inside `internal/worker/job_executor_dispatch_test.go` (or a
new sibling file), one integration test that registers your executor
in a fresh `Registry` and runs `Worker.executeJob` end-to-end. The
existing `fakeSceneComposite` is the model.

## Step 5 — Capability advertisement (already wired)

`Worker.buildHello` reads `executor.BuildCapabilityReport(registry, host)`
which iterates `registry.Descriptors()` and maps each into
`api.ExecutorCapability`. There is nothing to wire — once you
Register at the composition root, hello + heartbeat advertise the
executor for free. Tests asserting this live in
`internal/worker/worker_hello_test.go`.

## Step 6 — CI guards (already in place)

`scripts/ci/check-architecture.sh` PR-3.9 rule blocks reintroduction
of `case "render":`, `case "process_video":`, `executeWorkflowJob`,
`runRenderJob`, `runVideoJob`, `runAudioJob`, `newVideoWorkflow`
inside `RemoteCodex/native/worker-agent-go/internal/worker/*.go`.
If you ever feel tempted to add a new case arm there, that's a red
flag — add a new executor instead.

`scripts/ci/check-single-writer.sh` continues to enforce single
writer for SQL/lifecycle symbols. If your executor mutates task or
job lifecycle, it's WRONG — talk to the canonical owner
(`internal/taskgraph`, `internal/taskattempts`) instead.

## Step 7 — OWNERSHIP.md row

Add (or extend) a row claiming your adapter. Example:

```
| Audio mix adapter (worker-side) | `internal/taskrunner/executors.AudioMix` (registered under `audio.mix.v1@1` in `cmd/velox-worker-agent/main.go`) | Hand-rolled wav concatenation inside executor bodies; bypass of registry from any other entry point
```

Update `.github/CODEOWNERS` so reviewers are auto-assigned.

## Determinism / cacheability / seeds — in depth

- `Deterministic: true` = the master may re-run the same payload
  twice and trust the second run's output equals the first. Implies
  reproducible RNG (Mulberry32 with explicit seed; NEVER
  `math/rand` default source).
- `Cacheable: true` = the master may keep the `ArtifactRef` by hash
  and reuse it across jobs that have identical `SpecHash`. Implies
  outputs are content-addressed (`sha256` over every byte of the
  output file before populating `ArtifactRef.Hash`).
- Seeds MUST flow through `spec.Payload["seed"]` (uint64
  canonicalised, never derived from `time.Now().UnixNano()`).

If your executor cannot meet these constraints, set the flags
`false`. A wrong `true` is a master-side scheduling bug (the master
will cache a non-deterministic output and reuse it across runs that
should have produced different bytes).

## Quick checklist before opening the PR

- [ ] `Descriptor` ID/version match your PR title.
- [ ] `Validate` returns one of the canonical sentinel errors
      (`executor.ErrInvalidDescriptor`, `executor.ErrValidation`,
      etc.) with `errors.Is`-compatible wrapping.
- [ ] `Execute` produces exactly ONE `TaskExecutionReport`-shaped
      result; no goroutine leaks; no `os.Exit`; no direct SQL.
- [ ] Composition root calls `MustRegister` exactly once for your
      executor; no second registration site exists.
- [ ] OWNERSHIP.md row added/extended; CODEOWNERS updated.
- [ ] `go test -race -count=1 ./internal/taskrunner/executors/...`
      passes; `SKIP_HEAVY=1 make verify` (or its worker-only
      subset) passes locally.
- [ ] No new imports of `pkg/video/pipeline` from `internal/worker/*`.
- [ ] `scripts/ci/check-architecture.sh` and
      `scripts/ci/check-single-writer.sh` exit 0 locally.

If any box fails, your PR regresses a PR-3 invariant.
