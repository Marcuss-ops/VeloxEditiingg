# Velox repo identity and overlay flow

This note is the orientation point for people and coding agents entering this
repository. Read it before discussing the Master, renderer, or overlay flow.

## Who we are

This repository is **Velox**. It contains the Velox **Master server** and the
worker-side rendering implementation. The Master is not an external service
that this repository must negotiate with: `DataServer/` is the Master. The
worker agent and native media engine are also present under `RemoteCodex/`.
Shared wire contracts and timeline compilers live under `shared/`.

In broad terms, Velox accepts render jobs, validates and resolves their
inputs, compiles/persists the execution task, schedules it to a worker,
receives the rendered artifact, verifies and stores it, and runs the
configured delivery flow. The Master owns orchestration and durable job/task
state; the worker executes the render task. Both sides' code is in this repo.

Useful entry points:

- `README.md` — repository layout and operator overview.
- `docs/architecture/current-architecture.md` — current Master boundaries and
  intake flow.
- `docs/pipeline.md` — end-to-end job, worker, artifact, and delivery flow.
- `docs/architecture/OWNERSHIP.md` — canonical ownership map.
- `DataServer/internal/handlers/server/pipeline/intake_types.go` — submit
  request types, including `Layers`, `Overlays`, and `VisualReplacements`.

## Job flow in this repository

```text
Producer / creator / manifest
          │ HTTP submit (or PRE then FINALIZE)
          ▼
DataServer Master: validate → normalize assets → compile/project payload
          │ persist Job + Task; schedule and dispatch over worker control plane
          ▼
RemoteCodex worker agent → native media engine renders
          │ report result and upload artifact
          ▼
DataServer Master: verify/finalize artifact → delivery
```

The exact route depends on the producer. Canonical job submission is handled
by the pipeline HTTP intake. Creator flows and remote-engine responses are
adapted into the Master payload before enqueue. A two-stage job can submit its
base timeline at PRE and attach generated assets in FINALIZE. The Enqueuer
normalizes and persists the task; it is not an external rendering Master.

## Three fields that must not be conflated

### `layers`

`Layers` are independent visual/editorial inputs, such as titles, names,
important phrases, or additional media. They are not the same wire concept as
`overlays`. Their presence in an intake type does not imply that every render
path or compiled-plan version supports arbitrary alpha compositing; check the
selected executor and renderer contract before making that claim.

### `overlays`

The current canonical `Overlays` contract is **editorial visual replacement**
intent. Each entry identifies an asset and a half-open frame window
`[start_frame, end_frame)`, with fields such as `mode`, `z_index`, and
`audio_mode`. In the supported `replace` path, the asset supplies the picture
for that interval; it is not a transparent layer composited over the base
footage. The final audio timeline is preserved independently.

The Master validates the window and asset reference, canonicalizes accepted
asset references, and projects the typed values into the worker payload. For
`replace`, windows must be valid and non-overlapping; the creator path also
rejects a replacement that covers a clip scene. The current intake rejects
raw `mode: "composite"` because that is not an executable native overlay
layer in this contract.

On strict V2 input, the Master compiler resolves supported `replace` windows
into segments of the compiled video track. Thus the worker receives a resolved
video timeline, not a stack of transparent layers. On compatible legacy
paths, inspect the payload projection and executor behavior for that path;
do not infer compositing semantics from the field name alone.

### `visual_replacements`

`VisualReplacements` are already-prepared, finished video segments bound to
absolute timeline windows (in microseconds) and a canonical profile. They are
not overlay source layers. The Master checks them against the render manifest
and compiles them as ordinary video sources replacing portions of the base
track. The current documented workflow requires a finished video asset for
each prepared replacement.

## Current constraints and the architectural implication

The current strict V2 plan represents ordered video segments and a separate
final-audio asset. It has no general-purpose visual layer list with alpha,
transforms, blend/composite operations, or z-order evaluation. The existing
`z_index` field on editorial overlay input does not by itself add those
capabilities to the V2 plan or native renderer.

Therefore, sending a single complete timeline **as the plan exists today** can
reduce ambiguity and let the Master own the complete edit, but it does not by
itself remove pre-rendered opaque video replacements. To eliminate those
intermediate videos, the shared plan contract and the worker/native renderer
must support the needed layer assets and composition semantics, and the worker
must perform the final composition/render. This is an implementation change
inside this repository, not a negotiation with an outside Master.

No design note should promise sub-second rendering solely from changing the
payload shape. Render time still depends on duration, resolution, source decode,
effects, encoder settings, and worker hardware; benchmark the actual target
workload.

## Source references

- Master/worker repository layout: `README.md`.
- Intake distinctions: `DataServer/internal/handlers/server/pipeline/intake_types.go`.
- Overlay validation and replacement overlap rules:
  `DataServer/internal/handlers/server/pipeline/intake_validation_overlay.go`.
- PRE/FINALIZE flow and prepared replacement handling:
  `DataServer/internal/handlers/server/pipeline/job_prepare_finalize.go`.
- Compiled V2 timeline and overlay compilation:
  `shared/contract/compiled_render_plan_v2.go` and
  `shared/contract/compiled_render_plan_v2_compiler.go`.
- Replacement-to-video-segment resolution:
  `shared/contract/visual_timeline_resolver.go`.
- Current two-stage operator contract: `ops/jobs/README.md`.
