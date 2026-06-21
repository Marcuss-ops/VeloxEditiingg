# Velox distributed rendering roadmap

Status: implementation plan

Owner: architecture / rendering runtime

Target branch for implementation PRs: `main`

This directory is the operational source of truth for evolving Velox from whole-job rendering into a modular distributed rendering runtime.

## Mission

Velox must compile one video project into an immutable execution plan, execute independent work across CPU/GPU workers, reuse cached artifacts, and report exactly where time is spent.

The target flow is:

```text
Editor project
  -> RenderPlan
  -> Task DAG
  -> Scheduler
  -> Executor registry
  -> CPU/GPU workers
  -> Artifact store/cache
  -> Final composition
```

## Architectural invariants

1. `jobs.Job` represents the user-visible render objective.
2. `taskgraph.Task` represents one schedulable unit of work.
3. `artifacts` owns every reusable output: normalized assets, overlays, precomps, masks, shards, stems, and final files.
4. `taskattempts` records what happened on one worker attempt.
5. The master owns planning, dependency resolution, lifecycle transitions, scheduling, and canonical metrics.
6. Workers are simple executors. They do not reinterpret the project or invent dependencies.
7. Every executor, resolver, sampler, and capability must enter a canonical registry. Do not add parallel switch/case routing.
8. All persistent state remains SQLite-first through repositories. Do not add JSON/RAM authoritative state or a second writer.
9. Large binary data remains in the BlobStore/artifact storage, never in SQLite.
10. No silent fallback. Missing critical dependencies fail explicitly.
11. Velox remains headless and server-side. This roadmap does not add an editor UI, browser renderer, or GUI runtime.
12. GPU support is capability-based and optional. CPU rendering remains a valid first-class path.

## Required PR order

Implementation is split into four sequential PRs. Do not develop the four branches in parallel.

1. [PR 1 - Task contracts and observability](PR-01-TASK-CONTRACTS-OBSERVABILITY.md)
2. [PR 2 - Task DAG, artifacts, and late composition](PR-02-TASK-DAG-LATE-COMPOSITION.md)
3. [PR 3 - Executor registry and modular worker runtime](PR-03-EXECUTOR-REGISTRY-WORKERS.md)
4. [PR 4 - Adaptive scheduler, cost model, and temporal sharding](PR-04-SCHEDULER-COST-SHARDING.md)

Each PR starts only after the previous PR is merged into `main`.

## Required Git workflow for every implementation PR

```bash
git fetch origin
git checkout main
git pull --ff-only origin main
git checkout -b codex/<pr-specific-name>
```

Before push:

```bash
git fetch origin
git rebase origin/main
git status -sb
git diff origin/main...HEAD --stat
```

Then run the tests listed in the corresponding PR document.

After push:

```bash
git log -n 5 --oneline
```

Rules:

- One PR, one responsibility.
- Do not touch files outside the documented scope unless compilation requires it.
- Do not add generated files, binaries, output directories, or local caches.
- Do not introduce a second queue, artifact store, registry, scheduler, or state writer.
- Do not combine refactoring, new features, deployment changes, and UI work in one PR.
- Keep compatibility read-only, explicitly owned, and time-bounded.

## Canonical domain model

```text
Job
  owns the user request and overall lifecycle

RenderPlan
  immutable compiled representation of the project

TaskGraph
  dependency graph derived from one RenderPlan

Task
  one schedulable operation with explicit inputs and outputs

TaskAttempt
  one execution of one Task on one Worker

Artifact
  immutable binary or metadata output identified by content hash

Worker
  executor host with advertised capabilities and resources
```

## Canonical task phases

All timing reports must use these names. Do not create free-form aliases.

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

## Canonical task categories

Initial executor IDs:

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

Additional executors must be registered through the same registry and versioned.

## Initial performance targets

These are measurement targets, not hard-coded assumptions:

- Drive must not be on the normal render critical path.
- Duplicate download of the same content hash must be zero.
- Cache byte hit ratio should be measurable per project and worker.
- Task queue wait, asset wait, render, encode, and upload must be separately visible.
- Finalization should concatenate/mux compatible shards instead of re-rendering the full project.
- Scheduler decisions must be explainable from capability, load, estimated cost, and data locality.

## Metrics required before optimization

At project level:

```text
wall_clock_ms
critical_path_ms
total_worker_busy_ms
parallel_efficiency
workers_allocated
workers_peak_active
cache_byte_hit_ratio
bytes_from_drive
bytes_from_blobstore
bytes_from_local_cache
retry_count
straggler_count
```

At task-attempt level:

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
cpu_time_ms
gpu_time_ms
peak_rss_bytes
peak_vram_bytes
estimated_ms
estimation_error_ratio
```

## Definition of done for the roadmap

The roadmap is complete when:

- one job can expand into multiple dependent tasks;
- independent overlay/precomp tasks can run while stock ingestion is pending;
- workers advertise capabilities from a registry, not hard-coded maps;
- task scheduling accounts for worker load and cached inputs;
- temporal render shards can execute independently with pre/post-roll;
- compatible shards are assembled without a full re-encode;
- project reports identify the actual critical path and optimization opportunities;
- all state follows the existing single-writer and repository rules.

## Explicit non-goals

This roadmap does not include:

- frontend/editor development;
- browser rendering;
- a Blender or After Effects clone;
- PostgreSQL cutover;
- Kubernetes adoption;
- peer-to-peer worker distribution;
- machine-learning cost prediction;
- a second renderer path beside the canonical pipeline;
- speculative features not required by the four PRs.
