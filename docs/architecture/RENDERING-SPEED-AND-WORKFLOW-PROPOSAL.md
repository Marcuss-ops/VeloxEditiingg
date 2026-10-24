# Rendering speed and workflow proposal

## Purpose

This proposal describes the changes most likely to make Velox jobs start and
finish sooner while making asset handling easier for producers and operators.
It is grounded in the current Master, worker, native renderer, benchmarks, and
distributed-rendering roadmap in this repository.

The repository contains both sides of the system: `DataServer/` is the Velox
Master, while `RemoteCodex/` contains the worker agent and native media engine.
The proposal therefore describes changes within Velox, not a contract
negotiation with an external Master.

## Start with the measured bottleneck

Rendering latency has at least two different parts:

1. **Time before rendering starts:** asset generation, upload/download,
   verification, prefetch, queueing, and worker placement.
2. **Time spent rendering:** source reads, decode, visual composition, encode,
   audio mux, and artifact upload/finalization.

The repository contains two useful measurements:

- A 300-second, 24-clip H.264 job with already-final audio assembled using
  packet-copy in **333 ms p50**. This path decodes and encodes zero video
  frames. This is an assembly result, not a compositing render.
- A separate 177-clip production job rendered in **20.3 seconds**, while
  waiting **3 minutes 35 seconds** from job creation to task start. The
  recorded issue was asset preparation and worker reassignment; delivery took
  about 59 seconds.

These measurements imply that asset readiness and locality are the first
operational bottlenecks to address. A target below one second is demonstrated
for warm, copy-safe assembly. It must not be assumed for a render that changes
video pixels and has to decode and encode them.

Evidence: [Phase 1 benchmark](../benchmarks/phase1-zero-spawn-vs-ffmpeg-2026-08-12.md),
[production rerun notes](../../CHANGELOG.md).

## Proposed changes, in priority order

### 1. Start asset preparation earlier and keep it with the selected worker

**Change:** As soon as the producer knows the base media, let the Master
validate and prefetch it while the producer generates later assets such as
voice, music, cards, or maps. Deduplicate assets by stable identity and hash.
Once a worker is selected, bind preparation to that worker or transfer its
verified cache state deliberately if placement changes.

**Why:** The measured production run spent far more time waiting to start than
rendering. Repeating prefetch on a newly selected worker discards useful
locality.

**Measure:** Job-created-to-ready time; asset wait time; bytes fetched; unique
versus duplicate assets; cache hit ratio; worker changes after prefetch.

### 2. Give producers one immutable render plan and direct asset upload

**Change:** Let a producer upload generated assets directly to the Master,
receive stable `asset_id`, SHA-256, size, and media metadata, then submit one
versioned render plan referencing those assets. Drive remains an optional
source for shared or pre-existing files; it is not a required staging step.

**Why:** A single plan is easier to validate, retry, inspect, and reproduce
than a series of per-overlay MP4 submissions. Direct upload avoids requiring
Drive for transient pipeline output. The plan JSON should contain references
and metadata, not binary file contents.

**Measure:** Producer integration steps; request count; upload retries; asset
verification failures; duplicate uploaded bytes; time from last asset ready
to worker-ready.

**Boundary:** Existing artifact upload is designed around worker-produced
outputs. Confirm or add a source-asset ingest contract rather than assuming
the final-artifact upload endpoint is automatically suitable for producer
inputs.

### 3. Add real layer compositing to the shared plan and worker renderer

**Change:** Extend the shared render contract to describe visual layers, not
only replacement video segments. A visual layer needs an asset reference,
media kind, start/end time, z-order, position/size, transform, opacity, and
explicit alpha/color-space behavior. Start with static PNG assets with alpha
and simple deterministic motion instructions. Add animated alpha media only
after the renderer has a defined, tested format contract.

The worker should read the base video, composite all active layers in one
render pass, and write one final video. The Master validates and compiles the
plan; the worker executes it.

**Why:** Current V2 video tracks describe ordered video segments and a
separate final-audio asset. Current `replace` overlays become video segments;
they are not transparent layers. A plan-only change cannot remove opaque
intermediate videos unless the renderer can composite the layer assets.

**Measure:** Intermediate video count and bytes; decoded/composited/encoded
frames; render wall time; peak memory; output sync and visual correctness;
retry cost when one source asset changes.

### 4. Cache generated assets and reusable precompositions by content

**Change:** Use deterministic cache keys derived from semantic input bytes,
render parameters, output profile, and renderer/executor version. Verify the
hash on every cache hit. Reuse common cards, logos, generated title plates,
and other independent precompositions across jobs and retries.

**Why:** Repeated assets should not be regenerated, uploaded, downloaded, or
rendered for every video. Versioned cache keys make reuse safe across engine
changes.

**Measure:** Cache hit ratio by count and bytes; generation time saved; corrupt
entry count; cold-cache versus warm-cache output equivalence.

### 5. Keep packet-copy assembly as a fast path and reduce its remaining I/O

**Change:** Preserve the current zero-external-process packet-copy path for
copy-safe, profile-compatible inputs. Continue reducing repeated input opens
and reads where profiling shows a benefit. Keep unsupported or unsafe cut
points fail-closed rather than silently triggering a whole-video encode.

**Why:** This path already demonstrates sub-second assembly for a five-minute
fixture. Its speed comes from not decoding or encoding video. Compositing
cannot use that same zero-decode path for frames whose pixels change.

**Measure:** Packet-copy eligibility rate; input reopen count; read
amplification; mux time; exact output duration and synchronization.

### 6. Parallelize independent work; shard video only with evidence

**Change:** Run independent asset preparation and graphics generation in
parallel. Consider temporal video sharding only for workloads with safe frame
boundaries and a measured benefit after scheduling, transfer, and final
assembly overhead.

**Why:** Parallel asset work can reduce the critical path without changing
video semantics. Video sharding adds timeline-boundary, codec, audio-sync,
retry, and assembly complexity; small shards can cost more to coordinate than
they save.

**Measure:** Critical-path duration, useful compute versus orchestration time,
parallel efficiency, straggler/retry rate, and output equivalence against the
single-worker render.

### 7. Keep final audio ready for copy-mux where possible

**Change:** If the producer can supply a verified, already-mixed final audio
asset matching the timeline, preserve the `FINAL_AUDIO_COPY` path. Make audio
mixing an explicit upstream or dedicated stage when inputs require a mix, and
avoid making a temporary mixdown the only source of truth for the render plan.

**Why:** Re-encoding final audio adds work and can serialize a pipeline that
otherwise copies compatible streams. Audio work should be measured separately
from video composition.

**Measure:** Audio mix and encode passes; mux time; audio/video duration and
sync; whether the final audio artifact is reusable across retries.

## Rollout sequence

1. Record phase timings for representative jobs and establish cold-cache and
   warm-cache baselines.
2. Fix asset prefetch timing, deduplication, and worker locality using the
   production run evidence.
3. Define producer asset ingest and one immutable plan referencing verified
   asset IDs.
4. Prototype static alpha layers end to end in the shared plan and worker
   renderer; compare output against reference fixtures.
5. Add verified content-addressed reuse and then evaluate animated layers.
6. Consider DAG fan-out or temporal sharding only when measurements show that
   the work is large enough to overcome orchestration overhead.

At every stage, compare both **single-job latency** (p50/p95) and **fleet
throughput** (completed jobs and output minutes per hour). A change can improve
one while worsening the other.

## Success criteria

- Producers can submit a complete plan and upload transient assets without
  depending on Google Drive.
- The Master validates and persists one immutable, hash-bound render plan.
- Asset preparation overlaps producer work and remains local to the selected
  worker where possible.
- Repeated verified assets and precompositions are reused safely.
- Static overlays can be composited in one worker render without one opaque
  MP4 intermediate per overlay.
- Copy-safe jobs retain the existing packet-copy fast path.
- Benchmarks report asset wait, queue, download, decode, composite, encode,
  upload, and finalization separately.
- Performance claims are tied to a named workload, cache state, output
  profile, worker class, and p50/p95 measurements.

## Related repository documents

- [Repository identity and current overlay flow](REPO-IDENTITY-AND-OVERLAYS.md)
- [Distributed rendering and scale plan](../100-percent-plan/04-DISTRIBUTED-RENDERING-PERFORMANCE-AND-SCALE.md)
- [Master performance engine](PERFORMANCE-ENGINE.md)
- [Phase 1 packet-copy benchmark](../benchmarks/phase1-zero-spawn-vs-ffmpeg-2026-08-12.md)
