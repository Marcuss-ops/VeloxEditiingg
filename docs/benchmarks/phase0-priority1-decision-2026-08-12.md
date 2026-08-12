# Priority #1 — Formal decision (2026-08-12): FINAL_AUDIO_COPY vs in-process packet pipeline

Decision record based on the Phase-0 receipt
(`docs/benchmarks/receipts/phase0-receipt-2026-08-12.{md,json}`), the
reference profiling evidence (`docs/benchmarks/evidence/phase0-2026-08-12/`)
and the perf symbol analysis (`docs/benchmarks/phase0-perf-report-2026-08-12.md`).

Supersedes the ordering in `phase0-priority-decision-2026-08-12.md` where it
differs: this record aligns ship order with the receipt's verdict
(Priority 1 = FINAL_AUDIO_COPY, Priority 2 = in-process packet pipeline).
The two candidates are **not mutually exclusive** — both are approved and
both are on the critical path; the decision is the order and the acceptance.

## Context — the receipt is the base

Reference workload: copy-only ~4m44s (30×9.47s segments, single non-loop
audio track, warm cache, engine `v1.2.28-canonical`), worker
`velox-deb-57.131`. Render total **17.4s**, `unaccounted_ms = -3 ms
(-0.02%)` → **PASS (< 5%)** — the receipt explains the wall clock.

| phase | ms | % | note |
|---|---|---|---|
| video segments (serial) | **9330.8** | **53.7%** | 30 × 311ms avg |
| └ spawn + 2×ffprobe overhead | 9308.6 | 53.6% | ~99.8% of segment time; stream-copy work only 22ms |
| **audio mux (AAC ENCODE)** | **7719.0** | **44.5%** | `final_mux_audio_mode=ENCODE`, `reason=not_final_mix` |
| concat (segments.txt) | 203.0 | 1.2% | stream_copy |
| audio download/prepare | 105.4 | 0.6% | |
| asset download (warm) + finalize | 2.6 + 8.8 | 0.1% | |
| unaccounted | -3.0 | -0.02% | ✓ < 5% |

Corroborating evidence:

- strace: **wait4 10.1s / 282 calls (52.8%)**, **execve = 189**, futex 7.0s
  (AAC encode threading), 307 967 syscalls.
- perf stat: **1.003 CPUs utilized (serial)**, 428 911 page-faults (spawn
  churn), user+sys ≈ wall (CPU-bound).
- perf report: **99.5% of CPU in ffmpeg/ffprobe children, engine 0.47%**;
  **ld-linux (dynamic linker) 36%** + **kernel 15.6%** ≈ 52% infrastructure
  overhead; libavcodec 38.66% (no single hot function — AAC + per-segment
  demux/mux).

## The two candidates

| | B — FINAL_AUDIO_COPY | A — in-process packet pipeline |
|---|---|---|
| evidence impact | **7.7s / 44.5%** removed (audio AAC re-encode → stream copy) | **9.3s / 53.6%** removed (spawn + 2×ffprobe + wait4 tax, segments) |
| what it kills | single-track AAC encode, `-shortest`, offset truncation | 2×ffprobe per clip, per-segment ffmpeg spawn, `segment_N.mp4`, `segments.txt`, `video_only.mp4`, concat process, cache→tmp copies, **ld.so + kernel churn (≈52% CPU infra)** |
| effort / risk | LOW–MEDIUM / LOW–MEDIUM | HIGH / HIGH (determinism must be preserved) |
| repo state | landed: C++ resolver + `muxAudio` COPY + V2 `final_audio` gate (tests green); worker has CLI `buildFinalAudioCopyArgs` (`-c:a copy`) | landed engine-side: Demuxer + PacketTrimmer + TimestampRewriter + `muxCopyOnly`, V2 receiver int64, atomic publish (tests green); **worker routing pending** |
| independence | independent of A (different pipeline stage) | independent of B; bigger ceiling |
| combined ceiling | **≈ 336ms (0.3s)** of fixed cost on the reference job | |

## Decision

**Priority #1 = B — FINAL_AUDIO_COPY (7.7s, 44%): ship first.**
**Priority #2 = A — in-process packet pipeline (9.3s, 54%): ship immediately after.**

Rationale:

1. **Receipt-driven**: the receipt's verdict ranks the phases by measured
   impact and identifies FINAL_AUDIO_COPY as the largest *independent*
   removal; the packet pipeline is the largest overall (and the only one
   that also eliminates the 52% linker/kernel CPU overhead).
2. **Ship order = risk/effort, not ceiling**: B is low-risk, already
   partially wired, and gives the first certifiable win on the same
   reference job + tooling; A is the architectural prize and reuses B's
   measurement infrastructure (receipt regeneration, strace execve count,
   perf re-record).
3. **Both are approved and landed in the repo** — the remaining work is
   activation/routing on the worker and fleet + certification, then
   measuring the new baseline and setting the tier-2 budgets (currently
   TBD, `docs/performance-gates.md`).

## Plan

### Phase B — FINAL_AUDIO_COPY (ship #1)

1. Certify the engine-side COPY path end-to-end on a V2 job
   (`final_mux_audio_mode=COPY`, `reason=verified_final_mix`,
   `audio_encode_passes=0`, single mux) — repo tests already green
   (`test_render_plan_v2`, zero-intermediates); run the CLI smoke on the
   reference plan.
2. Route worker final-audio jobs to the V2 receiver with the
   `FINAL_AUDIO_COPY` gate (complete/replace the CLI
   `buildFinalAudioCopyArgs` path; the receiver does the copy in the same
   single mux as the video packets).
3. Re-run the reference job → assert `audio_encode_passes=0` and the
   `mux_audio` phase collapses 7719ms → ≈0; regenerate the receipt
   (`unaccounted_ms < 5%` must hold).
4. Enable `FINAL_AUDIO_COPY` on the fleet (4 workers), verify
   `VELOX_AUDIO_MIX_PROFILE` stays off, and measure per-worker.

### Phase A — in-process packet pipeline (ship #2)

1. Route copy-only V2 jobs to the C++ zero-spawn receiver
   (`muxCopyOnly` + V2 int64 timeline already landed; worker routing is the
   remaining step).
2. Re-run the reference job → assert the Phase-1 target (below): execve
   189 → **~1**, `temp_bytes` 5.37MB → **0**, `file_copy_count=0`,
   `mux_passes=1`, segments phase 9331ms → ~0.3s.
3. Re-record perf → ld-linux (36%) and kernel (15.6%) must collapse to
   ≈0; engine CPU becomes the dominant (in-process) share.
4. Set the tier-2 performance budgets (wall p50/p95, throughput,
   CPU/wall, amplifications) from the new baseline and arm the two-tier
   gates (`velox-fixture-gate -tier deterministic|performance`).

## Acceptance

Both phases are accepted on the reference copy-only job when:

```
external_ffmpeg_processes = 0
external_ffprobe_processes = 0
video_frames_decoded       = 0
video_frames_encoded       = 0
temporary_segment_files     = 0
temporary_video_files       = 0
asset_cache_copies          = 0
audio_encode_passes         = 0   (FINAL_AUDIO_COPY)
mux_passes                  = 1
execve_count                ≤ 1   (single engine process, strace)
unaccounted_ms              < 5%  (regenerated receipt)
accounted_ratio             > 0.95
```

Determinism invariants stay enforced by the tier-1 fixture gate (execve /
encode / decode / temp files / artifact SHA), per `docs/performance-gates.md`.

## Verification tooling

- `scripts/ops/run-reference-profiling-job.sh --mode strace|perfstat|perf`
  → execve count, perf stat, perf record (worker `velox-deb-57.131`)
- Receipt regeneration → `docs/benchmarks/receipts/phase0-receipt-*.json`
  (`unaccounted_ms < 5%`, phase table)
- `velox-fixture-gate -tier deterministic` (CI) + `-tier performance`
  (self-hosted benchmark worker) once budgets are set
- perf report re-run on the new `perf.data` → compare ld.so/kernel shares

## Owners / notes

- B: worker routing in `render_batch_executor` (currently CLI
  `buildFinalAudioCopyArgs`); cert on `velox-deb-57.131`.
- A: worker routing of copy-only V2 → C++ receiver; the natural next step
  after B is certified.
- Combined ceiling ≈ 336ms fixed cost on the reference job; new baseline
  unblocks the tier-2 budget numbers (TBD in `benchmark_fixtures.go`).
