# Phase 0 — Reference profiling: copy-only ~4m44s job (2026-08-12)

Diagnostic worker: `velox-deb-57.131` (Debian 12, container `velox-worker`,
engine `v1.2.28-canonical`). Workload replicated through the profiling
wrappers inside the production container; **no Velox logic was touched**.

## Workload

RenderPlan v1, `copy_only: true`, canvas 640x360@30:

- 30 video segments × 9.466666667s ≈ 284s (~4m44s), same local source
  (warm cache → cache→tmp copy path), `include_audio: false`
- 1 audio track (non-loop) — the single-track fast path
- `frames_encoded = 0`, `frames_decoded = 0`, `encode_passes = 0`,
  `concat_mode = "stream_copy"`

## Runs (wrapper in-container, engine rc=0 all runs)

| mode            | wall_ms | trace                                                              |
|-----------------|---------|--------------------------------------------------------------------|
| strace -f -c    | 25004   | `strace-20260812-091017-280.txt` (evidence/phase0-2026-08-12/)     |
| perf stat -d -d | 17304   | `velox-perfstat-20260812-091042-533.txt` (evidence/…)              |
| perf record     | 17812   | `velox-20260812-091059-784.data` (51.5MB, on worker)               |

## Receipt (sidecar `output.mp4.progress.json`, perf run)

```
render total       17 364 ms   (phase "render" completed)
  segment_build      3 156 ms   (30 × ~105ms: 2×ffprobe + 1×ffmpeg per segment)
  mux_audio          7 710 ms   ← audio ENCODE (AAC), not stream copy
  concat               203 ms   (segments.txt → video_only.mp4)
  audio_download       105 ms
  asset_download         3 ms   (warm cache)
  copy_final            9 ms
temp_bytes         5 370 994   (segment_N.mp4 + video_only + mux intermediates)
```

Phases metadata (engine):

```
final_mux_audio_mode   = ENCODE
decision_reason        = not_final_mix      ← single-track fast path RE-ENCODES AAC
final_mux_audio_encode_passes = 1
audio_metadata_verified = true
```

Output verified: h264 640x360 + aac, container duration **285.014s** vs plan
284.0s (audio-driven; `-shortest` did not clamp to the video duration).

## strace -f -c (25.0s wall, 19.16s traced)

| signal | value |
|---|---|
| wait4         | **52.8% of time — 10.12s — 282 calls (94 errors)** |
| futex         | 36.7% — 7.03s — 76 680 calls |
| mmap          | 4.3% — 90 807 calls |
| execve        | **189 calls** (ffprobe/ffmpeg/shell children) |
| clone+vfork   | 104 + 94 |
| openat        | 23 170 (1 880 errors) |
| read / write  | 24 161 / 6 122 |
| total         | 307 967 syscalls, 10 141 errors |

→ The engine spends **~half its traced time blocked in wait4** on external
media processes; ~189 child execs per 284s copy-only render.

## perf stat -d -d (17.3s wall)

| counter | value |
|---|---|
| task-clock      | 17 258.86 msec, **1.003 CPUs utilized (serial)** |
| context-switch  | 25 981 (1.5 K/sec) |
| page-faults     | 428 911 (24.9 K/sec) |
| user / sys      | 14.74s / 2.85s |
| cycles / instructions / branches | `<not supported>` (cloud kernel, no PMU) |

→ ~100% of one CPU, serialized; orchestration + AAC encode, not disk-wait.

## Findings vs. analysis predictions

1. ✅ **Audio ENCODE is the #1 hotspot**: 7.7s of 17.4s render (44%),
   `decision_reason=not_final_mix` — matches "single-audio fast path
   re-encodes AAC" (P0 #9). FINAL_AUDIO_COPY would eliminate it.
2. ✅ **Segment orchestration = 3.2s serial**: 30 × (2 ffprobe + 1 ffmpeg
   spawn); wait4 dominates the trace (P0 #3, #4, #5).
3. ✅ **~189 child execs / 307k syscalls** per job — external-process tax
   (P0 #1–#5).
4. ✅ **Serial execution**: 1.003 CPUs utilized (P0 #3 "è seriale").
5. ✅ **Temp intermediates**: 5.4MB written, `concat_mode=stream_copy`
   via segments.txt (P0 #7).
6. ⚠️ **`-shortest` evidence**: output 285.014s vs plan 284.0s (P1 #14).
7. ✅ **frames=0/encode=0 confirmed**: copy-only really is stream-copy; the
   cost is orchestration + audio, not video encoding (the core thesis).

## Evidence inventory

- Raw text traces committed under `docs/benchmarks/evidence/phase0-2026-08-12/`
- `perf.data` (51.5MB, binary) kept on the worker at
  `/var/lib/velox/perf/velox-20260812-091059-784.data`
  (`perf report -i <file>` for callgraphs)
- Fixtures/plan/logs on the worker at `/var/lib/velox/perf/refjob/`
- Re-run anytime: `scripts/ops/run-reference-profiling-job.sh --mode all`
