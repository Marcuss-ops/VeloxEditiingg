# Phase 0 — Priority #1 decision (2026-08-12)

Decision doc: which single optimization to attack first, based on the worker
fleet comparison and the Phase-0 receipt
(`docs/benchmarks/receipts/phase0-receipt-2026-08-12.md`).

## Fleet comparison (probed 2026-08-12)

| fact | velox-deb-57.131 | velox-ovh-523925 | velox-ovh-13197 | velox-hetz-57.129 |
|---|---|---|---|---|
| reachable via SSH | ✅ | ✅ | ❌ (publickey denied) | ❌ (port closed, TBD) |
| OS | Debian 12 | Ubuntu 26.04 | n/d | n/d |
| CPU | 8× Intel Haswell | 8× AMD EPYC-Milan | n/d | n/d |
| RAM | 22 GB | 22 GB | n/d | n/d |
| disk | 197G, 77% used | 193G, 56% used | n/d | n/d |
| image id | 45bf779dd837 | 45bf779dd837 | n/d | n/d |
| engine | v1.2.28-canonical | v1.2.28-canonical | n/d | n/d |
| engine sha | ddc34c08f00239fe | ddc34c08f00239fe | n/d | n/d |
| render backend | native | native | n/d | n/d |
| worker profile | unset | unset | n/d | n/d |
| `VELOX_AUDIO_MIX_STRATEGY` | **unset** | **unset** | n/d | n/d |
| `VELOX_AUDIO_MIX_PROFILE` | **unset** (off ✓) | **unset** (off ✓) | n/d | n/d |
| `FINAL_AUDIO_COPY` | unset | unset | n/d | n/d |

Fleet reads: homogeneous engine on the reachable workers; the audio
optimizer is **not active** anywhere (code default
`audio_plan.cpp:47` → `LegacyAmix`); the audio mix profiler is off (good);
the V2 final-audio-copy contract is not enabled on the fleet yet.

## Evidence (receipt, 17.4s render of the ~4m44s copy-only reference)

| phase | ms | % |
|---|---|---|
| segments (serial) — **spawn + 2×ffprobe per clip** | **9 331** | **53.7%** |
| └ ffmpeg stream-copy actual work | 22 | 0.1% |
| **final audio mux (AAC ENCODE)** | **7 719** | **44.5%** |
| concat (segments.txt) | 203 | 1.2% |
| disk copies (cache→tmp, warm) | 2.6 | 0.0% |
| unaccounted | ≈0 | < 5% ✓ |

Correlations: wait4 10.1s / 282 calls; **execve = 189**; 307 967 syscalls;
**1.003 CPUs utilized** (serial); user+sys ≈ wall (CPU-bound).

## Candidates

| # | optimization | evidence impact | effort | risk | state |
|---|---|---|---|---|---|
| A | zero-spawn packet pipeline (in-process LibAV: MediaProbe + Demuxer + PacketTrimmer + TimestampRewriter + ConcatMuxer) | **~9.5–10s (54%+)**: kills ffprobe×2, per-segment ffmpeg spawn, segment_N.mp4, segments.txt, video_only.mp4, concat process, cache→tmp copies | HIGH (new C++ core path) | HIGH (determinism must be preserved) | new |
| B | FINAL_AUDIO_COPY (single-track AAC stream-copy instead of re-encode) | **7.7s (44.5%)** | LOW–MEDIUM | LOW–MEDIUM | **already landed in repo**: `render_engine.cpp:738` resolver, `render_batch_executor.go buildFinalAudioCopyArgs`, `test_media_probe.cpp` green; part of the V2 pipeline (pending worktree) |
| C | remove disk copies | 2.6ms (0.0%) in reference; scales with real asset size | MEDIUM | LOW | folded into A |
| D | parallelize segments | up to ~9.3s (53.7%) * (1−1/8) | MEDIUM | MEDIUM | follow-up; needs deterministic output guarantee |

## Decision

**Priority #1 = A — Zero-Spawn Copy Pipeline (in-process LibAV packet
backend).**

Rationale:

1. **Largest measurable block** (54%+) and the only phase that also
   explains the strace tax (189 execve, wait4 10.1s, 1.003 CPUs serial).
2. One architectural change eliminates **four** of the user's candidates at
   once: the 2×ffprobe per clip, the per-segment spawn, the intermediate
   files/concat process, and the cache→tmp copies — with a single target:
   `external_ffmpeg_processes=0, external_ffprobe_processes=0, temp_files=0,
   asset_cache_copies=0, mux_passes=1`.
3. It preserves the property that made Velox fast: **deterministic output +
   stream-copy video** (never decode/encode in the copy-only path).

**Priority #2 (run in parallel, fast win) = B — FINAL_AUDIO_COPY.**
Already landed in the repo with tests; the work is activating + certifying
it on the V2 path (44.5% removed). Because it is independent of A
(different pipeline stage, different risk surface) it can ship first and be
measured with the same `phase0-receipt` tooling.

**Not priority now:** C (0.0% in reference; absorbed by A), D (follow-up),
GPU, AVFrame pool, Tracy, Nsight (per the original analysis phasing).

## Acceptance for A (from the analysis)

```
copy-only job:
  external_ffmpeg_processes = 0
  external_ffprobe_processes = 0
  video_frames_decoded       = 0
  video_frames_encoded       = 0
  temporary_segment_files     = 0
  temporary_video_files       = 0
  asset_cache_copies          = 0
  audio_encode_passes         = 0   (if FINAL_AUDIO_COPY)
  mux_passes                  = 1
```

Verification = re-run `scripts/ops/run-reference-profiling-job.sh --mode
strace` and assert execve count collapses from 189 toward ~1 (single engine
process), then regenerate the receipt and re-check `unaccounted_ms < 5%`.

## Owners / notes

- Diagnostic worker (velox-deb-57.131) still wired in profiling mode
  (`install-worker-perf-wrappers.sh --mode off` to restore).
- Disk: deb worker at 77% — prune `/var/lib/velox/perf` before accumulation
  becomes an issue.
- Fleet env to verify when the two unreachable workers are accessible:
  `VELOX_AUDIO_MIX_STRATEGY` / `VELOX_AUDIO_MIX_PROFILE` / `FINAL_AUDIO_COPY`.
