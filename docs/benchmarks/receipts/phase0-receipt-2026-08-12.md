# Phase-0 receipt — copy-only ~4m44s (tag 2026-08-12)

Worker `velox-deb-57.131`, engine `v1.2.28-canonical`, containerized
(cap_drop ALL + seccomp:unconfined + paranoid=1). Evidence:
`docs/benchmarks/evidence/phase0-2026-08-12/`.

## Where the 17.4s render goes

| phase | ms | % | note |
|---|---|---|---|
| video segments (serial) | **9331** | 53.7% | 30 × 311ms avg |
| ├─ ffmpeg stream-copy work | 22 | 0.1% | copy-only, speed ~13.000x |
| └─ spawn + 2×ffprobe overhead | **9309** | 53.6% | ~99.8% of segment time |
| audio download/prepare | 105 | 0.6% | |
| concat (segments.txt) | 203 | 1.2% | stream_copy |
| **audio mux (AAC ENCODE)** | **7719** | 44.5% | `final_mux_audio_mode=ENCODE` `reason=not_final_mix` |
| asset download (warm cache) | 3 | 0.0% | inside segments, cache→tmp copy |
| finalize (copy final / workdir) | 9 | 0.1% | atomic rename publication |
| **unaccounted** | **-3** | -0.0% | < 5% ✓ |

`unaccounted_ms = -3 ms (-0.02% of 17364 ms)` —
**PASS (target < 5%).**
The engine `phase_ms.segment_build_ms` summary undercounts the segments
(3.2s vs 9330.8ms in the segments[] timeline) — the timeline is
authoritative; that gap was the bulk of the apparent "unaccounted" time.

## Correlations

- **strace -f -c**: wait4 = 10.1s / 282 calls (52.8%)
  ≈ the serial segment+child waits; futex = 7.0s
  (76680 calls) ≈ audio encode threading; **execve = 189**
  external processes; 307,967 syscalls.
- **perf stat**: 1.003 CPUs utilized (serial);
  user+sys = 17.6s ≈ wall 17.2s (CPU-bound);
  25,981 context-switches;
  428,911 page-faults (process spawn churn).
- **runs**: strace=25004ms, perfstat=17304ms, perf=17812ms.

## Verdict

- Video stream-copy **work** is ~22ms of 17364ms;
  the rest is orchestration + audio re-encode.
- Priority 1 = **FINAL_AUDIO_COPY** (removes ~7719ms, 44%).
- Priority 2 = **in-process packet pipeline** (removes ~9309ms,
  ~54%: the per-segment spawn + 2×ffprobe + wait4 tax).
- Combined ceiling ≈ 336ms of
  fixed cost (0.3s).
