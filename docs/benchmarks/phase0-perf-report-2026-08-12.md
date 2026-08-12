# Phase 0 — perf report: dominant CPU symbols of the reference copy-only job (2026-08-12)

Analysis of the 51.5MB `perf.data` recorded on the diagnostic worker
(`velox-deb-57.131`, container `velox-worker`, engine `v1.2.28-canonical`)
while rendering the reference ~4m44s copy-only job.

## Data & method

- Data: `/var/lib/velox/perf/velox-20260812-091059-784.data` (51,508,464 B ≈ 51.5MB)
- Capture: `perf record --inherit -F 249 -g` via the `velox_video_engine_perf`
  wrapper (`VELOX_VIDEO_ENGINE_CPP_BIN`), **no Velox logic touched**
- Run: reference copy-only RenderPlan v1 (30×9.47s segments, single audio
  track, warm cache), engine rc=0, wall 17.81s (`summary.tsv` row `perf`)
- Samples: **4K of `cpu-clock:pppH`** ≈ 17.8s × 249Hz
- Limits: cloud kernel exposes no PMU → `cycles/instructions/IPC` unavailable
  (same `<not supported>` as `perf stat`); Debian ffmpeg/ffprobe link
  dynamically and the system `ld-linux` is stripped, so its hot offsets are
  resolved by disassembly (evidence below)

## CPU by process (self, no children)

| overhead | command           | interpretation |
|---|---|---|
| **64.55%** | ffmpeg            | per-segment stream-copy + final audio AAC mux |
| **34.98%** | ffprobe           | 2× metadata probe per segment |
| **0.47%**  | velox_video_engine | the C++ engine itself |

→ **99.5% of CPU is inside external media children.** The C++ engine is a
pure orchestrator — measured, not assumed (confirms the phase-0 thesis).

## CPU by shared object

| overhead | dso |
|---|---|
| **38.66%** | libavcodec.so.59.37.100 |
| **36.08%** | ld-linux-x86-64.so.2  (dynamic linker) |
| **15.60%** | [kernel.kallsyms] |
| 2.69% | libm.so.6 |
| 2.53% | libc.so.6 |
| 1.31% | libhwy.so.1.0.3 |
| 1.22% | libavutil.so.57.28.100 |
| 0.73% | libavformat.so.59.27.100 |
| 0.21% | libavfilter.so.8.44.100 |
| 0.19% | ffmpeg (executable) |
| < 0.2% | libstdc++, libtasn1, libSvtAv1Enc, libaom, libX11, vdso |

## Dominant symbols (self)

| overhead | symbol | location |
|---|---|---|
| **7.76%** | `0x8e2a` | ld-linux — symbol-lookup hash loop |
| 2.46% | `0x9069` | ld-linux (same region) |
| 2.41% | `0x8deb` | ld-linux (same region) |
| 2.11% | `0x90c0` | ld-linux (same region) |
| 1.90% | `0x8e79` | ld-linux (same region) |
| **1.19%** | `clear_page_erms` | kernel — page zeroing |
| **1.12%** | `hwy::platform::TimerResolution` | libhwy (in ffprobe) |
| **0.89%** | `do_user_addr_fault` | kernel — page faults |
| 0.84% | `0x8e04` | ld-linux (same region) |
| 0.77% | `0xfbe28` | libavcodec (callchain → `avcodec_send_frame`) |
| 0.75% | `0x8e5f` / `0x905f` | ld-linux (same region) |
| 0.70% | `0x8e37` | ld-linux (same region) |
| **0.59%** | `copy_page` | kernel |
| **0.59%** | `_raw_spin_unlock_irqrestore` | kernel |
| 0.56% | `next_uptodate_page` | kernel — page-cache mapping |

The top ~13 ld-linux offsets (0x8bc7…0x999a) sum to **≈22% of all samples**
and are one code region: disassembly shows the dynamic linker's
**symbol-lookup hash-bucket scan** (`do_lookup_x`-style; nearest exported
symbol `_dl_rtld_di_serinfo+0x400`). libavcodec's own hottest symbol is
`0xfbe28` at 0.77% — its 38.66% is spread across many functions (no single
hot function).

## Findings

1. ✅ **The C++ engine is 0.47%.** 99.5% of CPU is in ffmpeg/ffprobe
   children — the engine is an orchestrator, exactly as theorized; there is
   no C++ hot function to optimize.
2. 🆕 **Surprise: the dynamic linker is 36% of CPU.** The single hottest
   "symbol" in the whole trace is ld.so's symbol-resolution loop. The 189
   child spawns each pay relocation + lazy-PLT lookup, and ffprobe/ffmpeg
   are PLT-heavy → ~22% of all samples in that one lookup region.
3. 🆕 **Kernel = 15.6%, mostly page-fault/zeroing** (`clear_page_erms`,
   `do_user_addr_fault`, `copy_page`, `next_uptodate_page`) — consistent
   with the 428 911 page faults measured by `perf stat`; process churn.
4. → **≈52% of CPU (36% linker + 15.6% kernel) is infrastructure
   overhead, not media work.** The real media CPU is libavcodec 38.66%,
   dominated by the final AAC encode (phase receipt: `mux_audio` 7.7s of
   17.4s render) plus per-segment demux/mux.
5. The zero-spawn Phase-1 pipeline (Priority #1 = A) attacks exactly this:
   no children → ld.so + kernel churn collapse to ≈0, and the same
   libavcodec work runs in-process. No C++ function-level optimization is
   needed — the win is architectural (process elimination).

## Evidence inventory

- Raw tables committed under
  `docs/benchmarks/evidence/phase0-2026-08-12/`:
  - `perf-report-by-comm.txt`
  - `perf-report-by-dso.txt`
  - `perf-report-top-symbols.txt`
- Binary `perf.data` (51.5MB) kept on the worker at
  `/var/lib/velox/perf/velox-20260812-091059-784.data`
  (`sudo perf report -f -i <file>` to re-run; file is uid 10001, hence `-f`)
