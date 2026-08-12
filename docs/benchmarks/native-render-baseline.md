# Native render baseline: Go → C++ → FFmpeg

## Scope

This document records the current RenderPlan V1 path before any LibAV
migration. The benchmark is intentionally local and reproducible: it uses
synthetic H.264/AAC fixtures, does not contact the Master, and writes evidence
under `/tmp` unless `VELOX_BENCH_OUTPUT_DIR` is supplied. The harness is
Linux/GNU-specific because it uses `strace`, `/usr/bin/time -f`, and GNU
`stat`; it is not a portable macOS/Windows benchmark.

It measures the current implementation; it does **not** claim to represent the
latency of a production worker until the same workload is repeated on each
worker class with real cached assets.

## Real execution path

```text
TaskRunner / pipeline.Runner
  └─ RenderClient.RenderWithMetrics
       ├─ preparePlanTemp
       │    ├─ json.MarshalIndent(RenderPlan)
       │    └─ os.WriteFile(render_plan.json)
       └─ runEngineProcess
            ├─ exec velox_video_engine --render --plan <temp plan>
            ├─ Setpgid + Pdeathsig
            └─ waits for the C++ process group
                 └─ cmdRenderPlan
                      ├─ readFile + hand-written RenderPlan parser
                      └─ RenderEngine::render
                           ├─ makeTempDir(/tmp/velox_video_engine_plan/...)
                           ├─ per timeline item:
                           │    ├─ downloadAsset
                           │    │    └─ local/cache source → workdir copy
                           │    ├─ ffprobe stream metadata (video copy-only)
                           │    ├─ ffprobe duration (video copy-only)
                           │    └─ /bin/sh → ffmpeg → segment_N.mp4
                           ├─ write segments.txt
                           ├─ /bin/sh → ffmpeg concat -c copy
                           │    └─ video_only.mp4
                           ├─ audio download + ffprobe checks
                           ├─ /bin/sh → ffmpeg mux/AAC
                           │    └─ final_muxed.mp4
                           ├─ fs::copy_file(final_muxed, output.mp4)
                           └─ emit output.mp4.progress.json

TaskRunner quality boundary
  ├─ SHA-256 final artifact
  └─ independent ffprobe final artifact validation
```

### Important interpretation

`frames_encoded = 0` and `frames_decoded = 0` only mean that no video frame
was decoded or encoded in the copy-only segments. They do not mean that the
attempt avoided process creation, probing, segment files, concat, audio work,
or filesystem copies.

## Instrumentation already present

The worker exposes these timings in `pipeline.RenderMetrics`:

| Metric | Owner | Meaning |
|---|---|---|
| `PlanMarshalMs` | Go `preparePlanTemp` | JSON serialization duration |
| `PlanWriteMs` | Go `preparePlanTemp` | render-plan file write duration |
| `ProcessStartMs` | Go `runEngineProcess` | `cmd.Start()` duration for the C++ process |
| `ProcessWaitMs` | Go `runEngineProcess` | wait duration after start, excluding start |
| `TotalMs` | Go `RenderWithMetrics` | Go native-client wall time, including plan and engine |
| `PhaseMS` | C++ sidecar | engine phases such as asset, segment, concat and mux |
| `Segments[]` | C++ sidecar | per-segment duration, bytes and frame counters |
| `TempBytes` | C++ sidecar | generated segment/concat/final-mux bytes written to the temp workdir |

The final quality `ffprobe` is deliberately outside the C++ sidecar. It is an
independent post-render barrier and must remain in the first optimization
baseline.

## Benchmark command

```bash
scripts/benchmarks/native-render-baseline.sh
```

A larger copy-only workload:

```bash
VELOX_BENCH_RUNS=5 \
VELOX_BENCH_SEGMENTS=30 \
VELOX_BENCH_OUTPUT_DIR=/tmp/velox-native-baseline-30 \
scripts/benchmarks/native-render-baseline.sh
```

The script builds the engine in `/tmp/velox-native-baseline-build` by default.
Use an existing binary when build time is not part of the experiment:

```bash
VELOX_BENCH_BUILD=0 \
VELOX_BENCH_ENGINE=/usr/local/bin/velox_video_engine \
scripts/benchmarks/native-render-baseline.sh
```

Do not compare runs across different FFmpeg builds, CPU limits, storage
classes, canvas/fps, segment counts, or cache states without recording those
variables alongside the result.

## Baseline output

Each evidence directory contains:

- `baseline.tsv`: one row per render;
- `summary.json`: mean, median, minimum and maximum for numeric counters;
- `run-N/strace.log`: process lifecycle trace;
- `run-N/engine.stderr.log`: C++/FFmpeg logs and disk-copy metrics;
- `run-N/output.mp4.progress.json`: native sidecar;
- `run-N/final-ffprobe.json`: independent final-artifact validation.

The TSV columns are:

| Column | Definition |
|---|---|
| `total_ms` | `/usr/bin/time` wall duration of the C++ render command |
| `engine_execs` | direct `velox_video_engine` `execve` count under `strace` |
| `process_forks` | `clone`/`fork`/`vfork` count under `strace` |
| `engine_ffprobe_execs` | C++ child `ffprobe` `execve` count |
| `engine_ffmpeg_execs` | C++ child `ffmpeg` `execve` count |
| `final_validation_ffprobe` | always `1`: independent post-render quality probe |
| `asset_copy_ops` | `file_utils::copyFile` staging-copy count when the benchmark env gate is enabled |
| `asset_copy_bytes` | bytes copied by those asset staging operations |
| `estimated_final_copy_ops` | explicit final `fs::copy_file`, currently `1` |
| `estimated_final_copy_bytes` | output size used as a conservative estimate; final `fs::copy_file` is not syscall-instrumented |
| `sidecar_temp_bytes` | C++ generated temporary segment/concat/mux bytes |
| `frames_decoded` | C++ sidecar count |
| `frames_encoded` | C++ sidecar video-frame count |
| `encode_passes` | C++ sidecar video encode passes |
| `concat_mode` | current sidecar concat mode |

## Observed local baseline

A smoke-sized run was executed after adding the benchmark harness:

```text
VELOX_BENCH_RUNS=2
VELOX_BENCH_SEGMENTS=4
canvas=640x360@30
synthetic local H.264 source + finite AAC track
```

| Metric | Result |
|---|---:|
| C++ render wall time | 2,130–2,180 ms; median 2,155 ms |
| Direct engine execs | 1 per render |
| Process forks/clones | 20 per render |
| C++ `ffprobe` execs | 10 per render |
| C++ `ffmpeg` execs | 6 per render |
| Independent final `ffprobe` | 1 per render |
| Asset staging copies | 5 per render; 78,207 bytes |
| Estimated final full-file copy | 1 per render; 75,789 bytes |
| C++ generated temp bytes | 94,193 bytes |
| Frames decoded | 0 |
| Video frames encoded | 0 |
| Video encode passes | 0 |
| Concat mode | `stream_copy` |

This is a local synthetic baseline, not a production-worker SLA. It confirms
the central observation: four copy-only segments still invoke ten C++
`ffprobe` processes, six C++ `ffmpeg` processes, twenty process forks/clones,
five asset staging copies, one final full-file copy, and an independent final
quality probe even though the video frame counters remain zero.

Evidence was retained during the run under `/tmp/velox-native-baseline-check`
(`baseline.tsv`, `summary.json`, per-run traces and sidecars). Repeat the same
command on each worker class and preserve the evidence directory with the
commit SHA, FFmpeg versions and cache state before using the numbers for
capacity or SLA decisions.

The benchmark enables `VELOX_BENCH_DISK_COPY_METRICS=1` only for its
child engine. Production runs do not emit the copy metrics unless that env
variable is explicitly set to a truthy value.

A post-change run with the LibAV backend used the same four-segment workload
and produced:

| Metric | LibAV result |
|---|---:|
| C++ render wall time | 1,010 ms in the captured run |
| Direct engine execs | 1 |
| Process forks/clones | 10 |
| C++ `ffprobe` execs | **0** |
| C++ `ffmpeg` execs | 6 |
| Asset staging copies | 5; 78,207 bytes |
| Estimated final copy | 75,789 bytes |
| Frames decoded/encoded | 0 / 0 |

The independent final quality `ffprobe` remains `1` and is intentionally
outside the C++ trace. The C++ copy-only path now opens each local asset once
with LibAV and performs no `ffprobe` child execution. The wall-time result is
one local run, not an SLA; repeat the benchmark with multiple runs and real
worker fixtures before comparing capacity.

The benchmark is intentionally explicit about two different notions of
spawn:

1. `engine_execs`, `process_forks`, `engine_ffprobe_execs` and
   `engine_ffmpeg_execs` measure the direct C++ invocation and its descendants;
2. Go `ProcessStartMs`, `ProcessWaitMs` and `TotalMs` require a worker-side
   RenderClient execution and should be collected from the task metrics on a
   real worker. A direct C++ benchmark cannot manufacture those Go timings.

## Expected current shape

For `N` local copy-only video timeline items with one finite audio track, the
current code path is expected to show approximately:

```text
asset staging copies       N video copies + 1 audio copy
video metadata ffprobe     2 × N
video segment ffmpeg       N
concat ffmpeg              1
audio stream ffprobe       1
final audio metadata probe 1
final mux ffmpeg           1
final validation ffprobe   1 (outside C++ trace)
```

The exact process count can be higher because the current implementation uses
`system()`, `popen()`, and shell-mediated FFmpeg progress handling. That shell
process overhead is part of the baseline and must not be silently excluded.

`sidecar_temp_bytes` is not a copy-byte counter: it accounts for generated
intermediates, while `asset_copy_bytes` measures the instrumented asset
staging copies and `estimated_final_copy_bytes` uses the output size as an
explicit final-copy estimate. The final `fs::copy_file` is not syscall-
instrumented by this harness; the estimate is labelled deliberately so it
cannot be mistaken for an exact kernel byte count.

## Reproducibility and acceptance

Record at minimum:

- commit SHA and dirty-tree status;
- worker CPU, RAM, storage and container limits;
- FFmpeg/ffprobe version and binary paths;
- engine binary SHA-256;
- `VELOX_BENCH_RUNS`, `VELOX_BENCH_SEGMENTS`, canvas and fps;
- cold or warm asset-cache state;
- the complete `summary.json` and evidence directory.

The first LibAV packet-backend comparison should preserve the same plan and
acceptance gates, then target:

```text
engine_ffmpeg_execs      = 0
engine_ffprobe_execs     = 0
asset staging copies     = 0
intermediate segments    = 0
intermediate video_only  = 0
final mux passes         = 1
frames decoded/encoded   = 0  (copy-only case)
```

The independent final `ffprobe` remains enabled until the new backend has
passed the media-contract, duration, stream, hash, and artifact-quality
regression suites.
