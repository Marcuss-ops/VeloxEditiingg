# Complex canonical benchmark

`COMPLEX_CANONICAL_5M_V1` is the second immutable benchmark track. It is
separate from `COPY_ONLY_CANONICAL_5M_V1`: this workload intentionally does
real video and audio work and must not be compared to the 0.33 s packet-copy
fast path.

The fixture pins:

- 300 seconds of output at 1920×1080/30 fps;
- 24 H264 source clips with deterministic 1280×720, 1920×1080 and 1080×1920
  source geometries;
- per-segment `cover` scaling;
- 14 deterministic AAC 48 kHz stereo audio tracks with fixed volume, offsets,
  roles and ducking metadata;
- warm-cache execution and an immutable spec digest.

The generator writes only to the evidence directory. It produces
`complex-manifest.json`; the manifest is checked against the registered spec
before every benchmark. The benchmark uses the existing V1 complex renderer,
so its FFmpeg/FFprobe, segment, audio, CPU, memory and I/O telemetry is the
baseline to optimize.

## Run

On a dedicated worker:

```bash
VELOX_COMPLEX_RUNS=5 \
VELOX_COMPLEX_CONCURRENCY=1 \
VELOX_COMPLEX_CACHE=warm \
scripts/benchmarks/complex-baseline.sh
```

The report is written to
`.velox/benchmarks/complex-canonical-2026-08-12/run.json` by default. The
script prints one JSON object per receipt with the first breakdown fields.

The raw receipt remains authoritative. For a complete view:

```bash
jq '.receipts[] | {
  run: .run_index,
  wall_ms: .wall_ms,
  phases: .receipt.phases,
  segments: .receipt.segments,
  media: .receipt.media,
  process: .receipt.process,
  cpu: .receipt.cpu,
  memory: .receipt.memory,
  io: .receipt.io,
  derived: .receipt.derived
}' .velox/benchmarks/complex-canonical-2026-08-12/run.json
```

The first baseline must not set timing budgets. After the controlled baseline
is recorded, the same immutable fixture and evidence shape are used for the
candidate renderer and the budget/compare step can be pinned.
