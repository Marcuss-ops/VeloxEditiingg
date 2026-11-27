# Architecture audit performance evidence — 2026-11-27

This is the bounded local baseline and its current production comparison for
the P2 performance audit. It records copy-only rendering separately from
decode/encode work and uses the existing benchmark and SQLite benchmark
harnesses.

## Local native copy-only baseline

Run on Linux/amd64, Intel Core Processor (Haswell, no TSX), with the existing
C++ engine binary built from `e8a0d0f9`. The fixture is generated locally; no
Master or worker is contacted. Each run copies four one-second video windows
from one 640×360 H.264 source and muxes a one-second AAC track. OS page-cache
state is not controlled, so these values are a local smoke baseline, not a
production cold/warm comparison.

```bash
VELOX_BENCH_BUILD=0 \
VELOX_BENCH_ENGINE="$PWD/RemoteCodex/native/video-engine-cpp/build/velox_video_engine" \
VELOX_BENCH_RUNS=5 \
VELOX_BENCH_SEGMENTS=4 \
VELOX_BENCH_OUTPUT_DIR=/tmp/velox-audit-render-safe-5x4 \
scripts/benchmarks/native-render-baseline.sh
```

| Measure | Result |
| --- | ---: |
| Render wall time | 90 ms median, 100 ms maximum |
| User CPU | 30 ms per run |
| System CPU | 50–70 ms per run |
| Peak RSS | 43,916–44,392 KiB |
| Engine process launches | 1 per render |
| Forks, in-engine FFmpeg/FFprobe | 0 |
| External final validation FFprobe | 1 per render, after the traced engine |
| Asset copies, temp bytes, decoded/encoded frames | 0 |
| Output | 76,174 bytes, packet_copy |

Raw observations and machine-readable summary:

- [native-render-baseline.tsv](evidence/architecture-audit-2026-11-27/native-render-baseline.tsv)
- [native-render-summary.json](evidence/architecture-audit-2026-11-27/native-render-summary.json)

The harness previously generated `scale_mode=stretch`, which the current
copy-only contract rejects. It now uses `cover`; the five-run command above
completed successfully. The harness also records user/system CPU, peak RSS,
and output size in its TSV and summary.

## SQLite concurrent writer/read benchmark

Ran the existing production-DSN-shaped pool benchmark (WAL, busy timeout,
NORMAL sync) with four writer goroutines, each doing an UPSERT and a bounded
read. It uses a small synthetic table and is not a production database trace.

```bash
cd DataServer
GOMAXPROCS=2 go test -run=^$ -bench="BenchmarkSQLiteConcurrentWriters_Pool(1|2|4|8)_W4$" -benchtime=1000x -benchmem -count=5 ./internal/platform/database
```



| Pool | Median ns/op | Range ns/op | B/op range | allocs/op range |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 47,752 | 45,615–49,606 | 1,175–1,182 | 31–32 |
| 2 | 44,439 | 40,672–48,121 | 931–1,149 | 26–31 |
| 4 | 44,171 | 42,591–47,794 | 899–914 | 26 |
| 8 | 44,108 | 42,415–51,022 | 901–909 | 26 |

Raw repeated results are in
[sqlite-pool-benchmark.txt](evidence/architecture-audit-2026-11-27/sqlite-pool-benchmark.txt).
The small median difference is outweighed by run-to-run overlap. This does
not justify changing the production SQLite pool size or adding indexes.

## Production timing evidence and decision

The deployed-worker cold/warm pair in
[VIDEO-PERFORMANCE-LIVE-2026-11-26.md](../architecture/VIDEO-PERFORMANCE-LIVE-2026-11-26.md)
reports a 51.66 s cold end-to-end job with a 0.66 s copy task and 1.60 s source
transfer (2,572,743 bytes), versus a 0.73 s warm job with a cache hit and zero
transfer. The cold job spent about 48 s waiting for worker readiness/lease;
the observable delay is scheduling/readiness and cold transfer, not the
packet-mux CPU path. These are synthetic six-second production renders and
remain distinct from the full `gervais-final-v1` matrix tracked in #40.

No actionable persistence, allocation, or copy-render optimization is
supported by these measurements. The SQLite run is intentionally a screening
benchmark, and no production slow-query evidence was present to justify an
index change. The copy-only render is bounded and showed no child-process or
temporary-file growth. Do not extrapolate these results to decode/encode
renders or to another worker class.
