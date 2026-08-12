#!/usr/bin/env bash
# Run the canonical complex-path baseline on a dedicated worker.
# The generated track is evidence input and is never committed.
set -Eeuo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
EVIDENCE="${VELOX_COMPLEX_EVIDENCE:-${ROOT}/.velox/benchmarks/complex-canonical-2026-08-12}"
RUNS="${VELOX_COMPLEX_RUNS:-5}"
CONCURRENCY="${VELOX_COMPLEX_CONCURRENCY:-1}"
CACHE="${VELOX_COMPLEX_CACHE:-warm}"

mkdir -p "$EVIDENCE"
VELOX_BENCH_REAL=1 \
VELOX_BENCH_FIXTURE=COMPLEX_CANONICAL_5M_V1 \
VELOX_BENCH_RUNS="$RUNS" \
VELOX_BENCH_CONCURRENCY="$CONCURRENCY" \
VELOX_BENCH_CACHE="$CACHE" \
VELOX_BENCH_EVIDENCE="$EVIDENCE" \
VELOX_BENCH_KEEP=1 \
VELOX_BENCH_SKIP_COMPARE=1 \
  "$ROOT/scripts/benchmarks/benchmark-worker.sh"

RUN_JSON="$EVIDENCE/run.json"
printf '%s\n' '[complex-baseline] phase breakdown (one JSON object per run):'
jq -r '
  .receipts[] |
  {
    run: .run_index,
    wall_ms: .wall_ms,
    video_segment_ms: ([.receipt.segments[]?.duration_ms] | add // 0),
    audio_mix_ms: ([.receipt.phases[]? | select((.name // "") | test("audio|mix"; "i")) | .duration_ms] | add // 0),
    concat_ms: ([.receipt.phases[]? | select((.name // "") | test("concat"; "i")) | .duration_ms] | add // 0),
    decoded: .receipt.media.frames_decoded,
    encoded: .receipt.media.frames,
    encode_passes: .receipt.media.encode_passes,
    external: .receipt.process.external_process_count,
    ffmpeg: .receipt.process.ffmpeg_exec_count,
    ffprobe: .receipt.process.ffprobe_exec_count,
    read_amp: .receipt.derived.read_amplification,
    write_amp: .receipt.derived.write_amplification,
    peak_rss: .receipt.memory.peak_rss_bytes,
    producer_wait_ms: (.receipt.frame_pipeline.producer_wait_ms // 0),
    consumer_wait_ms: (.receipt.frame_pipeline.consumer_wait_ms // 0),
    sha: .artifact_sha
  } | @json
' "$RUN_JSON"
