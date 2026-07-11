#!/usr/bin/env bash
# =============================================================================
# tests/e2e/cap-9-capacity/profiles.sh
# =============================================================================
# Three workload profile definitions for the cap. 9 capacity curve. Each
# profile is hermetic — no FFmpeg, no docker, no real engine — but its
# parameters match the production workload classes the operator would
# exercise on a real VPS:
#
#   SMALL  — 2 s @ 320×180  (deterministic short-clip).
#            Used to ASSERT NR-16 (sha256 equality across 5 reruns) +
#            NR-17 (frame channel PMF equality).
#
#   MEDIUM — 60 s @ 720p    (10–20 composite scene cuts at ~3–6 s each).
#            Used to ASSERT NR-18 (capacity-curve no-degradation), NR-19
#            (dispatcher-warm latency), NR-20 (upload throughput).
#
#   LARGE  — 10 min @ 1080p (audio + heavy frames).
#            Used to ASSERT NR-21 (bounded RSS slope) + NR-25 (per-
#            executor retry bounded).
#
# Capacity multipliers (1× 2× 5× 10×) are NOT profile properties — they are
# separate cell axes. The simulator (capacity-curve.sh) iterates 12 cells
# via `for prof in small medium large; do for mult in 1 2 5 10; do ...`.
# =============================================================================

# profile_small — 320×180, 60 frames @ 30fps (= 2 s).
# Seed fixed at 42 so byte-determinism is reproducible across hosts.
profile_small() {
  PROFILE="small"
  W=320; H=180
  N_FRAMES=60
  DURATION_S=2
  EXECUTOR_ID="scene.composite.tiny.v1"
  SEED=42
  BASE_RSS_BYTES=$(( 80 * 1024 * 1024 ))   # ~80MB baseline expectation
}

# profile_medium — 1280×720, 1800 frames @ 30fps (= 60 s).
# 10–20 scene cuts at ~3–6 s each (variable partition). Deterministic
# partition function keys off job_id so the cut schedule is reproducible.
profile_medium() {
  PROFILE="medium"
  W=1280; H=720
  N_FRAMES=1800
  DURATION_S=60
  EXECUTOR_ID="scene.composite.medium.v1"
  SEED=1717
  N_CUTS=15
  BASE_RSS_BYTES=$(( 240 * 1024 * 1024 ))
}

# profile_large — 1920×1080, 18000 frames @ 30fps (= 600 s = 10 min).
# Audio strip is encoded as a buffer presence flag (audio_buffer_size_bytes).
# Repeated scene loops are NOT re-seeded; the engine's pool reuse is what
# NR-21 / NR-22 asserts.
profile_large() {
  PROFILE="large"
  W=1920; H=1080
  N_FRAMES=18000
  DURATION_S=600
  EXECUTOR_ID="scene.composite.large.v1"
  SEED=89101
  N_CUTS=120
  BASE_RSS_BYTES=$(( 480 * 1024 * 1024 ))
  AUDIO_BUFFER_SIZE_BYTES=$(( 96 * 1024 ))   # 96 KB audio buffer per cut
}

# profile_for NAME — dispatcher. All other scripts call this.
profile_for() {
  case "$1" in
    small)  profile_small  ;;
    medium) profile_medium ;;
    large)  profile_large  ;;
    *) fail "unknown profile: $1" ;;
  esac
}
