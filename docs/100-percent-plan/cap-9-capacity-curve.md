# Cap. 9 — Capacity Curve + C++ Engine Benchmark (Phase 8 / 100% Velox)

Three workload profiles × four capacity multipliers + a 150-frame C++
engine benchmark. CI-runnable hermetic harness with the same evidence
shape as the on-VPS production run.

## What it proves

| Invariant  | Profile          | Semantic                                                                                     |
| ---------- | ---------------- | -------------------------------------------------------------------------------------------- |
| **NR-16**  | Small            | Same-seed synthetic frame is byte-identical across **5 reruns**                              |
| **NR-17**  | Small            | Frame-channel PMF (lex-sorted histogram) is byte-identical across reruns                      |
| **NR-18**  | 12 cells         | Capacity-curve has no regression; 10× multiplier's queue isn't left behind                    |
| **NR-19**  | All cells        | LEASED→RUNNING ratio ≤ 3 × READY→LEASED (dispatcher warm)                                     |
| **NR-20**  | Medium           | At least **N** SUCCEEDED tasks per cell — throughput sanity                                   |
| **NR-21**  | Large            | **Bounded RAM growth**: `TOT_GROWTH < 1.5×MIN(RSS)` AND `|slope| < BASE_RSS/600`             |
| **NR-22**  | C++ engine       | `velox_fallback_count_total_delta == 0` across 150 frames (no full-dirty fallback)           |
| **NR-23**  | C++ engine       | Clearnode restore at frame 30 (`dirty_node_count == 0`)                                      |
| **NR-24**  | C++ engine       | Pool telemetry steady at frame 150 (`dirty == 0`; `free + in_use == constant`)               |
| **NR-25**  | All cells        | Per-executor retry rate ≤ 2 retries per SUCCEEDED                                             |

## Capacity-curve matrix (12 cells)

| Profile          | Cap Mul | Executor ID                       | Rationale                              |
| ---------------- | ------- | --------------------------------- | -------------------------------------- |
| `small`          | 1×      | `scene.composite.tiny.v1`         | byte-determinism baseline              |
| `small`          | 2×      | `scene.composite.tiny.v1`         | 2 × concurrency, determinism preserved |
| `small`          | 5×      | `scene.composite.tiny.v1`         | 5 × concurrency                        |
| `small`          | 10×     | `scene.composite.tiny.v1`         | 10 × concurrency                       |
| `medium`         | 1×      | `scene.composite.medium.v1`       | baseline throughput                   |
| `medium`         | 2×      | `scene.composite.medium.v1`       | queue-pressure test                    |
| `medium`         | 5×      | `scene.composite.medium.v1`       | saturation                            |
| `medium`         | 10×     | `scene.composite.medium.v1`       | 10× stress                              |
| `large`          | 1×      | `scene.composite.large.v1`        | 1× RSS baseline                        |
| `large`          | 2×      | `scene.composite.large.v1`        | 2× RSS slope                           |
| `large`          | 5×      | `scene.composite.large.v1`        | 5× RSS slope                           |
| `large`          | 10×     | `scene.composite.large.v1`        | 10× RSS slope — must not be linear      |

The simulator holds the job count at `BASELINE_JOBS × multiplier` and
scales the concurrency limiter (mirrors `concurrency.go`'s
`MaxActiveJobs`) from 1 → 10. This exercises the dispatcher warm-up and
the queue buildup — the most likely failure point.

## Determinism protocol (Small / NR-16, NR-17)

```
seed=42, frame=320×180, ppm-P3

1. python3 -c "import random,sys; random.seed(42); print('P3\n320 180\n255\n' + ...)"
   → frame_r1.ppm
2. tail -n +4 frame_r1.ppm | sort | uniq -c > frame_r1.pmf
3. for run in 1..5: produce frame_r${run}.ppm + frame_r${run}.pmf
4. cmp frame_r1.ppm frame_r${run}.ppm    # NR-16
5. cmp frame_r1.pmf frame_r${run}.pmf    # NR-17
```

Every rerun uses the same seed (42). The harness reports PIXEL EXACT
sha256 and PMF identity. CI checksum is byte-level — the same N runs
produce the same N bytes.

## Bounded RAM slope (Large / NR-21)

The simulator samples RSS every tick; for Large 600s of synthetic work
that yields ~20 samples. Linear regression yields a slope; the
verdict rejects the cell if:

  - `abs(slope_bytes_per_sec) > BASE_RSS_BYTES / 600`     ← linear leak
  - `TOT_GROWTH > 1.5 × MIN(RSS)`                           ← sawtooth bound

For the Large profile with `BASE_RSS_BYTES = 480 MB`, the slope
threshold is 480 MB ÷ 600 s = **800 KB/s**. A production leak at this
rate would be audible in our monitoring; the test gates it out.

## C++ engine 150-frame benchmark (NR-22..NR-24)

The harness accepts `ENGINE_MODE=auto|real|mock`.
- **auto** picks `real` if `$VELOX_ENGINE_BIN` exists, otherwise `mock`.
- **real** invokes the actual C++ binary with `--compose-frames 150` and
  parses its progress CSV (line protocol matches the
  ffmpeg_progress_parser).
- **mock** runs a Python stand-in that realistically models the
  engine's pool/clearnode/fallback semantics. The verdict records
  `engine_simulation_mode = "real" | "mock"` so an operator can tell
  which one ran.

The bench produces `frame_timeline.csv` with one row per frame
(`frame,mode,elapsed_ms,pool_dirty,pool_free,pool_in_use,fallback_delta,
cleared`). The harness asserts:

  - At frame 30: `pool_dirty == 0` AND `cleared == 1`.
  - At frame 150: `pool_dirty == 0` (steady state, no dirty holds).
  - Across all 150 frames: `SUM(fallback_delta) == 0`.

## Per-executor counter schema

Each cell writes a `per_executor.sqlite` snapshot of the
`per_executor_counters` table. The orchestrator consolidates the
per-cell snapshots into a single `per_executor_stats` map in
`verdict.json`:

```json
"per_executor_stats": {
  "scene.composite.tiny.v1":    {"succeeded": 4, "failed": 0, "timed_out": 0,
                                  "lease_expired": 0, "retries": 0,
                                  "fallback_full_dirty": 0},
  "scene.composite.medium.v1":  {"succeeded": 8, "failed": 2, "timed_out": 0,
                                  "lease_expired": 0, "retries": 1,
                                  "fallback_full_dirty": 0},
  "scene.composite.large.v1":   {"succeeded": 10, "failed": 0, "timed_out": 0,
                                  "lease_expired": 0, "retries": 0,
                                  "fallback_full_dirty": 0}
}
```

The label set mirrors `RemoteCodex/.../internal/telemetry/metrics.go`'s
SAFE-label allowlist (`executor_id`, `worker_class`, `phase`, `status`).

## How to run

```bash
make cap-9-capacity              # full 12 cells + C150
make cap-9-c150-engine           # C++ engine only
make cap-9-small-determinism     # 5-rerun PPM byte-equality
make cap-9-capacity-dry          # bash -n + python3 preflight
```

Or override the evidence root:

```bash
EVIDENCE_ROOT=/var/lib/velox/evidence/cap9 make cap-9-capacity
```

## Verdict schema (canonical)

```json
{
  "schema":                  "velox.cert-9-capacity.v1",
  "final_status":            "PASS",
  "capacity_curve_pass":     true,
  "c150_engine_pass":        true,
  "failed_invariants":       [],
  "failed_cells":            [],
  "invariants":              { "NR-16-small-byte-determinism": true, ... },
  "per_executor_stats":      { ... },
  "cell_results":            [ ... ],
  "engine_simulation_mode":  "mock",
  "evidence_root":           "/tmp/velox-cap9-evidence",
  "generated_at":            "2026-05-15T22:54:12Z"
}
```

## Failure modes & recovery

| Symp­tom                                  | Diagnose                                   | Recover                                              |
| ----------------------------------------- | ------------------------------------------ | ---------------------------------------------------- |
| `NR-16 FAIL` (small non-determinism)      | Synthetic PPM gen drift; PYTHONHASHSEED    | Re-pin generator's `random.seed(42)` call           |
| `NR-21 FAIL` (linear RAM growth)          | Engine / Go GC retained pointers           | Profile + bisect retention; PR rejected             |
| `NR-22 FAIL` (full-dirty fallback)        | Engine sentinel hit during frame 31–150    | `velox_fallback_count_total > 0` ⇒ engine regression |
| `NR-23 FAIL` (clearnode miss at frame 30) | Engine's GC schedule > 30 frames           | Adjust sweep trigger; rejects if > 30 frames         |
| `NR-25 FAIL` (per-executor retry storm)   | Dispatcher attempt-rotation loop           | Bound task retry budget per `attempts <= 3`          |

## What this runbook explicitly does NOT do

- It does NOT spin up docker. The synthetic PPM generator + Python
  pool simulator are deterministic enough to assert the invariants.
- It does NOT pull FFmpeg. The Medium profile samples per-cell
  throughput from FSM transitions, not from a real ffmpeg-progress
  pipeline.
- It does NOT require the C++ binary. The mock stand-in is honest
  about its limitations (`engine_simulation_mode = "mock"`) and the
  verdict is unambiguous.
- It does NOT generate human-only evidence. Every assertion is
  machine-checkable, written to the same per-cell sqlite path, and
  consumed by cap. 11/12 packaging.

## Cross-references

- `scripts/cert/executor-matrix.py` — the 9-case executor dispatch
  matrix; the cap. 9 simulator's per-executor counter struct is
  modelled after its `valid_spec` / `corrupt_asset` / `ffmpeg_fail` /
  `timeout` rows.
- `RemoteCodex/.../internal/telemetry/metrics.go` — the canonical
  executor_id-keyed counter family; cap. 9 emits the same shape.
- `DataServer/internal/metrics/procstat.go` — master-side
  `/proc/self/status` VmRSS reader; cap. 9 inherits the same scheme
  but runs against itself (`/proc/self/status` inside the harness).
- `RemoteCodex/.../internal/worker/concurrency/concurrency.go` — the
  `MaxActiveJobs` limiter that cap. 9 stress-tests at 1×/2×/5×/10×.
- `docs/100-percent-plan/04-DISTRIBUTED-RENDERING-PERFORMANCE-AND-SCALE.md`
  — the canonical capacity model the cap. 9 matrix is derived from.
