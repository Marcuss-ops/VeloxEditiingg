# Performance gates: two tiers (plan §17)

Velox gates performance changes with **two tiers** that differ in where
they are allowed to run. The split is structural, not advisory: shared
CI jobs can only reach tier 1, and the tier-2 workflow runs on a
self-hosted-only runner label.

## Tier 1 — Deterministic invariants (normal CI, any runner)

`pkg/performance.CheckFixtureGate` fails **hard** on invariants that do
not depend on neighbor load, CPU model, storage speed or thermal state:

- execve forbidden (no external process spawn)
- encode forbidden / decode forbidden (copy-only Phase-1)
- unexpected temp files (count + leftover names)
- unexpected artifact SHA

Safe on shared GitHub runners. In normal CI these are enforced by the
Go test suite (`fixture_gate_test.go`) via `test.yml → make verify`,
plus the `velox-fixture-gate -tier deterministic` CLI as the manual/CI
hook.

## Tier 2 — Performance budgets (dedicated self-hosted worker ONLY)

`pkg/performance.CheckPerformanceBudgets` gates **distribution
aggregates** on a whole `BenchmarkRun`:

- wall p50 / p95 (from the run summary)
- throughput = content-seconds per wall-second (a **lower** bound)
- CPU/wall ratio (p50)
- read / write amplification (p50)

On shared runners these are noise (neighbor load, different CPU,
thermal state, VM scheduling) — that is exactly why they live on a
**dedicated, stable host**.

| Where | What runs |
|---|---|
| Normal CI (`.github/workflows/test.yml` → `make verify`) | Go tests: tier-1 invariants only |
| Dedicated worker (`.github/workflows/benchmark-worker.yml`, `runs-on: [self-hosted, benchmark]`) | `scripts/benchmarks/benchmark-worker.sh`: build track → benchmark → tier-2 gate → baseline compare (`-fail-on-regression`) |
| Manual hook | `velox-fixture-gate -tier deterministic \| performance` |

## Budgets

All performance budgets use `BudgetMax{Set, Value, Min}`:

- `Set=false` → TBD after the Phase-1 zero-spawn baseline; never enforced
- `Set=true` → enforced (a `0` is a real zero invariant)
- `Min=true` → lower bound (e.g. minimum throughput), violated when below

Fixtures carry `P50WallMSMax`, `P95WallMSMax`, `MinThroughput` and
`MaxCPUWallRatio` — all currently **TBD** until the baseline is
measured (`benchmark_fixtures.go`, plan §16: "I numeri temporali li
fissiamo dopo aver misurato il nuovo baseline").

## Running the dedicated worker

```bash
# On the self-hosted benchmark host (stub renderer, plumbing proof only):
VELOX_BENCH_EVIDENCE=/tmp/bw-evidence make benchmark-worker

# Real zero-spawn benchmark (production renderer over the generated track):
VELOX_BENCH_REAL=1 VELOX_BENCH_BASELINE=/data/baseline make benchmark-worker
```

The worker pipeline: builds the performance cmds → generates the
canonical fixture track with `velox-fixture-gen` and verifies the
manifest against the pinned spec digest → runs `velox-benchmark`
(`VELOX_BENCH_REAL=1` drives the production zero-spawn renderer: one
engine process, in-process libavformat packet copy, no
ffmpeg/ffprobe execs, atomic output) → evaluates tier 2 → compares
against the stored baseline with `velox-benchmark-compare
-fail-on-regression` and persists the new baseline.
