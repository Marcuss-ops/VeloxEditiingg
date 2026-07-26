# Master performance engine

The Master owns placement decisions. Workers expose measurements and execute
the granted task; they do not read `/proc`, choose their own FFmpeg budget, or
select another worker.

## Canonical owners

- `internal/costmodel`: compatibility, pressure and admission gates. The
  `Registry` path converts heartbeat metrics into one `ResourceSnapshot`.
- `internal/performance`: executor-version `Estimator` registry, historical
  baseline fallback, explainable placement choice and overhead-aware shard
  planning.
- `internal/store`: the only atomic task claim/write owner. Estimation never
  mutates task state.

## Decision sequence

1. Build the worker profile from the current heartbeat and metrics snapshot.
2. Reject draining/offline, capacity-full, memory/disk/swap/I/O pressure and
   resource-budget violations.
3. Estimate queue, transfer, compute and upload time from the executor version
   and the matching `performance_baselines` row. Unknown history receives low
   confidence and a conservative estimate.
4. Select the lowest predicted finish time plus failure/uncertainty penalty.
5. Create shards only for frame-local/windowed work when useful compute is
   greater than orchestration overhead. Stateful/global work remains one shard.
6. Pass the selected worker and resource budget to the existing atomic claim.

The output is intentionally explainable: `Estimate` includes finish time,
confidence, sample count, memory/temp reservations and EUR cost when the
operator price sheet is configured. This is a prediction, not a replacement
for post-attempt measurements; completed attempts must continue to feed the
baseline store.
