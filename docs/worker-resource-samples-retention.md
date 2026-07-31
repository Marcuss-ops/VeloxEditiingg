# Worker resource samples: retention and rollup policy

The worker resource sampler runs inside each worker session. It samples host
resources periodically and publishes the latest typed snapshot on
`Heartbeat.resources`. The master persists that snapshot in
`worker_resource_samples` as part of the existing heartbeat transaction.

## Timestamp and identity semantics

Each raw row contains:

- `worker_id`: the authenticated worker identity;
- `session_id`: the Hello-created worker session, so restarts cannot be mixed;
- `sampled_at`: the worker-observed UTC timestamp from the sampler;
- `ingested_at`: the master UTC timestamp assigned when the heartbeat is
  persisted.

Retention uses `ingested_at`, not `sampled_at`, because worker clocks can be
skewed. Queries must keep the `worker_id` predicate and may additionally scope
to `session_id` when diagnosing one worker runtime.

The unique key `(worker_id, session_id, sampled_at)` makes a replay of the same
observation idempotent while allowing the same worker to produce an equivalent
sample in a new session.

## Raw and rollup windows

Defaults are configured by environment variables:

- `VELOX_RETENTION_WORKER_RESOURCE_RAW_DAYS=90`: raw samples;
- `VELOX_RETENTION_WORKER_RESOURCE_ROLLUP_DAYS=365`: hourly rollups.

Setting either value to `0` disables that prune pass. Raw samples are retained
for short-horizon Gantt/resource investigations. Rollups are calculated per
`worker_id`, `session_id`, and UTC hour and are retained for longer capacity
planning and regression analysis.

## Maintenance

The master supervisor runs `worker-resource-maintenance` every five minutes.
Each tick, in one SQLite transaction, it:

1. recomputes/upserts hourly rollups from raw samples;
2. prunes raw rows older than the raw `ingested_at` cutoff;
3. prunes rollups older than the rollup `hour_bucket` cutoff.

Rollup writes use an upsert, so repeated ticks and late heartbeats do not
create duplicates. Resource sample writes remain on the heartbeat transaction
and do not open a second writer path.

The disk and network values are cumulative counters. Their legacy rollup
column names end in `_avg` for schema compatibility, but maintenance stores
the hourly `MAX` (the latest/high-water cumulative value), not an arithmetic
average. This preserves counter monotonicity and allows downstream consumers
to derive deltas between buckets.
