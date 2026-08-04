# Worker parallelism certification

## Scope

This runbook certifies the existing worker concurrency limiter at
`MaxActiveJobs=1`, `2`, and `3`. It does **not** implement or replace leases,
placement, cache leases, or download singleflight:

- lease ownership remains the master;
- many-to-many asset leases and singleflight remain the worker cache owner;
- the certification harness only submits jobs and observes the resulting run.

The harness is `tests/worker-cert/parallel_bench.py`.

## Reproducible protocol

Use the same worker, image digest, canonical asset fixture, destination,
number of jobs, poll timeout, and metric endpoint for all three cells. Run the
cells sequentially in this order:

```text
1 → wait for cap convergence and active_jobs=0 → submit the batch → collect evidence
2 → wait for cap convergence and active_jobs=0 → submit the batch → collect evidence
3 → wait for cap convergence and active_jobs=0 → submit the batch → collect evidence
```

The harness requires an explicit operator-owned cap command. This is
intentional: there is no generic unaudited HTTP endpoint for changing worker
configuration. The command receives `{cap}`, `{worker_id}`, and `{master_url}`
placeholders and must use the deployment's existing configuration/command
path. Example shape (replace with the site's approved command):

```bash
export VELOX_MASTER_URL=http://127.0.0.1:8000
export VELOX_ADMIN_TOKEN='...'
export PARALLEL_BENCH_WORKER_ID='worker-001'
export PARALLEL_BENCH_METRICS_URL='http://worker-001:9090/metrics'
export PARALLEL_BENCH_SET_CAP_CMD='ssh worker-001 "sudo velox-admin-worker set-max-active-jobs {cap}"'

python3 tests/worker-cert/parallel_bench.py \
  --worker-id "$PARALLEL_BENCH_WORKER_ID" \
  --metrics-url "$PARALLEL_BENCH_METRICS_URL" \
  --set-cap-command "$PARALLEL_BENCH_SET_CAP_CMD" \
  --jobs 6 \
  --output /var/lib/velox/evidence/parallelism-certification.json
```

The command must be safe to repeat and must return non-zero on failure. The
harness verifies the admin worker read model reports the requested cap and
`active_jobs=0` before each cell. It restores cap `1` after the matrix unless
`--leave-cap` is explicitly supplied.

Dry-run validation does not contact the master or change a worker:

```bash
python3 tests/worker-cert/parallel_bench.py \
  --worker-id worker-001 \
  --metrics-url http://worker-001:9090/metrics \
  --set-cap-command 'echo set {worker_id} {cap}' \
  --dry-run
```

## Evidence collected per cell

The report schema is `velox.parallelism-certification.v1` and includes:

| Measure | Collection method |
| --- | --- |
| throughput | successful jobs / batch wall time, normalized to jobs/hour |
| mean latency | arithmetic mean of successful job submit-to-terminal latency |
| p95 latency | interpolated p95 of successful job latencies |
| CPU average/peak | worker Prometheus CPU utilization gauge samples |
| RAM average/peak | worker Prometheus process RSS gauge samples |
| disk wait | worker Prometheus iowait ratio samples |
| cache hit/miss | delta of existing cache request counters |
| downloads | delta of existing cache download counter |
| duplicate downloads | delta of an explicitly exported duplicate-download counter; missing means incomplete, never zero |
| errors | failed jobs, merged with the existing error counter delta |

Prometheus labels are not copied into the report as dimensions. The harness
uses one worker endpoint (or a worker-scoped projection), so it does not add
`job_id`, `asset_id`, `hash`, or other high-cardinality labels.

The current worker/master telemetry already provides the resource and cache
families documented in `docs/metrics-catalog.md`, including
`velox_worker_cpu_iowait_ratio`, `velox_worker_process_rss_bytes`,
`velox_cache_requests_total`, `velox_cache_downloads_total`, and cache size /
entry gauges. A cell is marked `INCOMPLETE` when a required measurement is not
exported. It is never silently treated as zero.

## Efficient-limit rule

A cap is eligible when:

1. every job succeeds;
2. error rate is within the configured limit (default `0`);
3. p95 is within `--max-p95-ms` when an SLA is supplied;
4. disk wait is at or below `--max-iowait-ratio` (default `0.35`);
5. throughput improves by at least `--min-throughput-gain-pct` versus the
   previous eligible cell (default `5%`).

The efficient limit is the highest eligible cap. A higher cap is rejected when
throughput flattens while p95, iowait, RSS, errors, or duplicate downloads
worsen. A missing duplicate-download metric prevents certification rather than
being interpreted as “zero duplicates”.

## Current certification status

On the development host checked on 2026-08-04:

- the local master health endpoint responded on `http://127.0.0.1:8000`;
- worker API access returned `401` without credentials;
- `VELOX_ADMIN_TOKEN`, `TOKEN_FILE`, and benchmark asset variables were unset;
- no worker process was running locally.

Therefore no 1/2/3 performance numbers are claimed. The live matrix is
blocked until an operator supplies the token, a connected worker, canonical
READY assets, the worker metrics URL, and the approved cap command. The
repository contains no fabricated benchmark result.
