# Parallelism certification preflight — 2026-08-05

## Result

**Status: BLOCKED / NOT CERTIFIED.**

No `MaxActiveJobs=1`, `2`, or `3` live workload was submitted during this
preflight. Consequently, this report contains no throughput, p95, CPU, RAM,
disk-wait, cache, duplicate-download, or error measurements, and does not
recommend a production concurrency threshold.

The existing certification harness remains the source of truth:

```text
tests/worker-cert/parallel_bench.py
```

It owns neither lease handling nor cache download singleflight. Those remain
implemented by the existing master and worker paths.

## Checks completed

| Check | Result |
| --- | --- |
| Worker certification unit tests | PASS — 7 tests in `test_parallel_bench.py` |
| Metrics projection unit tests | PASS — 3 tests in `test_metrics_projection.py` |
| Dry-run cap matrix | PASS — validated caps `1`, `2`, `3` and placeholder expansion |
| Local master health probe | HTTP 200 on `http://127.0.0.1:8000/health` |
| Local master metrics probe | HTTP 200 on `http://127.0.0.1:8000/metrics` |
| Live jobs | NOT RUN |
| Artifact correctness verification | NOT RUN |
| Recommended threshold | NOT DETERMINED |

Commands used for the offline gate:

```bash
python3 tests/worker-cert/test_parallel_bench.py
python3 tests/worker-cert/test_metrics_projection.py
python3 tests/worker-cert/parallel_bench.py \
  --worker-id worker-cert-local \
  --metrics-url http://127.0.0.1:9101/metrics \
  --set-cap-command 'echo set {worker_id} {cap} via {master_url}' \
  --destination destination-cert \
  --dry-run
```

The dry-run does not contact the master, change a worker, submit jobs, or
produce performance evidence.

## Missing live prerequisites

The following operator-owned inputs were not available in the execution
environment. Secret values were not printed or recorded:

- `VELOX_ADMIN_TOKEN` or a readable `TOKEN_FILE`;
- target `PARALLEL_BENCH_WORKER_ID`;
- worker-scoped `PARALLEL_BENCH_METRICS_URL`, or a detached
  `metrics_projection.py` endpoint;
- approved `PARALLEL_BENCH_SET_CAP_CMD` containing `{cap}`, `{worker_id}`, and
  `{master_url}`;
- explicit `BENCH_DESTINATION_ID`;
- `PARALLEL_BENCH_CORRECTNESS_CMD` containing all required artifact-verifier
  placeholders;
- response directory for terminal job evidence.

The canonical payload fixture and artifact verifier are present, but their
presence alone is not evidence that a live worker can execute the workload.

## Required follow-up run

Once the prerequisites are supplied, run the existing harness on an idle host
and keep all cells isolated and comparable:

```text
MaxActiveJobs=1 → wait for cap convergence and active_jobs=0 → run batch
MaxActiveJobs=2 → wait for cap convergence and active_jobs=0 → run batch
MaxActiveJobs=3 → wait for cap convergence and active_jobs=0 → run batch
```

Use the same worker, image digest, assets, destination, job count, timeout,
metrics projection, and artifact verifier for every cell. The recommendation
must be based on `correct_videos_per_hour`, not lifecycle success alone. A cap
is eligible only when all jobs and canonical artifact verifications pass,
required metrics are present, error rate and iowait meet their limits, and
correct-videos/hour improves by the configured minimum over the previous
eligible cell. If the live matrix cannot satisfy those conditions, the
recommended threshold remains **not determined**.

## Scope and working tree safety

This preflight report does not alter runtime concurrency, lease ownership,
placement, cache leases, or singleflight. Existing unrelated local worker
downloader changes were intentionally excluded from the certification result.
