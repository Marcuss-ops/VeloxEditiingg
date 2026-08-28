# Worker parallelism certification

## Scope

This runbook certifies the existing worker concurrency limiter at
`MaxActiveJobs=1..N` (default `N=8`). It does **not** implement or replace leases,
placement, cache leases, or download singleflight:

- lease ownership remains the master;
- many-to-many asset leases and singleflight remain the worker cache owner;
- the certification harness only submits jobs and observes the resulting run.

The harness is `tests/worker-cert/parallel_bench.py`.

## Reproducible protocol

Use the same worker, image digest, canonical asset fixture, destination,
number of jobs, poll timeout, and metric endpoint for every cell. By default
the harness tests every cap from 1 through 8; use `--max-cap N` to extend the
matrix. Run the cells sequentially in this order:

```text
1 → wait for cap convergence and active_jobs=0 → submit the batch → collect evidence
2 → … → N → wait for cap convergence and active_jobs=0 → submit the batch → collect evidence
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
  --max-cap 8 \
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
| correct videos/hour | artifact verifier exit-0 count / batch wall time; this is the decision metric |
| mean latency | arithmetic mean of successful job submit-to-terminal latency |
| p95 latency | interpolated p95 of successful job latencies |
| CPU average/peak | worker Prometheus CPU utilization gauge samples |
| RAM average/peak | worker Prometheus process RSS gauge samples |
| host RAM average/peak | worker host memory used/available gauges; peak ratio is a safety gate |
| file descriptors | worker FD utilization peak |
| disk free | minimum worker free-disk gauge |
| scratch peak | maximum worker scratch occupancy gauge |
| disk wait | worker Prometheus iowait ratio samples |
| cache hit/miss | delta of existing cache request counters |
| downloads | delta of existing cache download counter |
| duplicate downloads | delta of an explicitly exported duplicate-download counter; missing means incomplete, never zero |
| artifact correctness | operator-owned `--correctness-command` hook, with `{job_id}`, `{worker_id}`, `{master_url}`, `{artifact_url}`, and `{response_json}` placeholders; it must run the canonical artifact/media verifier and exit 0 only for a correct video |
| errors | failed jobs, merged with the existing error counter delta |

Prometheus labels are not copied into the report as dimensions. The harness
uses one worker endpoint (or a worker-scoped projection), so it does not add
`job_id`, `asset_id`, `hash`, or other high-cardinality labels.

### Metrics projection (required for the current deployment)

The worker telemetry emits every series with a static label
(`velox_cache_downloads_total{label="asset"}`, ...) and the master projects
per-worker resource gauges labelled `worker_id=...`, while the harness
looks up the unlabelled base names (and the cache hit/miss by
`result=` label). The local deployment therefore feeds the harness a
worker-scoped projection instead of the raw endpoints:

```bash
# serves :9101/metrics, scraping worker :9090 + master :8000/metrics,
# selecting worker_id=velox-worker-local, normalizing master micro/milli
# units back to ratios in [0,1], and stripping the static labels.
TOKEN_FILE=/var/lib/velox/evidence/velox-token.env \
  python3 tests/worker-cert/metrics_projection.py \
    --worker-id velox-worker-local --port 9101

python3 tests/worker-cert/parallel_bench.py \
  --metrics-url http://127.0.0.1:9101/metrics ...
```

Start the projection with `setsid` (or an equivalent detach) so it survives
its launching shell for the whole matrix; a died projection makes every
required measurement missing and voids the run.

The current worker/master telemetry provides the resource and cache families
documented in `docs/metrics-catalog.md`, including
`velox_worker_cpu_iowait_ratio`, `velox_worker_process_rss_bytes`,
`velox_cache_requests_total`, `velox_cache_downloads_total`, and the
singleflight dedup counters `velox_cache_duplicate_downloads_total` /
`velox_cache_duplicate_download_bytes_total` (exported by the worker since
2026-08-05). A cell is marked `INCOMPLETE` when a required measurement is not
exported. It is never silently treated as zero.

## Efficient-limit rule

A cap is eligible when:

1. every job succeeds;
2. every succeeded job has a correctness result and the canonical verifier passes;
2. error rate is within the configured limit (default `0`);
3. p95 is within `--max-p95-ms` when an SLA is supplied;
4. disk wait is at or below `--max-iowait-ratio` (default `0.35`);
5. peak host memory is at or below `--max-peak-memory-ratio` (default `0.85`);
6. FD utilization is at or below `--max-fd-util-ratio` (default `0.80`);
7. free disk never drops below `--min-disk-free-bytes` (default 10 GB);
8. correct-videos/hour improves by at least `--min-throughput-gain-pct` versus the
   previous cell (default `5%`). A lifecycle success without a verified output
   is never counted as a correct video.

The efficient limit is the highest eligible cap. A higher cap is rejected when
correct-videos/hour flattens while p95, iowait, host memory, FD usage, disk
space, errors, or duplicate downloads worsen. The report exposes this as
`certified_max_jobs`; `recommended_production_jobs` is one step lower (floor 1)
to leave operational headroom. Each cell also receives one canonical
`limiting_resource` classification: `CPU_BOUND`, `MEMORY_BOUND`, `IO_BOUND`,
`FD_BOUND`, or `UNKNOWN`. A missing duplicate-download metric or correctness hook
prevents certification rather than being interpreted as “zero duplicates” or
“correct”.

For a live run, configure the verifier explicitly. The hook receives the
terminal response in `--response-dir` and the response's artifact URL, for
example:

```bash
--response-dir /var/lib/velox/evidence/parallel-responses \\
--correctness-command 'worker_id={worker_id}; tests/worker-cert/verify_parallel_artifact.sh --job-id {job_id} --response-json {response_json} --artifact-url {artifact_url} --master-url {master_url}'
```

The harness validates that the hook template contains all five placeholders
(`{job_id}`, `{worker_id}`, `{master_url}`, `{artifact_url}`,
`{response_json}`); `verify_parallel_artifact.sh` only needs four, so prefix
the template with a harmless `worker_id={worker_id};` assignment to satisfy
the contract. Omitting `{worker_id}` rejects every job as
"correctness hook must include all job/artifact/response placeholders".

The deployment-specific wrapper must download the artifact and invoke the
canonical `verify_artifact.sh`; the harness does not guess local paths or
silently validate status alone. A missing hook or missing verifier result keeps
the cell `INCOMPLETE`.

## Current certification status

On the development host on 2026-08-05:

- the local master + worker are running under systemd; the worker advertises
  `max_active_jobs` correctly (heartbeat `task_slots` fix landed same day);
- the worker now exports the certification counters (`velox_cache_*`,
  `velox_worker_errors_total`) pre-seeded at zero, so before/after deltas are
  always defined;
- a live 1→2→3 matrix was attempted with 6 jobs/cell. The run was
  **inconclusive and NOT certified** because:
  1. the metrics projection process died mid-run (all required measurements
     missing → every cell `INCOMPLETE`);
  2. the correctness hook template was missing `{worker_id}` (fixed above), so
     no artifact was verified;
  3. 2 of 18 jobs failed under a heavily contended host (load ≈ 27 from
     co-located agent workloads; the two failures were not diagnosed);
  4. the host was shared with other workloads, so CPU/RAM samples would have
     been noisy even with a live projection.

No 1/2/3 performance numbers are claimed from that attempt. Re-run protocol:
start the projection detached (`setsid`), run the matrix on an idle host with
all cells pinned to the same worker, then record the evidence JSON. The
repository contains no fabricated benchmark result.

## Related gate

After a battery, `scripts/ci/check-ac-taskresult-convergence.sh` proves the
AC/TaskResult convergence contract for every job (job SUCCEEDED → task
SUCCEEDED → attempt SUCCEEDED → artifact READY → commit ACK → delivery
COMPLETED with Drive file ID) plus the zero-state checks (no RUNNING
jobs/tasks, no expired leases, no old spool rows, no non-terminal
deliveries). It is wired into `golden-e2e.yml` / `ci.yml`.
