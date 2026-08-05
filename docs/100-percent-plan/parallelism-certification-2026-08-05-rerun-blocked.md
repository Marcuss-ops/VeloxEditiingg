# Worker parallelism certification rerun — 2026-08-05

## Verdict

**BLOCKED / NOT CERTIFIED.**

A fresh preflight was performed for the requested live matrix:

```text
MaxActiveJobs=1 → MaxActiveJobs=2 → MaxActiveJobs=3
```

The matrix was **not executed** and this gate submitted no jobs (unrelated
jobs already existed in the active database).
Therefore this report contains no live throughput, p95, CPU, RAM, disk-wait,
cache-hit, duplicate-download, or error measurements, and it does not set a
recommended concurrency threshold.

No worker configuration, service state, database row, lease, cache, or job was
changed by this gate.

## Preflight timestamp

```text
2026-08-05T12:14:51Z
```

## Confirmed prerequisites

| Check | Result | Evidence |
|---|---|---|
| Branch | PASS | current branch `main` |
| Repository synchronization | PASS | `HEAD == origin/main == 26688218200183e9d28116e60c8cdadb39830735` at preflight |
| Master health | PASS | `GET http://127.0.0.1:8000/health` → HTTP 200 |
| Master metrics | PASS | `GET http://127.0.0.1:8000/metrics` → HTTP 200 |
| Worker readiness | PASS | `GET http://127.0.0.1:8081/health/ready` → HTTP 200 |
| Worker identity | PASS | `velox-worker-local` |
| Worker image pin | PASS | `sha256:2d4b488aff08a2cb01d7dabd89b251dc6b28ba72157a481318de2f0b3b7ff35e` |
| Explicit destination | PASS | read-only DB query found enabled `comedy_test` (`provider=drive`) |
| Canonical fixtures/builder | PASS | fixture and strict payload builder exist |
| Artifact verifier | PASS | `verify_parallel_artifact.sh` and `verify_artifact.sh` exist; shell syntax passed |
| Offline harness | PASS | `test_parallel_bench.py`: 7 tests passed; `test_metrics_projection.py`: 3 tests passed |
| Harness dry-run | PASS | cap values `1`, `2`, `3` and placeholder expansion validated |

## Blocking conditions

### 1. No clean database baseline

The active database was queried read-only. Exact job status counts were:

```text
AWAITING_ARTIFACT  1
CANCELLED          49
FAILED             212
RENDER_FINISHED    2
SUCCEEDED          260
cancelled          3
```

`AWAITING_ARTIFACT` and `RENDER_FINISHED` are non-terminal lifecycle states
and prevent clean-state isolation before each cell. This read-only probe did
not query the worker's `active_jobs` field directly. The 49 uppercase
`CANCELLED` rows are canonical terminal records and are not counted as a
blocker; the three lowercase `cancelled` records are not one of the canonical
uppercase terminal values used by the certification harness
(`SUCCEEDED`, `FAILED`, `CANCELLED`) and require operator investigation before
being treated as clean terminal evidence.

This gate did not cancel, reconcile, resume, or otherwise mutate those rows.

### 2. Metrics projection unavailable

Current endpoint probes returned:

```text
http://127.0.0.1:8000/metrics  → HTTP 200
http://127.0.0.1:9090/metrics  → HTTP 200
http://127.0.0.1:8081/metrics  → HTTP 404
http://127.0.0.1:9101/metrics  → connection refused
```

The certification harness requires a worker-scoped metrics endpoint. The raw
worker/master endpoints are not substituted because doing so could mix fleet
metrics or misattribute CPU, RAM, disk wait, cache, duplicate-download, and
error counters. The projection process was not started by this gate.

### 3. Cap-setting path is not a harness-approved parametrized command

The repository contains `tests/worker-cert/set_local_cap.sh`, which accepts a
positive integer, edits `/var/lib/velox-worker/worker_config.json`, validates
JSON, and restarts `velox-worker.service`. It is therefore a mutating service
restart helper, not the parametrized operator command required by
`parallel_bench.py` with `{cap}`, `{worker_id}`, and `{master_url}`.

No `PARALLEL_BENCH_SET_CAP_CMD` was configured. The helper was not executed:
changing the live cap and restarting the worker without a clean baseline and
live metrics projection would not produce a valid certification.

The worker currently reports:

```json
{
  "worker_id": "velox-worker-local",
  "max_active_jobs": 1,
  "prometheus_port": 9090
}
```

### 4. Live correctness hook was not observed in this session

No correctness command or response directory was observed in the current
execution session. A valid run must provide an operator-owned verifier with all placeholders:

```text
{job_id} {worker_id} {master_url} {artifact_url} {response_json}
```

Lifecycle success without canonical artifact verification cannot count as a
correct video or support a threshold recommendation.

### 5. Credential wiring is not configured in the shell

`/tmp/velox-token.env` exists with mode `0600`; its contents were not printed
or recorded. No matching token or benchmark environment variables were present
in the shell. A future run must explicitly pass `TOKEN_FILE` or
`VELOX_ADMIN_TOKEN` without exposing the secret.

## Historical evidence excluded

The previous `/tmp/velox-cert/parallelism-certification.json` and earlier
blocked reports were not reused as live measurements. The historical artifact
was already `certified=false`, had incomplete metrics, and had no valid
recommended limit.

## Required next steps before a real matrix

1. Reconcile or otherwise operator-clear the non-terminal rows through the
   approved operational path; do not delete or manually edit SQLite rows.
2. Provide an approved repeatable cap command that the harness can expand with
   `{cap}`, `{worker_id}`, and `{master_url}` and that verifies convergence.
3. Start a detached worker-scoped metrics projection and prove it remains
   available for the full matrix.
4. Wire the token file, explicit `comedy_test` destination, correctness hook,
   and writable response directory.
5. Use the same worker, image digest, fixtures, job count, timeout, and verifier
   for all three cells.
6. Run sequentially only after every cell begins with `active_jobs=0`, then
   recommend the highest eligible cap using `correct_videos_per_hour`, zero
   errors, complete metrics, artifact correctness, p95/iowait limits, and the
   documented minimum throughput gain.

Until those conditions are met, the recommended threshold remains **not
determined**.
