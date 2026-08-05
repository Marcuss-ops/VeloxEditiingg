# Worker parallelism certification — 2026-08-05

## Verdict

**BLOCKED / NOT CERTIFIED.**

The requested live matrix for `MaxActiveJobs=1`, `2`, and `3` was **not
executed**. No benchmark jobs were submitted by this gate, and this report
contains no throughput, p95, CPU, RAM, disk-wait, cache, duplicate-download,
or error measurements for the requested matrix. No concurrency threshold is
recommended.

The existing lease, placement, cache-lease, and download-singleflight paths
were not changed. The existing harness remains the source of truth:

```text
tests/worker-cert/parallel_bench.py
```

## Verification timestamp

Read-only runtime probes completed at:

```text
2026-08-05T12:10:36Z
```

## Checks performed

| Prerequisite | Result | Observed evidence |
|---|---|---|
| Main branch state | PASS | `main`, `HEAD == origin/main == 7128fc18497a6d995fd1b5d35dd59c023cb078df` at preflight; unrelated local changes were present and excluded from this report commit |
| Master health | PASS | `GET http://127.0.0.1:8000/health` returned HTTP 200 |
| Master metrics | PASS | `GET http://127.0.0.1:8000/metrics` returned HTTP 200 and exposed worker/cache metric families |
| Worker health/readiness | PASS | `GET http://127.0.0.1:8081/health/ready` returned HTTP 200; worker `velox-worker-local` reported ready |
| Worker identity/image | PASS | config reports `worker_id=velox-worker-local`, `max_active_jobs=1`, image digest `sha256:2d4b488aff08a2cb01d7dabd89b251dc6b28ba72157a481318de2f0b3b7ff35e` |
| Explicit destination | PASS | read-only DB query found enabled `comedy_test` destination (`provider=drive`) |
| Canonical fixture/builder | PASS | `tests/worker-cert/fixtures/assets.json` and `build_real_payload.py` exist; Python compilation passed |
| Canonical artifact verifier | PASS | `verify_parallel_artifact.sh` and `verify_artifact.sh` exist; shell syntax validation passed |
| Offline harness tests | PASS | `test_parallel_bench.py`: 7 tests passed; `test_metrics_projection.py`: 3 tests passed |
| Harness dry-run | PASS | caps `1`, `2`, `3` and placeholder expansion validated; dry-run made no runtime changes and submitted no jobs |

## Blocking prerequisites

### 1. No approved cap-setting command

The live harness requires an operator-owned repeatable command containing
`{cap}`, `{worker_id}`, and `{master_url}`. No
`PARALLEL_BENCH_SET_CAP_CMD` was configured, and repository inspection found
only the documented example. The worker configuration currently advertises
`max_active_jobs=1`.

Editing `/var/lib/velox-worker/worker_config.json` or restarting
`velox-worker.service` was not performed: neither is an approved certification
cap command, and doing so would change the live runtime outside the requested
read-only preflight.

Without a verified command, the harness cannot safely perform the required
sequence:

```text
set 1 → wait for cap convergence and active_jobs=0 → run batch
set 2 → wait for cap convergence and active_jobs=0 → run batch
set 3 → wait for cap convergence and active_jobs=0 → run batch
```

### 2. Worker-scoped metrics projection unavailable

The required endpoint checks returned:

```text
http://127.0.0.1:8081/metrics  → HTTP 404
http://127.0.0.1:9101/metrics  → connection refused
http://127.0.0.1:8000/metrics  → HTTP 200
http://127.0.0.1:9090/metrics  → required cache families observed
```

The raw `9090` and aggregated master metrics are not substituted silently:
the harness requires a worker-scoped projection so CPU, RAM, disk wait, cache
counters, duplicate downloads, and errors are attributable to the certified
worker without adding high-cardinality labels. The projection process was not
started by this gate.

### 3. Existing non-terminal jobs prevent a clean baseline

A read-only query of the active database found:

```text
status           count
---------------  -----
RENDER_FINISHED  2
```

These rows are non-terminal and violate the clean isolation requirement before
each cap cell. The gate did not alter, cancel, reconcile, or submit jobs to
avoid contaminating another operation's state.

## Credential and correctness-hook state

`/tmp/velox-token.env` exists with restrictive permissions (`0600`); its value
was not printed or recorded. No token environment variable or benchmark
configuration variable was present in the shell used for the preflight. The
harness must be invoked with an explicitly configured `TOKEN_FILE` or
`VELOX_ADMIN_TOKEN` without recording the secret.

No live correctness command or response directory was configured for the
matrix. This is a hard blocker because lifecycle `SUCCEEDED` alone is not a
correct video. A future run must provide an operator-owned verifier command
containing all required placeholders:

```text
{job_id} {worker_id} {master_url} {artifact_url} {response_json}
```

and a writable `--response-dir`.

## Historical evidence deliberately excluded

`/tmp/velox-cert/parallelism-certification.json` was inspected only to
identify prior evidence. It is not reused as current certification data: it
has `certified=false`, `efficient_limit=null`, incomplete metrics, an
`INCOMPLETE` cap-1 cell, and failed cap-2/cap-3 cells. It supplies no valid
1/2/3 recommendation.

The previous preflight report and offline tests likewise do not constitute a
live workload result.

## Required conditions for the next real run

Before running the matrix, provide and verify all of the following:

1. an approved, repeatable cap command with `{cap}`, `{worker_id}`, and
   `{master_url}`;
2. a clean worker/database state with no non-terminal jobs and
   `active_jobs=0` before every cell;
3. a detached worker-scoped metrics projection that remains alive for the
   entire matrix and exposes CPU, RSS, iowait, cache hit/miss, downloads,
   duplicate-download counters, and errors;
4. explicit token-file wiring without recording the token;
5. the same worker, image digest, fixture, explicit destination, job count,
   timeout, and correctness verifier for all three cells;
6. a correctness command with all required placeholders and a writable
   response directory;
7. post-cell convergence evidence proving terminal results and artifact
   correctness before advancing to the next cap.

Only after those conditions pass should the existing harness be run. The
recommended threshold must be based on `correct_videos_per_hour`, with zero
errors, complete required metrics, verified artifacts, acceptable p95/iowait,
and the documented minimum throughput gain. If any condition fails, the
threshold remains **not determined**.
