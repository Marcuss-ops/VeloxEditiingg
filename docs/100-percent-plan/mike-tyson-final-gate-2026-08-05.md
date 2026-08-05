# Mike Tyson final gate — 2026-08-05

## Verdict

**BLOCKED / NOT CERTIFIED.**

The requested 10-job Mike Tyson end-to-end battery was **not submitted**. No
10/10 result is claimed. No production or local master database was mutated by
this gate attempt, and no Drive deletion was performed.

The repository has deterministic Mike Tyson normalization/render tests, but no
official operational runner that submits ten Tyson jobs as one final battery.
The available `ops/jobs/submit_benchmark_five_boxers.sh` submits one
`benchmark-five-boxers` job and is not a substitute for the requested 10-job
Tyson gate.

## Evidence matrix

| Required check | Verdict | Evidence / reason |
|---|---|---|
| 10/10 jobs `SUCCEEDED` | **NOT VERIFIED** | No ten-job Tyson runner and no jobs submitted |
| Outputs only in requested folder | **NOT VERIFIED** | No 10-job artifact/delivery result set or authoritative DB available |
| Zero duplicate outputs | **NOT VERIFIED** | No batch manifest or authoritative delivery DB snapshot available |
| Zero `origin/scope_mismatch` | **PASS for current scrape only** | No matching mismatch series on `http://127.0.0.1:8000/metrics`; this does not prove a Tyson batch had zero mismatches |
| Cache metrics present | **PASS for endpoint presence** | Worker `:9090/metrics` exported hit/miss, downloads, bytes, durations, SHA verification, cleanup, eviction, skip, size and entries families |
| Tyson cache benefit on this batch | **NOT VERIFIED** | No Tyson jobs were submitted; observed counters predate this gate |
| Cleanup safe | **PASS for implementation/tests; NOT RUN live** | `velox-admin cleanup-drive-duplicates` and focused tests are present; no live manifest/Drive apply was executed |
| No residual `ffmpeg`/engine process | **PASS at observation time** | No active `ffmpeg` or `velox_video_engine` process detected |
| No non-terminal jobs/tasks/attempts/leases/spool | **NOT VERIFIED** | No authoritative master DB, worker spool DB and per-batch worker log were available |
| AC/TaskResult/Drive delivery convergence | **NOT VERIFIED** | The convergence gate requires the batch job ID, master DB, worker spool DB and worker log |

## Safe read-only observations

- `http://127.0.0.1:8000/health` returned `{"status":"healthy"}`.
- The local admin worker view was reachable with the documented development
  configuration, but the configured local worker was reported disconnected by
  the master API in the initial preflight snapshot.
- A later read-only health probe of the worker returned status `ok` for
  `velox-worker-local` and `registered=true`; this later observation does not
  prove that the worker remained connected for a Tyson batch, because no batch
  was submitted.
- Worker cache metrics at `http://127.0.0.1:9090/metrics` included observed
  counters/gauges such as:
  - `velox_cache_requests_total{result="hit"} 346`;
  - `velox_cache_requests_total{result="miss"} 6`;
  - `velox_cache_downloads_total{label="asset"} 6`;
  - `velox_cache_download_bytes_total{label="asset"} 981320`;
  - `velox_cache_size_bytes{label="total"} 490660`;
  - `velox_cache_entries{label="total"} 3`.
  These are endpoint observations, not Tyson-batch deltas.
- Offline Tyson normalization and hybrid compiler tests passed, as did the
  worker cache Prometheus tests.

## Why the gate stopped before submission

The required evidence and safe execution contract were not complete:

- no authoritative master DB path was accessible for this operator session;
- no explicit production/local Tyson delivery destination and output folder
  were supplied for the 10-job run;
- no dedicated ten-job Tyson submitter exists in the repository;
- the configured local worker was disconnected from the master view;
- no batch-specific artifact verifier/report was available to prove folder
  exclusivity and Drive IDs;
- no batch-specific worker log/spool evidence was available for ACK, stale
  spool, lease and terminal-state checks.

Submitting arbitrary jobs to the running master would create incomplete
records without a defensible final verdict, so the gate failed closed.

## Rerun conditions

Before rerunning, provide or provision all of the following:

1. An operator-approved ten-job Tyson submission command using the canonical
   Mike Tyson payload and unique idempotency keys.
2. The authoritative master DB path, worker state/spool path, worker log, and
   the exact requested delivery destination/output folder.
3. A connected worker target with Prometheus enabled and a stable image/engine
   digest.
4. A canonical artifact verifier that checks every output and its Drive file
   ID, plus a manifest for duplicate cleanup.
5. A detached worker-scoped metrics projection if the raw worker/master
   endpoints cannot be scoped safely.

Then run the ten jobs sequentially or under the approved concurrency cap,
record per-job IDs and evidence, execute the AC/TaskResult convergence gate for
each job, verify output-folder exclusivity and duplicate manifest state, check
cache deltas and telemetry mismatch counters, scan for residual processes, and
only then mark the battery certified.
