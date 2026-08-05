# Mike Tyson final gate — 2026-08-05

## Verdict

**CERTIFIED FOR THE OBSERVED LOCAL ENVIRONMENT — GATE: ALL PASS (battery 6).**

The requested 10-job Mike Tyson end-to-end battery was executed against the
running master + local worker (`velox-worker-local`) with the explicit
`comedy_test` destination. All 10 jobs `SUCCEEDED` and every operational
criterion checked by the gate passed (16/16 checks). This certifies the
observed environment, worker, destination, and battery window; it is not a
universal guarantee for other deployments or future runs. Fixes required to
reach this state are listed below with their commits; all are on `main`.

The earlier report (bot commit `a9b73dc4`, 08:54 UTC) recorded
**BLOCKED / NOT CERTIFIED** because it was written before any ten-job battery
had been submitted. It is superseded by this document.

## Battery evidence

Battery 6 window: `2026-08-05T09:02:19Z → 2026-08-05T09:03:08.96Z`
(10 jobs pinned to `velox-worker-local`, destination `comedy_test`).

| # | Required check | Verdict | Evidence |
|---|---|---|---|
| 1 | 10/10 jobs `SUCCEEDED` | **PASS** | 10/10 in battery results |
| 2 | Outputs only in requested folder | **PASS** | 10 deliveries, all `destination_id=comedy_test`, zero foreign/undelivered rows |
| 3 | Zero duplicate outputs | **PASS** | Zero duplicate `remote_id` in `comedy_test` |
| 4 | Zero `origin/scope` mismatch | **PASS** | Zero quarantined telemetry events in window; zero `TELEMETRY_QUARANTINE` log lines; 263 events persisted |
| 5 | Cache hit/miss + bytes | **PASS** | `velox_cache_requests_total{result="hit"} 157`, `{result="miss"} 3`, downloads 3, bytes 490660 — first-use MISS+download, then HIT+0 bytes |
| 6 | Zero cleanup before first snapshot | **PASS** | Runtime proxy plus barrier implementation/tests: `no_snapshot` skips 0, `protection barrier is not ready` 0, first cleanup `removed=0`; timestamped readiness evidence is not emitted by this gate |
| 7 | Zero shared stocks deleted | **PASS** | Zero cache removals during battery, `protected_kept=27`, zero pressure evictions, all batch assets present on disk |
| 8 | Zero ffmpeg residue | **PASS** | No residual `ffmpeg` process |
| 9 | Zero non-terminal state | **PASS** | jobs=0, tasks=0, leases=0, deliveries=0, stale spool=0 |
| 10 | AC/artifact/delivery convergence evidence | **PASS** | Gate verified for all 10 jobs: artifact `READY`, attempt commit `COMMITTED`, and `comedy_test` delivery with Drive `remote_id`; task/attempt status and explicit commit-ACK receipt were not independently queried by this gate |

## Battery-driver caveat

The driver completed all ten jobs as `SUCCEEDED`, but returned `EXIT=1` because its
legacy artifact subcheck printed `no artifact_url` for the job responses. The
independent database/log/Prometheus gate does not depend on that response field
and returned `GATE: ALL PASS`; this response-field discrepancy should be fixed
in the driver before treating its exit code alone as a certification signal.

## Fixes required on the path to certification

1. **Global delivery fan-out (master)** — `completion.InsertDeliveriesForJob`
   CROSS JOINed the final video with *all enabled global*
   `delivery_destinations`, delivering every job to unrelated folders and
   defeating the explicit per-job `delivery_plan`. Now routes only to the
   job's explicit plan (`job_delivery_plans`); render-only jobs get zero
   deliveries. Commit `2f8d7132` + invariant test
   `TestCommitAttempt_DeliversOnlyToExplicitPlanDestinations`.
2. **Telemetry origin/scope contract (worker + shared catalog)** — the
   master quarantined `quality.sha256` / `quality.ffprobe` events because the
   worker emitted them as `artifact`-scoped before any `artifact_id` exists.
   The shared catalog now maps these two render-validation events to
   `attempt` scope (worker and master validate through the same catalog), so
   zero events are quarantined and the TaskResult never fails. Commit
   `d599c354`.
3. **Asset cache first-use MISS → HIT (worker)** — payloads without an
   expected digest wrote `<assetID>.<ext>`, but the remembered-integrity
   upgrade made later lookups expect `<assetID>_<sha12>.<ext>`, so every
   access re-downloaded (battery 5: hit=0). The download manager now embeds
   the computed digest in the final filename, restoring the contract
   "primo → MISS+download, successivi → HIT+downloaded_bytes=0". Commit
   `3658fafc` (canonical asset download manager).

## Operational harness

- Driver: 10-job submitter pinned to the local worker with explicit
  destination and unique idempotency keys (`/tmp/tyson/driver.sh`). Its
  terminal result rows were 10/10 `SUCCEEDED`; see the caveat above about its
  separate `artifact_url` response-field subcheck.
- Gate: 16-check verification over master DB, worker log, Prometheus
  (`:9090`), and per-job artifact/delivery rows (`/tmp/tyson/verify_gate.sh`).
  The AC check currently verifies artifact/commit/delivery proxies; it should
  be extended if explicit task/attempt status and commit-ACK receipt are
  required as independent assertions.
- Reusable CI gates for the same invariants live in the repo under
  `scripts/ci/` (AC/TaskResult convergence, golden E2E, delivery-plan
  invariants in `DataServer/internal/completion` tests).

## Repeatability

To re-run the certified battery:

1. Deploy `main` (currently includes `2f8d7132`, `d599c354`, `3658fafc`).
2. Start master + worker with Prometheus enabled on `:9090` and the worker
   state dir at `/var/lib/velox/worker` (asset cache lives under
   `asset-cache/assets/{audio,image}`).
3. Submit 10 Tyson jobs pinned to the worker with `--destination comedy_test`
   and unique idempotency suffixes.
4. Run the 16-check gate and require `GATE: ALL PASS` before certifying a new
   battery.
