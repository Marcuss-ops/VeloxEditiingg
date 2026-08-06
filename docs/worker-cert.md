# Worker Certification Matrix

> **Purpose.** Single source of truth for which workers in the fleet are
> cert-ready. Each row in the matrix below is updated incrementally after
> a smoke run by reading `tests/worker-cert/workers/<worker_id>/smoke.json`
> + `pass_criteria.json` and bumping the 3 columns. The PASS criterion
> flow at the bottom of this doc describes what "cert-ready" means in
> observable terms; the matrix tracks who-meets-it.

## Fleet matrix (4 targets)

The 4 worker-cert targets per the user-spec checklist. Each cell carries
a binary verdict that reflects the most recent smoke run; check the
**Evidence** column for the `job_id` / `task_id` / `attempt_id` lineage.

| Worker | Connessione verificata | Executor verificato | Render completo verificato | Evidence (job / status) |
| --- | :-: | :-: | :-: | --- |
| `host_57_129_132_133` | ✅ | ✅ | ✅ | `job_6138f86480ce8762` → SUCCEEDED |
| `host_57_131_20_173` | ✅ | ✅ | Da testare | placeholder — run `pass_criteria.sh` |
| `velox-worker-13197` | ✅ | ✅ | Da testare | placeholder — run `pass_criteria.sh` |
| `velox-worker-523925eb` | ✅ | ✅ | Da testare | placeholder — run `pass_criteria.sh` |

**Column semantics.**

| Column | Asserts | Source |
| --- | --- | --- |
| Connessione verificata | `GET /api/v1/workers/<worker_id>.status == "CONNECTED"` AND `.session_active == true` | `pass_criteria.sh` steps 1+2 |
| Executor verificato | `executors[]` includes `id=scene.composite.v1` `version=1` AND a real-asset smoke completes (`render_time_ms > 0`) | `pass_criteria.sh` steps 3+8 |
| Render completo verificato | `pass_criteria.sh` reports a SUCCEEDED job with the artifact verified by `verify_artifact.sh` (`ffprobe` + SHA-256 round-trip + master side `status=SUCCEEDED`) | `pass_criteria.sh` steps 9+10 |

## PASS criterion flow (canonical 10 steps)

The "cert-ready" verdict for a worker is the conjunction of all 10 steps
being satisfied in sequence. Each step has an observable evidence
source — green CI is mechanical, not heuristic.

| # | Step | Asserts on | Evidence (canonical) |
| --- | --- | --- | --- |
| 1 | CONNECTED | `GET /api/v1/workers/<worker_id>.status` | `status=CONNECTED` |
| 2 | session_active | `.session_active == true` | worker_sessions ACTIVE row visible |
| 3 | `scene.composite.v1@1` advertised | `.executors[]` includes the executor | hello/heartbeat capabilities match `pkg/api.CapabilitySchemaVersion = 1` |
| 4 | TaskLeaseGranted | master log + `smoke_scrape_lease` | `[GRPC] TaskLeaseGranted sent to worker <ID> ... task=<TID> attempt=<AID> lease=<LID>` |
| 5 | TaskAccepted | master log | `[GRPC] TaskAccepted task=<TID>` |
| 6 | asset scaricati | master log + `task_attempt_metrics.download_ms` | download_ms > 0 OR asset_paths populated |
| 7 | RUNNING | master log OR `worker_task_runtime.runtime_status` | `runtime_status=RUNNING` |
| 8 | rendering | master log | `engine_render_started` OR `StageExecutor` marker |
| 9 | artifact verificato | `verify_artifact.sh` rc=0 on downloaded artifact | h.264 video + AAC audio + duration/w/fps thresholds + master side `status=SUCCEEDED` |
| 10 | SUCCEEDED | `GET /api/v1/jobs/<job_id>.status` | `status=SUCCEEDED` AND `artifact_url` populated |

**Binary verdict:** `tests/worker-cert/pass_criteria.sh <worker_id>` returns
`exit 0` (PASS) on full green, `exit 10+<step>` on the first failing step
(steps are recorded in `workers/<worker_id>/pass_criteria.json` so an
operator reading a post-mortem can identify which step regressed without
re-running).

## Update procedure (after each smoke)

The matrix is meant to be updated incrementally. After running a smoke,
the operator runs one of the `jq` snippets below and bumps the
corresponding 3 cells. The snippets read from the canonical per-worker
JSON output of the worker-cert harness — no manual copy-paste needed.

### Remote worker update certification

Before promoting a pinned worker image, run the update mode against a
canary worker with the environment in
`scripts/cert/remote-worker-cert.env.example`:

```bash
# Load a protected, untracked dotenv file into the child process.
set -a
. ./scripts/cert/remote-worker-cert.env
set +a
bash scripts/cert/remote-worker-cert-config.sh --update-json > update.json
jq -e '.overall == "PASS"' update.json
```

The `U01`–`U06` checks are deliberately observational: the Master API and
`UpdateExecutor` remain the owners of the mutation. They verify, in order:

1. the pre-update worker is connected and advertises a complete `ReleaseIdentity`;
2. the update is accepted as HTTP `202` with a queued operation;
3. automatic drain is visible and the canonical active lease/slot count reaches zero;
4. the update operation reaches `SUCCEEDED` after restart/readiness and the
   executor's own Level-D smoke/rollback decision;
5. the worker reconnects with the requested `ReleaseIdentity.image_digest` and
   fresh Level-D smoke evidence;
6. any remaining operator-owned drain/quarantine is cleared only through the
   canonical asynchronous resume operation, then the worker is placement-eligible.

`RW_UPDATE_TARGET_IMAGE` must be a complete immutable `ghcr.io/...@sha256:<64hex>`
reference; this exact reference is sent as the API `target_digest` value.
`RW_UPDATE_TARGET_DIGEST` must match its `sha256:<64hex>` suffix and is used
for the post-update `ReleaseIdentity.image_digest` comparison.
Never run update mode against the whole production fleet automatically; use a
canary first and preserve the JSON report as release evidence.

### Cert-ready verdict (binary)

```bash
# For a single worker:
jq -er '.verdict' tests/worker-cert/workers/<worker_id>/pass_criteria.json
# → "PASS" (all 10 steps) | "FAIL" (first failing step recorded)

# For all 4 workers:
for w in host_57_129_132_133 host_57_131_20_173 velox-worker-13197 velox-worker-523925eb; do
  printf '%-30s %s\n' "$w" \
    "$(jq -er '.verdict // "NOT_RUN"' tests/worker-cert/workers/$w/pass_criteria.json 2>/dev/null)"
done
```

### Per-worker first-failing step (when verdict != PASS)

```bash
jq -er '"first_failing_step=\(.first_failing_step) name=\(.steps[.first_failing_step - 1].name)"' \
  tests/worker-cert/workers/<worker_id>/pass_criteria.json
```

### Render timing + size (for the "Render completo verificato" cell)

```bash
jq -er '"artifact_size_bytes=\(.artifact_size_bytes) render_ms_engine=\(.render_ms_engine)"' \
  tests/worker-cert/workers/<worker_id>/pass_criteria.json
```

### CI integration (recommended for `tests/e2e/grpc-control-plane/run.sh`)

Wire `pass_criteria.sh` as a post-flight assertion per case row. After
each case-N matrix row completes, run `pass_criteria.sh <worker_id>` and
assert exit 0; surface the failing step in the JUnit XML so the CI
matrix reports a precise root cause.

## Deterministic job fixtures and lifecycle evidence

The P02/P03 job mode accepts `RW_JOB_FIXTURE_FILE` and uses the committed
fixtures under `tests/worker-cert/fixtures/jobs/`:

- `minimal-render-job.json` — one short clip/voiceover scene;
- `shared-stock-job.json` — two scenes reusing the same stock asset, proving
  shared-stock normalization and cache behavior;
- `invalid-job.json` — `duration_seconds=0.05`, which must be rejected by
  intake with HTTP 422 and must never enter worker polling.

For a valid fixture, `--job-json` records every response from
`GET /api/v1/jobs/<job_id>` and fails unless the required ordered sequence is
observed exactly as configured by `RW_JOB_REQUIRED_STATES` (default:
`PENDING,LEASED,RUNNING,AWAITING_ARTIFACT,SUCCEEDED`). Intermediate states are
allowed, but no required state may be skipped or observed out of order. The
final artifact is downloaded only after `SUCCEEDED`; the runner computes
SHA-256 and runs `ffprobe -v error -show_entries format=duration,size -of json`,
checking that the probed byte size matches the downloaded file and any server
reported size.

Example valid run:

```bash
set -a; . ./scripts/cert/remote-worker-cert.env; set +a
RW_JOB_FIXTURE_FILE=./tests/worker-cert/fixtures/jobs/minimal-render-job.json \\
  bash scripts/cert/remote-worker-cert-config.sh --job-json > job.json
jq -e '.overall == "PASS" and ([.checks[] | select(.id == "P02-poll" and .status == "PASS")] | length == 1)' job.json
```

Example intake-negative run:

```bash
RW_JOB_FIXTURE_FILE=./tests/worker-cert/fixtures/jobs/invalid-job.json \\
RW_JOB_DESTINATION_ID=drive-certification \\
RW_JOB_EXPECTED_SUBMIT_STATUS=422 \\
  bash scripts/cert/remote-worker-cert-config.sh --job-json > invalid-job.json
jq -e '.overall == "PASS" and .job_id == null' invalid-job.json
```

## Fleet certification runner

Use `scripts/cert/certify-remote-fleet.sh` to apply the existing single-worker
certification contract to an explicit worker list. The runner is serial by
default (and `--serial` documents the contract), isolates evidence per worker,
and writes an aggregate `fleet-report.json`, `report.json`,
`fleet-report.junit.xml`, and `commands.log` under the fleet artifact
 directory.

```bash
# Fast post-deploy certification
bash scripts/cert/certify-remote-fleet.sh \
  --mode quick --workers worker-01,worker-02 --serial

# Release promotion gate
bash scripts/cert/certify-remote-fleet.sh \
  --mode full --workers worker-canary --serial
```

Mode composition:

- `quick`: runs `--worker-json` for every worker;
- `full`: runs `--worker-json`, `--lifecycle-json`, `--update-json`, and
  `--job-json` in that order, stopping later phases for a worker after the
  first failed prerequisite;
- `destructive`: invokes `tests/worker-cert/worker_offline_recovery.sh` for
  each worker and must never be used against production.

Destructive mode is fail-closed. It requires `VELOX_CERT_ENV` to be one of
`staging`, `canary`, `development`, `test`, or `local`, requires
`VELOX_CERT_ALLOW_DESTRUCTIVE=1`, the exact acknowledgement
`VELOX_CERT_DESTRUCTIVE_ACK=I_UNDERSTAND_DESTRUCTIVE_CERT`,
`RW_WORKER_CRASH_CMD`, and `RW_JOB_DESTINATION_ID`. Production-like environment
names, `VELOX_PRODUCTION=1`, and production-like master URLs are rejected even
when the opt-in variables are present. The wrapper never prints or writes
credential values.

Each worker gets a directory named by its validated worker ID. The aggregate
fleet report is `PASS` only when every requested worker passes; failures are
recorded while the remaining workers continue. Duplicate or malformed worker
IDs are rejected before any worker is invoked.

The fleet runner installs an EXIT trap and performs idempotent final cleanup:
network restoration runs only when `RW_FLEET_NETWORK_RULES_APPLIED=1`, and
worker-start verification runs for every live mode. Only a run-owned `mktemp`
transit directory and intermediate NDJSON are removed; evidence files remain
available for audit. Configure `RW_FLEET_RESTORE_NETWORK_CMD` and
`RW_FLEET_WORKER_START_CMD` with the operator-owned restoration/start commands.

Before the final report, the runner executes the configured
`RW_FLEET_ORPHAN_CHECK_CMD`. It must emit JSON with non-negative
`leases`, `jobs`, `tasks`, and `operations` counts. Any non-zero count fails
the fleet verdict and is recorded under `invariants`; invalid/missing output
also fails closed. Offline tests may explicitly set
`RW_FLEET_INVARIANTS_MODE=skip`, but live certification defaults to
`required`.

## Machine-readable run evidence

Every CLI certification mode (`--network-json`, `--worker-json`,
`--lifecycle-json`, `--update-json`, `--smoke-json`, and `--job-json`) writes a
run-scoped evidence directory. `RW_RUN_ID` may be supplied explicitly;
otherwise the runner generates `cert-<UTC timestamp>-<pid>`. The directory is
selected with `RW_ARTIFACT_DIR` and defaults to `/tmp/velox-cert-<run_id>`.

The runner always creates these files, including when configuration or runtime
checks fail:

- `report.json` — final `run_id`, mode, exit code, `overall` (`PASS`/`FAIL`),
  check list, and the raw mode result under `result`;
- `report.junit.xml` — JUnit-compatible testcase/failure projection of the
  check list;
- `commands.log` — sanitized method/path and SSH markers only; credentials and
  request bodies are never recorded;
- `worker-before.json` / `worker-after.json` and
  `master-before.json` / `master-after.json` — first and latest observed JSON
  snapshots (or `NOT_OBSERVED` markers);
- `operations.json` — every observed API operation with method, path, HTTP
  status, response, `run_id`, and final PASS/FAIL status;
- `artifact-ffprobe.json` — artifact verifier report, SHA-256, file and final
  PASS/FAIL status when P03 runs (otherwise `NOT_RUN`).

The JSON emitted on stdout remains the mode-specific result for existing
operators and CI consumers. The evidence files are supplementary and can be
archived as release evidence.

## Cross-references

- `tests/worker-cert/smoke_one.sh` — per-worker SUCCEEDED smoke (writes `smoke.json`)
- `tests/worker-cert/pass_criteria.sh` — 10-step PASS criterion flow (writes `pass_criteria.json`)
- `tests/worker-cert/verify_artifact.sh` — offline `ffprobe` + master-side status check
- `tests/worker-cert/build_real_payload.py` — canonical real-asset payload (no `velox-asset://voiceovers/<file>.mp3`)
- `tests/worker-cert/fixtures/assets.json` — curated asset_ids for the smoke harness
- `tests/worker-cert/lib/pluck.sh` — M2M provisioning + worker-list + lease-scraping helpers
- `docs/worker_deployment.md` — operator-side deployment runbook
- `docs/operations/04-velox-final-smoke-checklist.md` — e2e checklist (precedes worker-cert)

## Update history (chronological)

| Date | Worker | Step regressed / Smoked | Source |
| --- | --- | --- | --- |
| 2026-07-25 | `host_57_129_132_133` | Render ✅ — full 10-step PASS via `job_6138f86480ce8762` | historical (master log + smoke.json) |

> **Convention.** Append a row each time a smoke or pass_criteria run
> completes. Newest at the bottom. Do not delete rows — keep the
> regression trail visible.
