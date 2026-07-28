# tests/smoke/full_payload/

Reusable smoke matrix for the post-2026-07-28 fleet validation. Owned by the
VeloxEditingg fleet smoke owner; touches no production code.

## Files

- `fixtures/scenario.json` — the canned SubmitJobRequest payload. **4 scene**
  (within the 3–5 range), mixed **ASS+SRT** `subtitle_tracks`, `layers[]` covering
  all three overlay roles (`role=name` Jackie Chan, `role=important_phrase` "Una
  scoperta incredibile", `role=overlay` "Mai"), `destination_id=comedy_test`,
  `target_executor_id=scene.composite.v1@1`. Reused verbatim across runs.
  `idempotency_key` / `target_executor_id` / `delivery_plan[0].destination_id`
  are NOT substituted at file-load time; they are overridden per run by
  `run.sh` via `jq` field assignment (no `sed` on JSON).
- `run.sh` — submitter. Mints an ephemeral M2M key via
  `POST /api/v1/admin/m2m/keys`, POSTs the substituted payload to
  `${VELOX_MASTER_URL}/api/v1/jobs`, polls `GET /api/v1/jobs/{job_id}` until
  `SUCCEEDED` (exp backoff 1→2→4→8→16s, capped at `FULLPAYLOAD_POLL_TIMEOUT_S`),
  and writes an evidence file under `evidence/run-<EPOCH>.json`.
- `evidence/` — produced by `run.sh --mode=submit`. Each successful run writes
  ONE `run-<EPOCH>.json` (atomic via tmp + mv).

## Quick-start

```sh
# CI shape: render the substituted payload + a short summary, no HTTP.
tests/smoke/full_payload/run.sh --mode=dry

# CI / pre-merge shape: build + forbidden-pattern self-check, no HTTP.
tests/smoke/full_payload/run.sh --mode=selftest

# Operator shape: full HTTP submit + poll + evidence (writes
# tests/smoke/full_payload/evidence/run-<EPOCH>.json).
VELOX_MASTER_URL=http://127.0.0.1:8080 \
VELOX_ADMIN_TOKEN="$(cat /etc/velox/admin.token)" \
TOKEN_FILE=/etc/velox/admin.env \
tests/smoke/full_payload/run.sh
```

`--mode=submit` is the default when no mode flag is passed.

## Environment overrides

| Variable | Default | Notes |
|---|---|---|
| `VELOX_MASTER_URL`               | `http://127.0.0.1:8080` | master HTTP base. Trim trailing `/`. |
| `VELOX_ADMIN_TOKEN`              | _required_ for `--mode=submit` | bearer for `/api/v1/admin/m2m/keys` |
| `TOKEN_FILE`                     | _unset_                 | dotenv alternative; first `VELOX_ADMIN_TOKEN=` line wins |
| `FULLPAYLOAD_IDEM_KEY`           | `full-payload-4-scenes-<EPOCH>` | idempotency_key |
| `FULLPAYLOAD_DESTINATION_ID`     | `comedy_test`           | `delivery_plan[0].destination_id` |
| `FULLPAYLOAD_TARGET_EXECUTOR_ID` | `scene.composite.v1@1`  | `target_executor_id` |
| `FULLPAYLOAD_SCENARIO`           | `<self-dir>/fixtures/scenario.json` | absolute path override |
| `FULLPAYLOAD_EVIDENCE_DIR`       | `<self-dir>/evidence`   | evidence output directory |
| `FULLPAYLOAD_POLL_TIMEOUT_S`     | `240`                   | poll cap (seconds) |

## Exit codes

See header in `run.sh`. Quick map:

- `0`  SUCCEEDED + evidence written (or `selftest`/`dry` built OK).
- `2`  bad usage / missing token / missing curl|jq / unreadable scenario.
- `3`  network unreachable (M2M provisioning OR POST/GET curl-level failure).
- `4`  HTTP non-{201,202} (M2M issue OR POST rejected at intake).
- `5`  POST 202 received but `.job_id` missing in body.
- `6`  job reached `FAILED`/`CANCELLED` during poll.
- `7`  poll timeout without terminal state.
- `8`  HTTP non-200 on GET during poll.
- `9`  `selftest` detected a forbidden asset-URI pattern (script regression).

## Asset reuse

`scenario.json` references ONLY asset IDs from
[`tests/worker-cert/fixtures/assets.json`](../worker-cert/fixtures/assets.json):

| Kind       | Asset IDs                                                          |
|------------|--------------------------------------------------------------------|
| voiceover  | `asset-recording-001`, `e2e-narrator-001`                          |
| clips      | `opening-clip-01`, `main-clip-01`, `aqueduct-clip-01`, `aqueduct-clip-02` |
| subtitles  | `subtitle-001` (ASS), `subtitle-002` (SRT)                         |

Do **NOT** introduce new `asset_id` rows in this directory. If a future scene
needs an asset that is not yet in the master registry, register it via the
master admin surface (`201` + `status=READY`) and then add a row to
`tests/worker-cert/fixtures/assets.json` so other workers / smokes can reuse it.

## Evidence layout

For each successful `--mode=submit`, `evidence/run-<EPOCH>.json` is written
atomically (tmp + mv). Schema:

```jsonc
{
  "schema": "tests/smoke/full_payload@1",
  "job_id": "job_...",
  "status": "SUCCEEDED",
  "target_executor_id": "scene.composite.v1@1",
  "destination_id": "comedy_test",
  "render_time_ms": 2157,
  "artifact_size_bytes": 1915466,
  "started_at": "2026-07-28T...",
  "completed_at": "2026-07-28T...",
  "artifact_url": "...",
  "scene_count": 4,
  "voiceover_paths_count": 2,
  "subtitle_tracks_count": 2,
  "layer_count": 3,
  "smoke_runner_rev": 3,
  "written_at": "2026-07-28T..."
}
```

`smoke_runner_rev` mirrors `SMOKE_PLUCKER_VARS_REV` from
`tests/worker-cert/lib/pluck.sh`. Bump it there when changing helper signatures.

## Cross-references

- Drives the unfinished checks listed at the bottom of
  [`docs/operations/04-veloxediting-final-smoke-checklist.md`](../../docs/operations/04-veloxediting-final-smoke-checklist.md)
  §1–4 (subtitle sync accuracy, ASS/SRT special characters, overlay text
  rendering with special names + important phrases + highlighted words,
  audio-preservation alongside voiceover).
- Complements — does NOT replace — [`tests/worker-cert/smoke_one.sh`](../worker-cert/smoke_one.sh),
  which is the per-worker cert smoke (placement pin). This directory owns the
  **fleet-wide** matrix payload; `smoke_one.sh` keeps the per-worker shape.
- Source-of-truth schema:
  [`shared/contract/canonical_payload.go`](../../shared/contract/canonical_payload.go),
  [`DataServer/internal/apiwire/apiwire.go`](../../DataServer/internal/apiwire/apiwire.go)
  (SubmitJobRequest), [`DataServer/api/openapi.yaml`](../../DataServer/api/openapi.yaml).
