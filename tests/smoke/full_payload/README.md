# tests/smoke/full_payload/

Reusable smoke matrix for the post-2026-07-28 fleet validation. Owned by the
VeloxEditingg fleet smoke owner; touches no production code.

## Files

- `fixtures/scenario.json` — the canned SubmitJobRequest payload for **2 stock
  scenes of 6 seconds each**, with nested voiceover bindings, a background-music
  track, one ASS subtitle source (derived into one global track), `delivery_plan`
  and worker placement pin.
  `run.sh` resolves the two clips and voiceover from the canonical worker-cert
  fixture, and requires real registered music, ASS subtitle and worker IDs at
  runtime. A fresh `idempotency_key` and all asset references plus worker pin,
  `target_executor_id` and `delivery_plan[0].destination_id` are overridden by
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
# Supply only IDs returned by the Master asset registry with status READY. The
# runner validates SHA-256 syntax and duration, but does not replace the required
# Master-side READY/accessibility preflight. The music and ASS IDs must be
# 64-character SHA-256 asset IDs; the music must last at least 12 seconds.
export FULLPAYLOAD_WORKER_ID=host_57_129_132_133
export FULLPAYLOAD_BACKGROUND_MUSIC_ASSET_ID="replace-with-real-64-hex-music-sha256"
export FULLPAYLOAD_BACKGROUND_MUSIC_DURATION_SECONDS=12
export FULLPAYLOAD_SUBTITLE_ASSET_ID="replace-with-real-64-hex-ass-sha256"

# CI shape: render the substituted payload + a short summary, no HTTP.
tests/smoke/full_payload/run.sh --mode=dry

# CI / pre-merge shape: build + forbidden-pattern self-check, no HTTP.
tests/smoke/full_payload/run.sh --mode=selftest

# Operator shape: full HTTP submit + poll + evidence (writes
# tests/smoke/full_payload/evidence/run-<EPOCH>.json).
VELOX_MASTER_URL=http://127.0.0.1:8000 \
VELOX_ADMIN_TOKEN="$(cat /etc/velox/admin.token)" \
TOKEN_FILE=/etc/velox/admin.env \
tests/smoke/full_payload/run.sh
```

`--mode=submit` is the default when no mode flag is passed.

## Environment overrides

| Variable | Default | Notes |
|---|---|---|
| `VELOX_MASTER_URL`               | `http://127.0.0.1:8000` | master HTTP base. Trim trailing `/`. |
| `VELOX_ADMIN_TOKEN`              | _required_ for `--mode=submit` | bearer for `/api/v1/admin/m2m/keys` |
| `TOKEN_FILE`                     | _unset_                 | dotenv alternative; first `VELOX_ADMIN_TOKEN=` line wins |
| `FULLPAYLOAD_DESTINATION_ID`     | **required**            | `delivery_plan[0].destination_id` |
| `FULLPAYLOAD_TARGET_EXECUTOR_ID` | `scene.composite.v1@1`  | `target_executor_id` |
| `FULLPAYLOAD_WORKER_ID`          | _required_              | `placement_pin_worker_id` |
| `FULLPAYLOAD_BACKGROUND_MUSIC_ASSET_ID` | _required_         | registered SHA-256 asset ID |
| `FULLPAYLOAD_BACKGROUND_MUSIC_DURATION_SECONDS` | _required_ | must be at least 12 seconds |
| `FULLPAYLOAD_SUBTITLE_ASSET_ID`  | _required_              | registered SHA-256 ASS asset ID |
| `FULLPAYLOAD_SCENARIO`           | `<self-dir>/fixtures/scenario.json` | absolute path override |
| `FULLPAYLOAD_EVIDENCE_DIR`       | `<self-dir>/evidence`   | evidence output dir |
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

| Kind       | Source in this smoke |
|------------|----------------------|
| voiceover  | `.voiceover[0].asset_id` from `assets.json` |
| clips      | `.clips[0]` and `.clips[1]` from `assets.json` |
| background music | `FULLPAYLOAD_BACKGROUND_MUSIC_ASSET_ID` (required runtime SHA-256 for a Master `READY` asset) |
| subtitles | `FULLPAYLOAD_SUBTITLE_ASSET_ID` (required runtime SHA-256 for a Master `READY` ASS asset; attached to scene 0 and derived into one track) |

Do **NOT** introduce new `asset_id` rows in this directory. If a future scene
needs an asset that is not yet in the master registry, register it via the
master admin surface (`201` + `status=READY`) and then add a row to
`tests/worker-cert/fixtures/assets.json` so other workers / smokes can reuse it.

## Evidence layout

For each successful `--mode=submit`, `evidence/run-<EPOCH>-<client_id>.json`
is written atomically (tmp + mv). The `<client_id>` is the ephemeral M2M
client_id returned by `POST /api/v1/admin/m2m/keys`; including it makes
back-to-back runs in the same epoch second collision-safe. Schema:

```jsonc
{
  "schema": "tests/smoke/full_payload@1",
  "job_id": "job_...",
  "status": "SUCCEEDED",
  "target_executor_id": "scene.composite.v1@1",
  "destination_id": "<explicit-destination-id>",
  "render_time_ms": 2157,
  "artifact_size_bytes": 1915466,
  "started_at": "2026-07-28T...",
  "completed_at": "2026-07-28T...",
  "artifact_url": "...",
  "scene_count": 2,
  "scene_voiceover_count": 2,
  "subtitle_tracks_count": 1,
  "layer_count": 0,
  "smoke_runner_rev": 3,
  "written_at": "2026-07-28T..."
}
```

`smoke_runner_rev` mirrors `SMOKE_PLUCKER_VARS_REV` from
`tests/worker-cert/lib/pluck.sh`. Bump it there when changing helper signatures.

## Cross-references

- Drives the unfinished checks listed at the bottom of
  [`docs/operations/04-velox-final-smoke-checklist.md`](../../docs/operations/04-velox-final-smoke-checklist.md)
  §1–4 (subtitle sync accuracy, ASS/SRT special characters, overlay text
  rendering with special names + important phrases + highlighted words,
  audio-preservation alongside voiceover).
- Complements — does NOT replace — [`tests/worker-cert/smoke_one.sh`](../worker-cert/smoke_one.sh),
  which is the per-worker cert smoke. This directory owns the complete
  **two-scene stock/audio/ASS matrix payload**; `smoke_one.sh` keeps the
  per-worker shape.
- Source-of-truth schema:
  [`shared/contract/canonical_payload.go`](../../shared/contract/canonical_payload.go),
  [`DataServer/internal/apiwire/apiwire.go`](../../DataServer/internal/apiwire/apiwire.go)
  (SubmitJobRequest), [`DataServer/api/openapi.yaml`](../../DataServer/api/openapi.yaml).
