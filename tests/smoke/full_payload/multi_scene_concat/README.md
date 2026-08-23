# Multi-Scene Concat Smoke (`tests/smoke/full_payload/multi_scene_concat/`)

A re-runnable smoke that submits a `>=10`-scene concatenated real-asset payload to
the Velox Master pinned to a single worker, polls until `SUCCEEDED`, and
verifies four things:

| Tier | Verifies | Mechanism |
|---|---|---|
| **T1** | `SUCCEEDED` reached via the master state machine | poll `GET /api/v1/jobs/{job_id}` until `.status == SUCCEEDED` (1→2→4→8→16s exp backoff, cap `MULTISCENE_POLL_TIMEOUT_S=600`) |
| **T2** | duration coherence (sum of `scene.duration_seconds` vs measured output `duration_seconds` via `ffprobe`) | download artifact via the bearer-authed `artifact_url`, run `ffprobe -show_format`, compare against expected `± max(500ms, 5%)` |
| **T3** | Drive delivery to the explicitly selected destination | `status == SUCCEEDED` ⇒ all `delivery_plan` entries committed (canonical monotonic guarantee per `SubmitJobStatusResponse`) **and** `artifact_size_bytes > 0` (HEAD probe via `smoke_artifact_size`) |
| **T4** | placement-pin enforcement (lease was issued to `<worker_id>`) | scrape master log line `TaskLeaseGranted sent to worker <ID> (...) task=<TID> job=<JID> attempt=<AID> lease=<LID>` via `smoke_scrape_lease`; fallback to `current_task_id` race-prone check |

In addition, the smoke records **CPU/RAM/disk** measurement: a worker metrics
snapshot is taken BEFORE submit and AFTER `SUCCEEDED`, then the harness reports
`disk_free_bytes` delta, `cpu_utilization_ratio`, and `memory_used_bytes` for
the duration of the run. The metrics map is sourced from
`DataServer/internal/handlers/server/api/workers_dto.go §Worker.Metrics`.

## Quick start

```bash
# Dry run (no HTTP) — verifies payload shape + forbidden-pattern self-check
tests/smoke/full_payload/multi_scene_concat/run.sh --mode=dry

# Selftest (no HTTP) — same as dry + extra regex-walk of the emitted JSON
tests/smoke/full_payload/multi_scene_concat/run.sh --mode=selftest

# Full HTTP submit (requires VELOX_MASTER_URL + VELOX_ADMIN_TOKEN)
VELOX_MASTER_URL=https://velox.example.com \
VELOX_ADMIN_TOKEN=$(cat ~/.config/velox/admin-token) \
MULTISCENE_TARGET_WORKER_ID=host_57_131_20_173 \
tests/smoke/full_payload/multi_scene_concat/run.sh

# Override defaults
tests/smoke/full_payload/multi_scene_concat/run.sh \
  --worker-id host_57_131_20_173 \
  --scenes-count=12 \
  --duration-per-scene=3 \
  --artifact-verify=1 \
  --poll-timeout-s=900
```

## Modes

| `--mode=` | HTTP? | What runs |
|---|---|---|
| `submit` (default) | yes | M2M provision → snap worker metrics → POST → poll → snap metrics → verify T1-T4 → write evidence |
| `dry`              | no  | build payload via `build_real_payload.py --strict`, jq-summary on stdout |
| `selftest`         | no  | `dry` + extra regex-walk forbidden-pattern check (matches `tests/smoke/full_payload/run.sh §assert_no_forbidden`) |

## Exit codes

| Code | Meaning |
|---:|---|
| `0`  | All tiers PASS (T1+T2+T3+T4) |
| `2`  | usage / env (missing admin token, missing `--worker-id` in submit, etc.) |
| `3`  | network (curl could not reach master during M2M/POST/GET) |
| `4`  | non-201 on M2M / non-202 on POST / non-200 on GET `/api/v1/workers/{id}` |
| `5`  | POST 202 but `.job_id` missing |
| `6`  | terminal-fail state `FAILED`/`CANCELLED` during poll |
| `7`  | poll timeout without reaching terminal state |
| `8`  | non-200 on `GET /api/v1/jobs/{id}` during poll |
| `9`  | selftest forbidden-pattern hit (script regression) |
| `10` | worker-mismatch (T4 FAIL) |
| `11` | duration coherence FAIL (T2) |
| `12` | drive delivery FAIL (T3) |

Most-severe-tier ordering on the final exit: `T2 > T4 > T3 > T1`.

## Args (CLI overrides env)

| Arg | Default | Notes |
|---|---|---|
| `--scenes-count=N` | `10` | Warn (not error) if `<10` |
| `--duration-per-scene=N` | `2` | per-scene `duration_seconds` |
| `--worker-id=<id>` | `$MULTISCENE_TARGET_WORKER_ID` | required in submit mode |
| `--artifact-verify=1` | `1` | set `0` to skip the T2 download+ffprobe step |
| `--poll-timeout-s=N` | `600` | poll cap |

## Environment

| Env var | Default | Notes |
|---|---|---|
| `VELOX_MASTER_URL` | `http://127.0.0.1:8000` | master base URL |
| `VELOX_ADMIN_TOKEN` | unset | admin bearer for `/api/v1/admin/m2m/keys` |
| `TOKEN_FILE` | unset | dotenv alternative |
| `MULTISCENE_TARGET_WORKER_ID` | unset | pinned worker_id |
| `MULTISCENE_DESTINATION_ID` | **required** | destination_id |
| `MULTISCENE_POLL_TIMEOUT_S` | `600` | poll cap seconds |
| `VELOX_MASTER_LOG_PATH` | unset | lease-scrape source (fallback: `journalctl -u velox-server`) |
| `FULLPAYLOAD_TARGET_EXECUTOR_ID` | `scene.composite.v1@1` | executor to pin |

## Evidence

Each run writes a JSON envelope to
`tests/smoke/full_payload/multi_scene_concat/evidence/run-<EPOCH>-<client_id>.json`:

```json
{
  "schema": "tests/smoke/full_payload/multi_scene_concat@1",
  "worker_id": "host_57_131_20_173",
  "job_id": "J-2026-...",
  "task_id": "T-...",
  "attempt_id": "A-...",
  "lease_id": "L-...",
  "status": "SUCCEEDED",
  "scenes_count": 10,
  "duration_per_scene": 2,
  "expected_duration_ms": 20000,
  "measured_duration_ms": 20134,
  "duration_delta_ms": 134,
  "duration_coherence_verdict": "PASS",
  "drive_delivery_verdict": "PASS",
  "worker_pin_verdict": "PASS",
  "render_time_ms": 8247,
  "artifact_size_bytes": 1915469,
  "metrics": {
    "before": { "disk_free_bytes": ..., "cpu_utilization_ratio": ..., "memory_used_bytes": ..., "ram_bytes": ... },
    "after":  { "disk_free_bytes": ..., "cpu_utilization_ratio": ..., "memory_used_bytes": ... },
    "delta":  { "disk_used_bytes": ..., "memory_delta_bytes": ... }
  }
}
```

## How payload generation works

The harness delegates payload construction to
`tests/worker-cert/build_real_payload.py --scenes-count=N --duration-per-scene=N`,
which was extended in this commit. The builder modulo-cycles through
`tests/worker-cert/fixtures/assets.json`:

- `scenes[i].clip_link = velox-asset://<clips[i % len(clips)]>.asset_id`
- `voiceover_paths = [velox-asset://<voiceover[i % len(voiceover)]>.asset_id for i in scenes]`

The canonical `assert_no_forbidden` (Python regex walk over JSON leaves)
guarantees no path-form `velox-asset://<kind>/<file>.<ext>` shape is emitted.

## Files

| Path | Role |
|---|---|
| `tests/smoke/full_payload/multi_scene_concat/run.sh` | this harness |
| `tests/smoke/full_payload/multi_scene_concat/README.md` | this README |
| `tests/smoke/full_payload/multi_scene_concat/evidence/` | run envelopes (created on first submit; gitignored) |
| `tests/worker-cert/build_real_payload.py` | extended with `--scenes-count`, `--duration-per-scene` |
| `tests/worker-cert/fixtures/assets.json` | canonical asset_id set (modulo-cycled) |
| `tests/worker-cert/lib/pluck.sh` | M2M + lease-scrape helpers (sourced) |
| `tests/_lib/sh/_lib.sh` | cross-test helpers (sourced) |
| `docs/smoke-results/2026-07-28-multi-scene-concat.md` | operator-facing run report |

## Cross-references

- Sibling smoke: `tests/smoke/full_payload/run.sh` (4-scene full-payload)
- Per-worker cert: `tests/worker-cert/smoke_one.sh` (2-scene per-worker)
- SubmitJobRequest shape: `DataServer/internal/apiwire/apiwire.go`
- Worker metrics: `DataServer/internal/handlers/server/api/workers_dto.go`
- Lease log shape: `DataServer/internal/grpcserver/handler_stream.go:455`
