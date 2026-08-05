# Mike Tyson end-to-end gate — 2026-08-05

## Verdict

**BLOCKED — no jobs submitted.**

The gate intentionally stopped before the 10-job battery because required
manifest assets were not all available from the active master storage. No
results are inferred from the prior Tyson battery report, prior `/tmp/tyson`
logs, or historical successful jobs.

## Run context

- Date: `2026-08-05`
- Branch: `main`
- Master URL from active worker configuration: `http://localhost:8000`
- Worker: `velox-worker-local`
- Worker health: `http://127.0.0.1:8081`
- Explicit destination: `comedy_test`
- Active DB inspected read-only: `/opt/velox/current/.velox/data/velox.db`
- Tyson manifest: `/home/pierone/Projects/company/VeloxEditiingg/tests/worker-cert/fixtures/assets.json`
- Planned battery size: 10 jobs
- Actual submissions: **0**

## Prerequisite matrix

| Prerequisite | Verdict | Fresh evidence |
|---|---|---|
| Master process/health | PASS | `velox-server` active; `GET http://127.0.0.1:8000/health` returned HTTP 200 `{"status":"healthy"}` |
| Master readiness | PASS | `GET http://127.0.0.1:8000/health/ready` returned HTTP 200 with `{"status":"ready"}` |
| Worker process/registration | PASS | Worker health returned HTTP 200 with `worker_id=velox-worker-local`, `registered=true` |
| Worker readiness/cache barrier | PASS | Worker readiness returned HTTP 200 with `registered=true`, `cache_ready=true`, `cache_protection_ready=true`, `blob_ready=true`, `executors_count=5` |
| Worker control channel | PASS | TCP `127.0.0.1:9000` was open |
| Admin authorization | PASS | `/tmp/velox-token.env` exists with mode `600`; a read-only `GET /api/v1/workers` using the token returned HTTP 200. Token contents were not recorded. |
| Prometheus availability | PARTIAL | Master `GET /metrics` returned HTTP 200 and cache/job metrics were present. Worker `GET http://127.0.0.1:8081/metrics` returned HTTP 404; this is recorded as an observability gap. |
| Explicit destination | PASS | Read-only DB query found `comedy_test`, provider `drive`, external destination ID present, `enabled=1` |
| Tyson manifest syntax/schema | PASS | Manifest JSON parsed; required `voiceover`, `clips`, `subtitles`, and `images` lists are present. |
| Payload contract | PASS | `build_real_payload.py` compiled and strict dry-run succeeded with integer suffix, worker `velox-worker-local`, and destination `comedy_test`. Output was written to `/tmp/tyson-preflight-payload.json`. |
| Required asset availability | **BLOCKED** | See asset table below; two selected asset blobs are missing from their master `storage_key` paths. |

## Required asset evidence

The strict payload builder selects the voiceover list and cycles through the
clip list. The active DB reported all selected records as `READY`, but READY
metadata alone is insufficient: the master blob must also exist at its
`storage_key` before submission.

| Asset ID | Kind | DB status/provider | DB size | Master storage blob |
|---|---|---|---:|---|
| `4d027993c4c3c68540eaab5dcecacaca1a9600a5fdf090b8ac1a924e753b1d15` | voiceover | READY / local | 484940 | **MISSING** at `/opt/velox/current/.velox/data/storage/assets/4d027993c4c3c68540eaab5dcecacaca1a9600a5fdf090b8ac1a924e753b1d15_1785843328094.mp3` |
| `27063c51bda8444a7b4d98ab4d9828826c9424f12cf1bbdeefe351afc3a4bd5c` | stock_clip | READY / local | 415206 | Present |
| `a01f8acfc7a8c919df3179e14c935c6101fe03fd965dfae80c62b57129f57223` | stock_clip | READY / local | 325632 | **MISSING** at `/opt/velox/current/.velox/data/storage/assets/a01f8acfc7a8c919df3179e14c935c6101fe03fd965dfae80c62b57129f57223_1785239412861.mp4` |
| `d4382e86b7475fed0b9e5b074cff68055a3ef1a633a9d2913ba4780b21fef561` | stock_clip | READY / local | 321883 | Present |

Additional evidence:

- All four selected DB records reported `storage_provider=local`; the missing
  paths are therefore missing from the configured master-local source, not an
  unverified remote provider.
- The worker cache contained successful historical hits for some selected
  IDs, but that does not prove the missing master blobs can serve a new job.
- `a01f8acfc7a8c919df3179e14c935c6101fe03fd965dfae80c62b57129f57223` had no
  matching recent worker cache/download log entry.
- A read-only query at `2026-08-05T12:01:03Z` showed the newest DB jobs were
  created at `09:02:29Z` or earlier. No job rows were created during this
  preflight, corroborating the driver was never executed.
- The manifest includes placeholder subtitle/image IDs; the builder's current
  payload path uses voiceover and clips, but the placeholders remain a
  manifest-quality issue to resolve before broader scene variants.

## Why the battery was not run

Submitting 10 jobs with known missing master blobs would not be a real gate:
it would create avoidable failures and could contaminate delivery/cache
observations. The gate therefore stops at the prerequisite barrier and records
`BLOCKED` rather than fabricating job outcomes.

## Required remediation before rerun

1. Restore or regenerate the missing master storage blobs for the voiceover and
   `a01f...` clip, preserving the DB `storage_key` and verifying SHA/size.
2. Re-run the read-only asset correlation against every asset selected by the
   strict builder; require `READY`, existing blob, expected size, and matching
   SHA before submitting.
3. Resolve the worker metrics endpoint (`/metrics` returned 404) if worker
   Prometheus evidence is required by the final acceptance gate.
4. Re-run this prerequisite gate. Only after every required check is PASS may
   the 10 unique Tyson jobs be submitted and verified end-to-end.
