# PR 5 — Real Workload E2E Test

End-to-end test exercising the **full Velox pipeline** with no mocking of
the critical path:

```
Hello → HelloAck → TaskOffer → TaskAccepted → TaskLeaseGranted
→ executor reale → TaskResult → artifact upload
→ artifact verification → Job SUCCEEDED
```

## Minimal deterministic fixture

| Property | Value |
|----------|-------|
| Scene | Pure teal (#008080) PNG, 320×180 |
| Audio | 2s silent AAC (or MP3 fallback) |
| Encoder | H.264 via FFmpeg |
| Worker | CPU-only, plaintext gRPC (dev mode) |

## Quick start

```bash
# From the repo root
make e2e-workload

# Custom workdir
E2E_WORKDIR=/tmp/my-test make e2e-workload
```

## Verification (5 checks)

| # | Check | Method |
|---|-------|--------|
| 1 | Artifact exists | `find $STORAGE_DIR -name '*.mp4'`, size > 1 KB |
| 2 | ffprobe inspection | Codec, resolution, duration via `ffprobe -print_format json` |
| 3 | SHA-256 recorded | `sha256sum` stored alongside artifact (not enforced — varies by FFmpeg version) |
| 4 | Worker in API | `GET /api/v1/workers` contains worker_id |
| 5 | Metrics non-zero | `/metrics` shows `velox_job_succeeded_total > 0` and `velox_compute_seconds_total` |

## No mocking

The following components are **real** (no mock, no stub):

- Master (`velox-server`)
- gRPC bidi stream
- Worker agent (`velox-worker-agent`)
- Executor registry (`scene.composite.v1`)
- FFmpeg video engine
- Artifact upload
- SQLite persistence
- Artifact finalization

Only **external providers** (Google Drive, YouTube) are absent — the
fixture uses local `file://` assets.

## Artifacts

All test output is preserved in `$E2E_WORKDIR`:

```
$E2E_WORKDIR/
├── bin/                  velox-server + velox-worker-agent
├── data/                 SQLite DB (velox.db)
├── staging/              scene.png + silent.aac
├── storage/              output .mp4 + artifact.sha256
├── logs/                 master.log + worker.log
└── master.env / worker.json
```

Set `E2E_WORKDIR` to change the root directory (default: `/tmp/velox-e2e-workload`).
