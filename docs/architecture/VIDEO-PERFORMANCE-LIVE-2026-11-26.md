# Production video performance evidence — 2026-11-26

Master: `51.91.11.36` (local production host). All times UTC.

## Deployment and smoke gate

- Master runs commit `a5dc832368f1c1f34b0e1ac45c6668ee14a2644a` (v1.4.54), image digest `ghcr.io/marcuss-ops/velox-server@sha256:869c1ff9cf7e36ed77770c0dd6ac6c1cce99ec272ddb474d747cc30e71a07840`.
- Worker `host_57_129_132_133` runs signed image digest `ghcr.io/marcuss-ops/velox-worker@sha256:2a05e76c13d47a1aa9cc3e79217a93a456419de6656bacaba405f0be27aa87c0`, is CONNECTED/HEALTHY, and registered `video.source.certify.v1`.
- Pickup uses the configured Master public URL; the smoke gate used the configured smoke asset.
- The canonical resume operation succeeded: `b384d6c6-2de4-4188-9b82-e87337c5ae4e`. The smoke gate was not bypassed.

## Focused cold/warm production renders

These are successful six-second synthetic H.264 packet-copy renders using a canonical 1920x1080, 24 fps source and the deployed production worker. They are useful PERFLIVE evidence for source certification, cache transfer, and direct assembly. They are **not** the full `gervais-final-v1` matrix (two stock clips, voiceover, background music, ASS subtitle, three jobs per worker), so they are not inserted into `performance_benchmark_runs` under that case ID.

The full matrix was not launched: the current worker-cert fixture's Master asset rows are marked READY, but the corresponding local storage files for its stock clips and voiceover are missing; the usable SHA-addressed music candidate's storage file is also absent. The ASS fixture exists. Restore/re-register verified source bytes before running the official harness; do not treat a stale READY row as usable media.

| Mode | Job | Result | Job end-to-end | Render task wall | Source transfer | Tasks | Output |
|---|---|---|---:|---:|---:|---:|---|
| Cold | `job_f9f1babdeeb5ad96` | SUCCEEDED | 51.66 s | 0.66 s | 2,572,743 B, 1.60 s, cache miss | source certify + copy assemble (2) | 1,923,119 B, 6,000 ms |
| Warm | `job_3761c65044b513c3` | SUCCEEDED | 0.73 s | 0.73 s | 0 B, cache hit | copy assemble (1) | 1,923,119 B, 6,000 ms |

Both final artifacts have SHA-256 `18d8fce639d11aeaf3c6ce23c2630607c69161c11626048a3b8a591ee5aa4e7d`. The cold certificate task completed in 817 ms and recorded the exact source SHA, 2,572,743-byte size, 192 decoded frames, H.264 High profile, 24 fps, verified zero timestamp origin, and authenticated 1 MiB block hashes. The warm job reused the verified source cache. Both assembly task records show `ffmpeg_encode_ms=0`; the source-certificate task emitted no media artifact. The cold job's 51.66 s includes about 48 seconds waiting for the worker's readiness/lease, and must not be read as render time. The available production timing record does not expose OS-level process-launch counts, so process counts are not claimed here.

## Persisted legacy TaskGraph audit

Read-only report generated from `/opt/velox/current/.velox/data/velox.db` with `scripts/ops/audit-legacy-video-tasks.py` on 2026-11-26:

- 12,687 legacy tasks: 11,121 SUCCEEDED, 401 FAILED, 1,165 CANCELLED.
- 11,685 attempts: 11,123 SUCCEEDED, 498 FAILED, 47 TIMED_OUT, 17 CANCELLED.
- 5,108 task-spec references and 783,337 task-ID references remain.
- The initial 498 OPEN legacy dead-letter entries were reconciled on the Master: 47 entries whose tasks later succeeded are `RESOLVED`; 451 failed/cancelled terminal entries with `retryable=0` are `CANCELLED`. No legacy task or parent job was live. The operation changes only dead-letter queue status and preserves all task/attempt/spec/payload history.
- The 5,108 task-spec references and 783,337 task-ID references are retained historical audit records, not live retries. `video.assemble.slice.v1` remains a current assembly adapter; executor/contract removal is outside this drain and requires a separate compatibility change.

The full audit JSON was kept outside the repository because it contains row-level production task and attempt identifiers. The aggregate above is the reviewable operational result.
