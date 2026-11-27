# Production video performance evidence — 2026-11-26

Master: `51.91.11.36` (local production host). All times UTC.

## Final producer cutover and executor retirement — 2026-11-26

- Current Master image: `ghcr.io/marcuss-ops/velox-server@sha256:ef1c6833c58570955781cf2f584e3c6da3b1e2da3312326dc980fb721f113445` (v1.4.55).
- Producer cutover was verified from 13:13 UTC. New tasks since then use only
  `video.source.certify.v1` and `video.assemble.copy.v1`; no prepare or slice
  task IDs were emitted.
- Worker release commit `87bde607` runs as signed digest
  `ghcr.io/marcuss-ops/velox-worker@sha256:3f6f4ed7cb7bd4d319101a6b453f85fad5d3600e80696da6b773953a3804c854`.
  The worker is CONNECTED, readiness `ok`, and advertises only copy assembly
  and source certification. Level-D smoke operation
  `4ab50fe5-e04a-49bc-9dfe-129ed4557f8c` succeeded.
- Two abandoned pre-cutover jobs (`job_77e43e9d8c6c4e0f`,
  `job_d5d823fa08456bb6`) each had one READY slice task. Both were moved to
  CANCELLED through the transactional job repository; task/spec/attempt rows
  and cancellation history remain. No active legacy tasks remain.
- Removal gate, worker test/vet/build and capability-contract checks passed.
  GitHub Worker image release, `main-baseline`, `video-pipeline`, and full CI
  passed. Previous worker digest retained for rollback:
  `sha256:e602d620671b156951ff17e56bb9cbcddaaf7a812b426e9c0f1ce3e5dfc84a56`.

Issue [#41](https://github.com/Marcuss-ops/VeloxEditiingg/issues/41) is closed.
The remaining work is copy-only cold/warm and transfer/cache measurement in
[#40](https://github.com/Marcuss-ops/VeloxEditiingg/issues/40); media and
upstream audio/subtitle preparation are supplied by remote 77.

## Initial source-certification rollout (historical baseline)

- Master runs commit `a5dc832368f1c1f34b0e1ac45c6668ee14a2644a` (v1.4.54), image digest `ghcr.io/marcuss-ops/velox-server@sha256:869c1ff9cf7e36ed77770c0dd6ac6c1cce99ec272ddb474d747cc30e71a07840`.
- Worker `host_57_129_132_133` runs signed image digest `ghcr.io/marcuss-ops/velox-worker@sha256:2a05e76c13d47a1aa9cc3e79217a93a456419de6656bacaba405f0be27aa87c0`, is CONNECTED/HEALTHY, and registered `video.source.certify.v1`.
- Pickup uses the configured Master public URL; the smoke gate used the configured smoke asset.
- The canonical resume operation succeeded: `b384d6c6-2de4-4188-9b82-e87337c5ae4e`. The smoke gate was not bypassed.

## Focused cold/warm production renders

These are successful six-second synthetic H.264 packet-copy renders using a canonical 1920x1080, 24 fps source and the deployed production worker. They are useful PERFLIVE evidence for source certification, cache transfer, and direct assembly. They are **not** the full `gervais-final-v1` matrix (two stock clips, voiceover, background music, ASS subtitle, three jobs per worker), so they are not inserted into `performance_benchmark_runs` under that case ID.

At the time of this initial measurement, the canonical harness could not use
its fixture because the READY rows lacked backing files for the stock clips,
voiceover, and music. The operator later clarified that remote 77 owns upstream
media, audio, and subtitle preparation. This report therefore keeps the
synthetic evidence distinct and tracks only copy-path measurements in #40; it
does not claim the full `gervais-final-v1` matrix.

| Mode | Job | Result | Job end-to-end | Render task wall | Source transfer | Tasks | Output |
|---|---|---|---:|---:|---:|---:|---|
| Cold | `job_f9f1babdeeb5ad96` | SUCCEEDED | 51.66 s | 0.66 s | 2,572,743 B, 1.60 s, cache miss | source certify + copy assemble (2) | 1,923,119 B, 6,000 ms |
| Warm | `job_3761c65044b513c3` | SUCCEEDED | 0.73 s | 0.73 s | 0 B, cache hit | copy assemble (1) | 1,923,119 B, 6,000 ms |

Both final artifacts have SHA-256 `18d8fce639d11aeaf3c6ce23c2630607c69161c11626048a3b8a591ee5aa4e7d`. The cold certificate task completed in 817 ms and recorded the exact source SHA, 2,572,743-byte size, 192 decoded frames, H.264 High profile, 24 fps, verified zero timestamp origin, and authenticated 1 MiB block hashes. The warm job reused the verified source cache. Both assembly task records show `ffmpeg_encode_ms=0`; the source-certificate task emitted no media artifact. The cold job's 51.66 s includes about 48 seconds waiting for the worker's readiness/lease, and must not be read as render time. The available production timing record does not expose OS-level process-launch counts, so process counts are not claimed here.

## Full-length production copy-only cold/warm comparison — 2026-11-27

The Master (`51.91.11.36`) created both jobs through the canonical V2
preflight/finalize path, using the saved production request and its 328 runtime
asset bindings. This is the 46-clip compilation material prepared by remote
77, with its finalized audio and stock pool; no audio or subtitle generation
was added to Velox. Both attempts ran on `host_57_129_132_133`, worker v1.4.55,
image digest
`ghcr.io/marcuss-ops/velox-worker@sha256:3f6f4ed7cb7bd4d319101a6b453f85fad5d3600e80696da6b773953a3804c854`.
The producer timeline SHA-256 is identical in both jobs:
`175ed7dbe8556c8c3083ac3839d3d44cadca0b099395a451e73e01d2a9a61a82`;
duration is 2,384.687 s (39:44.687). Each job emitted one assembly task and
194/194 packet-copy segments, with zero encoded frames.

| Measurement | Pool-cold attempt | Warm attempt |
|---|---:|---:|
| Job | `job_8a8f3abe3da63117` | `job_ffeeaac321d14712` |
| Result | SUCCEEDED | SUCCEEDED |
| Master job end-to-end | 47.20 s | 22.67 s |
| Worker task wall | 41.365 s | 17.376 s |
| Worker render critical path | 2.726 s | 2.568 s |
| Cache lookups / unique assets requested | 703 / 233 | 703 / 233 |
| Cache hits / misses | 563 / 140 | 703 / 0 |
| Download count / bytes | 140 / 158,347,051 B | 0 / 0 B |
| Hash verification phase | 626 ms | cache hits verified; no separate aggregate phase recorded |
| Logical bytes read from worker cache | 2,738,202,910 B | 2,896,549,961 B |
| Worker cgroup disk read / write | 4,096 / 212,881,408 B | 8,192 / 45,629,440 B |
| Worker cgroup network RX / TX | 164,388,236 / 54,341 B | 3,598,722 / 43,883 B |
| Packet-copy segments / all segments | 194 / 194 | 194 / 194 |
| Output media duration | 2,384.71 s | 2,384.71 s |
| Output size / SHA-256 | 445,406,637 B / `ccc2db94a15f761ddc786bec682dba1078f515783bb62ee89e5d1b241b27d783` | 446,125,686 B / `0ff5f90b1eba6dea7981cbf99c03ecb444b00be4e821e581343adda14b0e837f` |

“Pool-cold” is deliberately narrower than a fully cold worker cache. The
supported `velox-cache-evict` command removed the selected stock-pool entries,
but rejects SHA-namespaced asset keys; some fixed assets therefore remained
warm. The measured attempt had 140 misses and 563 hits, not a zero-hit cold
run. The warm attempt had 703 hits and no misses. The two realized plan hashes
and output hashes differ because stock-pool selection is deterministically
seeded by job ID; the common producer timeline, duration, pool, worker, and
assembly shape make this a comparable operational cold/warm sample, but not a
bit-identical input-media replay.

The attempt report attributes 158.3 MB of transfer to 140 misses and provides
separate worker cache, cgroup disk, network, render, and packet-mux metrics.
Neither the persisted Master telemetry nor the attempt report records HTTP
status or `Content-Range` per source transfer. Therefore these runs do not
establish authenticated-range versus full-download behavior; no range claim
is made. That comparison requires per-transfer range/status evidence for the
same immutable sources. The results do establish that the full-length copy
assembly completed successfully without re-encoding, and resolve the earlier
`manifest binding is not exact` rejection after the Master-side stock-pool
certification fix.

## Initial persisted legacy TaskGraph audit (before final terminalization)

Read-only report generated from `/opt/velox/current/.velox/data/velox.db` with `scripts/ops/audit-legacy-video-tasks.py` on 2026-11-26:

- 12,687 legacy tasks: 11,121 SUCCEEDED, 401 FAILED, 1,165 CANCELLED.
- 11,685 attempts: 11,123 SUCCEEDED, 498 FAILED, 47 TIMED_OUT, 17 CANCELLED.
- 5,108 task-spec references and 783,337 task-ID references remain.
- The initial 498 OPEN legacy dead-letter entries were reconciled on the Master: 47 entries whose tasks later succeeded are `RESOLVED`; 451 failed/cancelled terminal entries with `retryable=0` are `CANCELLED`. No legacy task or parent job was live. The operation changes only dead-letter queue status and preserves all task/attempt/spec/payload history.
- The 5,108 task-spec references and 783,337 task-ID references are retained historical audit records, not live retries. At the time of this initial queue audit, slice executor removal was still pending; it was completed in the final cutover section above.

The full audit JSON was kept outside the repository because it contains row-level production task and attempt identifiers. The aggregate above is the reviewable operational result.
