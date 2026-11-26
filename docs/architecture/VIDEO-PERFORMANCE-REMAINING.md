# Video performance: remaining work

Production evidence: [2026-11-26 live report](VIDEO-PERFORMANCE-LIVE-2026-11-26.md).

Open follow-up:
[#40 — production copy-only assembly measurements on media prepared by remote 77](https://github.com/Marcuss-ops/VeloxEditiingg/issues/40).

Executor retirement [#41](https://github.com/Marcuss-ops/VeloxEditiingg/issues/41)
was completed on 2026-11-26 and verified in production.

Audit base: `main` at `87bde607` (2026-11-26).

## Already implemented

- New FINALIZE jobs certify one immutable source identity, reuse the proof in
  the same job, and dispatch direct packet-copy assembly for compatible windows.
- New ordinary jobs do not create window MP4 fragments when source certification
  is available. Unauthenticated sparse source preparation has been removed.
- Sparse inputs use source-certificate block SHA-256 evidence; authenticated
  blocks are staged directly in the private sparse file.
- Runtime source SHA-256 and size are reconciled before deciding whether to
  plan per-window work.
- Exact decoded frame count, packet keyframe timestamps, stream identity,
  rational timestamp origin/timebase and frame-rate consistency are collected
  in a complete in-process LibAV probe. The worker rejects incomplete evidence,
  and the probe cache schema invalidates older entries. Sparse bytes cannot
  produce a full-source certificate.
- A race in the sparse-fetch test counter was fixed in `8b6617af`; the full
  worker race suite passes locally. Commit `e8a0d0f9` adds complete native
  source inspection and was pushed to `main`.

## Status and remaining work

### 1. Native source certification — deployed and verified

The worker now calls `velox_video_engine --probe-source`, backed by a complete
LibAV packet walk and decoded-frame count. It validates codec/profile/extradata,
exact frame count, rational timebase and timestamp origin, matching average and
declared frame rates, and packet keyframe timestamps. New source certificates
retain the verified origin and average-rate facts. The packet mux continues to
inspect the H.264 access unit and requires IDR at each requested boundary.
The worker image build now requires LibAV, so this capability cannot silently
ship in the FFprobe-only compatibility configuration. Production deployment
and successful cold/warm execution are recorded in the live report.

### 2. Persisted legacy work

The production inventory is complete and recorded in the live report. Its
498 OPEN dead letters were reconciled against the Master DB: 47 refer to failed
attempts followed by a successful task and are `RESOLVED`; 451 refer to failed
or cancelled terminal tasks and are `CANCELLED` in the queue. All 498 had
`retryable=0`; no live legacy task or live parent job was found. The operation
preserves task, attempt, payload, spec, and audit history. It does not delete
rows or touch non-legacy dead letters.

The 5,108 task-spec references and 783,337 task-ID references are historical
records retained for audit and forensic recovery; their presence is not active
retry work. The producer now emits `video.assemble.copy.v1` for compiled V2
plans. The worker no longer registers the prepare or slice executors; their
historical payloads remain readable. Two abandoned pre-cutover slice jobs were
terminalized through the transactional job repository, preserving task,
attempt, spec, event, and history rows. No active retired tasks remain.
Before any exported-symbol or cross-package removal commit, run:

```bash
bash scripts/ci/pre-removal-verify.sh
```

The read-only report is available as
`scripts/ops/audit-legacy-video-tasks.py --db "$VELOX_DB_PATH" --json`.
Legacy executor retirement is complete in
[#41](https://github.com/Marcuss-ops/VeloxEditiingg/issues/41); task and attempt
history was retained.

### 3. Transfer and cache measurements

The already-prepared media and finalized plans come from remote 77. Our open
measurement is the copy-only assembly path: consolidate a comparable cold/warm
pair plus cache/transfer and local disk-I/O measurements in
[#40](https://github.com/Marcuss-ops/VeloxEditiingg/issues/40). Upstream audio,
subtitle, and media preparation are owned by remote 77 and are not worker
implementation work here.
Keep cryptographic verification; optimize the read/hash path only when
measurements show a material gain.

### 4. Image release and production proof

- [x] Worker commit `87bde607` passed race-image build, signing and
  certification in [release run 45876322519](https://github.com/Marcuss-ops/VeloxEditiingg/actions/runs/45876322519).
- [x] FleetController deployed digest
  `ghcr.io/marcuss-ops/velox-worker@sha256:3f6f4ed7cb7bd4d319101a6b453f85fad5d3600e80696da6b773953a3804c854`;
  readiness and Level-D smoke passed.
- [x] Master runs v1.4.55 at
  `ghcr.io/marcuss-ops/velox-server@sha256:ef1c6833c58570955781cf2f584e3c6da3b1e2da3312326dc980fb721f113445`.
- [x] Production cold/warm source-certification and direct-copy evidence is
  recorded in the live report. It remains distinct from the copy-only
  measurements outstanding in #40.

The production legacy queue drain and executor retirement are complete. Original
task, attempt, spec and reference rows remain retained; retired payloads remain
readable for audit, but their executors are not registered or dispatched.

Do not claim production performance improvements based on source inspection,
local tests or an image build alone.

## Verification already run

- DataServer: `bash scripts/ci/pre-removal-verify.sh` passed all three full
  module checks (`go vet`, `go build`, and `go test -count=1 ./...`).
- Worker: `go test ./...` passed.
- Worker race gate: `go test -race -timeout 5m ./internal/... ./pkg/...` passed
  after the test counter fix.
- C++/LibAV: `render_copy_only_zero_intermediates_tests` passed with real media.

These checks do not replace release certification or production measurements.
