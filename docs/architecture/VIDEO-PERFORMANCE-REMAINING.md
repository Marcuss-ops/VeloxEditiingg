# Video performance: remaining work

Production evidence: [2026-11-26 live report](VIDEO-PERFORMANCE-LIVE-2026-11-26.md).

Open follow-ups: [#39 — drain legacy TaskGraph work](https://github.com/Marcuss-ops/VeloxEditiingg/issues/39)
and [#40 — full production `gervais-final-v1` PERFLIVE matrix](https://github.com/Marcuss-ops/VeloxEditiingg/issues/40).
Do not remove legacy executors until open dead letters and persisted references
have a safe migration/drain plan.

Audit base: `main` at `e8a0d0f9` (2026-11-26).

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
  and the probe cache schema invalidates older entries. FFprobe remains only
  for prepared-fragment output certification and persisted sparse-window
  compatibility; sparse bytes cannot produce a full-source certificate.
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

The production inventory is complete and recorded in the live report. It found
498 OPEN dead-letter entries, 5,108 task-spec references, and 783,337 task-ID
references. Migrate or drain persisted work before retiring executors,
contracts, cache paths or wrappers. Before any exported-symbol or cross-package
removal commit, run:

```bash
bash scripts/ci/pre-removal-verify.sh
```

The read-only report is available as
`scripts/ops/audit-legacy-video-tasks.py --db "$VELOX_DB_PATH" --json`.
Legacy retirement remains open; no destructive migration was attempted.

### 3. Transfer and cache measurements

Focused production cold/warm cache behavior, transfer bytes, source-certificate
time and task counts are recorded in the live report. The exact
`gervais-final-v1` stock/voiceover/music/ASS comparison, authenticated-range
versus full-download matrix, and local disk-I/O measurements remain open.
Keep cryptographic verification; optimize the read/hash path only when
measurements show a material gain.

### 4. Image release and production proof

- [x] Local worker image build succeeds from commit `e8a0d0f9`; native LibAV
  is enabled and the complete C++ test suite passes in the build.
- [x] GitHub built, signed, verified and certified worker digest
  `ghcr.io/marcuss-ops/velox-worker@sha256:2a05e76c13d47a1aa9cc3e79217a93a456419de6656bacaba405f0be27aa87c0`
  in [run 45824073441](https://github.com/Marcuss-ops/VeloxEditiingg/actions/runs/45824073441).
- [x] FleetController deployed the signed worker digest to
  `host_57_129_132_133`; update operation
  `fed9b40b-1344-4281-842e-9195ce7721b6` SUCCEEDED.
- [x] Master image digest
  `ghcr.io/marcuss-ops/velox-server@sha256:869c1ff9cf7e36ed77770c0dd6ac6c1cce99ec272ddb474d747cc30e71a07840`
  was signed/verified, deployed locally, and reports commit `a5dc8323` ready.
- [x] Correct the Master smoke pickup URL and pass the canonical resume gate;
  worker is CONNECTED/HEALTHY.
- [x] Run a production cold/warm synthetic source-certification and direct-copy
  render pair; record job IDs, task counts, cache transfer, duration, output
  hashes, and deployed digests in the live report. This is not the full
  `gervais-final-v1` PERFLIVE matrix; OS process-launch counts are unavailable.

The production legacy task inventory is complete. Executor retirement remains
open because 498 dead letters are OPEN and historical references remain. No
legacy executor removal is safe until those entries and references are
migrated or drained.

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
