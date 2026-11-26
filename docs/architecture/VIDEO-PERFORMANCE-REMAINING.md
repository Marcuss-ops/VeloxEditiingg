# Video performance: remaining work

Tracking issue: [#38 — Complete source certification and retire remaining render work](https://github.com/Marcuss-ops/VeloxEditiingg/issues/38)

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

## Remaining

### 1. Native source certification — implemented locally

The worker now calls `velox_video_engine --probe-source`, backed by a complete
LibAV packet walk and decoded-frame count. It validates codec/profile/extradata,
exact frame count, rational timebase and timestamp origin, matching average and
declared frame rates, and packet keyframe timestamps. New source certificates
retain the verified origin and average-rate facts. The packet mux continues to
inspect the H.264 access unit and requires IDR at each requested boundary.
The worker image build now requires LibAV, so this capability cannot silently
ship in the FFprobe-only compatibility configuration.

### 2. Persisted legacy work

Inventory persisted TaskGraph tasks, attempts, retries and recovery references
for `video.prepare.window.v1` and `video.assemble.slice.v1`. Migrate or drain
persisted work before retiring their executors, contracts, cache paths or
wrappers. Before any exported-symbol or cross-package removal commit, run:

```bash
bash scripts/ci/pre-removal-verify.sh
```

The read-only report is available as
`scripts/ops/audit-legacy-video-tasks.py --db "$VELOX_DB_PATH" --json` and
must run against the production Master database before any legacy removal.

### 3. Transfer and cache measurements

Measure cold and warm cache behavior, full-download versus authenticated-range
bytes, local I/O, duplicate downloads/certifications and task counts. Keep
cryptographic verification. Optimize the read/hash path only when measurements
show a material gain.

### 4. Image release and production proof

- [x] Local worker image build succeeds from commit `e8a0d0f9`; native LibAV
  is enabled and the complete C++ test suite passes in the build.
- [x] GitHub worker-image workflow builds, signs, verifies and certifies the
  immutable digest
  `ghcr.io/marcuss-ops/velox-worker@sha256:bff23b4b910851bbf9cdb40adeb972d2111adeaed931b70a6434c462cb1d30e4`.
  Worker race tests and image smoke passed in
  [run 45814976896](https://github.com/Marcuss-ops/VeloxEditiingg/actions/runs/45814976896).
- [x] FleetController drained `host_57_129_132_133`, deployed that exact
  digest, and verified the authenticated running digest. Update operation:
  `deploy-host_57_129_132_133-1795694584711984868` (SUCCEEDED).
- [ ] Resume the worker. The canonical resume operation refused placement
  because its fresh Level-D smoke could not download the configured asset:
  `curl` on the worker could not connect to `localhost:8000`. The worker
  remains DRAINING; repair the Master smoke asset pickup URL/configuration,
  then rerun the resume smoke gate. Do not bypass the gate.
- [ ] Run authorized cold/warm production renders and record process counts,
  intermediate bytes/uploads, transferred bytes and wall time against the
  agreed PERFLIVE baselines.

The legacy task inventory is also still open. The production API token can
read fleet operations, but the canonical Master SSH identity rejected the
session and no SQLite snapshot was available. Run the read-only audit against
the Master DB once that access is restored; do not remove legacy executors
before inventorying persisted tasks, attempts, and references.

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
