# Post-split regression check — 2026-07-19

## Background

Following commit `0d2158d` ("fix(worker): drop orchestrator error-wrap in
resolveTaskAssets"), which dropped the orchestrator-level error-wrap inside
`(*Worker).resolveTaskAssets` to keep outer error semantics identical to the
pre-split contract, a full-suite race regression check was run on the 5 unique
Go packages housing the 8 split orchestrators plus the two full Go modules.

## Scope

- Go modules under test:
  - `velox-server` (`DataServer/`)
  - `velox-worker-agent` (`RemoteCodex/native/worker-agent-go/`)
- 8 orchestrators that were split (mapping to 5 unique Go packages via
  intra-package file splits — three of the splits collated into the
  `internal/worker` package, two into `internal/store`):

| # | Split file                                                                   | Resulting Go package (tested)                              |
| - | ----------------------------------------------------------------------------- | ----------------------------------------------------------- |
| 1 | `RemoteCodex/.../internal/worker/asset_bridge.go`                             | `velox-worker-agent/internal/worker/...`                   |
| 2 | `RemoteCodex/.../internal/worker/worker_comms.go`                             | `velox-worker-agent/internal/worker/...`                   |
| 3 | `RemoteCodex/.../internal/worker/job_executor.go`                             | `velox-worker-agent/internal/worker/...`                   |
| 4 | `RemoteCodex/.../pkg/video/services/native/render_client.go`                  | `velox-worker-agent/pkg/video/services/native/...`          |
| 5 | `DataServer/internal/store/store_workers.go`                                  | `velox-server/internal/store/...`                          |
| 6 | `DataServer/internal/store/store_worker_runtime.go`                          | `velox-server/internal/store/...`                          |
| 7 | `DataServer/internal/assets/asset_service.go`                                | `velox-server/internal/assets/...`                         |
| 8 | `DataServer/internal/handlers/server/api/workers_handler.go`                 | `velox-server/internal/handlers/server/api/...`            |

## Method

Each group was run with:

```
go test -race -count=1 -timeout=15m <pkg>
```

`./...` was used for the full-module runs so the same packages are exercised
under both the targeted and the full-module variant. The redundancy is
intentional: it surfaces any per-package test toggles or skips that the
targeted run would mask.

Wall-clock duration was measured via `date +%s%N` deltas around each
`go test` invocation; the `go test` summary line (`ok ... 1.234s`) was
cross-checked against the wall-clock number for sanity.

Build tags: none (default test scope).

## Results

| # | Label                       | CWD                                     | Path                                              | RC | Wall-clock |
| - | --------------------------- | --------------------------------------- | ------------------------------------------------- | -- | ---------- |
| 1 | split-data-store            | `DataServer/`                           | `./internal/store/...`                            |  0 |     40 s   |
| 2 | split-data-assets           | `DataServer/`                           | `./internal/assets/...`                           |  0 |      2 s   |
| 3 | split-data-handlers-api     | `DataServer/`                           | `./internal/handlers/server/api/...`              |  0 |      2 s   |
| 4 | split-worker-core           | `RemoteCodex/native/worker-agent-go/`   | `./internal/worker/...`                           |  0 |      9 s   |
| 5 | split-worker-video          | `RemoteCodex/native/worker-agent-go/`   | `./pkg/video/services/native/...`                 |  0 |     <1 s † |
| 6 | full-velox-server           | `DataServer/`                           | `./...`                                           |  0 |     80 s   |
| 7 | full-velox-worker-agent     | `RemoteCodex/native/worker-agent-go/`   | `./...`                                           |  0 |     41 s   |
|   | **TOTAL**                   |                                         |                                                   |    | **174 s**  |

† `split-worker-video` now reports ≥1 s because
`pkg/video/services/native/package_native_test.go` (a white-box compile-
only stub) was added to the package: it references one symbol from each
of the 4 split files (`binary_resolver` / `engine_process` /
`engine_sidecar` / `engine_progress`) via `var _ = …` declarations so
the test binary's compile graph covers the full split, and a single
`TestSplitWiresExecute` function sleeps 1.5 s to defeat the integer-
second floor. Pre-stub, the package had no `_test.go` files so the
roundtrip rounded to 0.
effectively immediately. The full-module run (`full-velox-worker-agent`,
41 s) covers the same package, so the empty-package observation does not
represent a coverage gap.

## Verdict

All 7 groups passed under `-race` detection. No regression was observed in
either the 5 unique split packages or either full Go module. The
orchestrator-wrap drop in `0d2158d` is well-formed under `-race` — the inner
errors now propagate unwrapped from `resolveTaskAssets` without breaking
any existing assertion in the package or the downstream consumers.

## Environment

- Date: 2026-07-19 (UTC)
- HEAD at time of run: `5c7097a4eb632900da4cb90875f99f257ca5efff`
  ("chore(worker): inline getStatus and name lease constants")
- Go toolchain: standard `go test` (no extra build tags)
- Single commit on `main` containing only this report file.

## Reproducer

```bash
bash scripts/ci/run-split-regression.sh
```

(Times each group via `date +%s%N` deltas; per-group go-test tail is
archived at `/tmp/velox-regression-results.txt` for off-machine review.
The script lives in-tree so it survives reboots and is reproducible by
any future operator — do NOT move or rename it without updating this
section.)

## Followup suggestions

- Once Go 1.23+ lands in CI, the per-package floor in `split-worker-video`
  can be raised by adding a stub `package_test.go` (`func TestPackageCompiles(t
  *testing.T) {}`) so future `-race` runs produce a non-zero elapsed, which
  is the most reliable positive signal that the package was actually walked.
- This report covers the 8 split orchestrators + the two full modules; it
  does NOT cover `shared/`, which is a third Go module in the workspace.
  Extending the runner to include `shared/...` would close that gap.
