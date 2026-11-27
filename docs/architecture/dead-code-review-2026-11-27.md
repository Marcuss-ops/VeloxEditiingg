# Dead-code candidate review — 2026-11-27

## Scope and method

Reviewed the preserved candidate output at
[`evidence/deadcode-candidates-2026-11-27.txt`](evidence/deadcode-candidates-2026-11-27.txt)
against current `main`. The output is a static call-graph snapshot and predates
several commits already in the current tree. For each candidate, checked
current Go references, route/bootstrap registration, interface satisfaction,
test-only use, and whether the symbol sits under a Go `internal` boundary.
Analyzer output alone was not treated as removal evidence.

## Confirmed removals

| Candidate / surface | Evidence | Result |
| --- | --- | --- |
| `Handlers.CreatePipelineRun` and its creation service | `/api/v1/pipeline-runs` is explicitly retired in `pipeline/routes.go`; no route registration or caller remained | Handler, service and wiring removed in `ec70fc6c` |
| `NewCurrentTaskLoader`, `TaskSummary`, `LoadCurrentTaskRow` | No caller remained; worker API exposes registry-backed `current_task_id`, not this SQL projection | Adapter and query removed in `40fee3dc` |
| `NewSSHClient` | No callers; production construction is through `NewSSHClientFromRegistry` | Removed in `8ed374b3` |
| `CanonicalWorkerRuntimePreflightCommand` | No callers, including tests; actual gate is `CanonicalWorkerRuntimePreflight.Check` | Removed in `8ed374b3` |
| `FreshSmokeRunner` | No constructor or method callers; current smoke health path reads the durable latest run and update/resume flows use their current runners | Removed in `8ed374b3` |
| `NewExternalCosignVerifierFromConfig` | No callers; bootstrap uses `NewExternalCosignVerifier` and applies runtime config at composition | Removed in `be1695f1` |

Each removal commit passed the mandatory full DataServer gate before push:
`go vet ./...`, `go build ./...`, and `go test -count=1 ./...`.

## Candidate classifications retained

- The old `jobs/ingress` registry, HTTP response compatibility helper, and
  `DataLayerAuditor.IsLegacyAllowed` were already removed on `main` by
  `90cf4c9e`, `be8b3f04`, and `4735bbf3` respectively.
- `BlobStore.RemoveStaging` has production callers in artifact and remote
  asset cleanup. It is live despite the analyzer's report.
- `ConfigFromRuntime` is called by pipeline bootstrap and social-client setup.
- `Error()` candidates implement Go's `error` contract. `Backoff()` is called
  by the `Policy.Delay` implementation; lifecycle and runner methods are
  reached through their interfaces and bootstrap wiring.
- Candidate methods on test stubs, plus clock and drive fakes used by tests,
  are test support rather than production dead code.
- `SQLiteStore.observeDBTransaction` was confirmed live during the original
  review and remains present.

The remaining hits in the snapshot do not establish dead production paths.
This review does not authorize speculative deletion based only on naming or
file-level call counts. Re-run the analysis from the current tree before
opening a new cleanup item.
