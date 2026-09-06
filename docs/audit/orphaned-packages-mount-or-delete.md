# Mount-or-delete decision brief — orphaned `internal` packages (2026-09-06)

- **Status**: ✅ EXECUTED — 2026-09-06, all 11 packages deleted in one atomic
  removal commit after `scripts/ci/pre-removal-verify.sh` passed on the
  deletion tree (full-module `go vet` 0 / `go build` 0 / `go test -count=1` 0,
  zero fallout: no orphan `_test.go` references, no unused imports). The
  table below is preserved as the decision record; restore anything from
  git history if a product need resurfaces.
- **Author**: Audit remediation run (6-area technical audit)
- **Verification**: every row re-verified on `main` (8b3ca0b1) via
  `go list -deps ./cmd/...` over all 9 `cmd/*` entrypoints — zero references —
  and by plain `grep` over all `*.go` in the module (aliased / dot / side-effect
  imports included). Workspace siblings (`shared/`,
  `RemoteCodex/native/worker-agent-go`) cannot import
  `velox-server/internal/...`, so cross-module reachability is impossible by
  construction.

## Why this matters

These packages are not dormant code — they are **maintained dead code**: test
cycles, review attention, and migration work are being spent on surfaces no
request can reach. `internal/handlers/server/calendar` alone absorbed 8
commits in two months while mounted nowhere, and its tests were repaired
through `09940dd6` (AGENTS.md §4) — i.e., the dead code is being kept
*passing*, which is the most expensive way to keep code dead. Keeping it
passing also preserves semantic-drift risk: a live package with the same name
elsewhere can drift from this copy invisibly.

## Per-package disposition

| Package | Non-test LOC | Churn (2 mo) | Recommended disposition | Notes |
|---|---|---|---|---|
| `internal/handlers/server/calendar` | 1,049 | **8 commits** | **DELETE** unless a route mount is planned | Highest drift risk: actively maintained but unreachable. If the calendar API is still wanted, mount the handlers in `cmd/server` route registration FIRST and only then treat it as live. |
| `internal/integrations/news` | 419 | 2 | **DELETE** | Self-contained news fetcher; no route, no consumer. Note: CHANGELOG already documents hardening work here — that effort is currently invisible to production. |
| `internal/translation` | 260 | 2 | **DELETE** (after extracting nothing — `scenes.go` concurrency fix already landed) | Scene-translation helper used by no route. If a translate endpoint is planned, resurrect from git history. |
| `internal/handlers/remote/install` | 241 | 2 | **DELETE** | Remote install HTTP surface; superseded by Ansible-driven provisioning per ops runbooks. |
| `internal/handlers/remote/workers/management` | 228 | 2 | **DELETE** | Management endpoints never registered; worker lifecycle now flows through gRPC commands. |
| `internal/handlers/server/smoke` | 160 | 0 | **DELETE** | Superseded by `internal/fleet` Level-D smoke executor (the live surface). |
| `internal/preparedassets` | 121 | 1 | **DELETE** | Resolver abstraction with no consumer; prepared-asset gating lives in `internal/assets` + reservation certificates. |
| `internal/store/contracts` | 68 | **5 commits** | **DELETE** | Interface-only package. Kept passing for 5 commits while imported by nothing. If store interfaces are wanted as a seam, define them in the consuming packages (Go convention: consumer-side interfaces), not in a detached package. |
| `internal/slo` | 25 | 2 | **DELETE** | Trivial SLO type definitions, no reader. |
| `internal/metricscenter` | 37 | 1 | **DELETE** | Module aggregator stub; the live registry is `internal/metrics`. |
| `internal/handlers/server/health` | 11 | 0 | **DELETE** | 11-LOC stub; real health/readiness is `cmd/server/bootstrap_readiness.go` + healthz wiring. |
| `internal/handlers` (empty shell) | — | — | **DELETE** dir | Zero top-level `.go` files; pure directory husk. |

Total reclaimable: **~2,600 non-test LOC** plus their test files.

## Deletion protocol (per AGENTS.md §1 + ADR 0008)

1. Operator confirms each package is not referenced by external callers
   (C1) — they are not reachable from outside the repo (C2 fails), so ADR
   0008 §(b) point 1 mandates **full removal in a single atomic layer**,
   no soft-deprecation.
2. Delete package directories + their `_test.go` files in ONE commit.
3. Run `bash scripts/ci/pre-removal-verify.sh` and require exit 0 before
   pushing (full-module vet/build/test — orphan `_test.go` references are
   exactly what scoped checks miss).
4. Update CHANGELOG `[Unreleased]` §Removal with the package list.

## Regression risk if deleted

| Risk | Packages | Rationale |
|---|---|---|
| **Basso** | `store/contracts`, `slo`, `metricscenter`, `health`, `handlers` shell | Interface-only or stub; zero behavioral surface. `go build` catches any missed reference immediately. |
| **Basso/Medio** | `news`, `translation`, `preparedassets`, `smoke`, `install`, `management` | Self-contained logic, no live route depends on them; the only risk is that a product roadmap item silently assumed this code was live. Confirm with the operator per row. |
| **Medio** | `calendar` | Recent churn (8 commits) + test repair history suggests someone believes it is live. Deleting without an explicit operator decision would be wrong; mounting it without an operator decision would add an unauthenticated-ish API surface nobody asked for. This is precisely the decision this brief escalates. |

## Related findings already actioned (2026-09-06, commits `7616cb69`…`8b3ca0b1`)

- Dead `RevokeToken` + dead `outputArtRepo` DI edge + dead `sshPass` param
  removed (Area 1).
- Validation ingress RFC3339 contract enforced with 400 + pin test (Area 2).
- `folders.go` getLinks TTL re-check under lock; `translation/scenes.go`
  semaphore acquired before goroutine spawn (Area 3).
- Cache-stats derivation WARN converted from one-shot `sync.Once` to
  time-throttled (Area 4).
- `internal/persistedtime` leaf extracted; 4 duplicated timestamp parsers
  collapsed (Area 5).
