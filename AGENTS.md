# AGENTS.md — Operational rules for AI/code agents working on this repo

This file documents operational rules that any AI agent or human contributor
MUST follow when working on this repository. The rules below were codified
in response to specific failures observed on `main` during the
soft-deprecate → full-removal pivot of `/api/remote/pipeline` (see
`docs/CREATOR-PUSH.md §Removal` and `CHANGELOG.md [Unreleased]`).

## 1. Mandatory verification gate for removal commits

**Before pushing ANY commit that retires an exported symbol or a
cross-package helper** (i.e., a "removal commit"), the following
full-module verification gate MUST pass:

```bash
bash scripts/ci/pre-removal-verify.sh
```

The script runs three checks from `DataServer/` (the discrete Go module):

| Check | Command | Purpose |
|-------|---------|---------|
| 1 | `go vet ./...` | Catch unused imports + suspicious constructs in all packages |
| 2 | `go build ./...` | Catch undefined symbols in all production code |
| 3 | `go test -count=1 ./...` | Catch orphan `_test.go` references + cross-package type drift |

All three are **full-module** (`./...`), not scoped. Scoped verification
(e.g., `go vet ./internal/foo/...`) is insufficient: orphan `_test.go`
references to a now-removed helper, plus unused imports in sibling files,
are not caught by the scoped check.

### Exit codes

| Code | Meaning |
|------|---------|
| `0`  | All three checks green — safe to push |
| `1`  | Runtime error (missing module dir, missing tooling) |
| `2`  | At least one check failed (read `/tmp/velox-pre-removal-verify.txt`) |

### Why this gate exists

The `c322182` fixup commit on `main` (2026-07-25) is the canonical case
study. Scoped `go vet ./internal/handlers/server/pipeline/...` +
`go build ./internal/handlers/server/pipeline/...` had passed at the
boundaries of `c9c2ae4` and `716426a`, but a later full-module run caught
two compile residuals:

1. An orphan reference to `normalizeRemoteEngineIntake` in
   `creator_push_test.go` (helper removed in `c9c2ae4`).
2. An unused `enqueue` import in `pipeline_run_actions.go`
   (sync-forward branch removed in `c9c2ae4`).

Both residuals were repaired in a subsequent fixup commit. Without the
gate codified here, the next removal would produce the same pattern of
"scoped verification passes, full-module verification catches residuals".

The gate is the durable enforcement; the ADR is the rationale.

### Cross-references

- `scripts/ci/pre-removal-verify.sh` — the executable wrapper (this file's
  enforcement target).
- `docs/adr/0008-soft-deprecate-vs-remove-pivot.md §(b) point 2` — the
  codified gate decision (the rationale).
- `scripts/ci/run-split-regression.sh` — the broader CI regression runner
  that includes a `full-velox-server` group for the same gate at release
  time.

## 2. Soft-deprecate vs full removal decision

When retiring an exported symbol (public function, public type, exported
constant, public HTTP route handler, public CLI command) or a
cross-package helper, consult ADR 0008 §(b) point 1:

| Condition | Check |
|-----------|-------|
| **C1**: confirmed external callers still use the symbol | telemetry counter shows non-zero traffic OR operator confirms known un-migrated client |
| **C2**: the symbol is reachable from outside the repo | public HTTP route, exported Go function in a published module, public CLI command, or doc referenced from a customer-facing artifact |

- Both C1 and C2 hold → soft-deprecate with telemetry + sunset date.
- Either fails → full removal in a single atomic layer (docs → refactor → cleanup).

## 3. Commit discipline

- All work goes directly on `main`; no branches, no PRs.
- Atomic commits: each commit must be self-contained and revertable in
  isolation.
- Push after every modification; do not accumulate local-only work.
- Removal commits MUST satisfy §1 above before push.

## 4. Pre-existing test failures surfaced by the gate

When this gate is run, it may surface pre-existing test failures that
were not introduced by the current commit. These are findings, not
blockers for pushing unrelated work — but they MUST be tracked as
followups.

**2026-07-25 deployment** of this gate surfaced:

- `TestTick_EffectiveClaimBatch_ParallelNoDeadlock` in
  `DataServer/internal/forwarding/runner_failure_injection_test.go:178`
  — deterministic deadlock when the mock remote engine at
  `http://localhost:1` refuses connection; the tick does not release the
  semaphore on dial failure. Pre-existing on `main` before the gate
  deployment; tracked as critical followup.

**2026-07-28 re-deployment** of the gate (commit 6b21b82 series) added:

- 4 `TestCalendarAPI_*` deterministic failures in
  `DataServer/internal/handlers/server/calendar/*_test.go`:
  - `TestCalendarAPI_CreateQueuesAndReturnsAgentFields`
  - `TestCalendarAPI_UpdateCompletesQueuedJobWithoutDuplicate`
  - `TestCalendarAPI_ExternalIDIsIdempotent`
  - `TestCalendarAPI_StatusLifecycleAndOutputs`
  Calendar event lifecycle / queue ops. Pre-existing on `main` (well
  before the 2026-07-28 commit) and unrelated to the AGENTS-plan
  test-file refactor; tracked as followups.