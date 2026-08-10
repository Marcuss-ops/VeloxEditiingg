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
  `DataServer/internal/forwarding/runner_failure_injection_test.go`
  — failure message claimed "the tick does not release the semaphore on
  dial failure". **RESOLVED** (2026-08-07): the semaphore IS released via
  `defer func() { <-r.sem }()` in `runner_tick.go` (verified across all
  git history, including `298a8d8e` / `c322182` / `6b21b82`); the
  reported failure was a slow-test timeout misdiagnosed as a deadlock.
  The test pointed the client at `http://localhost:1` with `Retries: 0`,
  but `remoteengine.DefaultRetryPolicy` maps `Retries<=0` to 3 retries
  (1s+5s+15s backoff ≈ 21-25s per lease), so under full-module gate load
  the test could exceed its own 30s deadline. Fixed by pointing the three
  affected tests at a local httptest server returning 404 (PERMANENT →
  no retry backoff): package runtime dropped 74s → 8s and the tests now
  pass deterministically (`go test -race ./internal/forwarding/`).

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

  through the `09940dd6` reconcile commit. `go test -count=1
  ./internal/handlers/server/calendar/...` now passes, including under
  `-race`; re-verified with `-count=3`. No further action required.

**2026-08-08 deployment** of the gate surfaced:

- `TestEnqueue_ConcurrentForwardingRetriesConverge` in
  `DataServer/internal/jobs/enqueue/enqueue_forwarding_concurrency_test.go`
  — fails intermittently (atomic create persistence error) only under the
  full-module gate load, passes deterministically in isolation
  (`go test -count=1 -run TestEnqueue_ConcurrentForwardingRetriesConverge
  ./internal/jobs/enqueue/` passes 3/3). Unrelated to the `ssh-check`
  worker-name work surfaced alongside it (that surface lives in
  `internal/fleet` + `internal/handlers/server/api`). Tracked as a
  followup: likely a timing/concurrency flake under aggregate load rather
  than a regression; re-verify under `-race` and `-count=3` before
  classifying.

**2026-08-10 deployment** of the gate surfaced:

- `TestMigrationUpgradesLegacyValidationTable` in
  `DataServer/internal/handlers/remote/workers/validation/handler_test.go`
  — deterministic failure (`no such table: publication_states`) caused by
  the then-uncommitted (untracked WIP) migration
  `DataServer/internal/store/migrations/sqlite/142_publication_submission_identity.sql`,
  which runs `ALTER TABLE publication_states ADD COLUMN submitted_remote_id`
  in every migration-enabled store, including the legacy-upgrade test
  fixture that seeds history through 136 and creates only the legacy
  tables it exercises. The table `publication_states` is created by
  migration 126; the fixture does not create it. Attribution verified:
  `go vet` and `go build` were clean; only the full-module `go test` was
  red, and only on migration 142. Not a regression from the assetref/
  telemetry/alert work verified in the same run. **RESOLVED** (2026-08-10):
  migration 142 landed on main and the legacy-upgrade fixture in
  `handler_test.go` now provides the pre-142 `publication_states` shape
  (mirroring 126, same pattern as the pre-138 `worker_task_runtime`
  fixture) with a pin asserting 142 is applied; the merge-into-126 option
  was rejected because 126 is already applied in production and rewriting
  it would break checksum validation.

- `TestSucceededWriterIsFinalizationOnly` in
  `DataServer/internal/artifacts/scan_test.go` — deterministic failure
  surfaced on the 2026-08-10 gate run: the audited allowlist pin
  (`SET status = 'SUCCEEDED'` SQL fragments must be allowlisted with a
  documenting comment or routed through the verified `FinalizeVerified`)
  flags `DataServer/internal/store/store_publication_state.go`, which
  performs a direct `SET status='SUCCEEDED'` UPDATE introduced by commit
  `3b6dcdd9 fix(deliveries): require reconciled publication evidence`.
  The offending file is NOT in the scan allowlist. Attribution verified:
  the fragment is on `HEAD` (not working-tree WIP), the introducing commit
  is `3b6dcdd9`, and the failure is unrelated to the alert-contract work
  verified in the same run (vet 0 / build 0). **RESOLVED** (2026-08-10):
  `store_publication_state.go` was added to the audited allowlist in
  `internal/artifacts/scan_test.go` with a documenting comment — the
  SUCCEEDED flip targets `publication_phase_effects` (a SEPARATE
  lifecycle from jobs: the VERIFYING-phase reconciliation effect, gated
  by `reconciliation_verified` + `RowsAffected()==1` inside one
  transaction), so it does not need to route through `FinalizeVerified`.
  Same pattern as the existing deliveries/workflow/smoke_runs entries.

## 5. Worker identity model: `worker_id` is immutable, `worker_name` is mutable

Codified after the 2026-08-08 rename attempt. Changing a worker's
`worker_id` is NOT an ordinary rename: the ID is the mTLS-bound security
principal and the Master **rejects** registration when the client
certificate's CN does not match the declared `worker_id` (see
`DataServer/internal/grpcserver/handler_stream.go` — `worker_id
mismatch: cert=%s, declared=%s`). Attempting to rename `worker_id`
caused the canary worker to drop to DISCONNECTED until its original ID
was restored. Do NOT rename `worker_id` for cosmetic/ordering purposes.

### The two identities (never conflate)

| Field | Nature | Examples |
|-------|--------|----------|
| `worker_id` | **IMMUTABLE** — mTLS certificate identity, OpenBao identity, Master registration, historical jobs, leases, DB references | `velox-worker-13197`, `velox-worker-523925eb`, `host_57_129_132_133`, `host_57_131_20_173` |
| `worker_name` | **MUTABLE** — operator-facing display name shown in UI/tools | `velox-worker-01..04` |

### Rules

- `worker_id` MUST stay the value bound to the worker's mTLS certificate.
  If it must change (a genuine appliance replacement), it is a **security
  migration** (new OpenBao identity → new cert → allowlist → reconnect →
  reference migration), never a quick rename.
- Mutable naming/ordering goes on `worker_name` ONLY — set it in the
  worker's `worker_config.json` (`worker_name` field; `VELOX_WORKER_ID`
  env must keep the immutable ID). `worker_name` is NOT env-overridable by
  the agent (pkg/config: source is the JSON config only).
- Operator surfaces (dashboard, `fleetctl`, admin API) should render
  `worker_name` as `NAME` alongside `worker_id` — never rewrite `worker_id`
  inline. The admin surfaces: `GET /api/v1/admin/workers` (`hostname`
  field carries the worker_name) and `ssh-check` (now emits
  `worker_name` alongside `worker_id`).
- Allowlist `VELOX_ALLOWED_WORKERS` lists **security IDs**
  (`worker_id`), not display names.

## 6. Capability state machine: `DISABLED` / `READY` / `MISCONFIGURED` only

Codified after the 2026-08-10 fail-open architecture audit (fleet executor
registry noop fallback, Level-D smoke stub, opsalerts nil datasource,
alert error swallowing). Every capability (fleet executor, Level-D smoke,
fleet alerts, alert engine, update path, …) MUST be in exactly one of three
states, and MUST NEVER be "enabled but with a hidden nil/noop/stub":

| State | Meaning |
|-------|---------|
| `DISABLED` | Route/runner intentionally not registered; operator explicitly opted out. Does NOT fail readiness. |
| `READY` | All real dependencies present and wired. |
| `MISCONFIGURED` | Capability requested but a dependency is missing/nil/stub. /ready is RED and the operation is refused. |

Concrete rules:

- **Noop/test doubles are test-only.** `NoopOperationExecutor`,
  `StubAssetResolver` and similar doubles may appear ONLY in `_test.go`
  files or in explicit dev-mode wiring gated by an env var
  (`VELOX_SMOKE_MODE=development`) that production `Validate()` rejects.
  A production registry starts EMPTY; a missing executor yields
  `EXECUTOR_NOT_CONFIGURED`, never a noop success.
- **Constructors fail closed on missing dependencies.** `opsalerts.NewEngine`
  (and equivalents) MUST return an error for a nil/typed-nil datasource or
  store — never a nil-swallowing engine. A capability wired with a nil
  dependency is `MISCONFIGURED`.
- **Every capability exposure MUST have a fail-closed readiness gate.**
  Each `HealthModule.AddReadinessCapability("X", …)` in
  `cmd/server/bootstrap_readiness.go` MUST be paired with an
  `AddReadinessCheck("X-capability", …)` that returns an error when the
  capability is `MISCONFIGURED` (readiness rossa su dipendenze mancanti).
  `DISABLED` never fails readiness; `MISCONFIGURED` always does.
- **Enforcement.** `scripts/ci/check-capability-contract.sh` (wired as
  the `capability-contract` CI workflow) statically forbids the fail-open
  symbols in production code and pins the readiness pairing; the pairing
  is also pinned at runtime by
  `TestReadiness_CapabilityExposuresHaveFailClosedGates` in
  `cmd/server/bootstrap_readiness_test.go`.
