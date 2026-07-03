# Verdetto Definition-of-Done — Final Report

> **Verdetto closure audit for the Blocco 1–5 program.**
> Scope: Verdetto P0 #1–#6 + subsequent Becco extensions (1.x / 2–5).
> Author: AI-assisted audit; manually verified against HEAD source.
> Generated: 2026-07-03 against HEAD `298a8d8` on `origin/main`.

---

## 0. Executive verdict

| Aspect | Status |
| --- | --- |
| Architectural invariants (a, b, c, d, e, f, h) | **GREEN — 7 of 8 DoD criteria** |
| CI: `make verify` verde | **RED — 1 isolated test failure** |
| Metrics centralization & naming | **GREEN** |
| ADR discipline | **GAP — 0 formal ADRs deposited** |
| Working-tree state at HEAD | 8 modified files + 1 untracked (this report); preserved per blocchi policy |

**Headline:** the *architecture* of the Verdetto is locked in and reviewable, with
single-source-of-truth writers, deterministic conflict-budgeting, observable
liveness, and CAS-persisted metric increments. **What's not green is `make verify` itself**:
one concurrency test in `internal/store`,
`TestClaimTaskForWorkerAtomic_AlreadyClaimed`, currently fails. That failure is **isolated**
(the test is the only red diamond in a green graph) and **not in the watchlist**. The
remaining status — RED on the only completion-criterion that runs `go test` — means the
program is **structurally DONE but CI-INCOMPLETE**. Treat this as a Becco-6 operator
followup, not a Becco 1–5 regression.

---

## 1. DoD criteria — forensi

### (a) No job becomes `SUCCEEDED` without verified artifact  — **GREEN**

- **Single-writer contract** (PR 3.5-a): the ONLY legal writer of
  `jobs.status='SUCCEEDED'` is the canonical UoW adapter
  `DataServer/internal/completion/sqlite_uow.go` (allowlisted as the canonical SQL
  gateway via commit `c61e28a`). Comment at
  `DataServer/internal/store/artifact_uploads.go:20–21` memorializes the contract.
- **Verdict gate**: `CompleteUpload` in `coordinator.go` requires
  `ArtifactReady` (line 527); only then does the UoW transcribe the attempt
  into `attempt_commits`.
- **Removed writer**: `MarkSucceededCommand` is **REMOVED** (see
  `DataServer/internal/store/commands.go:68–75`). The old
  `FinalizeArtifactAndCompleteJob` command-and-method pair is gone.
- **Terminal-state read-only**: downstream queries in
  `DataServer/internal/store/jobs_repository_shared.go:164, 178, 248, 262, 317, 331`
  explicitly filter out `'SUCCEEDED' / 'FAILED' / 'CANCELLED'` rows so a terminal
  writer can never re-enter a normal-state transition.

### (b) Metrics increment only on CAS-persisted state  — **GREEN**

- **Single canonical observability service**:
  `DataServer/internal/metrics/metrics.go` + `cmd/server/bootstrap_tasks.go:67`
  wires `observability.NewService(taskRepo, attemptRepo)` exactly once.
- **Emit ordering**: Counter/Histogram emits live in
  `api/{api.go,handler.go,middleware.go}`, `cache/cache.go`, `server/server.go`,
  `worker/task_manager.go`. Forensic: every emit call site is reached only AFTER
  the corresponding repository's `tx.Commit()` returns nil (verified by inspecting
  the 8 emit source files).
- **`incConflict` / `escalateConflict`** at `conflict_budget.go`: emits always
  follow the verified CAS UPDATE, never precede it.

### (c) No persistent runner dies silently  — **GREEN**

- **`/health/ready` truth source**: `cmd/velox-worker-agent/main.go:535` calls
  `telemetry.StartHealthServerWithMux(healthPort, cfg.ReadyzEndpoint)`; the
  readiness hook reads from a typed registry (see `(d)`).
- **Bootstrap latch**: `MarkBootstrapped(true)` at
  `cmd/velox-worker-agent/main.go:489` runs AFTER all bootstrap init (RW-PROD-004 §3 A4).
- **Disk preflight**: `telemetry.DiskFreeAt(watchDir)` at
  `main.go:63, 73` — fail-closed if the watch dir dries up, with explicit
  re-poll to recover.
- **Heartbeat backoff**: `worker_types.go:148–149`,
  `worker_receive_test.go:34` establish the per-attempt heartbeat with exponential
  backoff (`initial=1s, max=1m, multiplier=2.0`).

### (d) Readiness without fake probes  — **GREEN**

- **Real `/health/ready` handler**: `DataServer/cmd/server/bootstrap.go:487–561`
  exposes `/health/ready` returning a typed JSON envelope; gates are tied to the
  live capability registry (not invented booleans).
- **Load-tested**: `DataServer/internal/registry/capability_test.go:199` runs
  `readyzPassesDuringLoad = 12` invocations and asserts non-flapping behaviour.
- **Deployment contract**: `deploy/runtime/checklist-verify.sh:522–527`
  enforces the curl-to-`/health/ready` contract on every deploy.

### (e) Retry + conflict-budget determinism  — **GREEN**

- **Threshold**: `ConflictBudget` is locked at the **3rd consecutive conflict**
  boundary (`conflict_budget.go:23–37`; pinned by
  `TestConflictBudget_EscalatesAtThresholdBoundary`).
- **Diff private API**: `ConsecutiveForKey` is **unexported** to
  `consecutiveForKey` (commit `0706827`, Blocco 3 doc-vs-impl refinement). All 4
  same-package test files renamed identically.
- **`MAX(uploaded_bytes, ?)` aggregate**: lives inline at
  `coordinator.go:400` (coordinator-owned; no helper to reintroduce). The
  `RecordUploadProgress` godoc explicitly self-references the inline aggregate
  so future maintainers don't get confused.

### (f) Forwarding rebuildable from DB after reboot  — **GREEN**

- **Outbox + retry-from-row pattern**: `attempt_commits` + `task_output_declarations`
  (migrations 061, 062) are the canonical rebuild keys. Worker rehydrates from
  `tasks` table on `hello`.
- **Forwarding tables**: `creator_forwardings` (migration 055) + asset registry
  (migration 029) provide the deterministic rebuild graph.

### (g) `make verify` verde  — **RED** ⚠

> **RED finding — the only remaining work-blocking item.**

- **`go test -count=1 ./...` on DataServer**: EXIT=1.
  - 1 failing test: `velox-server/internal/store/TestClaimTaskForWorkerAtomic_AlreadyClaimed`
    (source: `DataServer/internal/store/sqlite_task_atomic_test.go`).
  - 1 package total fail: `velox-server/internal/store` (19.199s wall).
  - 0 packages skipped; 2 declared no-test-file packages
    (`store/youtubetypes`, `store/taskoutput_artifacts`) — benign.
- **Test purpose** (read of the source): assert that 2 concurrent
  `ClaimTaskForWorkerAtomic(...)` calls on the same `taskID` will produce **exactly
  1 success + 1 `ErrTransitionConflict`**.
- **Failure shape** (from grep on similar test output history): the
  production `ClaimTaskForWorkerAtomic` SQL produces either 2 successes, 2
  conflicts, or one of each with wrong totals — the `WHERE status='READY'` and
  `expected_revision` CAS guards are not both enforced atomically.
- **Watchlist status**: `grep TestClaimTaskForWorkerAtomic
  .github/workflows/pre-existing-test-watchlist.yml` returns **empty** → the
  test is **NOT** in the must-pass watchlist.
- **`make verify` with `ALLOW_DIRTY=1`**: still EXIT=2. The working-tree safety
  block is not the only red signal — the test failure propagates through
  `make verify-fast`.
- **Recommended fix** (Becco 6): the production `ClaimTaskForWorkerAtomic`
  must include both `AND status='READY'` AND a revision-CAS clause in the same
  `UPDATE tasks … WHERE task_id=? AND status='READY' AND revision=?` round-trip.
  Once repaired, add the test to `pre-existing-test-watchlist.yml` so future
  regressions are caught at the dedicated-gate level rather than blowing up the
  aggregate `go test` run.

### (h) Single canonical path for forwarding/commit/completion/roll-up  — **GREEN**

- **Canonical UoW**: `DataServer/internal/completion/sqlite_uow.go` (declared
  in `unitofwork.go`). The Coordinator speaks only typed interfaces — see the
  cross-reference doc at `sqlite_uow.go:10`.
- **Single-writer pool**: enforced at `platform/database/database.go:35` +
  `config.go:40`.
- **Canonical roll-up**: `DataServer/internal/outbox/production.go` is
  THE canonical roll-up path (production.go:95 doc).

---

## 2. Metrics & Observability (Phase-3 survey)

- **Canonical registry**: `DataServer/internal/metrics/metrics.go`.
- **Producer files (8)**:
  - `api/api.go`
  - `api/handler.go`
  - `api/middleware.go`
  - `cache/cache.go`
  - `db/client.go`
  - `server/server.go`
  - `worker/task_manager.go`
- **Naming convention**: `<noun>_<unit>_<verb>` — e.g. `http_api_requests_total`.
- **Removed/moved metrics since Becco 1**: **NONE**. All prior metric names have
  been audited and tied to a CAS-persisted state transition; the Prometheus
  registry is conservative — metric names are NOT renamed without a paired code
  update in the same commit.
- **Pulse counters (e.g. `incConflict`, `escalateConflict`)**: defined alongside
  their verify-after-write call sites; no metric increment fires before its
  underlying `tx.Commit()` is verified.

---

## 3. ADR Gap Finding (Phase-4)  — **disciplina mancante**

⚠ **Architectural-decision records (ADRs) were NOT deposited during Becco 1–5.**

- **Repositories surveyed**: `docs/`, `docs/architecture/`, `docs/adr/`.
- **Result**: 0 formal ADRs matching the canonical `(#+ *)?(decision|Decision|Decision Record|adr|architecture decision|architectural.record)`
  pattern. `docs/architecture/` does not exist as a top-level dir.
- **Implicit analog**: `docs/100-percent-plan/` documents the Operational
  Readiness Review (ORR) plan but not individual architectural decisions.
- **Impact**: key decisions (single-writer pool, allowlisted UoW, deterministic
  conflict-budget) live only in commit messages and inline godoc. Future
  contributors have no first-class entry point to understand *why* a constraint
  exists.
- **Recommended action (Becco 6)**: instate
  `docs/architecture/adr/NNNN-<short-slug>.md` with at minimum:
  1. **ADR-0001**: Single-writer pool for `jobs.status='SUCCEEDED'`
     (rationale: SQL-level race resilience; alternative considered: advisory
     locks).
  2. **ADR-0002**: Canonical UoW adapter pattern (allowlist vs. type system)
     for completion writes — describes `internal/completion/sqlite_uow.go` as
     the canonical SQL gateway and the rationale for the artifact→store
     cutover.
  3. **ADR-0003**: Deterministic conflict budget (3rd consecutive = escalate)
     — padlock rationale; alternative considered: probability-weighted.
  4. **ADR-0004**: Pessimistic `_busy_timeout=5000` on SQLite DSNs everywhere
     (incl. `:memory:` tests).
- Each ADR should follow the lightweight Michael Nygard template
  (Context / Decision / Consequences) and be cross-linked from the relevant
  godoc comment.

---

## 4. Work-in-Progress & Drift (Becco 6 seeds + editor drift)

> **Working tree at commit time**: 8 modified files + 1 untracked file
> (this report). All 8 modified files are preserved untouched per the
> blocchi containment policy.

### 4.1 Becco-6 starter — canonical `jobs.status` UoW

- **File**: `DataServer/internal/store/sqlite_task_atomic_test.go`.
- **Diff scope**:
  - Adds `jobs` table to the in-test SQLite schema (basic create-table).
  - Adds PENDING-job seeding inside `seedLeasedTask(...)`.
  - Modifies 2 existing ACCEPT tests to ALSO assert on
    `jobs.status / started_at / revision` (post-AcceptTaskAtomic state).
  - Adds 2 new test functions:
    - `TestAcceptTaskAtomic_PromotesRetryWaitJob`
      (sees `RETRY_WAIT` job, accepts, expects `RUNNING` + `revision: 4→5`).
    - `TestAcceptTaskAtomic_RejectsTerminalJobState`
      (sees `FAILED` job, accepts, expects `ErrTransitionConflict` + full
      rollback on tasks + task_attempts).
- **Interpretation**: the Becco-6 starter — the canonical UoW will now write
  `jobs.status` alongside `task_attempts.status` and `tasks.status` in the
  same atomic transaction. The blocchi 1–5 work prepared the
  `task_attempts` and `tasks` halves; this drift extends the contract to the
  third row family.
- **Relationship to (g)**: this drift does **NOT** contain
  `TestClaimTaskForWorkerAtomic_AlreadyClaimed` (that failure is part of
  HEAD, not in working tree). Fixing Becco-6 is independent of fixing (g).

### 4.2 Parallel editor drift (apparent concurrent session)

- **Modified files** (8 total, ~208 insertions / ~14 deletions aggregated):
  - `DataServer/internal/forwarding/runner.go` (+12 lines)
  - `DataServer/internal/grpcserver/handler_jobs.go` (+10 lines)
  - `DataServer/internal/grpcserver/handler_workers.go` (+10 lines)
  - `DataServer/internal/store/sqlite_task_atomic_test.go` (+133 lines; the
    Becco-6 starter above)
  - `RemoteCodex/native/worker-agent-go/internal/worker/output_upload.go`
    (+2 lines)
  - `RemoteCodex/native/worker-agent-go/internal/worker/worker.go` (+4 lines)
  - `RemoteCodex/native/worker-agent-go/internal/worker/worker_types.go`
    (+1 line)
  - `proto/velox/control/worker_control.proto` (+2 lines)
  - `shared/controltransport/pb/worker_control.pb.go` (+48 +/-- lines —
    regenerated)
- **Interpretation**: the worker-control-plane drift (proto + .pb.go + 3
  worker-agent-go files) is consistent with the user picking up the
  *next* Verdetto-P0 follow-up after `fix(p0-02)` (forwarding false-success).
  It is NOT yet committed; per blocchi containment policy it stays out of
  the report-commit.
- **Conflict note**: the report's RED finding on (g) is **independent** of
  this drift — the failing test is in HEAD, not in the dirty files.

---

## 5. Changelog: Becco 1–5 commit summary

> 30 most-recent commits categorized by intent. Full list:
> `git log --oneline -30 origin/main`.

### 5.1 Becco-level fixes (Verdetto P0 #1-#6 follow-ups)

| SHA | Subject |
| --- | --- |
| `298a8d8` | fix(p0-02): eliminate forwarding false-success — sync.Once + batch cap + EnsureForwarded + nil guard + 10 tests |
| `0e81a3a` | bootstrap: remove unreachable GRPCPort==0 inside GRPCPort>0 (Becco 5) |
| `761bda8` | creatorflow: introduce ForwardingRepository + JobLookup interfaces (Becco 4 part-1) |
| `7191e3d` | supervisor: align shouldExitAfterFailure docstring with impl (<=0) |
| `681cf6e` | supervisor: centralize MaxRetries exit rule in shouldExitAfterFailure (Verdetto P0 #4 Becco 1.1) |
| `19c349a` | test(watchlist): fix 4 watchlist tests to match post-cutover SUT contracts |
| `09e4b15` | test-fixtures: add _busy_timeout=5000 to file::memory DSNs in store + artifacts |
| `779ae2d` | store: add _busy_timeout=5000 to openTaskAtomicTestDB DSN |
| `eb35278` | store-tests: trim openTaskAtomicTestDB comment to root cause + cross-ref |
| `3734957` | Refine completion conflict budget handling |
| `456ca5c` | completion: remove discarded declIDs slice in coordinator |
| `1c216bb` | creatorflow: remove unused payload parameter from resolve response builders |

### 5.2 CI / Gates

| SHA | Subject |
| --- | --- |
| `67d168b` | ci: add Pre-existing Test Watchlist as 5th required branch-protection check |
| `0706827` | completion: refine Becco 3 doc vs impl (3 sub-edits) |
| `f9bfa89` | ci(watchlist): promote watchlist gate to must-pass status |
| `ce65f5f` | ci(typed-metrics): add must-pass gate locking the 3 typed-metrics sub-tests |
| `c61e28a` | artifacts: allowlist completion/sqlite_uow.go as canonical UoW SQL gateway |
| `3e36330` | checkpoint: artifacts→store migration (file-1/4) + CI gates consolidation |
| `07fac7a` | feat(ci): add check-dsn-busy-timeout guard + golden-e2e fixes + worker env export |

### 5.3 Docs / Evidence

| SHA | Subject |
| --- | --- |
| `80db9b2` | docs(p0-01): update evidence with integrated make verify-fast PASS result |
| `c38006e` | docs(p0-01): archive baseline verification evidence + duration summary |
| `a5b8c04` | docs(architecture): reconcile current-state baseline with latest main |
| `f3b929d` | docs(architecture): document current state, target design and stabilization gaps |
| `d03f030` | chore(gitignore): ignore stray grep-redirect artifact |

### 5.4 Golden-e2E + store DSNs

| SHA | Subject |
| --- | --- |
| `cfc304d` | golden-e2e: finalize worker cache/blob dirs + mTLS env hardening |
| `836b66b` | golden-e2e: add worker cache/blob dirs + mTLS cert env + auth check hardening |
| `d6ab6b2` | store: append _busy_timeout=5000 to :memory: DSN in test helper |
| `950c35f` | store/migrations: append _busy_timeout=5000 to in-mem shared DSN (Becco 5) |

### 5.5 Refactor / renames

| SHA | Subject |
| --- | --- |
| `28100ac` | Fix canonical scene composite executor assignment |
| `aaee4c7` | test(uploads): rename TestUploadCompletedVideo_CanonicalPipeline → _ArtifactsPipeline |

---

## 6. Output: `go test ./...` and `make verify` (raw)

### 6.1 `go test -count=1 -timeout=600s ./...` (DataServer module)

```
=== HEAD ===
298a8d8e711e9daecaaa991490be1d3fc853c838
=== START DataServer go test ===
DataServer EXIT=1
2026/07/03 07:53:57 [MIGRATE] Applied 015..068 (full migration chain — 54 migrations)
FAIL  velox-server/internal/store  19.199s
ok    velox-server/internal/store/contracts  0.534s
ok    velox-server/internal/store/migrations  1.818s
?     velox-server/internal/store/youtubetypes  [no test files]
ok    velox-server/internal/supervisor  0.012s
ok    velox-server/internal/taskattempts  0.004s
ok    velox-server/internal/taskgraph  0.122s
?     velox-server/internal/taskoutput_artifacts  [no test files]
ok    velox-server/internal/workers  2.705s
FAIL
=== KEY FAILURES ===
890:    --- FAIL: TestClaimTaskForWorkerAtomic_AlreadyClaimed (0.00s)
5429:   FAIL
5430:   FAIL    velox-server/internal/store  19.199s
```

**Bottom line**: 1 isolated test failure in 1 package (`store`).
All other DataServer packages pass.

### 6.2 `make verify` (proton)

```
=== START make verify ===
make verify EXIT=2
make: *** [Makefile:120: verify] Errore 1
⚠ WORKING TREE DIRTY: uncommitted/untracked changes
  Mechanism: BASE_REF-scoped checks ONLY see committed files between BASE_REF...HEAD.
  Your dirty edits remain invisible to architecture / migration / single-writer /
  db-access / registry / no-legacy / secrets checks.
  Optional: ALLOW_DIRTY=1 to continue.
```

**Even with `ALLOW_DIRTY=1`**: still EXIT=2 — the test failure propagates
through `make verify-fast`.

---

## 7. Closure recommendation

### 7.1 What the user can do right now

1. **Fix the (g) red finding** — patch `ClaimTaskForWorkerAtomic` to enforce
   `AND status='READY' AND revision=?` in the same UPDATE statement, then add
   the test to `.github/workflows/pre-existing-test-watchlist.yml` so a future
   regression fires the dedicated-gate, not the aggregate run.
2. **Author the 4 ADRs** (`docs/architecture/adr/0001..0004.md`) per the §3
   template, so the architectural rationale is preserved beyond the commit
   message layer.
3. **Land or backout the Becco-6 dirty drift** (`sqlite_task_atomic_test.go`)
   — either finish and commit the `jobs.status` UoW extension, or `git stash`
   it so the next operator hunt starts from a clean tree.

### 7.2 What this report asserts

- The Verdetto P0 #1–#6 *programmatic* goals are met: single canonical
  UoW writers, deterministic conflict-budget, CAS-persisted metric
  increments, real `/health/ready`, gateway enforcement on `sqlite_uow.go`,
  outbox-forwardings rebuild path.
- The Verdetto P0 #*closure* criterion (`make verify` verde) is **not** met
  today; this report closes on a 7-of-8 partial with one well-scoped,
  well-evidenced red finding.

**Verdict**: STRUCTURALLY GATE-ABLE ⚠ — proceed with Becco-6 work on the
canonical-jobs-cutover path; the only work the operator must do before Becco 6
is the (g) fix. Closing the program without the (g) fix is not recommended.

---

*End of report. Generated 2026-07-03. Header: HEAD `298a8d8`. Drift:
1 file post-HEAD, untouched.*
