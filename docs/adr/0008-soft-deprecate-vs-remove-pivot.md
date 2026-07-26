# ADR: Soft-deprecate vs full removal — when to pivot, and the minimum verification gate

- **Status**: Accepted
- **Date**: 2026-07-25
- **Scope**: `DataServer` canonical intake paths (creator_push intake vs legacy remote-engine sync-forward). The verification gate in §(b) applies project-wide to every commit that retires an exported symbol or cross-package helper.
- **Supersedes**: (none — first ADR on the deprecation/removal policy)
- **Related commits**:
  - `51a307d` — `refactor(pipeline): unify creator_push + remote-engine normalization to eliminate drift`
  - `788a119` — `feat(pipeline): add deprecation telemetry for /api/remote/pipeline legacy path`
  - `61ac829` — `docs(creator-push+changelog): document /api/remote/pipeline deprecation timeline`
  - `57131fd`, `5983b61`, `5d484c4` — fixups on the soft-deprecate layer
  - `d433e97` — `docs(creator-push+changelog): replace §Deprecation timeline with §Removal`
  - `c9c2ae4` — `refactor(pipeline): fully remove legacy sync-forward path`
  - `716426a` — `chore(cleanup): delete 5 legacy tests + retire remote_engine_legacy metric label`
  - `c322182` — **fix(pipeline): delete orphan test + remove unused enqueue import**
  - `a557b8d` — `docs(changelog): add pivot narrative at top of [Unreleased] §Removal`

## (a) Contesto

The `DataServer` pipeline package exposes two parallel HTTP paths into the
canonical `creatorflow.Resolver`:

| Path | Handler | Origin | Role |
|------|---------|--------|------|
| `POST /api/remote/pipeline` | `forwardPipelineResultToWorker` (in `forwarding.go`) | legacy remote-engine sync-forward | master-initiated, waits for engine to return a complete result |
| `POST /api/v1/creator/jobs` | `CreatorPush` (in `creator_push.go`) | creator-machine push | creator-initiated, owns the full payload at submission time |

Both paths converged on the same `creatorflow.Resolver.Resolve` contract, but
each had its own normalizer (`normalizeRemoteEngineIntake` vs
`normalizeCreatorPushRequest`) and its own forwarder
(`syncForwardResult` vs `resolveCompletedPayload`). The duplication was the
seed of drift: a future maintainer adding a new field to the payload DTO
would have to remember to update both normalizers and both forwarders.

The first response to that drift was the **soft-deprecate layer** (commits
`51a307d → 5d484c4`, 6 commits):

1. Unify the normalizers behind one helper that both paths shared
   (`51a307d`).
2. Add a deprecation telemetry counter (`accepted_from=remote_engine_legacy`
   vs `creator_push`) so operators could observe callers that had not yet
   migrated (`788a119`).
3. Document the deprecation timeline in `docs/CREATOR-PUSH.md` and
   `CHANGELOG.md` (`61ac829`).
4. Three fixup commits to harden the telemetry guard, add failure-path
   tests, and clarify the sunset policy in CHANGELOG
   (`57131fd`, `5983b61`, `5d484c4`).

The soft-deprecate layer correctly avoided breaking external callers
mid-migration, but it produced an audit-trail of 6 commits with telemetry
gates, dual normalizers, and a parallel set of tests that the canonical
intake would have to drag forward forever. After review, the operator
**pivoted to full removal**: external callers had already been migrated off
the legacy path, and the soft-deprecate scaffolding was no longer paying
for itself.

The **removal layer** (commits `d433e97 → a557b8d`, 5 commits):

1. Replace the §Deprecation timeline with §Removal in docs (`d433e97`).
2. Inline the legacy normalizer into `normalizeCreatorPushRequest`,
   move `resolveCompletedPayload` and `firstStringResolver` to
   `creator_push.go`, delete `forwarding.go` (`c9c2ae4`).
3. Delete the 5 legacy tests + retire the `remote_engine_legacy` metric
   label (`716426a`).
4. **Fixup retroactively** (`c322182`): one test referencing the now-removed
   `normalizeRemoteEngineIntake` helper had been missed in `716426a`, and
   an `enqueue` import became unused after the sync-forward branch was
   removed in `c9c2ae4`. Both were compile residuals that **scoped
   `go vet ./internal/handlers/server/pipeline/...` + scoped `go build`
   had not caught at the prior commit boundary**, because the dead code
   sat in a sibling file (`pipeline_run_actions.go`) that the commit's
   scoped verification did not include.
5. Add the pivot narrative to `CHANGELOG.md [Unreleased]` (`a557b8d`).

This ADR records two operational lessons from the pivot:

1. **Which path to take when retiring an exported HTTP route or a
   cross-package helper**: soft-deprecate only when there are confirmed
   external callers still using the symbol; full removal otherwise.
2. **The minimum verification gate per atomic commit**: scoped `go vet`
   and scoped `go build` are insufficient when the commit retires symbols
   referenced from sibling files; the gate must include **full-module
   `go test ./...`** to surface compile residuals that scoped verification
   misses.

The second lesson is the one the `c322182` fixup commit taught us. Before
`c322182`, scoped `go vet` + scoped `go build` had passed on commits
`c9c2ae4` and `716426a`; both commits were pushed green. Only a later
full-module run caught the residual dead code in `pipeline_run_actions.go`
and the orphan test reference in `creator_push_test.go`. The cost of the
gap was one extra commit on `main` that should not have been necessary.

## (b) Decisione

### 1. Choose soft-deprecate **only** when both conditions hold

| Condition | Check |
|-----------|-------|
| **C1**: confirmed external callers still use the symbol | telemetry counter on the legacy path shows non-zero traffic OR operator confirms known un-migrated client in production |
| **C2**: the symbol is reachable from outside the repo | public HTTP route (`/api/...`), exported Go function in a published module, public CLI command, or document referenced from a customer-facing doc |

If both C1 and C2 hold → soft-deprecate with telemetry + sunset date + dual normalizer.

If either fails → **full removal in a single atomic layer** (docs → refactor → cleanup, no telemetry scaffolding).

The legacy `/api/remote/pipeline` satisfied C2 (public HTTP route) but
NOT C1 (telemetry had shown zero traffic post-`creator_push` rollout).
The pivot to full removal was therefore the correct call; the 6-commit
soft-deprecate layer was the over-engineered response to an over-conservative
reading of C1.

### 2. Minimum verification gate per atomic commit

Every commit that touches an **exported symbol** (public function, public
type, exported constant, public HTTP route handler, public CLI command) OR
a **cross-package helper** (function called from more than one Go package
in `DataServer/`) MUST pass the following three checks before the commit
is allowed to push:

```
1. go vet ./...                          (full module, not scoped)
2. go build ./...                        (full module, not scoped)
3. go test -count=1 ./...                (full module, not scoped)
```

The `c322182` fixup root-caused to the fact that checks #1 and #2 had been
run **scoped** (e.g. `go vet ./internal/handlers/server/pipeline/...`)
rather than full-module (`go vet ./...`). Scoped verification passed because
the dead code lived in `pipeline_run_actions.go`, which the scoped command
did include, BUT scoped `go build ./internal/handlers/server/pipeline/...`
caught the compile error at `c9c2ae4` push time. The actual gap was that
`pipeline_run_actions.go` had an unused `enqueue` import that only surfaced
when `go vet ./...` ran from a sibling package — and the
`creator_push_test.go` orphan reference only surfaced when `go test ./...`
linked the test binary against the production binary after both had been
recompiled together.

**The rule is therefore: scoped verification is acceptable for changes
that are guaranteed to be local to one package; full-module verification
is mandatory for changes that retire exported symbols or cross-package
helpers.** When in doubt, run the full-module gate.

### 3. Atomic commit shape

Each removal commit MUST be self-contained and revertable in isolation:

- docs commit (no code change): can be merged forward freely
- refactor commit: the symbol retirement + all immediate callers in the
  same package — verify with full-module gate
- cleanup commit (test deletion, metric label retirement): only after the
  refactor commit has landed AND full-module `go test ./...` has passed
  with the orphan reference removed
- fixup commit: only if the previous refactor or cleanup commit was
  pushed without the full-module gate; the fixup itself MUST be guarded
  by the full-module gate

A removal layer that produces **zero fixup commits** is the success
criterion. The current pivot produced one (`c322182`), which is the
evidence that the verification gate was not strong enough.

## (c) Conseguenze

### Benefits

- **Clearer audit trail**: future maintainers reading the CHANGELOG §Removal
  entry can locate this ADR by number (`0008-`) and discover the
  decision framework without re-deriving it from the 11-commit chain.
- **Faster removal paths**: subsequent legacy retirements (e.g. the
  next time a remote-engine intake is replaced) can skip the
  soft-deprecate layer entirely if C1 fails, saving the 6-commit
  scaffolding overhead.
- **Fewer fixup commits on `main`**: enforcing full-module `go test ./...`
  on every exported-symbol retirement prevents the post-push regression
  that `c322182` repaired.
- **Decision is reproducible**: the C1/C2 matrix makes the
  soft-deprecate-vs-remove choice a checklist, not a judgment call.

### Trade-offs

- **Full-module `go test ./...` is slower** than scoped verification
  (typically 60-120s on `DataServer` vs 10-20s scoped). The cost is
  acceptable because it replaces a manual fixup commit on `main` (which
  also blocks the next `main` consumer for the duration of the fix).
- **A removal layer still requires 3-5 atomic commits** (docs, refactor,
  cleanup, optionally a `c322182`-style fixup). The reduction vs the
  soft-deprecate layer is from 6+3=9 commits to 3-5 commits, a ~50%
  reduction in audit-trail noise.
- **C1 verification requires telemetry infrastructure**: the
  `accepted_from=remote_engine_legacy` counter that was added in
  `788a119` was what made the C1 check possible. Future intake paths
  MUST emit an analogous counter so the next removal can be decided on
  data, not judgment.

### Out-of-scope consequences

- The pre-`c322182` commits on `main` are NOT rewritten. The pivot
  narrative is the discoverable record of the soft-deprecate→remove
  arc; rewriting the git history would be more disruptive than the
  fixup commit itself.
- The `accepted_from=remote_engine_legacy` metric is RETIRED in
  `716426a`, not preserved as historical telemetry. The decision
  framework in §(b) is the durable record; the metric was scaffolding
  for the soft-deprecate layer and has no post-removal purpose.

## (d) Confini

### In scope

- `DataServer/internal/handlers/server/pipeline/*` — the canonical intake
  paths (creator_push and legacy remote-engine).
- The `creatorflow.Resolver.Resolve` contract that both paths share.
- Every future commit that retires an exported symbol or cross-package
  helper in any `DataServer` package.
- The verification gate (`go vet ./...` + `go build ./...` + `go test ./...`)
  for every such commit.

### Out of scope

- Other Go modules in the repo (`RemoteCodex/`, `shared/`,
  `DataServer/cmd/`, `RemoteCodex/native/`). These have their own
  intake paths and contracts; the C1/C2 matrix applies to them
  independently when they retire exported symbols.
- The `VeloxFrontend/`, `courserpierone/`, `Chronon3d/`, `PipelineGen/`,
  and `InstaeditLogin/` directories — frontend, TypeScript, C++, Python,
  and unrelated Go stacks. Each has its own deprecation policy.
- Schema or DDL changes — these are governed by `docs/2026-07-19-orchestrator-wrap-audit.md`
  and the migration runbook, not by this ADR.
- Runtime telemetry policy beyond the `accepted_from` counter family.
  Metrics catalog governance is owned by `docs/metrics-catalog.md`.

## Cross-references

- **Commits**:
  - `51a307d → 5d484c4` — soft-deprecate layer (6 commits)
  - `d433e97 → a557b8d` — full removal layer (5 commits)
  - `c322182` — the fixup commit that motivated the full-module gate

- **Authoritative docs**:
  - `docs/CREATOR-PUSH.md` — canonical creator-push intake contract
    (now with §Removal covering the legacy retirement)
  - `CHANGELOG.md [Unreleased]` — pivot narrative (`a557b8d`)
  - `docs/adr/2026-07-19-store-single-writer-contract.md` — sibling ADR
    on the single-writer tx contract; same author voice and same
    `(a)/(b)/(c)/(d)` structure

- **CI gates**:
  - `scripts/ci/pre-removal-verify.sh` — full-module verification gate
    (this ADR\'s §(b) point 2 enforced as an executable wrapper;
    runs `go vet ./...` + `go build ./...` + `go test ./...` from
    `DataServer/` before any removal commit is pushed).
  - `scripts/ci/run-split-regression.sh` — existing single-writer
    audit gate (cluster-scoped; pattern example for the
    pre-removal-verify.sh design).
  - `AGENTS.md` — operational note for AI/code agents, codifying
    the removal-commit verification gate + the C1/C2 soft-deprecate
    decision matrix.
  - `DataServer/internal/handlers/server/pipeline/creator_push_test.go` —
    post-removal test set, exercising only the canonical path.

- **Telemetry**:
  - `internal/metrics/creator_intake.go` — `CreatorIntakeSink` interface
    and the `accepted_from` counter family. The `remote_engine_legacy`
    label value was retired in `716426a`; the `creator_push` label value
    is the post-removal canonical.

## Change log

- **2026-07-25**: ADR accepted. Decision framework (C1/C2 matrix) and
  verification gate (full-module `go test ./...`) codified. The
  `c322182` fixup commit is the empirical evidence that the gate is
  necessary; this ADR is the durable record that future removal commits
  MUST apply it.