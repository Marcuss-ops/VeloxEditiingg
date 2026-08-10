# Pre-removal verification — 2026-08-10

## Scope

This record documents the full-module gate requested for `main`:

```bash
bash scripts/ci/pre-removal-verify.sh
```

The gate runs from the `DataServer/` Go module:

1. `go vet ./...`
2. `go build ./...`
3. `go test -count=1 -timeout=15m ./...`

The verification was evaluated at:

- `HEAD`: `6ab7c6f8efddc7a98fb47dfce985a43473cc2747`
  (`fix(alerts): propagate evaluation failures`)
- `origin/main`: same commit
- immediate parent baseline: `efa76929e4dc695ec3be8ddc3987789a382d0ce8`
  (`test(store): import errors for async publication assertions`)
- pipeline/projection extraction under review: `c15c576f`
  (`refactor(pipeline): extract raw payload envelope`)

## Clean worktree result

The gate was re-run in a temporary clean worktree detached at `HEAD`. The
temporary worktree was removed successfully after the run.

| Check | Result |
|---|---|
| `go vet ./...` | PASS (`0`) |
| `go build ./...` | PASS (`0`) |
| `go test -count=1 -timeout=15m ./...` | FAIL (`1`) |
| Gate exit | `2` |

The clean report was captured at `/tmp/velox-pre-removal-verify-clean.txt`
(the clean rerun completed at approximately `2026-08-10 12:46 UTC`). The
report is an ephemeral local artifact; the classification and decisive
failure excerpts are reproduced below so this commit remains auditable after
`/tmp` is reclaimed. The gate script reuses `/tmp/velox-pre-removal-verify.txt`;
the original dirty-worktree report was overwritten by the later clean rerun
and is not treated as a preserved artifact.

### Failure A — integration test

```text
FAIL velox-server/internal/integration_test
--- FAIL: TestIntegration_DeliveryRunnerForwardsOpaqueDestinationID
    opaque_destination_runner_test.go:150:
    timed out waiting for Social API request: context deadline exceeded
```

The run also logged:

```text
deliveries: permanent error: provider "social_gateway"
implements DeliveryReconciler but has no phase executor
```

### Failure B — store test

```text
FAIL velox-server/internal/store
--- FAIL: TestAcceptTaskAtomic_HappyPath
    sqlite_task_atomic_accept_test.go:119:
    canonical runtime reader: get worker task runtime: no such table: workers
```

## Classification

### Pre-existing failures — not introduced by this change

Both failures reproduce in a separate clean worktree at the immediate parent
`efa76929`, with the same test names and failure messages:

- `TestIntegration_DeliveryRunnerForwardsOpaqueDestinationID`
- `TestAcceptTaskAtomic_HappyPath`

Therefore they are classified as **pre-existing relative to `HEAD`** and are
not attributable to `c15c576f` or the later `opsalerts` commits. The pipeline
package itself passed in the clean gate, including its extracted
`pipeline/projection` package.

### Dirty-worktree artifact — not a committed regression

The first gate invocation was run while the worktree contained unrelated,
uncommitted telemetry edits. Those edits changed
`DataServer/internal/store/sqlite_tx_manager.go` to call
`SQLiteStore.observeDBTransaction`, while the corresponding untracked
`DataServer/internal/store/telemetry.go` did not define that method. The
follow-up inspection of that dirty state produced compiler errors such as
(the original dirty report was overwritten, so this is a worktree diagnosis,
not a preserved report excerpt):

```text
internal/store/sqlite_tx_manager.go:115:11:
m.store.observeDBTransaction undefined (type *SQLiteStore has no field or method observeDBTransaction)
```

This explains the transient `internal/fleet/opsalerts [build failed]` and
related package build failures seen in the dirty-worktree report. A clean
worktree run has `go vet` and `go build` green, so this is classified as a
**worktree-only artifact**, not a failure in committed `main`.

The unrelated telemetry files and all other pre-existing worktree changes were
left untouched and are intentionally excluded from this documentation commit.

## Decision

- No production fix is included here: the two clean-gate test failures are
  pre-existing and require separate follow-up investigations.
- The pre-removal gate is **not green** (`exit 2`) because the full test phase
  fails.
- `go vet` and `go build` are green on clean `HEAD`.
- The evidence is recorded on `main` by this commit and its push.

## Follow-up references

- Gate implementation: `scripts/ci/pre-removal-verify.sh`
- Operational gate policy: `AGENTS.md`, section 1
- Durable pre-existing-failure tracking: `AGENTS.md`, section 4
