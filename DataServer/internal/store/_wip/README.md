# _wip/ — scratch from the file-1/4 migration triage

This directory contains a `*_test.go` file that was once untracked
working-tree scratch. It was relocated here (from `../`) after a
triage against the migration's `go vet ./...` failure budget.

## Why this file is here

Test file: `sqlite_task_repository_executor_normalization_test.go`.

It references these symbols, of which:

| Symbol | Status on the file-1/4 branch |
|---|---|
| `seedClaimableTask`, `seedTaskSpec` | NOT DEFINED anywhere on this branch |
| `seedReadyTaskWithExecutor`, `openTaskAtomicTestDB` | defined in sibling `sqlite_task_atomic_test.go` (signature drift) |
| `seedCandidateTask`, `openCandidatesTestDB` | defined in sibling `sqlite_task_repository_list_ready_candidates_test.go` (signature drift) |
| `ClaimTaskForWorkerAtomic` (method) | defined at `sqlite_task_repository.go` — exists on the right receiver |

Classification: **ahead-of-itself**. The test was authored on a feature
branch whose scaffolding partially landed and partially didn't.
`go vet ./internal/store/...` exits non-zero when this file is in the
package directory.

## Disposition choice

User picked option C ("Move to a WIP branch / stash"). The execution
took the filesystem-level interpretation (rename + directory move): a
Git-branch-grade isolation is not what was applied. If you want the
literal git-branch interpretation, see "How to convert to a real
git branch" below.

Mechanism: this directory is named `_wip` per Go's convention that
directories prefixed with `_` or `.` are excluded from `go build` /
`go test` / `go vet`. The file stays on disk for future workers to
recover without breaking CI. It is *not* tracked by git.

### Toolchain behavior note (read this before adding more files here)

Any `.go` file dropped into this directory is **silently skipped** by
`go build` / `go test` / `go vet`. This is by design (the underscore
prefix is a toolchain convention), but it is invisible. If you add
production-critical code here, it will not be exercised by CI. Use
this directory strictly for scratch / WIP.

### How to convert to a real git branch

If you decide you want branch-level isolation (recommend before
sharing this state), the steps are:

```sh
git checkout -b wip/executor-normalization-tests   # create branch
git add DataServer/internal/store/_wip/            # stage the file
git commit -m "wip: stash executor-normalization test (ahead-of-itself)"
git checkout main                                  # back to clean main
# now main has no .go file living in _wip/, and the
# unblocker came from git removing it from main, not from
# Go's underscore convention.
```

## How to recover

To bring the test back into the build:

1. `git branch wip/executor-normalization-tests` (preserve as a
   branch instead of a directory move if you want the file to be
   on a branch where it can actually run).
2. Or land the missing helpers + normalize signatures on `main`,
   then `mv _wip/sqlite_task_repository_executor_normalization_test.go ../`
   (path: `../` is `internal/store/`).

## How to delete

If the work is not worth preserving:

```sh
rm -rf DataServer/internal/store/_wip
```

Both the directory AND the test file are untracked, so this is a
pure filesystem-level cleanup with no git history impact. Note:
`git clean -fd` will NOT delete `_wip/` (git clean honors
leading-underscore directory exclusions) — use the explicit `rm -rf`
above instead.

## Reviewer verdict

Code-review flagged two material items at the time of seeding:
the commit-hash citation rotted easily (replaced with a
file-path-based citation, which is a more durable fact), and the
user-picked label "branch" was executed as a filesystem move
(documented transparently above). The triage verdict (ahead-of-itself)
and the README's symbol-mapping table are accurate.
