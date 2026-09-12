# Muda cleanup checklist

This checklist tracks the repository cleanup from the September 2026 survey.
Historical changelog and audit records are retained; only live entrypoints,
tracked cruft, and redundant execution paths are changed.

- [x] Create this precise checklist and use it as the completion record.

## Dead weight

- [x] Remove the unused `scripts/verify_attempt_milestones_check.py` and
  `scripts/verify_attempt_milestones_e2e.sh` scripts.
- [x] Remove the vestigial `cmd/archcheck/scan/` module.
- [x] Remove the orphan `internal/application/images/` module.
- [x] Remove the two orphan module entries from `go.work`.
- [x] Remove the tracked `Instaedit/README.md` and `Instaedit/.gitignore`
  remnants while retaining the root ignore rule.
- [x] Update live documentation/comments that enumerate the workspace module
  set; retain historical references as historical records.

## CI compute

- [x] Remove the duplicate advisory `workspace-tests.yml` workflow.
- [x] Remove its stale live references from workflow comments and operator
  tooling.
- [x] Remove the standalone CI invocations duplicated by `make verify` for
  completion invariants, DSN busy-timeout, and AC/TaskResult convergence.
- [x] Remove the duplicate native dependency installation after the apt cache
  action, preserving one installation path.
- [x] Remove the daily LOC cron.
- [x] Remove the weekly drift crons from the three static-text guard workflows.
- [x] Move the six expensive guard self-tests out of every `make verify` CI
  run into a path-filtered workflow; keep them in local verification.

## Local-only cruft

- [x] Delete the six stale native CMake build trees and `Testing/` artifacts.
- [x] Delete root `.pytest_cache/` and Python cache artifacts.
- [x] Add an explicit root `.pytest_cache/` ignore rule.

## Slow tests

- [ ] Remove the artificial 1.5-second native package-test delay and preserve
  useful sub-second timing in the regression report.
- [ ] Replace fixed process-counter sleeps with condition-driven polling while
  preserving the integration assertions and bounded timeouts.

## Acceptance

- [ ] No live reference remains to a removed script/module/workflow.
- [ ] `gofmt`, YAML/shell syntax, repository guards, and relevant tests pass.
- [ ] Every removal commit passes `bash scripts/ci/pre-removal-verify.sh`.
- [ ] Each modification is committed atomically on `main` and pushed.
