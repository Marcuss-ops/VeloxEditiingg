# Architecture stabilization pass — 2026-11-07

## Goal

Use the 2026-11-07 repository review to complete an evidence-backed,
bounded stabilization pass. Prioritize the recently changing Drive stock and
render-plan paths, preserve existing canonical contracts, fix only confirmed
defects, reconcile stale architecture documentation, and record verification
results. Do not treat every historical TODO in the repository as part of this
pass.

## Acceptance criteria

- [ ] The Go package graph is checked for cycles in every Go module in this
  repository; any cycle found is removed or recorded with concrete evidence.
- [ ] Recent Drive stock preparation and rendering changes are reviewed from
  their current call sites through their canonical contracts and completion
  points. Confirmed correctness, concurrency, resource-bound, or false-success
  defects are fixed with focused regression coverage; unconfirmed suspicions
  are not turned into speculative refactors.
- [ ] Cross-layer ownership for stock-window preparation, render-plan
  validation, and packet assembly is documented against current code. Duplicate
  business decisions or semantic drift found in the path are eliminated or
  explicitly assigned to the canonical owner.
- [ ] The architecture docs describe the current repository state and the
  November 2026 Drive stock-window flow, with obsolete status dates corrected.
- [ ] Any change is checked with the narrowest relevant existing checks, then
  the full Go module gates for modules changed. No removal symbol is retired;
  if that scope changes, `bash scripts/ci/pre-removal-verify.sh` is mandatory.
- [ ] This checklist records evidence, completed items, and any remaining
  criterion blocked by an external dependency or unavailable runtime access.
- [ ] Each repository modification is committed and pushed to `main` per
  `AGENTS.md`; pre-existing untracked user data is left untouched.

## Work log

- [x] Confirmed clean tracked worktree on `main` at `7dee41d6`; identified one
  pre-existing untracked Codex session file and excluded it from this work.
- [x] Inspected repository modules, recent commit history, largest production
  files, existing architecture ownership documentation, and the Drive stock
  preparation path. Initial evidence: `DataServer/internal/assets/
  stock_drive_segments.go` (576 LOC), `video_trimmer.go` (687 LOC), recent
  Drive stock changes from 2026-11-05 to 2026-11-06.
- [x] Ran `go list ./...` successfully in `DataServer/` and `shared/`; these
  module package graphs contain no Go import cycle.
- [ ] Run `go list ./...` in `RemoteCodex/native/worker-agent-go/` and record
  result.
- [ ] Trace the current Drive stock data flow and inspect failure, cancellation,
  cleanup, cache and concurrency paths; record any confirmed defect and fix it.
- [ ] Compare current contract ownership at Master, worker and native engine
  boundaries; record canonical decision owners and remove confirmed duplicated
  policy if found.
- [ ] Reconcile `CURRENT-TO-TARGET-ARCHITECTURE.md`, `current-architecture.md`
  and `OWNERSHIP.md` with evidence from current code.
- [ ] Run focused verification and full `go vet`, `go build`, and `go test`
  gates for every Go module changed; record results and classify pre-existing
  failures per `AGENTS.md`.
- [ ] Update this work log with final evidence and remaining external/runtime
  follow-ups, then commit and push the final atomic change(s).

## Evidence and follow-ups

To be completed as work proceeds. A checked item must include evidence below
or in the repository history; checklist status alone is not proof.
