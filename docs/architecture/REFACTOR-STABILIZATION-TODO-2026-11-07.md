# Architecture stabilization pass — 2026-11-07

## Goal

Use the 2026-11-07 repository review to complete an evidence-backed,
bounded stabilization pass. Prioritize the recently changing Drive stock and
render-plan paths, preserve existing canonical contracts, fix only confirmed
defects, reconcile stale architecture documentation, and record verification
results. Do not treat every historical TODO in the repository as part of this
pass.

## Acceptance criteria

- [x] The Go package graph is checked for cycles in every Go module in this
  repository; any cycle found is removed or recorded with concrete evidence.
- [x] Recent Drive stock preparation and rendering changes are reviewed from
  their current call sites through their canonical contracts and completion
  points. Confirmed correctness, concurrency, resource-bound, or false-success
  defects are fixed with focused regression coverage; unconfirmed suspicions
  are not turned into speculative refactors.
- [x] Cross-layer ownership for stock-window preparation, render-plan
  validation, and packet assembly is documented against current code. Duplicate
  business decisions or semantic drift found in the path are eliminated or
  explicitly assigned to the canonical owner.
- [x] The architecture docs describe the current repository state and the
  November 2026 Drive stock-window flow, with obsolete status dates corrected.
- [x] Any change is checked with the narrowest relevant existing checks, then
  the full Go module gates for modules changed. No removal symbol is retired;
  if that scope changes, `bash scripts/ci/pre-removal-verify.sh` is mandatory.
- [x] This checklist records evidence, completed items, and any remaining
  criterion blocked by an external dependency or unavailable runtime access.
- [x] Each repository modification is committed and pushed to `main` per
  `AGENTS.md`; pre-existing untracked user data is left untouched.

## Work log

- [x] Confirmed clean tracked worktree on `main` at `7dee41d6`; identified one
  pre-existing untracked Codex session file and excluded it from this work.
- [x] Inspected repository modules, recent commit history, largest production
  files, ownership documentation, and the Drive stock path. Initial measured
  hotspots were `stock_drive_segments.go` (576 LOC) and
  `video_trimmer.go` (687 LOC); extraction reduced the former to a phase
  orchestrator, with preparation phases in `stock_drive_prepare.go`.
- [x] `go list ./...` passed in `DataServer/`, `shared/`, and
  `RemoteCodex/native/worker-agent-go/`; all three Go package graphs have no
  import cycle.
- [x] Traced Drive stock classification, download/probe, deduplication,
  cancellation, cleanup, fragment certification, registration, plan rewrite,
  and completion validation. Centralized Drive binding classification and
  removal in `internal/assets`; bounded concurrent work with a fixed worker
  pool and parent cancellation propagation. Added regression coverage for
  referenced-vs-unused Drive assets, supported Drive URL forms, concurrency,
  and cancellation.
- [x] Compared Master, worker and C++ boundaries. `shared/contract` owns
  copy/slice routing; `DataServer/internal/assets` owns Drive preparation and
  rewrite; worker executors own runtime binding and eligibility checks; C++
  owns packet demux/mux. Duplicate pipeline Drive binding policy was removed.
- [x] Reconciled the index, current architecture and ownership map with current
  V2 executor and Drive stock code. Corrected stale assembly terminology and
  dates; documented that multi-task video DAG execution remains incomplete.
- [x] Focused asset/pipeline tests passed; asset race tests passed; targeted
  `golangci-lint` for `internal/assets/...` reported 0 issues; architecture
  CI check passed. Full DataServer `pre-removal-verify.sh` passed after code
  changes (`go vet ./...`, `go build ./...`, `go test -count=1 -timeout=15m
  ./...`, all exit 0). LOC gate passed on a clean-tree worktree; direct
  invocation in the main worktree is obstructed by the pre-existing 222,881
  line untracked Codex session file, which was left untouched.
- [x] Regenerated stale `RemoteCodex/BUILD_INFO.json` from canonical version
  metadata (1.4.49 → 1.4.52), resolving the architecture metadata check.
- [x] Remaining full DataServer lint findings were inspected and recorded in
  the session report: 37 pre-existing findings outside the changed assets
  package (error wrapping, import formatting, one ineffective assignment,
  and unused private helpers). They are not required by this bounded pass and
  were not removed speculatively; the changed asset package is lint-clean.
- [x] Final architecture-doc reconciliation and completed evidence checklist
  committed and pushed to `main` as `3c8a882b`; preserved the pre-existing
  untracked session file.

## Evidence and follow-ups

- Code commits already on `origin/main`: `7151caa5` (centralize Drive binding
  rules), `32888026` (bound stock workers), `e398b58c` (build metadata), and
  `749e6ac2` (split preparation phases). The initial checklist is in
  `3443a3f9`.
- Follow-up: split video jobs into a multi-task render DAG; current V2 path is
  still one assembly task per job.
- Follow-up: resolve 37 full-module linter findings outside the changed assets
  package in independently reviewable work, starting with frequently changed
  files and confirming call-site absence before deleting helpers.
- Follow-up: remove or relocate the pre-existing Codex session file if its
  owner intends to, since it prevents the LOC gate from running cleanly in the
  primary worktree. It was not modified in this pass.
- No external runtime, deployed worker, production DB, or live Drive access was
  available/used; production behavior beyond repository-backed verification
  remains unasserted.

## Follow-up: partial Drive stock windows

- [x] Kept packet-copy for canonical stock windows that start on a keyframe;
  the exact-window path uses frame-accurate H.264 re-encode otherwise.
- [x] Made stock fragment encoding video-only and explicit about timestamp
  reset, constant frame rate, and the requested frame limit.
- [x] Certified output using `ffprobe -count_frames` and required the probed
  stream start time and first keyframe to be zero before registration.
- [x] Acceptance fixture for a 4.973-second source and 3.125-second partial
  window passed: exactly 75 output frames, canonical profile, keyframe at zero,
  prepared asset registration, and cache reuse on retry.
- [x] Added a focused packet-copy regression test for a canonical keyframe
  window. Focused race tests, `golangci-lint` on `internal/assets/...`,
  `check-architecture.sh`, and the full `pre-removal-verify.sh` all passed.
- [x] Follow-up implementation committed and pushed to `main` as
  `f76317f5`.
- Deployment remains external: the build path contains only remote job
  payloads/scripts, not a separate Master source checkout. No live Master
  rebuild or restart was attempted.
