# P0 — Completion must respect pending required deliveries

**Status:** Verified
**Area:** `DataServer/internal/completion`, `completionstore`, `artifactsstore`,
`jobs`
**Source:** Preliminary audit at `4d42269c`; see
`jobs/status.go`, `completion/coordinator_commit.go`,
`store/completionstore/completion_repository_finalize.go`, and
`artifactsstore/finalization_finalize.go`.

## Problem

The documented job state machine defines `DELIVERING` as a nonterminal state
while explicit delivery attempts are pending. The completion protocol can
promote a job to `SUCCEEDED` and insert delivery rows in the same transaction.
The artifact finalizer uses `DELIVERING` when destinations exist. Consequently,
two paths can give the same job-level `SUCCEEDED` status different meanings.

The transaction prevents readers from seeing an intermediate half-commit, but
it does not make the final status correct if required delivery rows remain
pending. The ticket must establish which delivery plans are required and how
terminal delivery failures affect the job before changing the roll-up rule.

## Required investigation

1. Trace all callers of `MarkCompletionJobSucceededIfTasksDone` and
   `InsertCompletionDeliveries`, including jobs with multiple tasks, no
   artifacts, render-only requests, disabled plans, and enabled destinations.
2. Confirm the uniqueness and lifecycle rules for `job_deliveries`, including
   statuses considered active, terminal, optional, or required.
3. Find every writer that moves a job into or out of `DELIVERING`, and the
   logic that completes the job after delivery success/failure/cancellation.
4. Check API, scheduler, calendar, and event consumers for observable meanings
   of `SUCCEEDED` and `completed_at`.
5. Decide and document the contract for all required deliveries succeeded,
   pending, permanently failed, cancelled, or absent. Do not infer failed
   delivery semantics solely from the pending-delivery rule.

## Implementation plan

1. Use `docs/architecture/OWNERSHIP.md` and `jobs/status.go` as the starting
   authority. Resolve any conflicting docs before changing a writer.
2. Ensure the completion transaction first materializes the delivery set, then
   derives the job state from the committed set and the canonical delivery
   policy. The externally visible transaction result must be consistent.
3. For a job with at least one required, nonterminal delivery, persist
   `DELIVERING` and do not set terminal `completed_at` as if the entire job
   were complete. For no required destinations, preserve the documented
   render-only/no-delivery completion behavior.
4. Reconcile the artifact-finalizer and completion-protocol paths through the
   existing job lifecycle/finalization owner. Avoid adding another direct SQL
   writer if the canonical owner can be extended.
5. Add an integration regression that commits a completion with a required
   destination and observes `DELIVERING` plus a pending delivery, then
   completes that delivery and observes the documented terminal state.
   Include no-destination and render-only cases, plus any failure/cancel cases
   required by the contract.

## Acceptance criteria

- Every job-level finalization path agrees on the meaning of `SUCCEEDED` and
  `DELIVERING`.
- A transaction that creates required pending deliveries does not expose the
  job as terminal `SUCCEEDED`.
- Jobs without required delivery work can still complete under the intended
  contract.
- Delivery success and terminal failure/cancellation behavior is explicitly
  documented and tested.
- `completed_at`, revisions, events/outbox messages, and idempotent retries
  remain consistent with the chosen state transition.
- The ownership map identifies one authority for the job success transition.

## Verification

Run focused completion integration tests, artifact finalizer tests, and job
state/lifecycle tests. Run the full DataServer pre-removal gate if a retired
exported or cross-package helper is part of the change. Record any pre-existing
failures separately as required by `AGENTS.md`.

## Implementation evidence

- Completion now materializes delivery rows before rolling up the job status;
  a job with pending deliveries remains `DELIVERING` and is not given a new
  terminal `completed_at` timestamp.
- Delivery fan-out only runs after all job tasks have succeeded. Callback and
  stale-delivery recovery now use the same `rollupDeliveringJob` transaction
  predicate to close the lifecycle after all rows become terminal.
- `jobs.FinalizationTargetStatus` is the shared decision for whether verified
  finalization targets `DELIVERING` or `SUCCEEDED`; artifact and task evidence
  remains guarded by each transaction owner.
- The acceptance regression asserts `DELIVERING` with a pending delivery and
  checks the plan retry budget reaches `job_deliveries.max_attempts`.
- Focused verification passed: `go test -count=1 ./internal/completion
  ./internal/deliveries ./internal/artifacts`.
- `bash scripts/ci/pre-removal-verify.sh` passed: full-module vet, build, and
  tests all returned zero.
- The lifecycle-writer inventory is recorded in
  [`2026-11-27-lifecycle-writer-map.md`](../architecture/2026-11-27-lifecycle-writer-map.md)
  and `OWNERSHIP.md`; transaction adapters remain separate to preserve atomic
  artifact/task/completion commits.
- Additional focused verification passed: `go test -count=1 -run
  'TestDeliveryPendingReconciler|TestApplyReconciledDelivery|TestMarkDeliverySucceeded|TestDeliveryChildren_' ./internal/store`,
  `go test -count=1 ./internal/completion`, `go test -count=1
  ./internal/artifacts ./internal/artifactsstore`, and
  `go test -count=1 ./internal/jobs`.
