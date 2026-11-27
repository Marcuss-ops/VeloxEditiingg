# P0 — Canonical publication success validation

**Status:** Verified
**Area:** `DataServer/internal/deliveries`
**Source:** Preliminary audit at `4d42269c`; compare `runner_process.go` and
`runner_phases.go`.

## Problem

The delivery preflight in `processLease` can observe a durable publication in
`PUBLISHED` and mark the delivery successful. The phase runner's already-
published path also checks that the final remote media ID is present and
distinct from the submission ID, then asks the store to verify reconciliation
evidence. These acceptance paths do not enforce visibly identical conditions.

The risk is accepting a persisted `PUBLISHED` row without the evidence that
the asynchronous reconciliation contract requires. This ticket does not
assume such a bad row currently exists. First establish which providers can
create publication state and which providers are allowed to use the shortcut.

## Required investigation

1. Trace every writer and transition into `publicationstate.Published`,
   including provider adapters, reconciliation, migrations, and recovery.
2. Trace every reader that turns `PUBLISHED` into delivery/job success.
3. Record the provider contract differences: synchronous providers may have
   no submission/reconciliation phase, while resumable providers require it.
4. Inspect `ValidatePublishedAfterReconciliation` and the persisted evidence
   it checks. Confirm whether it binds the publication, provider, destination,
   submission ID, final ID, and job/artifact identity.
5. Identify existing tests for missing final ID, equal submission/final IDs,
   missing reconciliation evidence, and valid already-published recovery.

## Implementation plan

1. Define the canonical success predicate at the existing delivery/publication
   ownership boundary. Keep provider-specific requirements explicit in the
   provider contract; do not force a synchronous provider to fabricate
   asynchronous evidence.
2. Make both the preflight shortcut and phase-runner recovery call that same
   predicate or the same small existing owner-level method.
3. Preserve fail-closed behavior: invalid durable evidence must not be turned
   into success. Return/classify the failure consistently and preserve lease
   and delivery persistence error reporting.
4. Avoid introducing a generic validation framework or another package for a
   single predicate. Keep any shared helper close to its current owner.
5. Add focused regression cases for valid recovered publication and each
   invalid-evidence condition. Include a legacy synchronous provider case if
   the investigation confirms that contract remains supported.

## Acceptance criteria

- Every path that marks a delivery successful from stored `PUBLISHED` state is
  enumerated and uses the canonical contract.
- Resumable provider success requires a distinct final media ID and verified
  reconciliation evidence bound to the expected publication.
- Synchronous provider behavior remains valid only under its documented
  contract; it cannot bypass resumable-provider checks by sharing a status
  string.
- Invalid evidence cannot mark the delivery `SUCCEEDED`; error classification
  and retry/permanent-failure behavior are covered.
- Regression tests cover both paths and valid idempotent replay.
- Any exported/cross-package helper removal follows `AGENTS.md`; run the full
  pre-removal gate before pushing if that scope is reached.

## Verification

Run focused `DataServer/internal/deliveries/...` tests and the relevant
publication store tests. If production code or an exported helper is removed,
run `bash scripts/ci/pre-removal-verify.sh`. Record exact commands and results
in the implementation commit.

## Implementation evidence

- `DeliveryRunner.validatePublishedEvidence` is now the shared predicate used
  by the preflight shortcut and phase recovery. It checks distinct submission
  and final IDs and exact reconciliation evidence.
- The preflight shortcut is limited to registered resumable providers. A
  synchronous provider cannot turn a raw `PUBLISHED` row into delivery success.
- Regression coverage rejects a `PUBLISHED` row without a resumable reconciler
  and confirms the delivery is not marked successful.
- Focused verification passed: `go test -count=1 ./internal/deliveries`.
- `bash scripts/ci/pre-removal-verify.sh` passed: full-module vet, build, and
  tests all returned zero.
