# P1 — Unify completion delivery creation policy

**Status:** Verified
**Area:** `DataServer/internal/store/completionstore`,
`DataServer/internal/artifactsstore`
**Source:** Preliminary audit at `4d42269c`; completion writer and
`insertPendingDelivery` use different insert policies.

## Problem

The completion-protocol writer inserts `job_deliveries` with
`INSERT OR IGNORE`, does not explicitly pass `max_attempts`, and relies on
the schema default for retry behavior. The artifact finalizer supplies a retry
budget and narrows conflict handling to `(artifact_id, publication_id,
destination_id)`. The two paths therefore encode idempotency and retry policy
differently. SQLite `OR IGNORE` can suppress constraint failures beyond the
intended duplicate-key case, hiding malformed rows or data errors.

## Required investigation

1. Inspect the `job_deliveries` schema, defaults, indexes, migrations, and
   uniqueness constraints. Confirm the authoritative idempotency key.
2. Trace how `job_delivery_plans.retry_budget` is validated, normalized, and
   represented in the artifact finalizer's destination projection.
3. Determine whether both writers may create the same logical delivery and
   what happens on replay or partially migrated rows.
4. Establish intended behavior for duplicate rows, invalid retry budgets,
   unrelated constraint violations, and existing delivery rows with a
   different retry value.
5. Check package ownership and import direction before selecting a shared
   implementation point. Do not create a new package just to share one SQL
   statement.

## Implementation plan

1. Write down one delivery identity and retry-budget policy, including legacy
   row/default behavior.
2. Route both writers through the existing persistence owner or a small
   owner-local function with the same inputs. If package boundaries prevent
   reuse, keep separate adapters but make them implement the same explicit
   contract and pin the contract with tests.
3. Use a conflict target matching the logical unique key rather than broad
   ignore semantics. Let unrelated SQL constraint errors propagate.
4. Pass the validated retry budget explicitly in every newly created row.
   Preserve configured zero/retry semantics and historical migration defaults
   exactly as the current contract requires.
5. Test first insertion, identical replay, conflict on the intended key,
   unrelated constraint failure, and retry-budget propagation from plan to
   claimed delivery.

## Acceptance criteria

- Both creation paths produce the same `max_attempts` for equivalent plans.
- Idempotency uses one documented logical key and one matching database
  uniqueness rule.
- Only the intended duplicate conflict is ignored; other constraint failures
  are visible to the transaction caller.
- Replay does not create duplicate logical deliveries or reset attempts/state
  unexpectedly.
- Schema defaults remain compatible with pre-existing rows and migrations.
- No new abstraction/package is introduced unless existing package ownership
  makes the smallest shared implementation impossible.

## Verification

Run targeted completionstore/artifactsstore tests and delivery retry-budget
tests. Validate against an in-memory or temporary SQLite database using the
real migrations. Run the pre-removal gate only if this change removes an
exported symbol or cross-package helper.

## Implementation evidence

- Completion delivery insertion now passes `job_delivery_plans.retry_budget`
  explicitly, targets only the logical delivery uniqueness conflict, and lets
  unrelated constraint errors abort the transaction.
- Fan-out waits for all task rows to reach `SUCCEEDED`, so an early task commit
  cannot create deliveries before the job-level roll-up is eligible.
- The completion acceptance test pins the propagated retry budget. Focused
  completion and artifact tests passed. The full gate also passed: vet, build,
  and full-module tests all returned zero.
