# Lifecycle state writer map — 2026-11-27

This is the concise ownership map produced for the architecture audit. It
names the business owner and the transaction adapters; SQL adapters are not
independent authorities when they enforce the same domain predicate under
different atomic transaction boundaries.

| State | Business owner / entry path | Persistence and guards |
| --- | --- | --- |
| Job | `internal/jobs` (`LifecycleService` for ordinary transitions); verified completion is initiated by artifact finalization or `completion.Coordinator` | `jobs_repository_*` CAS for ordinary transitions; `artifactsstore.SQLiteArtifactFinalizer` and `completionstore` apply their artifact/task gates atomically. `jobs.FinalizationTargetStatus` decides `DELIVERING` vs `SUCCEEDED`. |
| Job while delivering | `deliveries.DeliveryRunner` completion callbacks; `DeliveryPendingReconciler` for exhausted/stale delivery recovery | `store.rollupDeliveringJob` performs the guarded `DELIVERING` → `SUCCEEDED`/`FAILED` update only when no delivery is nonterminal; callback and reconciler share the same predicate. |
| Task | `taskgraph.LifecycleService` for lease, start, retry, fail, and readiness transitions | SQLite task repository CAS and readiness checks; artifact finalization and completion use transaction-bound adapters to commit verified task results; `stalereconcile` handles expired leases with revision/lease guards. |
| Attempt | `taskattempts.Repository` contract for attempt reports and lifecycle; completion coordinator commits task/attempt evidence | `store/attempt_lifecycle.go`, `completionstore`, and stale recovery update attempts within their own guarded transactions. |
| Artifact and upload | `artifacts.Service` owns admission/finalization; upload handlers use the artifact upload repository | `artifactsstore` owns artifact, upload, probe, and reconciliation persistence. Verified artifact/job/task transitions are committed atomically; upload session transitions use status/identity guards. |
| Publication | `deliveries.DeliveryRunner` and provider-specific reconciliation contract | `publicationstate` is the status contract; the delivery store persists submission/final IDs and reconciliation evidence. `validatePublishedEvidence` is shared by both recovery acceptance paths. |
| Delivery | `deliveries.DeliveryRunner` owns provider execution and retry decisions | `deliverystore` persists delivery CAS transitions; reconciliation may mark exhausted rows terminal, then calls the shared parent-job roll-up. |

## Transaction and event boundaries

- The completion protocol writes task/attempt evidence, delivery fan-out, job
  target status, and completion outbox records within its transaction.
- Artifact finalization keeps artifact verification, task/attempt promotion,
  and the job target transition in its finalization transaction.
- Delivery state changes and the parent job roll-up share the delivery
  transaction. Stale-delivery reconciliation records its reconciliation
  reason/version in the same roll-up update.
- Domain services own the transition rules. Persistence adapters implement
  compare-and-swap and transaction boundaries; they are not permission for
  handlers to issue direct lifecycle SQL.

## Repeatable source inventory

Review the production writer surface with:

```bash
rg -n -i '(UPDATE|INSERT[[:space:]]+INTO)[[:space:]]+(jobs|tasks|task_attempts|artifacts|artifact_uploads|publication_states|job_deliveries)\b|SET[[:space:]]+status' DataServer/internal --glob '*.go'
```

The search is a review aid, not an automatic completeness proof: it also
matches tests and unrelated status tables, and dynamic SQL may not contain a
literal table name. New lifecycle writes must update this map and the owning
domain contract. The SQLite transaction adapters above are the known
cross-entity exceptions to a single-package repository owner; no additional
independent business decision was identified in this review.
