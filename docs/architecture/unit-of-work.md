# Completion transaction boundary

> **Status:** current architecture.
> **Scope:** `DataServer/internal/completion` orchestration and
> `DataServer/internal/store/completion_repository.go` persistence.

The Completion Coordinator owns domain policy and orchestration. The
`internal/store` package owns SQL, transaction lifecycle, row projections and
transaction-bound repository methods. This is the canonical path for completion
writes; handlers and application services must not open completion transactions
or execute SQL directly.

## Interfaces

`DataServer/internal/store/completion_repository.go` exposes two typed surfaces:

```go
type CompletionStore interface {
    Run(context.Context, func(CompletionTx) error) error
    // typed non-transactional binding/scanning methods
}

type CompletionTx interface {
    // typed fence reads, declarations, upload transitions,
    // completion CAS operations, delivery and outbox writes,
    // and pre-commit result reads
}
```

`CompletionStore.Run` opens a serializable transaction, invokes the callback,
rolls back on callback failure, and commits only after the callback succeeds.
The callback receives `CompletionTx`, never `*sql.Tx`. The concrete SQLite
implementation is `store.NewSQLiteCompletionStore(*sql.DB)`.

The Coordinator is constructed with the store:

```go
completionStore := store.NewSQLiteCompletionStore(db)
coord, err := completion.NewCoordinator(completion.CoordinatorConfig{
    Store:   completionStore,
    HMACKey: key,
})
```

This keeps the application/store dependency one-way and leaves room for a
future store adapter without exposing SQL to the Coordinator.

## Transaction and invariant contract

Every completion mutation follows:

```text
validate command and fence
→ CompletionStore.Run
→ read fence and current state
→ typed CAS mutations on CompletionTx
→ read CommitResult before commit when a result is returned
→ commit
```

The store methods preserve these invariants:

- worker, lease and task revision are checked before fence-gated writes;
- terminal upload/artifact transitions are idempotent;
- `uploaded_bytes` is monotonic (`MAX`), so stale progress cannot regress it;
- artifact verification and upload completion are advanced only from allowed
  predecessor states;
- task-attempt, task and job success transitions remain in one transaction;
- delivery fan-out uses idempotent inserts;
- completion outbox events use idempotent event IDs;
- reconciliation scans are read-only until the Coordinator performs the
  corresponding typed transition.

A zero-row CAS is not silently interpreted as success where the application
contract requires a live transition. Store sentinel errors are mapped by the
Coordinator to domain errors such as `ErrTransitionConflict` and
`ErrAttemptCommitNotFound`.

## Ownership matrix

| Surface | Owner |
| --- | --- |
| `attempt_commits` | `CompletionTx` completion/fence/progress methods |
| `task_output_declarations` | `CompletionTx` declaration/progress methods |
| `artifact_uploads` and `artifacts` | `CompletionTx` upload verification methods |
| `task_attempts` | `CompletionTx.MarkCompletionTaskAttemptSucceeded` |
| `tasks` | `CompletionTx.MarkCompletionTaskSucceeded` |
| `jobs` | `CompletionTx.MarkCompletionJobSucceededIfTasksDone` |
| `job_deliveries` | `CompletionTx.InsertCompletionDeliveries` |
| `outbox_events` | `CompletionTx.InsertCompletionOutbox` |
| reconciliation candidate reads | `CompletionStore.ScanCompletionCandidates` |
| upload binding and token-hash reads | `CompletionStore` typed methods |

The ownership matrix does not authorize a second business writer. Artifact
finalization has its own documented lifecycle; completion may only perform the
transitions assigned to the completion protocol.

## Extending the boundary

When adding a completion persistence operation:

1. add the smallest typed method to `CompletionTx` or `CompletionStore`;
2. implement SQL only in `internal/store/completion_repository.go` (or a
   responsibility-specific file in `internal/store`);
3. call the method from Coordinator orchestration;
4. add a store/coordinator test covering success, stale fence, replay and
   rollback behavior;
5. run the SQL ratchet and the completion/store test suites.

Do not add `database/sql`, `*sql.Tx`, `ExecContext`, `QueryContext` or raw DML
to `internal/completion`. The SQL ratchet and package boundaries enforce this
rule.

## Verification

The migration is guarded by:

```bash
bash scripts/ci/ratchet-sql.sh
cd DataServer && go test ./internal/completion ./internal/store
```

Because the old completion UoW files and exported factory were removed, a
removal change must also pass:

```bash
bash scripts/ci/pre-removal-verify.sh
```
