# Unit of Work — `DataServer/internal/completion`

> **Status**: design accepted (Verdetto P1 #8 / #9, Blocco 3).
> **Audience**: contributors touching `internal/completion`, future
> Postgres adapter authors, the `metrics.Supervisor` integration
> owner, anyone evaluating a competing transactional-coupling.
> **Companion doc**: [`docs/completion-protocol.md`](../completion-protocol.md)
> — the protocol contract that this UoW seam exists to serve.

This chapter documents the **Unit of Work (UoW)** pattern as wired
inside the Completion Coordinator. After reading it you should be
able to: (1) explain *why* the pattern exists; (2) point at every
SQL surface the Coordinator owns; (3) reproduce the canonical
`BEGIN → tx → repo → Commit/Rollback` lifecycle from the source;
(4) avoid introducing a second writer for any of the nine tables
this chapter lists.

---

## 1. Motivazione — il bug tx-after-commit (Verdetto P1 #9, Blocco 3)

Before the UoW extraction the Coordinator read its `CommitResult`
snapshot **after** calling `tx.Commit()` on a closed `*sql.Tx`:

```go
// BEFORE (broken):
if err := tx.Commit(); err != nil { … }
committed = true
res := repos.GetCommitResultFromClosedTx()  // tx is gone; reads
                                            // hit the just-released
                                            // write lock under a
                                            // concurrent writer →
                                            // SUBSEQUENT regeneration
                                            // of the CommitResult
                                            // contract returns drift.
```

The drift surfaced as follows: between `tx.Commit()` returning and
the Coordinator re-opening a fresh `*sql.Tx` to read the snapshot,
a concurrent writer (e.g. the supervisor's reconcile candidate scan,
or another worker reporting through a different Coordinator method)
could amend the just-committed row. The Coordinator therefore
returned a `CommitResult` whose fields no longer matched the
transactional state it had just written.

The UoW extraction moves the snapshot read **inside** the same
`*sql.Tx` so the read is part of the same LevelSerializable write
lock. The Coordinator also stops opening its own transactions:
each Coordinator method now opens *one* `LevelSerializable` tx,
gets the `UnitOfWork` bundle via `factory.WithTx(tx)`, drives every
CAS through the typed repos, snapshots the result via
`AttemptCommitRepository.GetCommitResult(ctx, commitID)` *before*
`tx.Commit()`, then commits and returns.

The fix is captured by `coordinator_test.go::TestCoordinator_CommitAttempt_TxAfterCommitFix`
and complements the ConflictBudget instrumentation
(`conflict_budget.go::Record` doc-comment, Blocco 5).

A secondary motivation is **typing**: pre-UoW the Coordinator
talked SQL directly via `tx.ExecContext` + raw parameter binding;
post-UoW it speaks typed Go parameters (`commitID string`,
`fence SQLFencer`, `nowStr string`) and the `*sql.Tx` is private to
each adapter. A new driver (Postgres, sqlite-via-Driver) implements
the same six interfaces and is wired through `NewCoordinator`'s
factory seam without any change to Coordinator methods.

---

## 2. Interfacce

The UoW surface lives in
[`DataServer/internal/completion/unitofwork.go`][unit-of-work-source].
Two interfaces — both unexported for the `SQLFencer` adapter that
keeps the `FenceTuple` SQL projection out of method signatures:

```go
// UnitOfWork bundles the six repositories sharing a single *sql.Tx.
// Returned by UnitOfWorkFactory.WithTx; the tx is held internally by
// each repo's adapter.
type UnitOfWork interface {
    AttemptCommits() AttemptCommitRepository
    TaskAttempts()  TaskAttemptRepository
    Tasks()         TaskRepository
    Jobs()          JobFinalizationRepository
    Deliveries()    DeliveryRepository
    Outbox()        OutboxRepository
}

// UnitOfWorkFactory produces a UnitOfWork bound to a *sql.Tx. The
// factory holds the *sql.DB (or *SQLiteStore) needed by repos; the
// caller (Coordinator) supplies the tx on a per-method basis.
//
// The factory is the single seam between the completion package and
// the underlying DB driver. A future Postgres adapter implements
// the same interface and is wired in via the same NewCoordinator
// path.
type UnitOfWorkFactory interface {
    WithTx(tx *sql.Tx) UnitOfWork
}
```

[unit-of-work-source]: ../../DataServer/internal/completion/unitofwork.go

Each repository interface owns a typed method set scoped to one
domain decision (no cross-table methods on a single repo):

| Repo interface | Owns | File |
| --- | --- | --- |
| `AttemptCommitRepository` | `attempt_commits` + `artifact_uploads` + `artifacts` (one atomic decision over all three tables — the Verdetto §2.5 atomic-final-tx). Ten methods: `Find`, `GetArtifactUploadState`, `CompleteArtifactUpload`, `GetCommitResult`, `UpdateProgress`, `UpdateReadyCountExhaustive`, `SetExpired`, `SetExpiredByID`, `MarkCommitted`. | `unitofwork.go` |
| `TaskAttemptRepository` | `task_attempts.MarkSucceeded` only (canonical fence-gated). | `unitofwork.go` |
| `TaskRepository` | `tasks.MarkSucceeded` + winning-attempt-id stamping + revision increment. | `unitofwork.go` |
| `JobFinalizationRepository` | `jobs.MarkSucceededIfTasksDone` conditional CAS — only flips when every sibling task is SUCCEEDED. | `unitofwork.go` |
| `DeliveryRepository` | `(artifact × destination)` cross-join + idempotent `INSERT OR IGNORE` keyed on `idempotency_key`. | `unitofwork.go` |
| `OutboxRepository` | idempotent `INSERT OR IGNORE` keyed on `event_id` for `commit_protocol.committed` / `.expired` events. | `unitofwork.go` |

The `SQLFencer` adapter in `unitofwork.go` is the small piece that
lets the Coordinator pass a `FenceTuple` straight through to a
repo without ever touching `*sql.Tx` from outside the adapter
internals. A `FenceTuple` satisfies `SQLFencer` by exposing
`SQLWhere() string` + `SQLArgs() []any` — the repo embeds these in
its prepared statement inline.

---

## 3. La factory concreta — `NewSQLiteUnitOfWorkFactory`

The canonical SQLite-backed factory is
`NewSQLiteUnitOfWorkFactory(*sql.DB) UnitOfWorkFactory` in
[`DataServer/internal/completion/sqlite_uow.go`][sqlite-uow-source].

[sqlite-uow-source]: ../../DataServer/internal/completion/sqlite_uow.go

Construction:

```go
type sqliteUnitOfWorkFactory struct{ db *sql.DB }

func NewSQLiteUnitOfWorkFactory(db *sql.DB) UnitOfWorkFactory {
    return &sqliteUnitOfWorkFactory{db: db}
}

func (f *sqliteUnitOfWorkFactory) WithTx(tx *sql.Tx) UnitOfWork {
    return &sqliteUnitOfWork{tx: tx, db: f.db}
}
```

`NewCoordinator` (`coordinator.go::NewCoordinator`) wires this
factory automatically for the production caller. The constructor
exposes the function as a top-level entry point so callers that
want to opt out of the implicit factory creation (mostly tests with
a custom `UnitOfWorkFactory` mock) can inject their own seam.

A compile-time guard lives next to the constructor:

```go
var _ UnitOfWork = (*sqliteUnitOfWork)(nil)
```

so a driver-mismatch regression breaks the build instead of
silently routing through a wrong adapter at runtime.

### Why `db` AND `tx`?

`WithTx(tx)` returns a bundle that holds the `*sql.Tx` privately.
The `db` field on `sqliteUnitOfWork` is reserved for **post-commit
read** paths that intentionally bypass the tx — e.g. the
`recover_output` CLI re-check verification reads that must NOT see
uncommitted writes (the tx aborted or rolled back). Production
Coordinator code DOES NOT use the `db` field; the path that does is
explicitly commented as "post-commit, post-tx" at the only call site
that uses it.

---

## 4. Lifecycle — `BEGIN → tx → repo call → Commit / Rollback`

The canonical Coordinator-method call shape is:

```go
func (c *coordinator) CompleteUpload(/* … */) error {
    if err := cmd.Fence.Validate(); err != nil { /* … */ }
    if cmd.UploadID == "" { /* … */ }

    tx, err := c.db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
    if err != nil { /* … */ }
    committed := false
    defer func() {
        if !committed {
            _ = tx.Rollback()
        }
    }()

    if _, err := cmd.Fence.Read(ctx, tx); err != nil { return err }

    repos := c.uowFactory.WithTx(tx)        // <— THE seam.

    // 1. artifact_uploads read for the four-branch gate.
    uploadState, err := repos.AttemptCommits().GetArtifactUploadState(ctx, cmd.UploadID)
    // 2. … verdict + CAS pair via repos.AttemptCommits().CompleteArtifactUpload(…)
    // 3. attempt_commits ready_output_count bump via UpdateReadyCountExhaustive
    // 4. attempt_commits deadline-breach EXPIRED via SetExpired
    if err := tx.Commit(); err != nil { /* … */ }  // <— tx closed here.
    committed = true
    return nil
}
```

Each step on the right-hand side is a single typed repository
method. The Coordinator never calls `tx.ExecContext` or `tx.QueryContext`
itself outside the two fence reads documented at
`coordinator.go::DeclareOutputs` and `RecordUploadProgress` (see §6
below for the documented exceptions).

Helper rules:

1. **`defer tx.Rollback()` before any work.** The `committed` flag
   pattern above is the canonical guard. A helper like
   `defer commitGuard(tx, &committed)` is acceptable but the pattern
   must be `committed=false` at declaration and flipped to `true`
   immediately after each successful `tx.Commit()`.
2. **`isolation = sql.LevelSerializable`.** Do not downgrade to
   `ReadCommitted` or `RepeatableRead` on a single-method basis —
   the conflict-budget thresholds are calibrated for the
   LevelSerializable retry shape. Downgrading makes
   `ErrTransitionConflict` semantics drift.
3. **Pre-commit snapshot.** If a method returns a `*CommitResult`,
   the Coordinator MUST call `repos.AttemptCommits().GetCommitResult`
   *inside* the tx, before `tx.Commit()`. This is the Verdetto
   P1 #9 fix. Re-reads after `Commit()` return undefined values
   under concurrent writers.
4. **ConflictBudget on the canonical CAS paths.** Three CAS methods
   count toward the budget: `UpdateReadyCountExhaustive`,
   `SetExpired`, `MarkCommitted`. They are routed through
   `(*coordinator).recordAttemptCommitsCAS(err)` (see §5 below).
5. **Replay-safe paths reset the budget.** `RecordUploadProgress`'s
   idempotent `affected=0` short-circuit and `CompleteUpload`'s
   `artifact_uploads.status='COMPLETED'` short-circuit both call
   `c.recordAttemptCommitsCAS(nil)` to clear the streak — a fresh
   streak starts next time.
6. **`defer tx.Rollback()` runs even on success.** The `committed=true`
   gate is the discriminator; if a future reviewer adds a
   `if !committed { defer tx.Rollback() }` and forgets to flip the
   flag, the SUCCESS path will roll back its own commit and the
   next read returns the previous state.

---

## 5. Regola aurea — *"never access SQL outside the `internal/store` package boundary"*

> **The single-writer rule, raised to a typed seam.**
>
> See [`docs/architecture/OWNERSHIP.md`](../architecture/OWNERSHIP.md)
> for the project-wide rule. This chapter's addition is the **typed
> enforcement**: no package outside `internal/store` may hold a
> `*sql.Tx` or call `database/sql.Query` / `Exec` directly. ALL
> access is funnelled through `UnitOfWorkFactory.WithTx(tx)` and
> the six typed repositories.

This rule is the **structural** form of the single-writer rule
from `OWNERSHIP.md`: a grep audit (`scripts/ci/check-no-sql-outside-store.sh`,
forthcoming) will fail CI any time a call site adds
`tx.ExecContext` or `tx.QueryContext` outside the `completion.*_uow.go`
files. Each typed repo method accepts typed Go parameters and
returns typed Go errors — there is no place for a hand-rolled
`SELECT` to sneak in without a visible interface signature change.

### Tables of competence (canonical writers)

The Coordinator may legitimately write the following nine tables
inside the UoW tx:

| # | Table | Owner method(s) | Owning repo | Migrations |
| --- | --- | --- | --- | --- |
| 1 | `attempt_commits` | `MarkCommitted`, `UpdateProgress`, `UpdateReadyCountExhaustive`, `SetExpired`, `SetExpiredByID`, plus `Find` + `GetCommitResult` reads | `AttemptCommitRepository` | `061_attempt_commits.sql` |
| 2 | `task_attempts` | `MarkSucceeded` (fence-gated) | `TaskAttemptRepository` | `045_task_attempts.sql` |
| 3 | `tasks` | `MarkSucceeded` (+ winner stamping + revision++) | `TaskRepository` | `039_tasks.sql` ↑ migration tree plus `064_tasks_winning_attempt_terminal_pending.sql` |
| 4 | `jobs` | `MarkSucceededIfTasksDone` (conditional CAS) | `JobFinalizationRepository` | `013_jobs.sql` ↑ |
| 5 | `job_deliveries` | `InsertDeliveriesForJob` (idempotent) | `DeliveryRepository` | `022_job_deliveries.sql` |
| 6 | `outbox_events` | `InsertEvent` (idempotent) | `OutboxRepository` | `014_outbox_events.sql` |
| 7 | `artifact_uploads` | `GetArtifactUploadState`, `CompleteArtifactUpload` (CAS pair with `artifacts`) | `AttemptCommitRepository` (cross-table ownership — see §2 verification) | `030_artifact_uploads.sql` |
| 8 | `artifacts` | `CompleteArtifactUpload` (CAS pair with `artifact_uploads`) | `AttemptCommitRepository` (cross-table ownership) | `041_artifacts.sql` |
| 9 | `task_output_declarations` | read-only inside `CompleteArtifactUpload` + `GetCommitResult` via JOIN; zero direct UPDATE here | `AttemptCommitRepository` reads | `062_task_output_declarations.sql` |

Each row carries one or more CAS gates:

- `attempt_commits.status IN ('DECLARED','UPLOADING','RECEIVED','VERIFYING')`
  before any terminal-write flip.
- `task_attempts.status NOT IN (terminal)` before the
  `MarkSucceeded` tx writes.
- `tasks.status IN ('RUNNING','LEASED')` before `MarkSucceeded`.
- `jobs` additionally gates on `NOT EXISTS (tasks where status != SUCCEEDED)`.

A CAS that returns 0 rows affected is reported to the caller
through either `ErrTransitionConflict` (typed sentinel in
`conflict_budget.go`) or a no-op `MarkSucceededIfTasksDone` (a
sibling task is still pending — the gate is working as designed).
The coordinator never interprets zero-affected as an arbitrary
failure or a missing row.

---

## 6. Documented exceptions — `DeclareOutputs` + `RecordUploadProgress`

Two Coordinator methods **deliberately do NOT use the UnitOfWork**:

### 6.1 Why these two are exceptions

Both methods perform a tightly-coupled **HMAC + INSERT-OR-IGNORE**
dance that fences on a `FenceTuple.Read` we cannot delegate to a
typed repo without dragging the master-side HMAC key plumbing
through the repository package. The Coordinator owns the HMAC
key already (it derives `commit_token_hash` via
`generateDeterministicCommitToken(c, commitID, cmd.Fence)`);
exposing it through a type-safe repo would re-introduce a
second-handle path inside `internal/store` and bloat the repo
interface for a contract that the master ends up doing anyway.

### 6.2 `DeclareOutputs`

Raw SQL:

```sql
INSERT OR IGNORE INTO attempt_commits (...);
SELECT commit_id FROM attempt_commits WHERE task_id=? AND attempt_id=?;
INSERT OR IGNORE INTO task_output_declarations (...);
```

Lifecycle: same `BEGIN` → `tx.ExecContext` (NOT through a repo) →
`Commit` pattern as the UoW methods, but the SQL is **inline** in
the Coordinator because the FenceTuple-read-then-HMAC-derive-then-
INSERT sequence must complete in **a single** roundtrip on the
canonical `commit_id` row that the FenceTuple just produced. The
typed `attempt_commits` repo (`Find`, `MarkCommitted`, etc.) only
sees the row once `DeclareOutputs` returns it.

### 6.3 `RecordUploadProgress`

Raw SQL:

```sql
UPDATE attempt_commits SET last_progress_at=?, commit_deadline_at=?
   WHERE commit_id IN (SELECT ... FROM attempt_commits WHERE fence)
   AND status IN ('DECLARED','UPLOADING');
UPDATE task_output_declarations SET uploaded_bytes=?, updated_at=?
   WHERE commit_id IN (SELECT ...) AND upload_id=?;
```

Same rationale as `DeclareOutputs`: the chunk-upload handler that
calls this method runs as a streaming upload driver at the master
side, where the bytes-pushed watermark and the deadline-extend
stamp must be co-located with the FenceTuple read for idempotent
replays across chunked retries. Pulling this into `AttemptCommits
().UpdateProgress` would split the per-upload-id watermark from
the per-declaration `uploaded_bytes` increment and reintroduce a
two-repo roundtrip that doesn't match the schema.

### 6.4 Forward doc

Both raw-SQL exceptions are flagged with a
`// OUT OF UNITOFWORK SCOPE` block comment in `coordinator.go`.
If the master ever adds a non-FenceTuple HMAC path that does
not need to interleave with these two methods, push the SQL
into a repo and remove the comment. The check
`scripts/ci/check-no-sql-outside-store.sh` will surface any
addition that slips through review.

---

## 7. Cross-link — [`docs/completion-protocol.md`](../completion-protocol.md)

The protocol chapter (Phase 2, `Coordinator` + closing the
`TaskResult` desync surface) is the **contract** this UoW seam
serves. The atomic-final-tx spec in §2.5 of `completion-protocol.md`
maps one-to-one onto the `CompleteUpload / CommitAttempt /
ReconcileAttempt` methods that own the UoW tx. The Phase 2.6 / 2.8
edits there are the canonical migrations that delete the legacy
`IngestTaskResultAtomic` SUCCEEDED write on `tasks`, leaving the
Coordinator's tx as the only writer of `task SUCCEEDED` and
`job=SUCCEEDED`-via-MarkSucceededIfTasksDone.

A future reader who is investigating "why does the Coordinator
not just open a raw tx in handler_jobs.go?" is asking the wrong
question; the protocol chapter is the load-bearing answer.

---

## 8. How to extend the UoW correctly

If you need a new typed CAS path that touches any of the nine
tables in §5, follow this checklist:

1. **Add a method to the matching repo interface**, NOT a new
   repo. The `AttemptCommitRepository` cross-table ownership of
   `attempt_commits` + `artifact_uploads` + `artifacts` is
   intentional; do not split it into three repos for one
   atomic decision.
2. **Implement the method in `sqlite_uow.go`** under the
   `sqlite*Repo` struct whose name matches the repo interface.
3. **Update the Coordinator method** that needs the new CAS path
   to call `repos.<Repo>().<NewMethod>(...)` instead of raw SQL.
4. **Add a unit test in `coordinator_test.go` or a new
   `coordinator_cas_test.go`** that exercises the new path through
   `openCoordinatorTestDB(t) + newTestCoordinator(db)`.
5. **Run `scripts/ci/check-no-sql-outside-store.sh`** (when it
   ships) to confirm the new SQL lives only in `sqlite_uow.go`.
6. **Document in the relevant section of
   [`docs/completion-protocol.md`](../completion-protocol.md)** if
   the new path materially changes Phase 2.5's atomic-final-tx
   enumeration.

---

## 9. How to introduce a *new* seventh repo (rare)

`internal/completion` reserves the seven-repo slot for
**read-mostly metadata** that does NOT belong on the atomic-final-tx
list. A new repo is appropriate when:

- A new table is added that the Coordinator reads but never writes
  inside the tx, AND
- The new table is touched in a backwards-compatible way that does
  not require splitting a current repo, AND
- The new SELECT is exercised enough to be worth a typed shape rather
  than a raw tx.QueryContext call.

If you genuinely need a seventh repo, follow the same checklist in
§8 — but consider first whether a regularized typed method on
`sqlx` or a hand-rolled small struct would not be simpler. The UoW
pattern is **not a goal**; it is a means to keep SQL ownership
typed and central.

---

*Cross-link surface used by this chapter:*
[`docs/completion-protocol.md`](../completion-protocol.md) ·
[`docs/architecture/OWNERSHIP.md`](../architecture/OWNERSHIP.md) ·
[`DataServer/internal/completion/unitofwork.go`](../../DataServer/internal/completion/unitofwork.go) ·
[`DataServer/internal/completion/sqlite_uow.go`](../../DataServer/internal/completion/sqlite_uow.go) ·
[`DataServer/internal/completion/coordinator.go`](../../DataServer/internal/completion/coordinator.go) ·
[`DataServer/internal/completion/conflict_budget.go`](../../DataServer/internal/completion/conflict_budget.go)
