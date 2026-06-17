# `internal/store/postgres/` — scaffolding only

This package hosts the future PostgreSQL implementations of:

- `store.ArtifactRepository` (added in PR-1 of the §5 refactor)
- `store.DeliveryRepository` (added in PR-1)
- `store.JobRepository` (deferred to PR-2 — needs `CreateJobParams`, `ClaimParams`, `TransitionParams`, `ClaimResult`, `JobStatus` types added to package `store`)

Current status:

- `var _ store.X = (*Y)(nil)` compile-time guards prove each stub satisfies the SQLite-side contract.
- All method bodies return `store.ErrNotImplemented`.
- No driver / pool is opened yet; constructing a `postgres.X` is a no-op.

## How to bring this online

1. Drop in a real driver (`github.com/jackc/pgx/v5/pgxpool` is the natural fit — `database/sql + lib/pq` is fine too but loses `COPY`).
2. For each stub, replace `return store.ErrNotImplemented` with a single-transaction call. Do NOT expose BeginTx to callers — atomicity lives inside the method.## Compile-time guarantees today

- `internal/store/postgres/artifacts_repository.go` and `internal/store/postgres/deliveries_repository.go` carry `var _ store.X = (*Y)(nil)` guards — they change shape with the interface as stable contract reminders.
- `internal/store/postgres/jobs_repository.go` intentionally omits the guard: it owns local staging types (`CreateJobParams`, `ClaimParams`, `ClaimResult`, `TransitionParams`, `JobRow`) until PR-2 moves them into `package store` and exposes a cross-package `JobRepository` interface. The guard lands with PR-2.

## How to bring this online

  3. Wire a `NewPostgresXFactory(t)` for tests so the existing `internal/store/contracts/*_contract_test.go` suites can be re-pointed at Postgres.

4. Add `VELOX_DB_DRIVER=postgres` plus `VELOX_DATABASE_URL` support to `internal/config/config.go`. Default driver remains `sqlite` until §5b ships.
