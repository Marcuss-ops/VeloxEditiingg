// Package postgres hosts the upcoming PostgreSQL implementations of the
// narrow repository contracts declared in package store: ArtifactRepository,
// DeliveryRepository, and JobRepository (spec §5).
//
// Status: scaffolding only. SQLite remains the production backend. The
// concrete types in this package intentionally satisfy compile-time interface
// checks (`var _ store.X = (*Y)(nil)`) so that the contract tests can later
// be re-pointed at a Postgres factory without changing consumer code.
//
// To bring the package online:
//
//  1. Add a *pgxpool.Pool (or *sql.DB with lib/pq) lifecycle manager.
//  2. Replace each "return ErrNotImplemented" with a real driver call inside a
//     single transaction, ensuring all atomicity guarantees from spec §5 hold:
//     no BeginTx/Commit leaks to callers — atomic operations stay single-method.
//  3. Add a factory InPostgresArtifactRepositoryFactory(t) that opens a
//     throwaway test DB and constructs the impl; plug it into the existing
//     contracts/*.go test suites.
package postgres
