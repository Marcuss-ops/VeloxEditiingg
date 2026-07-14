// Package migrations / migrations.go (stub)
//
// This file is intentionally empty. The previously-monolithic
// migrations.go has been split into four responsibility files in the
// same package:
//
//   - runner.go     — the public RunMigrations orchestrator.
//   - discovery.go  — embeds the SQLite/Postgres migration FS, defines
//     the Migration type, and lists / splits the SQL.
//   - apply.go      — forward (UP) per-migration transactions,
//     AppliedVersions / PendingVersions.
//   - down.go       — rollback-status introspection surface
//     (ListMigrationStatus / MigrationStatus).
//
// Note: EnsureApplied and RunDown were retired in this split because
// their only callers were in the test surface; production boot paths
// route exclusively through RunMigrations. See runner.go / down.go
// for the rationale + behavior parity notes.
package migrations
