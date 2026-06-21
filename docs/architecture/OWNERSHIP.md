# Architecture ownership

This table is the canonical map of "who owns what". A change that adds a
second writer or a parallel entrypoint for any row below is a regression:
the new code will eventually drift, and one of the two paths will lose.
The canonical file is the only place that may legitimately write or read
this capability from outside the canonical owner.

| Responsibility | Canonical owner | Forbidden |
| --- | --- | --- |
| Job state (status, attempts) | `internal/jobs` repository + `LifecycleService` | Direct SQL writes from handlers or background jobs |
| Job finalisation (`SUCCEEDED` flip) | `internal/artifacts.Service` | A second writer that sets `status = 'SUCCEEDED'` outside the service |
| Asset registry | `internal/assets.ResolverRegistry` | Switch/case trees that pick a resolver by URL scheme |
| Asset upload / canonicalisation | `internal/assets.Service` | Hand-rolled blob persistence from a job handler |
| Configuration (env / file) | `internal/config` (loader + validator) | Sparse `os.Getenv` calls in handlers |
| Worker allowlist | `ValidateProductionWorkers` in `internal/config/workers_validator.go` | Re-implemented ID checks in bootstrap, ansible, or HTTP middlewares |
| Delivery providers | `internal/deliveries.Runner` (plan resolver) | Per-handler router uploads or media forks |
| Outbox event writers | `internal/outbox.Store` | Side-channel INSERTs into `outbox_events` from job/service packages |
| Outbox dispatcher registry | `internal/outbox.Registry` (registered in `cmd/server/bootstrap.go`) | Direct handler invocation from a worker goroutine |
| Worker command acknowledgement | `internal/workers.CommandManager` (registered in `cmd/server/bootstrap.go`) | HTTP fallback routes parallel to gRPC |
| Persistent state | SQLite via repository layer | JSON files or in-memory maps treated as authoritative |
| Binary storage | Filesystem / blob storage | Blobs persisted inside the DB |
| Versioning | `/VERSION.txt` (single root file) | CI fallback to `git describe`, `dev`, local snapshots |
| Worker ID minting | `internal/workers.Registry` | Random IDs generated from request payloads |
| Audit logging | `internal/audit/data_layer` | Free-form `log.Printf` calls for events that the auditor must observe |
| Migrations | Canonical SQL files + migration registration in `cmd/server/bootstrap.go` | Programmatic `CREATE TABLE IF NOT EXISTS` outside the migration registry |
| Queue package | **REMOVED** — `internal/queue` has been deleted. LifecycleService lives at `internal/jobs`. | Reintroducing `internal/queue`, `queue.Job`, `queue.QueueItem`, `queue.JobStatus`, or `*queue.FileQueue` |

## The single-writer rule

Every important state must have exactly one writer and exactly one entry
point. The shape we want everywhere is:

```
   HTTP / gRPC API
        \u2193
   Application Service
        \u2193
   Repository
        \u2193
   SQLite
```

What we explicitly forbid:

```
   Handler    \u2500\u2500\u2500\u2500\u2500\u2500\u2192 SQLite
   Service    \u2500\u2500\u2500\u2500\u2500\u2500\u2192 SQLite
   Background \u2500\u2500\u2500\u2500\u2500\u2500\u2192 JSON
   Other      \u2500\u2500\u2500\u2500\u2500\u2500\u2192 RAM
```

If a contributor is tempted to skip the service/repo layer, the answer is
always: extend the canonical owner. Adding a side path is a regression by
definition.

## Compatibility shims

A temporary adapter that lets old callers keep working while they migrate
MUST carry the following block at the top of the file (Go):

```go
// COMPATIBILITY:
// Owner:        issue #NNN
// Remove after: 2026-09-30
// Read-only:    yes
```

Rules:

- Compatibility allowed **only on the read path**.
- Never two write paths.
- Never dual-write.
- Never silent fallback.
- Issue number is mandatory.
- Removal date is mandatory.
- A CI check enforces the deadline: after the date, the build fails.

## How to add a new responsibility

1. Identify the canonical owner in this table (or extend the table with a
   new row before writing the code).
2. Update `.github/CODEOWNERS` so reviewers are auto-assigned.
3. Wire the new path into `cmd/server/bootstrap.go` (the composition root)
   \u2014 do not let it self-discover.
4. Add a `check-single-writer.sh` rule: assert that the symbol that
   performs the canonical mutation is the only one.
5. Add an invariant test (not a behaviour test) that fails if anyone
   attempts the forbidden pattern.
