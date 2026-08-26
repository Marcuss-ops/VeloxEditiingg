# ADR: Ownership of artifact persistence

- **Status**: Accepted — final separation
- **Date**: 2026-08-05
- **Scope**: `DataServer/internal/artifacts`, `DataServer/internal/artifactsstore`,
  `DataServer/internal/repository`, and the composition root in
  `DataServer/cmd/server`

## Context

The artifact lifecycle spans upload sessions, resumable chunks, artifact
metadata, verified finalization, media probing, reconciliation, garbage
collection, blob storage, and delivery-plan side effects. Historically, some
of the SQLite adapters for those concerns lived in the broad
`internal/store` package. That made the package a compatibility facade for
artifact persistence and allowed application code to depend on the wrong
boundary.

The repository contracts and application orchestration have now been
separated. The artifact SQL adapters are colocated in the focused
`internal/artifactsstore` package; the application package consumes narrow
contracts and does not own SQL.

## Decision

### Ownership by layer

`internal/artifacts` owns application orchestration only:

- Begin/receive/finalize upload workflows;
- artifact and upload lifecycle validation;
- filesystem/blob promotion and cleanup orchestration;
- media-probe worker orchestration;
- reconciliation scheduling and policy;
- translation of persistence outcomes into application errors;
- consumer-facing ports such as `ArtifactReader`, `UploadSessionWriter`, and
  `FinalizationWriter`.

`internal/artifactsstore` owns all artifact-domain SQLite persistence:

| Adapter | Responsibility | Canonical contract / boundary |
| --- | --- | --- |
| `SQLiteArtifactRepository` | Artifact metadata insert, lookup, and job projection | `repository.ArtifactRepository` |
| `SQLiteArtifactReader` | Read-only artifact projection used by the application | `repository.ArtifactReader` |
| `SQLiteUploadRepository` | Upload-session lifecycle, status CAS, resumable chunk CRUD, and active-upload lookup | `repository.UploadRepository` |
| `SQLiteUploadSessionWriter` | Atomic `artifacts` + `artifact_uploads` BeginUpload transaction | `artifactsstore.CreateUploadSessionParams` and `artifacts.UploadSessionWriter` |
| `SQLiteArtifactFinalizer` | Verified finalization transaction across task, job, artifact, upload, and delivery state | `artifactsstore.FinalizeVerifiedParams` and `artifacts.FinalizationWriter` |
| `MediaProbeRepository` via `NewSQLiteMediaProbeRepository` | Media-probe enqueue, lease, completion, failure, and quarantine persistence | `repository.MediaProbeEnqueuer` / media-probe worker port |
| `ArtifactReconcilerRepository` | READY/STAGING projections, stuck-artifact CAS, quarantine, and GC enqueue | `artifacts.Reconciler` port |
| `ArtifactGCStore` | GC candidate enqueue, leasing, and completion | `artifacts.RunArtifactGC` port |
| `SQLiteJobDeliveryCounter` | Expected-delivery count used by finalization/ffprobe invariants | artifact finalization service port |

The lifecycle vocabulary remains owned by the contract packages:

- artifact and upload projections: `internal/repository`;
- finalization and reconciliation persistence projections:
  `internal/artifactsstore`;
- application errors and workflow commands: `internal/artifacts`.

`internal/store` remains the general SQLite composition and persistence
package for non-artifact aggregates. It is not an artifact-domain adapter
owner and must not regain the extracted artifact implementations.

### Dependency direction

The final dependency shape is:

```text
HTTP / gRPC / worker protocol
              |
              v
internal/artifacts  -- application orchestration and ports
              |
              v
internal/artifactsstore  -- artifact-domain SQLite adapters
              |
              v
SQLite (*sql.DB)
```

The composition root opens and owns the database connection, then constructs
`internal/artifactsstore` adapters and injects them into `internal/artifacts`.
Application packages do not construct ad-hoc SQL repositories from the
`internal/store` facade, and adapters do not expose `*sql.Tx` to application
callers.

Blob bytes follow a separate boundary:

```text
internal/artifacts orchestration
              |
              v
repository.BlobStore
              |
              v
filesystem / configured blob backend
```

Binary storage is not persisted inside SQLite and is not part of the
artifact-domain SQL ownership decision.

## Invariants

1. **Single verified-finalization writer.** There is exactly one transaction
   that can promote a verified artifact and perform the associated terminal
   state transitions: `artifactsstore.SQLiteArtifactFinalizer`.
2. **Single `SUCCEEDED` gate.** The legal artifact-driven
   `jobs.status = 'SUCCEEDED'` transition is implemented by the finalization
   path and is not duplicated in upload handlers, reconciliation, or other
   adapters.
3. **Atomic BeginUpload.** Creation of the artifact row and its upload-session
   row is performed by `SQLiteUploadSessionWriter` in one transaction.
4. **CAS transitions are persistence operations.** Upload, artifact, delivery,
   and GC state guards remain in their owning `artifactsstore` adapter; the
   application layer decides policy but does not reimplement SQL predicates.
5. **No SQL in orchestration.** Production files under `internal/artifacts`
   must not open transactions, execute SQL, scan SQL rows, or import
   `database/sql` merely to reach an adapter.
6. **No second artifact write path.** New artifact persistence behavior must
   extend an existing `internal/artifactsstore` adapter or add a new focused
   adapter there. It must not be added to `internal/store` or implemented as a
   raw query in `internal/artifacts`.
7. **Fail closed.** Required artifact adapters are constructed explicitly at
   bootstrap. Missing dependencies are startup/configuration errors, never a
   hidden nil, noop, or stub success.
8. **Shared database boundary.** Adapters participating in one lifecycle use
   the same `*sql.DB` supplied by the composition root, so transaction and
   concurrency guarantees are preserved.

The SQL ownership ratchet must continue to report zero production SQL access
violations in `internal/artifacts`; SQL for this domain belongs in
`internal/artifactsstore`.

## Compatibility and migration status

The extraction from `internal/store` is complete for the artifact adapters
listed above. The following legacy implementation files no longer exist in
`internal/store`:

- `artifact_uploads.go`;
- `artifact_uploads_sessions.go`;
- `artifact_uploads_chunks.go`;
- `artifact_uploads_helpers.go`;
- `artifact_upload_session_writer.go`;
- `artifacts_repository.go`;
- `sqlite_artifact_reader.go`;
- `media_probe_jobs.go`;
- `artifact_gc.go`;
- `artifacts_reconciler.go`.

Compatibility wrappers that preserve application-facing APIs may remain in
`internal/artifacts`, but they delegate to `internal/artifactsstore` and do
not contain a second SQL implementation. They are wiring adapters, not
persistence owners, and must not import or reintroduce the old `internal/store`
artifact implementations.

The extracted adapters are constructed directly from the canonical database
handle, for example:

```go
uploadRepo := artifactsstore.NewSQLiteUploadRepository(db)
uploadWriter := artifactsstore.NewSQLiteUploadSessionWriter(db)
artifactReader := artifactsstore.NewSQLiteArtifactReader(db)
finalizer := artifactsstore.NewSQLiteArtifactFinalizer(db, resolver)
probeRepo := artifactsstore.NewSQLiteMediaProbeRepository(db)
```

## Consequences

### Positive

- Artifact persistence has one discoverable owner and one dependency boundary.
- `internal/store` can be reduced independently without carrying artifact
  compatibility implementations.
- Application tests can use `repository` contracts or focused adapters without
  importing the broad store package.
- Transactional ownership, CAS fencing, and finalization invariants are easier
  to audit statically.

### Costs

- The composition root must wire several focused adapters explicitly.
- Cross-package contract changes require updating the relevant focused leaf
  and its application port.
- Existing callers that still use general `internal/store` APIs require an
  intentional migration rather than an alias silently preserving the old
  boundary.

## Rules for future changes

- Add artifact SQL only under `internal/artifactsstore`.
- Add or change an application workflow only under `internal/artifacts`.
- Add shared persistence contracts under `internal/repository` or the focused
  contract package that owns the vocabulary; do not define a duplicate in
  `internal/store`.
- Wire all concrete adapters in `cmd/server`; do not self-discover them.
- Preserve the `DISABLED` / `READY` / `MISCONFIGURED` capability model when an
  artifact worker or reconciliation capability is optional.
- Any removal of an exported adapter, contract, or cross-package helper must
  pass the full-module removal gate:

```bash
bash scripts/ci/pre-removal-verify.sh
```
