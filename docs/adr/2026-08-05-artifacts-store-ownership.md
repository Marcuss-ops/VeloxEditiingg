# ADR: Artifacts SQL ownership in `internal/store`

- **Status**: Accepted
- **Date**: 2026-08-05
- **Scope**: `DataServer/internal/artifacts` and `DataServer/internal/store`

## Decision

The artifacts application package owns orchestration and filesystem work only.
All artifacts-domain SQL is implemented by small repositories in
`internal/store`:

- `SQLiteUploadSessionWriter` — atomic `artifacts` + `artifact_uploads`
  BeginUpload insert;
- `SQLiteArtifactFinalizer` — the single verified-finalization transaction
  for task, job, artifact, delivery, and upload state;
- `ArtifactReconcilerRepository` — READY/STAGING queries, CAS quarantine and
  GC candidate enqueue;
- `SQLiteArtifactReader` — the read-only artifact projection;
- `SQLiteUploadRepository` — upload-session and chunk CRUD.

`internal/artifacts` retains consumer-facing ports and compatibility adapters
only. It does not open transactions, execute SQL, or scan SQL rows.

## Invariants

```text
HTTP/gRPC or worker protocol
        -> artifacts.Service / Reconciler orchestration
        -> narrow store repository
        -> SQLite
```

There is one verified-finalization transaction and one owner of the
`jobs.status = 'SUCCEEDED'` transition. The SQL ratchet baseline must contain
zero production files under `internal/artifacts`.

## Migration policy

Compatibility constructors are temporary wiring shims; they delegate to the
store implementation and do not create a second write path. New artifact
persistence operations must be added to a focused repository in `internal/store`
 rather than to `internal/artifacts`.
