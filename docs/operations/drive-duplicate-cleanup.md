# Drive duplicate cleanup runbook

## Safety contract

The cleanup is performed only through `velox-admin`; do not delete files by
name or by searching a Drive folder manually.

1. Generate a complete manifest from the authoritative master DB.
2. Review `job_id`, `artifact_id`, canonical and duplicate `delivery_id`,
   `drive_file_id_correct`, `drive_file_id_duplicate`, `destination_id`, both
   delivery timestamps, and duplicate status.
3. Run `--dry-run` first. It verifies the current DB identities and the remote
   canonical file, and never calls the remote delete operation.
4. Run `--apply` only after the dry-run output is reviewed. Before deletion it
   rechecks the DB rows and verifies both remote IDs. Only the duplicate remote
   ID is passed to Drive `DeleteFile`.
5. Every plan and outcome is written to the append-only audit trail. Audit
   event identity includes operation mode, outcome, manifest timestamp,
   delivery ID, and duplicate remote ID, so a prior dry-run cannot suppress a
   later apply event.

A stale, edited, incomplete, malformed, non-Drive, non-terminal, or timestamp-
drifted manifest fails closed before any deletion. A typed Drive HTTP 404 for
the duplicate is treated as already absent; other errors fail closed.

## Commands

```bash
cd DataServer

# 1. Produce the immutable operator manifest from the master DB.
go run ./cmd/velox-admin duplicate-delivery-manifest \
  --db /var/lib/velox/velox.db \
  --output /var/lib/velox/evidence/drive-duplicate-manifest.json \
  --dry-run \
  --actor operator-id

# 2. Verify only; no remote deletion.
go run ./cmd/velox-admin cleanup-drive-duplicates \
  --db /var/lib/velox/velox.db \
  --manifest /var/lib/velox/evidence/drive-duplicate-manifest.json \
  --dry-run \
  --actor operator-id

# 3. After reviewing the dry-run result, delete only verified duplicates.
go run ./cmd/velox-admin cleanup-drive-duplicates \
  --db /var/lib/velox/velox.db \
  --manifest /var/lib/velox/evidence/drive-duplicate-manifest.json \
  --apply \
  --actor operator-id
```

The command loads the configured Drive token through the normal service
configuration. If the token/configuration is absent, it must stop; operators
must not substitute a worker cache DB, a local test DB, or an implicit folder.

## Verification checklist

Capture stdout and the audit event IDs with the operator evidence. The apply
result must show, per record:

```text
canonical_checked = 1
remote deletion target = drive_file_id_duplicate
remote deletion target != drive_file_id_correct
```

After apply, query the audit trail by the duplicate remote ID and confirm both
`DRIVE_DUPLICATE_CLEANUP_PLANNED` and `DRIVE_DUPLICATE_DELETED` (or
`already_absent`) are present. If the process stops after a remote delete and
before the outcome audit is persisted, rerun the same apply command: the
current DB verification and typed 404 handling make the operation convergent,
and the replay writes the missing outcome event.

Never use `rm`, Drive-name matching, or a folder-wide delete as a substitute
for this procedure.

## Validation performed in development

The implementation has unit coverage for:

- complete manifest fields and RFC3339 timestamps;
- same-ID, duplicate-ID, whitespace, and cross-record collision rejection;
- current DB identity, status, destination, remote ID, and timestamp checks;
- dry-run with zero delete calls;
- apply targeting only the duplicate ID;
- canonical-file preflight and duplicate metadata verification;
- typed 404 idempotency and audit replay identity.

A production cleanup was **not** run during development when Drive credentials,
token storage, and a production manifest were unavailable.
