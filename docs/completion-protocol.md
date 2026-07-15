# Artifact Commit Protocol — Implementation Action Plan

> **Status**: design accepted, awaiting Phase 1 implementation.
> **Rule (project-wide)**: no branches, no feature PRs — every commit lands
> directly on `main` and is pushed to `origin` immediately. Frequent small
> commits; large refactors are decomposed into atomic, individually
> shippable steps (one step per push).

Document set: **Part 1 — Master contracts and coordinator** · [Part 2 — Worker, reconciliation and delivery](completion-protocol-worker-and-reconciliation.md) · [Part 3 — Rollout and acceptance](completion-protocol-rollout-and-acceptance.md)

This plan replaces the current "TaskResult ⇒ `SUCCEEDED`" shortcut with a
**fence-gated commit protocol** in which the master is the sole authority
on Task and Job terminality and no terminal status can be written before
all required output bytes have been acquired, verified and promoted.

Companion documents:
- `docs/jobs/status.go` — `AWAITING_ARTIFACT` semantics (state machine).
- `DataServer/internal/artifacts/sqlite_finalization_repository.go` —
  existing sole writer of `jobs.status='SUCCEEDED'`. This plan extends
  that contract rather than re-implementing it.
- `DataServer/internal/artifacts/scan_test.go` — CI guard for canonical
  SUCCEEDED-writer ownership; do not relax.

---

## 0. Pre-flight invariants enforced before any phase

1. Today, `SQLiteTaskRepository.IngestTaskResultAtomic`
   (`DataServer/internal/store/sqlite_task_repository.go:747`) and
   `TaskReportIngestionService.IngestTaskResult` still write
   `tasks.status='SUCCEEDED'`. This is exactly the desync surface the
   protocol removes; Phase 2 disables this write.
2. `scan_test.go` must continue to pass at every step — the canonical
   SUCCEEDED writer on jobs must remain `finalization_repository.go`,
   and after Phase 2 the canonical writer on `tasks` becomes the new
   completion-coordinator tx, not the ingest path.
3. ID-tuple fencing `(task_id, attempt_id, worker_id, lease_id, revision)`
   must be present on every terminal transition. A step is **not
   done** until a test asserts the tuple is enforced.

---

## Phase 1 — Schema & contracts (no runtime behaviour change yet)

### 1.1 — Migration: `attempt_commits` table

File (new): `DataServer/internal/store/migrations/sqlite/061_attempt_commits.sql`

```sql
CREATE TABLE attempt_commits (
    commit_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    lease_id TEXT NOT NULL,
    task_revision INTEGER NOT NULL,

    status TEXT NOT NULL,                 -- DECLARED|UPLOADING|RECEIVED|VERIFYING|COMMITTING|COMMITTED|REJECTED|EXPIRED
    required_output_count INTEGER NOT NULL,
    ready_output_count INTEGER NOT NULL DEFAULT 0,

    commit_token_hash TEXT NOT NULL,
    commit_deadline_at TEXT NOT NULL,
    last_progress_at TEXT NOT NULL,

    committed_at TEXT,
    rejected_code TEXT,
    rejected_message TEXT,

    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,

    UNIQUE(task_id, attempt_id)
);
CREATE INDEX idx_attempt_commits_status ON attempt_commits(status);
CREATE INDEX idx_attempt_commits_deadline ON attempt_commits(commit_deadline_at);
```

**Acceptance**: migration applies cleanly on a fresh DB and on top of
migration 060; `describe attempt_commits` matches the schema above;
`PRAGMA integrity_check` still OK.

### 1.2 — Migration: `task_output_declarations` table (replaces artefact-authoritative role of `task_output_artifacts`)

File (new): `DataServer/internal/store/migrations/sqlite/062_task_output_declarations.sql`

```sql
CREATE TABLE task_output_declarations (
    declaration_id TEXT PRIMARY KEY,
    commit_id TEXT NOT NULL,
    task_id TEXT NOT NULL,
    attempt_id TEXT NOT NULL,

    output_kind TEXT NOT NULL,
    logical_name TEXT NOT NULL,
    mime_type TEXT NOT NULL,

    expected_size_bytes INTEGER NOT NULL,
    expected_sha256 TEXT NOT NULL,

    worker_spool_key TEXT,
    status TEXT NOT NULL,                 -- DECLARED|UPLOADING|RECEIVED|VERIFYING|READY|FAILED

    upload_id TEXT,
    artifact_id TEXT,

    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,

    UNIQUE(task_id, attempt_id, output_kind, logical_name)
);
CREATE INDEX idx_task_output_declarations_commit ON task_output_declarations(commit_id);
```

Important: existing `task_output_artifacts` is **kept** but stops being
authoritative — Phase 2 demotes its role to a non-canonical mirror. Do
not drop it in this phase.

**Acceptance**: migration applies; the two tables co-exist; existing
tests pass unchanged; `seq_scan=0` after schema-only test populates
declarations via integration test (sanity).

### 1.3 — Migration: `required_outputs` on `task_specs`

File (new): `DataServer/internal/store/migrations/sqlite/063_task_specs_required_outputs.sql`

```sql
ALTER TABLE task_specs ADD COLUMN required_outputs_json TEXT NOT NULL DEFAULT '[]';
```

Default `[]` keeps existing `Fabrizio_clips`-style specs untouched.
Higher-level schemas that need required outputs will be authored
explicitly in Phase 2.

**Acceptance**: existing spec round-trips are byte-identical with
`required_outputs_json='[]'`; new spec authoring path can write
non-empty JSON.

### 1.4 — Proto: capability handshake advertises `artifact.commit.v1`

File (edit): `shared/controltransport/pb/worker_control.proto` — add to
the existing `WorkerHello` (or whichever handshake message carries
worker capabilities) the new capability string, then regenerate via
`scripts/gen-proto.sh`. Mirror the existing capability probe messages
**only**; do not introduce a new handshake frame.

```protobuf
// Capability string literals (constants, not enum, so old workers
// can advertise older caps without rebuilding the proto).
//   "artifact.commit.v1"
//   "executor.hybrid.v1"
```

**Acceptance**: proto regenerates without lint errors; existing
`worker_control.pb.go` round-trips unchanged.

### 1.5 — Invariant queries as CI gate

File (new): `scripts/ci/check-completion-protocol-invariants.sh`

The script must execute four queries and assert zero rows:

```
SELECT j.job_id FROM jobs j
WHERE j.status='SUCCEEDED'
  AND NOT EXISTS (SELECT 1 FROM artifacts a
                  WHERE a.job_id=j.job_id AND a.status='READY');

SELECT t.task_id FROM tasks t
WHERE t.status='SUCCEEDED'
  AND EXISTS (SELECT 1 FROM task_output_declarations d
              LEFT JOIN artifacts a ON a.id=d.artifact_id AND a.status='READY'
              WHERE d.task_id=t.task_id AND d.required=1 AND a.id IS NULL);

SELECT job_id, output_kind, COUNT(*) FROM artifacts
WHERE status='READY' GROUP BY job_id, output_kind HAVING COUNT(*)>1;

SELECT d.delivery_id FROM job_deliveries d
JOIN artifacts a ON a.id=d.artifact_id WHERE a.status!='READY';
```

Wire the script into `.github/workflows/ci.yml` after the existing
test step. Wire the same queries (read-only) into the production doctor
(`scripts/ci/local-verify-mirror.sh`).

**Acceptance**: on the **current** codebase the queries return zero rows
on a freshly seeded test DB; the script is idempotent and runs in ≤2 s
on CI; failure mode prints offending rows and aborts.

### 1.6 — Unit tests for migrations

File (new): `DataServer/internal/store/migrations/sqlite/061_attempt_commits_test.go`
and 062/063 parallels.

**Acceptance**: `go test ./internal/store/migrations/...` passes; tests
exercise UNIQUE conflicts on `(task_id, attempt_id)` and on
`(task_id, attempt_id, output_kind, logical_name)`.

---

## Phase 2 — `internal/completion` Coordinator + closing the TaskResult desync surface

### 2.1 — New package `internal/completion`

Files (new):
- `DataServer/internal/completion/types.go` — command/result structs.
- `DataServer/internal/completion/coordinator.go` — Coordinator
  implementation.
- `DataServer/internal/completion/coordinator_test.go` — unit tests.

```go
type Coordinator interface {
    DeclareOutputs(ctx context.Context, cmd DeclareOutputsCommand) (*UploadPlan, error)
    RecordUploadProgress(ctx context.Context, cmd UploadProgressCommand) error
    CompleteUpload(ctx context.Context, cmd CompleteUploadCommand) error
    CommitAttempt(ctx context.Context, commitID string) (*CommitResult, error)
    ReconcileAttempt(ctx context.Context, commitID string) (*CommitResult, error)
}
```

The implementation depends only on the canonical repository interfaces,
NOT on the `ingest` package, NOT on the `taskoutput_artifacts` package,
NOT on `creatorflow`. Test seam: stub coordinator for downstream
callers (e.g. the WorkerHello handler).

### 2.2 — Fencing helper

File (new): `DataServer/internal/completion/fencing.go`

Centralises the
`(task_id, attempt_id, worker_id, lease_id, revision)`
gate. Each transition function MUST pass through this helper.

### 2.3 — Idempotent `DeclareOutputs`

- Upserts `attempt_commits` (UNIQUE on `task_id,attempt_id`).
- Insert-or-skip `task_output_declarations`.
- Returns an `ArtifactUploadPlan` referencing a freshly generated
  `commit_token` (stored as `commit_token_hash`).

### 2.4 — Idempotent `RecordUploadProgress`

CASes on `attempt_commits.last_progress_at`; updates
`commit_deadline_at`; bumps per-declaration `uploaded_bytes`.

### 2.5 — `CompleteUpload` + `CommitAttempt` (the atomic final transaction)

One `BEGIN IMMEDIATE` transaction; in order:

1. Fencing read on Task + Attempt.
2. Verify all `required_output_count` are READY on `artifacts`.
3. UPDATE `artifact_uploads.status='COMPLETED'`.
4. UPDATE `artifacts.status='READY'` (CAS `STAGING|VERIFYING → READY`).
5. UPDATE `attempt_commits.status='COMMITTED'`.
6. UPDATE `task_attempts.status='SUCCEEDED'` (only one writing this).
7. UPDATE `tasks.status='SUCCEEDED'`, set `winning_attempt_id`.
8. UPDATE `jobs.status='SUCCEEDED'` (only when all required tasks are
   SUCCEEDED **and** final artifact READY).
9. INSERT idempotent `job_deliveries`.
10. INSERT idempotent `outbox_events`.

This is **the** distributed commit. There must be no other code path
that writes any of these terminal statuses (Phase 2 closes the
IngestTaskResultAtomic write on tasks + job-AWAITING_ARTIFACT
promotion — see 2.6/2.7).

### 2.6 — Disable TaskResult ⇒ Task SUCCEEDED write

Edits:
- `DataServer/internal/store/sqlite_task_repository.go` —
  `IngestTaskResultAtomic`: when the TaskResult reports `task_status
  = SUCCEEDED`, the method must do work equivalent to the legacy
  transition for Attempts only and stamp `tasks.status='RUNNING'` with
  `winning_attempt_terminal_pending=true` (new bool column added in
  a tiny follow-up migration 064), leaving the SUCCEEDED write to the
  coordinator.
- `DataServer/internal/ingest/service.go`: stop calling job-rollup that
  promotes to AWAITING_ARTIFACT based on Task=SUCCEEDED. Instead,
  the rollup checks for `winning_attempt_terminal_pending=true` AND
  `attempt_commits.committed_at IS NOT NULL`.
- Update `scan_test.go` allowlist to include the new completion tx
  writer (Phase 2 touches one file outside `finalization_repository.go`).

### 2.7 — `ReconcileAttempt` + `Supervisor` integration

File (new): `DataServer/internal/completion/reconcile.go`

Reconciles the 11 candidate states listed in the proposal. Selection
logic stays in the supervisor (`DataServer/internal/metrics/supervisor.go`
already owns the periodic runner) but **does not transition states
itself**: it only selects candidates and calls `Coordinator.ReconcileAttempt`.
Repair-forward, not cleanup-only.

### 2.8 — Close out: ingest no longer promotes Job to AWAITING_ARTIFACT without a commit

The `Job = AWAITING_ARTIFACT` write in `TaskReportIngestionService`
becomes a write conditioned on:

```
EXISTS (SELECT 1 FROM attempt_commits WHERE attempt_id = ? AND status = 'COMMITTED')
```

If no commit exists yet, Job stays at `RUNNING`. The end of Phase 2
makes "Task SUCCEEDED, Job AWAITING_ARTIFACT, no artifact READY"
impossible by construction.

### 2.9 — Unit tests

`coordinator_test.go` covers at minimum:

- DeclareOutputs is idempotent on `(task_id, attempt_id)`.
- Stale `(worker_id, lease_id, revision)` returns ErrTransitionConflict.
- CompleteUpload before all required outputs ⇒ EXPIRED, not COMMIT.
- CommitAttempt on a duplicate `commit_id` is a clean no-op (idempotent).
- ReconcileAttempt on `AttemptCommit DECLARED` with worker dead
  rescinds and re-arms the Task.

---

## Continue reading

- [Part 2 — Worker publisher, reconciliation and delivery](completion-protocol-worker-and-reconciliation.md)
- [Part 3 — Rollout, E2E matrix and acceptance](completion-protocol-rollout-and-acceptance.md)
