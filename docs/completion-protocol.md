# Artifact Commit Protocol — Implementation Action Plan

> **Status**: design accepted, awaiting Phase 1 implementation.
> **Rule (project-wide)**: no branches, no feature PRs — every commit lands
> directly on `main` and is pushed to `origin` immediately. Frequent small
> commits; large refactors are decomposed into atomic, individually
> shippable steps (one step per push).

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

## Phase 3 — Worker publisher + transport registry

### 3.1 — Worker SQLite spool

Files (new): `RemoteCodex/native/worker-agent-go/internal/spool/store.go`
+ matching test.

Schema exactly as the proposal dictates:
`task_id, attempt_id, commit_id, worker_spool_key, local_path, sha256,
size_bytes, upload_id, uploaded_bytes, status, last_error,
created_at, updated_at` and the seven spool statuses.

### 3.2 — Output manifest computation

File (new): `RemoteCodex/native/worker-agent-go/internal/publisher/manifest.go`.

Computes SHA-256 (streaming), size, MIME via `http.DetectContentType`
plus a fast-path match on the well-known MP4 magic. ffprobe probes
duration / dimensions / codec when present in PATH.

### 3.3 — Proto: `TaskOutputDeclared` + `OutputManifest`

Edits `shared/controltransport/pb/worker_control.proto`; regenerate via
`scripts/gen-proto.sh`. Mirror the proposal's wire definitions; do not
add fields beyond the spec.

### 3.4 — Proto: `ArtifactUploadPlan` + `UploadTarget`

Same proto file; the master returns a list of `UploadTarget` per
declared output, each carrying opaque `transport_id`, `upload_url`,
`chunk_size`, `expires_at_unix`.

### 3.5 — Proto: `ArtifactUploadCompleted`

Same proto file; the worker reports back the bytes transferred and the
worker-side SHA-256.

### 3.6 — Proto: `TaskCommitAck`

Same proto file; the master sends back its verified `task_status` and
`job_status` plus the list of canonical `artifact_ids`. Worker deletes
its spool entry **only after receiving this ack** and only after it has
persisted the ack locally (in spool itself, status `COMMITTED`).

### 3.7 — Transport registry on the worker

File (new): `RemoteCodex/native/worker-agent-go/internal/publisher/transport_registry.go`.

Two registered transports:

- `master-stream.v1` — for dev / small files / single-host / E2E.
- `object-store-multipart.v1` — production; uses S3-compatible
  multipart upload with resumable chunk retries.

### 3.8 — `master-stream.v1` implementation

File (new): `.../internal/publisher/master_stream_v1.go`. Posts via
gRPC streaming back to the master; integrates with the existing
chunked upload handlers so we do **not** fork upload code.

### 3.9 — `object-store-multipart.v1` implementation

File (new): `.../internal/publisher/object_store_multipart_v1.go`.

Uses AWS SDK v2 (or generic S3-compatible). Reads presigned
`upload_id`, executes `UploadPart` with retries, completes multipart
upon the worker's `ArtifactUploadCompleted`. Checksum verification
matches what `Finalize` recomputes server-side.

### 3.10 — Hook into the executor

File (edit): `RemoteCodex/native/worker-agent-go/internal/jobexecutor/executor.go`.

After the existing `encode`/`upload` phase emits a successful upload,
the executor now:

1. Computes the manifest (3.2).
2. Sends `TaskOutputDeclared` (3.3).
3. Receives `ArtifactUploadPlan` (3.4).
4. Uploads through the registry (3.7–3.9).
5. Sends `ArtifactUploadCompleted` (3.5).
6. Waits for `TaskCommitAck` (3.6).
7. Persists the ack in spool (3.1).
8. Deletes local file (only on COMMITTED).

### 3.11 — Capability advertisement

`WorkerHello` advertises `artifact.commit.v1` (and any other capability
already in production).

### 3.12 — Unit tests

Worker-side `publisher_test.go` covers:
- Resume after worker restart (spool entry survives crash).
- Transport registry selects by `transport_id`.
- `object_store_multipart_v1` retries on transient S3 errors.
- Cleanup only fires after `COMMITTED` ack.

---

## Phase 4 — Reconciliation repair-forward

### 4.1 — Selection logic lives in the supervisor

`DataServer/internal/metrics/supervisor.go` already runs the periodic
tick. Extend its SELECT-only candidate scan with the 11 cases from the
proposal. **No state transitions in the supervisor itself** — it only
calls `Coordinator.ReconcileAttempt`.

### 4.2 — Per-case handler tests

`DataServer/internal/completion/reconcile_test.go` adds one fixture per
candidate case. Each fixture asserts the post-condition:

- `AttemptCommit DECLARED senza upload` → expires + Task requeue.
- `AttemptCommit UPLOADING senza progressi` → expires + Task requeue.
- `Upload RECEIVED ma non verificato` → triggers Verify → COMMIT or REJECT.
- `Upload VERIFYING interrotta` → resume Verify.
- `Blob promosso ma tx non committata` → commit (idempotent).
- `Artifact READY ma Task ancora RUNNING` → commit attempt (no Job
  promotion if other Tasks unfinished).
- `Task SUCCEEDED senza artifact READY` → impossible post-Phase 2;
  alarm-and-record-only as belt-and-braces.
- `Job AWAITING_ARTIFACT con commit recuperabile` → commit attempt
  then re-evaluate Job.
- `Artifact READY senza delivery richiesta` → create idempotent delivery.
- `Delivery mancante dopo finalizzazione` → create idempotent delivery.
- `Ack perso dopo commit` → re-emit ack from coordinator state (no
  re-upload, no re-commit).

### 4.3 — Metrics + alerts

New metrics: `completion_reconcile_total{case,action}`,
`commit_deadline_exceeded_total`. Alert when
`commit_deadline_exceeded_total` rises above zero per minute over a
10-minute window.

---

## Phase 5 — Drive separation

### 5.1 — Delivery plan at Job creation

`creatorflow.CreateJobWithPlan` already returns the canonical `Job`
shape. Add the delivery plan (`deliveries/plan_resolver.go`) acquisition
to `RenderPlan.Validate()`. Plan lives on `jobs.delivery_plan_json`
(via small migration).

### 5.2 — Idempotent delivery in the final commit tx

Step 9 of 2.5 creates the `job_deliveries` row with the canonical
`idempotency_key = artifact_id + destination_id + delivery_policy_version`.
Already-existing rows are skipped via the existing `ON CONFLICT DO
NOTHING` pattern.

### 5.3 — Retry independent from Job

`deliveries/runner.go` lease/heartbeat code already supports
`BLOCKED_AUTH`. Phase 5 adds:

- `retry_budget = policy.retry_budget` from the plan.
- Backoff in `RETRY_WAIT`, separate from Task retry budget.
- Sibling-side `RateLimit` detection → flip to `RETRY_WAIT`.

### 5.4 — `BLOCKED_AUTH` semantics

Spec: `Job=SUCCEEDED`, `Artifact=READY`, `Delivery=BLOCKED_AUTH`.
Verify: even after `RUNBOOK.md` recovery, the system must not retro-
fail the Job or the Artifact.

### 5.5 — Tests

`deliveries/integration_test.go` covers:
- Drive rate-limit → retry → eventual SUCCEEDED.
- Drive auth-revoked mid-flight → BLOCKED_AUTH then SUCCEEDED after
  re-credentials.
- Double-finalize → exactly one delivery row.

---

## Phase 6 — Rollout (Jackie Chan recovery path)

### 6.1 — Deploy master with new protocol

`deploy/playbooks/deploy-master-config.yml` deploys the master binary
that contains all of Phase 1–5. Master runs alongside the existing
workers **without** forcing them to upgrade (they advertise
`artifact.commit.v1` only after upgrade; master must accept and
demote legacy TaskResults to the "shadow" path during this window).

### 6.2 — Upgrade worker `13197`

Bootstrap (`scripts/cert/real-bootstrap.sh`) + certify (`scripts/cert/certify-worker-2a-2b.sh`).
Worker binary now advertises `artifact.commit.v1`.

### 6.3 — Submit Jackie Chan through the new path

`scripts/cert/submit_jackie_chan_doc_voiceover_clips.sh` is rewritten
to:

- Submit through `creatorflow.CreateJobWithPlan` with the canonical
  payload.
- Worker renders, uploads via object-store-multipart.v1.
- Master verifies, commits via new coordinator.
- Delivery lands on Drive.

### 6.4 — Recover the original Jackie Chan attempt (one-shot)

`velox-worker recover-output --task-id ... --attempt-id ...`:

- Re-registers existing MP4 in `worker_output_spool`.
- Re-computes SHA-256 and size.
- Sends `TaskOutputDeclared` on the original Attempt.
- Master verifies, commits (idempotent on `(task_id, attempt_id)`).
- DeliveryRunner picks up the new commit and runs Drive upload.

CLI does **not** perform any ad-hoc finalization. Everything flows
through `Coordinator`.

### 6.5 — Drain remaining legacy workers

Production-soak of 1 upgraded worker + 1 legacy worker. Master logs:
"The legacy worker produced a TaskResult; shadow path used;
`shadow_mode_legacy_task_results_total` incremented". Soak for at
least 30 minutes with one Jackie-Chan-style submit.

### 6.6 — Promote the new path to canonical

After soak + green metrics, mark the legacy TaskResult ⇒ SUCCEEDED
path **disabled** (read-only branch in the gate config).
Decommission legacy workers.

---

## E2E test matrix (24 scenarios)

Each entry becomes one case in
`DataServer/internal/completion/e2e_test.go` (master-side) or
`RemoteCodex/native/worker-agent-go/internal/publisher/e2e_test.go`
(worker-side).

| # | Scenario                                 | Where        |
|---|------------------------------------------|--------------|
| 1 | render → upload → verify → commit → Drive | both         |
| 2 | dichiarazione output duplicata            | master       |
| 3 | finalize duplicato                        | master       |
| 4 | ack finale perso                          | master+wrker |
| 5 | worker crash dopo render                  | worker       |
| 6 | worker crash a metà upload                | worker+master|
| 7 | worker crash dopo upload completo        | worker       |
| 8 | master crash durante ricezione            | master       |
| 9 | master crash dopo promozione blob         | master       |
| 10 | master crash dopo commit DB ma prima ack  | master       |
| 11 | hash errato                               | master       |
| 12 | dimensione errata                         | master       |
| 13 | MP4 corrotto (probe fail)                | master       |
| 14 | lease scaduta                             | master       |
| 15 | attempt vecchio che prova a finalizzare   | master       |
| 16 | due worker che competono                  | master       |
| 17 | riavvio del DeliveryRunner                | master       |
| 18 | Drive transient failure                   | master       |
| 19 | Drive rate limit                          | master       |
| 20 | Drive auth revocata                       | master       |
| 21 | resume multipart                          | worker       |
| 22 | nessun doppio artifact READY              | both         |
| 23 | nessun Job bloccato AWAITING_ARTIFACT     | master       |
| 24 | soak test con più worker                  | both         |

---

## Files to create / modify (summary)

### Master (new files)

- `DataServer/internal/store/migrations/sqlite/061_attempt_commits.sql`
- `DataServer/internal/store/migrations/sqlite/062_task_output_declarations.sql`
- `DataServer/internal/store/migrations/sqlite/063_task_specs_required_outputs.sql`
- `DataServer/internal/store/migrations/sqlite/064_tasks_winning_attempt_terminal_pending.sql` *(tiny follow-up for 2.6)*
- `DataServer/internal/store/sqlite_attempt_commits_repository.go`
- `DataServer/internal/store/sqlite_task_output_declarations_repository.go`
- `DataServer/internal/completion/types.go`
- `DataServer/internal/completion/coordinator.go`
- `DataServer/internal/completion/coordinator_test.go`
- `DataServer/internal/completion/fencing.go`
- `DataServer/internal/completion/reconcile.go`
- `DataServer/internal/completion/reconcile_test.go`
- `DataServer/internal/completion/e2e_test.go`
- `DataServer/internal/store/migrations/sqlite/061_attempt_commits_test.go`
- `DataServer/internal/store/migrations/sqlite/062_task_output_declarations_test.go`
- `DataServer/internal/store/migrations/sqlite/063_task_specs_required_outputs_test.go`
- `scripts/ci/check-completion-protocol-invariants.sh`

### Master (edits)

- `DataServer/internal/store/sqlite_task_repository.go`
  (`IngestTaskResultAtomic` stops writing Task SUCCEEDED).
- `DataServer/internal/ingest/service.go` (rollup gates on commit).
- `DataServer/internal/grpcserver/handler_jobs.go` (uses
  Coordinator for TaskResult handling).
- `DataServer/internal/artifacts/scan_test.go` (allowlist the
  coordinator tx writer).
- `DataServer/internal/metrics/supervisor.go` (candidate scan).
- `DataServer/cmd/server/bootstrap.go` (wire Coordinator).
- `.github/workflows/ci.yml` (run invariant script).

### Worker (new files)

- `RemoteCodex/native/worker-agent-go/internal/spool/store.go`
- `RemoteCodex/native/worker-agent-go/internal/publisher/manifest.go`
- `RemoteCodex/native/worker-agent-go/internal/publisher/transport_registry.go`
- `RemoteCodex/native/worker-agent-go/internal/publisher/master_stream_v1.go`
- `RemoteCodex/native/worker-agent-go/internal/publisher/object_store_multipart_v1.go`
- `RemoteCodex/native/worker-agent-go/internal/publisher/publisher.go`
- `RemoteCodex/native/worker-agent-go/internal/publisher/publisher_test.go`
- `RemoteCodex/native/worker-agent-go/cmd/worker/recover_output.go`

### Worker (edits)

- `RemoteCodex/native/worker-agent-go/internal/jobexecutor/executor.go`
  (publish-step replaces current `TaskResult` flow).
- `shared/controltransport/pb/worker_control.proto` (new messages).
- `RemoteCodex/native/worker-agent-go/internal/worker/worker.go`
  (advertise capability).

### Shared

- `shared/controltransport/pb/worker_control.pb.go` (regenerated).
- `shared/controltransport/capabilities.go` (constants).

### Operations

- `deploy/playbooks/deploy-master-config.yml` (deploy new master).
- `scripts/cert/real-bootstrap.sh` (call recover-output helper).
- `scripts/cert/submit_jackie_chan_doc_voiceover_clips.sh`
  (rewired to use new path).

---

## Acceptance criteria per phase

- **Phase 1**: migrations apply; existing tests pass; CI invariant
  script in CI and shows zero rows.
- **Phase 2**: ingest no longer writes Task SUCCEEDED; coordinator
  unit tests pass; scan_test recognises the new writer; a focused
  golden-E2E that exercises a happy-path upload + commit.
- **Phase 3**: worker publishes the new messages; master accepts and
  ack lands; a master-side golden-E2E that runs across an actual
  worker.
- **Phase 4**: 11 reconcile cases covered; supersevior transitions
  count is exactly 0 (selection only).
- **Phase 5**: BLOCKED_AUTH cases covered.
- **Phase 6**: Jackie Chan submit → Drive upload through the new path
  succeeds.

---

## Rollback strategy

Each phase ships behind a single feature flag
(`features.completion_protocol_v1`). Rollback flips the flag to off,
which causes ingest to fall through to the legacy TaskResult ⇒
SUCCEEDED write for **non-required** outputs. Phase 2's switch is
the only one without a graceful fallback: it ships together with
Phase 6's worker upgrade so the swarm either has the new path or
the old one, never a mix mid-flight.

---

## Open design questions

1. Storage backend target for `object-store-multipart.v1`:
   S3 / MinIO / GCS / Scaleway? Confirm before Phase 3.9.
2. Lease deadline during upload phase: extend `commit_grace_period` to
   the upload duration + a safety margin; concrete value TBD.
3. Capability set: confirm that `artifact.commit.v1` is the only new
   capability, or also gate `executor.hybrid.v1`.
4. Worker spool storage backend: SQLite only, or alternative? SQLite
   is the proposal default; confirm on Phase 3.1.
