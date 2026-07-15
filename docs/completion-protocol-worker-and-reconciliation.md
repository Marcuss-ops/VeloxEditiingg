# Artifact Commit Protocol — Worker, reconciliation and delivery

> Continuation of [Part 1 — Master contracts and coordinator](completion-protocol.md).
>
> Document set: [Part 1](completion-protocol.md) · **Part 2** · [Part 3 — Rollout and acceptance](completion-protocol-rollout-and-acceptance.md)

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

Continue with [Part 3 — Rollout, E2E matrix and acceptance](completion-protocol-rollout-and-acceptance.md).
