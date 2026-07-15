# Artifact Commit Protocol — Rollout, verification and acceptance

> Continuation of [Part 1 — Master contracts and coordinator](completion-protocol.md) and [Part 2 — Worker, reconciliation and delivery](completion-protocol-worker-and-reconciliation.md).
>
> Document set: [Part 1](completion-protocol.md) · [Part 2](completion-protocol-worker-and-reconciliation.md) · **Part 3**

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
