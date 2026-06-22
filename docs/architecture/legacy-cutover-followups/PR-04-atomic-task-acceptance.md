# PR-04 — Atomic task acceptance

> **Audit anchor:** [§P1.5](../LEGACY_SSOT_AUDIT.md#p15--start-task-e-creazione-attempt-non-atomici) (e prerequisito di PR-06/08).
> **Target milestone:** cutover P1.
> **Branch:** `cutover/pr-04-atomic-task-acceptance`
> **Dipendenze:** PR-03 (Attempt ID canonico). PR-01 utile ma non bloccante.

## Contesto

Una Task può oggi passare a `RUNNING` prima che il relativo
`TaskAttempt` venga creato. Se la creazione Attempt fallisce, il
risultato è:

```text
Task.status = RUNNING
TaskAttempt = assente
```

Stato non osservabile dai test invariant, non recuperabile se non
manualmente. Va sostituito con una transazione unica.

## Scope

- Introdurre un metodo transazionale `AcceptTaskOffer(ctx, command)`
  che esegue:
  - `tasks.status LEASED → RUNNING`,
  - `task_attempts.status PENDING → RUNNING`.
- Rollback completo se una delle due operazioni fallisce.
- Nessun handler deve poter transire la Task a RUNNING da solo.

## Files to touch

```text
velox-server/internal/taskgraph/lifecycle.go
velox-server/internal/taskattempts/repository_*.go
velox-server/internal/store/sqlite_task_repository.go
velox-server/internal/store/sqlite_task_attempt_repository.go
velox-server/internal/grpcserver/handler_workers.go
velox-server/internal/store/store_tasks.go (o transazioni condivise)
```

## Sequenza operativa

```text
1. AcceptTaskOffer(ctx, cmd):
     BEGIN TRANSACTION;
       /* lookup TaskAttempt(attempt_id, task_id) PENDING */
       /* verify task.worker_id, task.lease_id, task.status=LEASED */
       UPDATE tasks SET status='RUNNING', lease_id=?, worker_id=?
         WHERE id=? AND status='LEASED' AND lease_id=?;
       UPDATE task_attempts SET status='RUNNING', started_at=now
         WHERE id=? AND status='PENDING';
       INSERT INTO outbox (type=TASK_RUNNING, payload={attempt_id});
     COMMIT;

2. Se una delle UPDATE ritorna 0 rows affected → ROLLBACK.

3. Dopo commit → master invia TaskLeaseGranted{attempt_id}.
```

## Acceptance criteria

- [ ] Non esiste alcun call site che transisca `tasks.status` a `RUNNING`
      senza passare da `AcceptTaskOffer`.
- [ ] Test `AcceptTaskOffer_FailureOnAttemptUpdate_DoesNotTransitionTask`
      verde.
- [ ] Test `AcceptTaskOffer_TaskAlreadyRunning_ReturnsConflict` verde.
- [ ] L'invariant §9.5 dell'audit (Task RUNNING con Attempt attivo) è
      sempre soddisfatto.

## Test

- **Unit:**
  - atomic transaction (mock repo, force error nel secondo UPDATE,
    verifica rollback).
  - conflict con doppio AcceptTaskOffer.
- **Integration:**
  - simulare crash tra i due UPDATE: nessun `RUNNING` orfano.
- **Invariant:**
  - query SQL del §9.5 dell'audit ritorna sempre 0.

## CI guards introdotti

Aggiungere a `check-single-writer.sh`:

```bash
# Vietato al di fuori di velox-server/internal/taskgraph/lifecycle.go
# e del repository task:
#   tasks.status  → 'RUNNING'
#   SET status = 'RUNNING'
# in transazioni che non includono anche task_attempts.
```

In pratica il guard diventa: cerca `UPDATE tasks ... SET status='RUNNING'`
fuori dai due percorsi consentiti, fail.

## Rischi

- Se qualcuno aveva path paralleli (es. supervisor foreground che
  riattiva Task) vanno riallineati sul nuovo metodo, o tenuti vivi solo
  per `tasks.status` recovery da `FAILED` (escluso da questa PR).
- Performance: la transazione include lock su tasks e task_attempts.
  In caso di provisioning di 100 worker paralleli, è accettabile
  perché il contended record è sempre la **stessa** Task, e AcceptTask
  è ristretto al solo worker che ha ClaimNextReadyTask.

## Out of scope

- Reaper (PR-05).
- Ingestione completa del report (PR-06).
- Recovery Task orfane (vedi DoD §10).
