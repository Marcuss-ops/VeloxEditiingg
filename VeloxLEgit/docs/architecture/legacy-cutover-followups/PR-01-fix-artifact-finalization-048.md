# PR-01 — Fix artifact finalization post-migration 048

> **Audit anchor:** [§P0.1](../LEGACY_SSOT_AUDIT.md#p01--finalizzazione-artifact-incompatibile-con-migration-048)
> **Target milestone:** cutover P0
> **Branch:** `cutover/pr-01-fix-artifact-finalization-048`
> **Dipendenze:** nessuna (è la prima P0).

## Contesto

Migration `048_drop_jobs_runtime_columns.sql` elimina dalla tabella `jobs`
le colonne runtime `assigned_to`, `claimed_by`, `lease_id`, `lease_expiry`,
`retry_count`.

Il finalizzatore artifact (`velox-server/internal/artifacts.Service`
e/o `velox-server/internal/artifacts/storage.go`) contiene ancora query
del tipo `UPDATE jobs SET status='SUCCEEDED', lease_id=NULL, lease_expiry=NULL
WHERE job_id=? AND status='RUNNING' AND assigned_to=? AND lease_id=?`.

A seguito della 048 queste query **rompono** lo schema: SQLite restituisce
`no such column`. Il Job resta bloccato in `RUNNING` e l'utente vede un
completamento artifact che non produce mai `SUCCEEDED`.

## Scope

- Sostituire la verifica di identità sul **Job** con la verifica di
  identità sulla **TaskAttempt** che sta producendo l'artifact.
- Rimuovere ogni riferimento a colonne runtime della tabella `jobs`
  dai file del package `velox-server/internal/artifacts/`.
- Far transitare `worker_id` e `lease_id` attraverso un `AttemptContext`
  passato dal caller (handler gRPC o ingestione report — cfr. PR-06).
- Aggiungere test integration che:
  1. applichino la 048 su un DB di test,
  2. eseguano il path completo di finalizzazione artifact,
  3. asseriscano che il Job passa a `SUCCEEDED` *e* che nessuna colonna
     runtime del Job sia stata letta o scritta.

## Files to touch

```text
velox-server/internal/artifacts/service_finalize.go      # logica principale
velox-server/internal/artifacts/storage.go                # rimozione CAS su colonne Job
velox-server/internal/artifacts/sqlite_finalization_repository.go
velox-server/internal/artifacts/success_path_test.go     # nuovi test
velox-server/internal/artifacts/finalization_repository.go # nuove signature
velox-server/internal/grpcserver/handler_artifacts.go    # caller: passa Attempt ID
velox-server/internal/taskattempts/*.go                  # lookup AttemptContext
```

## Sequenza operativa

```text
1. Caller (handler_artifacts.go) riceve un report/result riferito a un
   TaskAttempt. Passa attempt_id, task_id, job_id al finalizzatore.
2. artifacts.Service.LoadAttemptContext(attempt_id):
     - legge task_attempts per attempt_id+task_id+job_id,
     - verifica worker_id==caller_worker_id e lease_id==caller_lease_id,
     - ritorna AttemptContext{Status, WorkerID, LeaseID}.
3. Se Attempt.Status in {RUNNING, RENDER_FINISHED}, l'Attempt è legittimo.
4. CAS atomicamente su artifact: status PASS -> READY su artifact_id.
5. CAS atomicamente su task_attempts: id=attempt_id, status RUNNING -> SUCCEEDED.
6. Se l'Attempt era l'ultimo, marcare task.status PASSED.
7. Se tutte le task sono PASSED, marcare jobs.status=SUCCEEDED.
   ⚠️ Solo qui viene toccato jobs.status, e solo dentro il package
      artifacts_service.go. Vedi PR-02 per il single-writer CI guard.
```

## Acceptance criteria

- [ ] Nessuna query SQL in `velox-server/internal/artifacts/` cita colonne
      runtime di `jobs` (né in lettura né in scrittura).
- [ ] Il CAS finale del Job usa `jobs.SetAggregateStatus` (vedi PR-08) e
      non SQL diretto.
- [ ] Il flusso `success_path_test.go` produce `SUCCEEDED` su schema
      post-048 senza manomettere SQLite.
- [ ] Replay dello stesso report è idempotente (nessun doppio
      `SUCCEEDED`, nessuna regressione di conteggio artifact).

## Test

- **Unit:** `success_path_test.go` con mock `TaskAttemptRepository`.
- **Integration:** DB SQLite migrato fino a 048 + 049 (placeholder per
  eventuali migration richieste dal PR), esecuzione completa
  start → finalize → SUCCEEDED.
- **Race:** doppio finalize concorrente, atteso un solo `SUCCEEDED`.
- **Invariant:** nessun riferimento a `assigned_to` / `lease_id` /
  `claimed_by` / `lease_expiry` / `retry_count` viene letto/scritto in
  `velox-server/internal/artifacts/`.

## CI guards introdotti

Nessuna nuova guard (PR-10 le consolida). Però questa PR **non deve far
fallire** `scripts/ci/check-migrations.sh` né `check-single-writer.sh`.

## Rischi

- Blast radius: medio. Il finalizzatore artifact è sulla hot path di
  completamento Job.
- Rollback: il revert è sicuro finché il caller non viene aggiornato;
  mantenere il vecchio path dietro flag `VELOX_LEGACY_FINALIZER` con
  deadline 30 giorni (pattern "compatibility shim" definito in
  `OWNERSHIP.md`).
- Monitoring: alerting su `jobs.status` bloccato in `RUNNING` > 5 min.

## Out of scope

- Eliminazione del BOOT Worker reaper per Job (PR-05 se ne occupa).
- Single-writer CI guard per `SUCCEEDED` (PR-02 + PR-10).
- Normalizzazione del payload in ingresso (PR-09).
- Validazione completa del report (PR-06).
