# PR-06 — TaskReportIngestionService

> ⚠️ **STATUS (PR-11, 22 giu 2026): CLOSED — no-op di verifica (doc-only closure).**
>
> L'analisi empirica dei file reali (vedi **Appendice A** in
> [PR-11 — Pre-flight empirical reconciliation](./PR-11-pre-flight-empirical-reconciliation.md))
> ha **confutato** la claim §P1.4 dell'audit: il typed `TaskResult`
> è **già pienamente ingerito** dal flusso `handleTaskResult` +
> `taskattempts.Repository` + `task.transition-task-batch` nel
> codice attuale, in modalità idempotente e versionata. Prove in
> linea:
>
> - `DataServer/internal/grpcserver/handler_jobs.go:280` (`handleTaskResult`
>   consuma lo `pb.TaskResult` con tutti i campi: `OutputArtifacts`,
>   `ErrorCode`, `ErrorDetail`, `LeaseId`, `AttemptId`, `Status`).
> - `DataServer/internal/store/sqlite_task_attempt_repository.go`
>   (`SetStatus` CAS su `report_version`, `PersistPhaseTimings`,
>   `PersistMetrics`).
> - PR-04 / §9.5 (vedi
>   [PR-04-atomic-task-acceptance.md](./PR-04-atomic-task-acceptance.md))
>   garantisce l'atomicità richiesta dalla sezione "Sequenza
>   operativa" sotto. La creazione di un nuovo servizio
>   `TaskReportIngestionService` è obsoleta: l'ingestion è già
>   orchestrata in modo atomico dai metodi `AcceptTaskAtomic` +
>   `TransitionTaskToTerminalAtomic`.
>
> **Nessuna modifica di codice è richiesta per chiudere §P1.4.**
> La presente PR è esclusivamente documentale: il design sotto è
> preservato come record storico della formulazione pre-analisi
> (introdotta pensando a un servizio dedicato che poi si è rivelato
> non necessario). La tabella di marcia è aggiornata a *no-op*.
> Per la matrice completa di copertura audit → codice, riferimento
> obbligatorio all'Appendice A in PR-11.
>
> **Convenzione di chiusura:** questa è una PR puramente
> documentale (`docs-only`). Non richiede code-review né guardie CI
> perché non tocca codice, sql, proto o scripts: ha il solo scopo
> di lasciare in `git log` il tracciato della claim originale così
> che chi legge `git blame` trovi immediatamente l'evidenza della
> confutazione. Il commit message porterà il prefisso
> `docs(cutover):` per distinguerlo dai commit di codice delle
> altre PR del cutover.

> **Audit anchor:** [§P1.4](../LEGACY_SSOT_AUDIT.md#p14--taskresult-non-ingerito-completamente) + cross-link §P0.1 e §P0.2.
> **Target milestone:** cutover (bloccante per PR-08).
> **Branch:** `cutover/pr-06-task-report-ingestion-service`
> **Dipendenze:** PR-01, PR-02, PR-03, PR-04. PR-05 desiderabile ma non bloccante.

## Contesto

Il protobuf `TaskResult` espone campi ricchi (`metrics`, `phase_markers`,
`output_artifacts`, `executor_id`, `executor_key`, `attempt_id`,
`lease_id`, `error_code`, `error_detail`). L'handler gRPC attuale non
li utilizza tutti e mescola la validazione identitaria con la
persistenza, producendo side-effect parziali.

## Scope

- Creare un servizio unico `TaskReportIngestionService` nel nuovo
  (o co-locato in `internal/taskattempts/`) che:
  1. valida identità (coerente con PR-03: `(task_id, attempt_id,
     job_id, worker_id, lease_id)`),
  2. applica idempotenza per `report_version` (replay-safe),
  3. salva report versionato,
  4. salva phase timings,
  5. salva metriche,
  6. registra output artifacts (`artifact_attachments`),
  7. completa TaskAttempt (`PENDING|RUNNING|RENDER_FINISHED → SUCCEEDED|FAILED|TIMED_OUT`),
  8. aggiorna Task,
  9. sblocca dipendenze,
  10. richiede finalizzazione Job (delega a `artifacts.Service`).
- Nessun handler gRPC deve scrivere direttamente più di un repository
  in ordine non atomico.

## Files to touch

```text
velox-server/internal/taskattempts/ingestion_service.go       # nuovo
velox-server/internal/taskattempts/repository.go
velox-server/internal/tasks/lifecycle.go                     # sblocco dipendenze
velox-server/internal/artifacts/service.go                   # caller della richiesta di finalize
velox-server/internal/grpcserver/handler_workers.go          # delega ingestione
velox-server/internal/observability/service.go               # nuovi metriche
velox-server/internal/store/sqlite_task_attempt_repository.go
velox-server/internal/store/migrations/sqlite/051_report_version.sql  # cronologia report
proto/velox/control/worker_control.proto                     # regen pb
```

## Sequenza operativa

```text
1. handler_workers.go::handleTaskResult → delega a
   TaskReportIngestionService.Ingest(ctx, report).

2. Ingest:
     a) ValidateIdentity (PR-03).
     b) Idempotency: SELECT report_versions WHERE id=attempt_id AND
        version=?. Se esiste, ritorna OK senza riscritture.
     c) Atomic write in transazione unica:
          - INSERT INTO task_attempt_reports (... report_version, blob),
          - INSERT INTO task_phase_timings (attempt_id, phase, ...),
          - INSERT INTO task_attempt_metrics (attempt_id, key, value, unit),
          - INSERT INTO artifact_attachments (attempt_id, artifact_id),
          - UPDATE task_attempts.status, finished_at, error.
        Se un singolo statement fallisce, ROLLBACK.
     d) Commit.
     e) Sblocco dipendenze Task (coda interna al lifecycle service).
     f) Job finalization richiesta (artifacts.Service).

3. NO scrittura diretta su tasks o jobs da handler Workers.
```

## Acceptance criteria

- [ ] Tutti i 10 step sono svolti atomicamente ogniqualvolta i dati
      sono presenti nel report.
- [ ] Replay di un report già visto è idempotente (no double status flip,
      no doppio `SUCCEEDED`).
- [ ] Le tabelle `task_attempt_reports`, `task_phase_timings`,
      `task_attempt_metrics`, `artifact_attachments` esistono e sono
      popolate.
- [ ] Nessun write diretto `tasks` / `jobs` in `handler_workers.go`
      resta.
- [ ] Test E2E verde: `task_executor_report_full_pipeline_test`.

## Test

- **Unit:**
  - `ingestion_service_test.go`: 10 step coperti, errori intermedi.
  - idempotenza.
- **Integration:**
  - invio di un report completo; verificare tutte e 4 le tabelle
    popolate e lo stato terminale corretto.
  - invio di un report con solo metrics → la persistenza è parziale ma
    idempotente.
- **Race:** doppio report identico → una sola transizione.
- **Invariant:** invarianti §9.5 e §9.6 dell'audit restano soddisfatte
  (Task RUNNING con Attempt attivo; lease attiva con expiry).

## CI guards introdotti

In `check-no-legacy.sh` (full-tree):

```text
# Qualsiasi INSERT/UPDATE diretto a:
#   task_attempt_reports, task_phase_timings, task_attempt_metrics,
#   artifact_attachments
# al di fuori di velox-server/internal/taskattempts/ è vietato.
```

In `check-single-writer.sh`: `tasks.status` aggiornabile solo da
`taskgraph`/`taskattempts`/`artifacts` (post PR-02, post PR-06).

## Rischi

- Performance: insert multi-tabella in una sola transazione. Su 100
  worker in burst si può saturare WAL. Mitigazione: usare
  `PRAGMA journal_mode=WAL` (già attivo?) + batched commit. Misurare
  con golden E2E.
- Storage: la tabella `task_attempt_reports` cresce linearmente con
  gli attempt. Prevedere pruning (rinviato: PR futura oltre DoD).

## Out of scope

- Eliminazione legacy handler outbox (PR-10).
- Drain outbox storico (PR-10 §9 + §P1.6).
- Rimozione completa dei vecchi `Result` kafka routes se esistono.
