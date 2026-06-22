# PR-02 — Restore single SUCCEEDED writer

> **Audit anchor:** [§P0.2](../LEGACY_SSOT_AUDIT.md#p02--due-writer-di-jobsstatus--succeeded)
> **Target milestone:** cutover P0
> **Branch:** `cutover/pr-02-single-succeeded-writer`
> **Dipendenze:** PR-01 (artifact finalization deve essere coerente
> prima di dichiarare single-writer).

## Contesto

L'audit evidenzia **due** percorsi che oggi possono scrivere
`jobs.status = SUCCEEDED`:

```text
TaskResult path
  └► grpcserver.handleTaskResult → maybeTransitionJob → jobsRepo.SetStatus(SUCCEEDED)

Artifact path
  └► artifacts.Service → finalizzatore → jobs.status = SUCCEEDED
```

Questo viola la regola "single writer" di `OWNERSHIP.md` e permette a un
Job di essere dichiarato `SUCCEEDED` anche quando l'artifact non è stato
verificato (download mancante, checksum errato, blob corrotto).

## Scope

- Eliminare `maybeTransitionJob → SetStatus(SUCCEEDED)`.
- Introdurre (se non esiste) lo stato `AWAITING_ARTIFACT` sul Job.
- Garantire che `jobs.StatusSucceeded` sia scritto solo da
  `velox-server/internal/artifacts/`.
- Aggiungere una CI guard full-tree che fallisce se `StatusSucceeded` o
  `'SUCCEEDED'` compare come valore di `jobs.status` fuori da quel
  package.

## Files to touch

```text
velox-server/internal/grpcserver/handler_jobs.go              # handleTaskResult
velox-server/internal/jobs/lifecycle_service.go               # maybeTransitionJob
velox-server/internal/grpcserver/handler_artifacts.go         # conferma ruolo artifacts
velox-server/internal/jobs/status_types.go (o equivalente)     # AWAITING_ARTIFACT
velox-server/internal/jobs/sqlite_jobs_writer.go              # transizioni consentite
velox-server/internal/store/jobs_writer_types.go              # firme
scripts/ci/check-single-writer.sh                             # nuova guard
scripts/ci/check-no-legacy.sh                                 # hard-blacklist eventuale
```

## Sequenza operativa

```text
1. CENSIRE: trovare ogni SetStatus(SUCCEEDED) e ogni
   UPDATE jobs SET status='SUCCEEDED' in tree.
2. IN jobs/lifecycle_service.go:
     - maybeTransitionJob passa a transizioni:
         * Task f definitivamente fallita → Job FAILED,
         * Tutte le Task SUCCEEDED ma artifact NON verificato →
           Job AWAITING_ARTIFACT (o resta RUNNING se AWAITING non esiste),
         * Tutte le Task SUCCEEDED E artifact verificato →
           NON scrivere: delega a artifacts.Service (vedi 3).
3. IN artifacts.Service:
     - dopo SUCCEEDED di TaskAttempt + Task,
     - verificare che tutte le Task siano SUCCEEDED e l'artifact sia READY,
     - scrivere jobs.status=SUCCEEDED in modo atomico (transazione
       con SetAggregateStatus).
4. CI guard full-tree: se trova StatusSucceeded / 'SUCCEEDED' / SET status='SUCCEEDED'
   in INSERT/UPDATE al di fuori di velox-server/internal/artifacts/ → fail.
5. Test che dimostrino che una Task SUCCEEDED senza artifact non
   completa il Job.
```

## Acceptance criteria

- [ ] `golangci-lint` non segnala regressions.
- [ ] `grep -R "StatusSucceeded" velox-server/` mostra **solo**
      `velox-server/internal/artifacts/`.
- [ ] `grep -R "'SUCCEEDED'" velox-server/` nei contesti
      `UPDATE jobs`, `INSERT INTO jobs`, `SetStatus(...)` mostra
      **solo** `velox-server/internal/artifacts/`.
- [ ] Test `TaskAllSucceededNoArtifactDoesNotCompleteJob` verde.
- [ ] CI guard full-tree pass.

## Test

- **Unit:**
  - `lifecycle_service_test.go`: transizioni consentite/vietate per
    `maybeTransitionJob`.
  - `artifacts/service_test.go`: happy path AWAITING → SUCCEEDED.
- **Integration:** due Test:
  - tutte le Task SUCCEEDED **senza** artifact → Job resta `RUNNING/AWAITING_ARTIFACT`;
  - tutte le Task SUCCEEDED **con** artifact READY → Job `SUCCEEDED` una sola volta.
- **Race:** due finalize concorrenti, atteso un solo commit SUCCEEDED.

## CI guards introdotti

```bash
# scripts/ci/check-single-writer.sh — nuova regola "single_succeeded_writer"
```

Pattern vietati fuori da `velox-server/internal/artifacts/`:

```text
StatusSucceeded
'SUCCEEDED'
"SUCCEEDED"
SET status = 'SUCCEEDED'
SET status="SUCCEEDED"
```

## Rischi

- Se dimentichiamo un sito, **un Job può ancora comparire come
  completato senza artifact**. Mitigazione: la CI guard fallisce
  prima del merge.
- Compatibilità: vecchi handler `JOB_SUCCEEDED` su outbox non devono
  scrivere (vedi PR-06 ingestione). Mantenere shim commentato per
  30 giorni.

## Out of scope

- Rimozione handler outbox generici (PR-10 + audit §P1.6).
- Eliminazione del BOOT reaper Job (PR-05).
- Riscrittura del payload `TaskResult` (PR-06).
