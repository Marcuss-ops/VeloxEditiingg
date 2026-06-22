# PR-11 — Pre-flight empirical reconciliation

> **Audit anchor:** riconsiderazione complessiva di `LEGACY_SSOT_AUDIT.md` vs codice reale
> **Target milestone:** pre-cutover (PREREQUISITO per PR-01..PR-10).
> **Branch:** `cutover/pr-11-pre-flight-reconciliation`
> **Dipendenze:** nessuna.

## Contesto

L'analisi empirica del codice, condotta PRIMA di aprire PR-01..PR-10,
ha rivelato scostamenti sistematici tra la fotografia dell'audit e lo
stato reale di `main`:

| Audit claim | Stato reale |
|---|---|
| [P0.3](../LEGACY_SSOT_AUDIT.md#p03--attempt-id-doppio) Attempt ID = lease ID | **REFUTED**: `attempt_id` è campo separato in `TaskOffer` (`proto/velox/control/worker_control.proto:134`), in `task_attempts.id` (SQLite), e consumato indipendentemente in `worker.go:370` + `job_executor.go:88`. Nessun `AttemptID = leaseID` residuo. |
| [P1.3](../LEGACY_SSOT_AUDIT.md#p13--payload-duplicato-top-level-e-parameters) `parameters` mirror top-level | **CLAIM INVERTITA**: `parameters` È il canonical, NON un mirror. `enqueue.go:281`, `calendar_payload.go:79`, `smoke_clip_stock.go:123`, `renderplan.go:59`, e i reader in `asset_service.go:365-471` confermano un envelope singolo. |
| [P1.4](../LEGACY_SSOT_AUDIT.md#p14--taskresult-non-ingerito-completamente) TaskResult non ingerito | **REFUTED**: `pb.TaskResult` typed (metrics, phase_markers, output_artifacts, executor_id, executor_key, attempt_id, lease_id, error_code, error_detail) è già inviato da `job_executor.go:159 submitTaskResult` e gestito da `handler_jobs.go:255 handleTaskResult`. Tabelle `task_attempt_reports`, `task_phase_timings`, `task_attempt_metrics`, `artifact_attachments` esistenti e popolate. |
| [P1.6](../LEGACY_SSOT_AUDIT.md#p16--drain-outbox-troppo-ampio) drain outbox generico | **REFUTED**: `DrainLegacyEvents(ctx, legacyTypes)` è già mirato (`outbox/store.go:334`). Nessuno sweep generico a ogni boot. |
| [P0.4](../LEGACY_SSOT_AUDIT.md#p04--lease-task-senza-expiry-persistita) reaper Job assente | **PARZIALMENTE REFUTED**: `jobs.Writer.RequeueExpiredLeases` esiste (`jobs/repository.go:102`) e gira in `bootstrap.go:384`. Ma opera su `jobs.lease_expiry`, colonna droppata in 048 ⇒ post-048 potrebbe essere silenziosamente rotto (vedi PR-13). |
| [P1.5](../LEGACY_SSOT_AUDIT.md#p15--start-task-e-creazione-attempt-non-atomici) transazione non atomica | **DA VERIFICARE**: `handleTaskResult` esegue `taskRepo.SetStatus(...)` e `taskAttemptRepo.CompleteFinal(...)` in rapida successione (`handler_jobs.go:281, 287`) ma non è confermato che siano in unica tx. PR-04 deve partire da una **lettura completa** dell'handler. |

Serve una PR puramente documentale che sanisca la matrice prima di
aprire le PR-01..PR-10.

## Scope

1. Annotare `docs/architecture/LEGACY_SSOT_AUDIT.md` con cross-link
   puntuali ai file reali per ogni claim confermato/confutato.
2. Aggiornare la sezione §3 ("Problemi già risolti") inserendo come
   **collassati** i claim che l'analisi empirica ha già chiuso:
   - §3.7 va rafforzato con riferimenti concreti (proto, .pb.go, worker code).
3. Aggiornare la §4 e §5 marcando come **[REFUTED]** i claim che
   risultano falsi o già risolti, mantenendo i link ai file.
4. Aggiornare il §10 (Definition of Done) togliendo le voci rese
   inert dal cutover reale.
5. Aggiungere un'appendice "Matrice di copertura effettiva" che elenca,
   per ogni PR-01..PR-16, lo status (aperto / collassato / fuso).
6. Aggiornare `docs/architecture/legacy-cutover-followups/README.md`
   per riflettere la sequenza rivista (PR-11 entra prima di PR-01).

## Files to touch

```text
docs/architecture/LEGACY_SSOT_AUDIT.md
docs/architecture/legacy-cutover-followups/README.md
```

## Sequenza operativa

```text
1. CENSIRE ogni claim P0/P1/§3 del audit.
2. Per ognuno, allegare cross-link al file .go/.sql/.proto che lo
   conferma o lo confuta.
3. Aggiungere un tag di stato: [VERIFIED] / [REFUTED] / [PARTIAL].
4. Aggiungere l'appendice "Matrice di copertura effettiva" con il
   mapping PR-NN ↔ claim §4-§5.
5. Aggiornare il README delle followup con la nuova sequenza (PR-11
   prima, PR-15 invece di PR-09 nella numerazione originale ove più
   fedele al codice reale).
```

## Acceptance criteria

- [ ] L'audit aggiornato cita ALMENO un file `.go/.sql/.proto` per ogni
      claim P0/P1 e ogni claim §3.
- [ ] L'appendice "Matrice di copertura" elenca almeno 16 righe
      (PR-11..PR-16 + PR-01..PR-10 mappati sui claim residui).
- [ ] Il README delle followup include PR-11 come PREREQUISITO.
- [ ] Nessuna claim residua "da verificare in code review" rimasta.

## Test

N/A (documentale).

## CI guards introdotti

Nessuno (PR puramente documentale).

## Rischi

- Rischio politico minimo: questa PR ribadisce che alcune PR del piano
  originale sono collassate. È esattamente lo scopo.

## Out of scope

- Qualsiasi modifica di codice, migration SQL, o script CI eseguibile.
- Decisioni di esecuzione (es. "PR-03 diventerà un no-op perché
  refutata"): tali decisioni si prendono nelle singole PR di codice.
