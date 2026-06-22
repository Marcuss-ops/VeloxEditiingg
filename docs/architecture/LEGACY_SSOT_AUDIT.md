# VeloxEditing — Legacy, Single Source of Truth e Runtime Cutover Audit

**Repository:** `Marcuss-ops/VeloxEditiingg`
**Branch verificato:** `main`
**Commit verificato:** `239007117a4319f84626f8b00ac88cc19e953a21`
**Data audit:** 22 giugno 2026
**Stato generale:** Documento **riconciliato** con lo stato empirico del codice (PR-11 — vedi [Appendice A](#appendice-a--matrice-di-copertura-effettiva)). La migrazione è avanzata ma non ancora sicura per produzione; quattro claim originari risultano confutati/invertiti e sono stati marcati come tali in §4 e §5.

> **Documento di snapshot.** Fotografa lo stato di `main` al commit indicato
> e definisce le operazioni necessarie per completare il passaggio al
> runtime Task-native.
>
> La regola architetturale principale è:
> > Ogni stato importante deve avere un solo owner, un solo writer e un solo percorso di mutazione.
>
> **Status legend**:
> - `[CONFIRMED]` claim verificata empiricamente contro il codice — lavoro ancora richiesto.
> - `[PARTIAL]` claim parzialmente confermata; dettagli in PR specifica.
> - `[REFUTED]` claim confutata: l'analisi del codice mostra che il problema è già risolto in main. La PR corrispondente collassa a no-op di verifica.
> - `[INVERTITA]` claim è vera ma nella direzione opposta (es. "X è un mirror" vs "X è il canonicale").

---

## Indice

1. [Obiettivo](#1-obiettivo)
2. [Architettura target](#2-architettura-target)
3. [Problemi già risolti](#3-problemi-gi%C3%A0-risolti)
4. [Problemi P0](#4-problemi-p0)
5. [Problemi P1](#5-problemi-p1)
6. [Ownership finale](#6-ownership-finale)
7. [Componenti da eliminare](#7-componenti-da-eliminare)
8. [Sequenza Pull Request consigliata](#8-sequenza-pull-request-consigliata)
9. [Guard CI da aggiungere](#9-guard-ci-da-aggiungere)
10. [Definition of Done](#10-definition-of-done)
11. [Decisione finale](#11-decisione-finale)

---

## 1. Obiettivo

Questo documento definisce:

- quali componenti legacy sono già stati eliminati;
- quali doppie fonti di verità sono ancora presenti;
- quali percorsi runtime devono essere rimossi;
- quale componente deve possedere ogni stato;
- l'ordine consigliato delle prossime Pull Request;
- i controlli CI necessari per impedire regressioni.

---

## 2. Architettura target

### 2.1 Job

`Job` rappresenta l'aggregato business.

**Responsabilità consentite:**

- identità del lavoro;
- progetto e render plan associati;
- stato business aggregato;
- priorità e requisiti di placement;
- timestamp business;
- stato finale derivato dalle Task e dagli artifact.

**Responsabilità vietate:**

- worker assegnato;
- lease runtime;
- attempt runtime;
- claim del worker;
- stato dell'esecuzione singola;
- risultato tecnico usato come fonte di verità.

### 2.2 Task

`Task` è l'unica unità schedulabile.

**Responsabilità:**

- executor ID e versione;
- status operativo;
- priority;
- worker ID;
- lease ID;
- lease expiry;
- numero tentativi;
- dipendenze;
- readiness;
- timestamp di avvio e completamento.

### 2.3 TaskAttempt

`TaskAttempt` è l'unica fonte di verità per una singola esecuzione.

**Responsabilità:**

- attempt ID;
- task ID;
- job ID;
- worker ID;
- lease ID;
- attempt number;
- status;
- report version;
- metriche;
- phase timings;
- errore;
- output prodotti.

### 2.4 Artifact

`artifacts.Service` e il relativo repository sono l'unico gate autorizzato
per portare un Job a `SUCCEEDED`.

Un worker che dichiara una Task riuscita non deve poter completare direttamente il Job.

---

## 3. Problemi già risolti

I seguenti problemi risultano corretti su `main`.

### 3.1 Package workflow rimosso

Sono stati eliminati:

- `DataServer/internal/workflow`;
- writer SQL workflow;
- `WriteEnabled`;
- tipi workflow usati dal dominio;
- test che riattivavano il writer;
- vecchi metodi `CreateRun`, `MarkStepRunning`, `CompleteStep`, `FailStep`, `CancelRun`.

### 3.2 Handler outbox no-op rimossi

Sono stati eliminati gli handler che ricevevano eventi e restituivano `nil` senza eseguire operazioni.

La CI impedisce la reintroduzione di:

- `StepReadyHandler`;
- `JobSucceededHandler`;
- `ArtifactReadyHandler`;
- `DeliveryCreatedHandler`.

### 3.3 HTTP orchestrator disaccoppiato da workflow

L'adapter `/api/v1/orchestrator/*` usa DTO dedicati in:

```text
DataServer/internal/handlers/server/orchestratorv1
```

Il package HTTP non dipende più da `internal/workflow`.

### 3.4 Creazione atomica Job + Task

`Enqueuer` non utilizza più il vecchio `JobQueue.SubmitJob`.

Il flusso corrente è:

```text
Input
  -> normalizzazione
  -> asset resolution
  -> compilazione Job + TaskSpec
  -> AtomicJobTaskCreator.CreateJobWithTask
```

Questo impedisce la creazione di Job eseguibili senza Task canonica.

### 3.5 Requirements spostati fuori dai JSON

`JobRequirements` viene letto da colonne dedicate.

Non deve più esistere:

```json
{
  "_requirements": {}
}
```

dentro `request_json` o `result_json`.

### 3.6 Costmodel worker-side eliminato

La formula di placement è posseduta dal master.

Il worker pubblica capability e descriptor, ma non deve mantenere una copia della formula master-side.

### 3.7 Dispatch Task-native introdotto `[STRONG: cross-link verificati]`

Sono presenti e **completi** (verifica empirica PR-11):

- `ClaimNextReadyTask` — `DataServer/internal/taskgraph/lifecycle.go` + `sqlite_task_repository.go:227` (CAS `SET status='LEASED'`).
- `TaskOffer` typed — `proto/velox/control/worker_control.proto:259`, `TaskOffer` con campi propri (`task_id`, `job_id`, `attempt_id`, `executor_id`, `executor_version`, `task_spec` structpb, `lease_id`, `lease_deadline` timestamppb, `requirements`).
- `TaskAccepted` — `proto/velox/control/worker_control.proto:274`, handler `DataServer/internal/grpcserver/handler_jobs.go:112`.
- `TaskRejected` — `proto/velox/control/worker_control.proto:282`, handler `handler_jobs.go:220`.
- `TaskLeaseGranted` — `proto/velox/control/worker_control.proto:298`, handler `handler_jobs.go:186` (incluso nella stessa transizione di claim).
- `TaskResult` typed completo — `proto/velox/control/worker_control.proto:131`, contiene `metrics`, `phase_markers`, `output_artifacts`, `executor_id`, `executor_key`, `attempt_id`, `lease_id`, `error_code`, `error_detail`. Worker invia da `RemoteCodex/native/worker-agent-go/internal/worker/job_executor.go:159 submitTaskResult`.
- Verifica dipendenze — `DataServer/internal/taskgraph/lifecycle.go` (transizione READY→LEASED gated su tutte le dipendenze terminali).
- `TaskAttempt` repository — `DataServer/internal/store/sqlite_task_attempt_repository.go` (CRUD + `CompleteFinal` con CAS su `(id, worker_id, lease_id, status)`).

---

# 4. Problemi P0

I seguenti problemi devono essere corretti prima di considerare il runtime production-ready.

> **Esito dell'analisi empirica (PR-11)**:
> - P0.1 ✅ **CONFERMATO** — query artifact finale usa colonne droppate in 048.
> - P0.2 ✅ **CONFERMATO** — due writer di `SUCCEEDED` (handler_jobs.go:309,309 + artifact finalizer).
> - P0.3 ❌ **CONFUTATO** — vedi Appendice A: già risolto in main con `attempt_id` come campo separato.
> - P0.4 ⚠️ **PARZIALMENTE CONFERMATO** — `tasks.lease_expires_at` manca ma reaper Job esiste ed è potenzialmente rotto post-048 (vedi PR-13).

## P0.1 — Finalizzazione artifact incompatibile con migration 048

### Problema

La migration:

```text
DataServer/internal/store/migrations/sqlite/048_drop_jobs_runtime_columns.sql
```

elimina dalla tabella `jobs`:

```text
assigned_to
claimed_by
lease_id
lease_expiry
retry_count
```

Il finalizzatore artifact continua però a utilizzare alcune di queste colonne nella query che porta il Job a `SUCCEEDED`.

La query corrente usa ancora concetti simili a:

```sql
UPDATE jobs
SET status = 'SUCCEEDED',
    lease_id = NULL,
    lease_expiry = NULL
WHERE job_id = ?
  AND status = 'RUNNING'
  AND assigned_to = ?
  AND lease_id = ?
```

Dopo la migration questa query è incompatibile con lo schema.

### Impatto

- completamento artifact fallito;
- Job bloccato;
- errore runtime `no such column`;
- impossibilità di raggiungere correttamente `SUCCEEDED`.

### Correzione richiesta

La proprietà dell'esecuzione deve essere verificata attraverso `TaskAttempt`.

Sequenza target:

```text
1. Carica TaskAttempt tramite attempt_id.
2. Verifica task_id, job_id, worker_id e lease_id.
3. Verifica che l'Attempt sia RUNNING o RENDER_FINISHED.
4. Verifica upload e artifact.
5. Porta artifact a READY.
6. Porta TaskAttempt a SUCCEEDED.
7. Porta Task a SUCCEEDED.
8. Porta Job a SUCCEEDED solo se tutte le condizioni finali sono soddisfatte.
```

### Criteri di completamento

- nessuna query artifact usa colonne runtime della tabella `jobs`;
- nessun CAS artifact verifica worker o lease sul Job;
- worker e lease vengono verificati su `task_attempts`;
- test con schema post-migration 048 verdi;
- test di finalizzazione reale verdi.

## P0.2 — Due writer di `jobs.status = SUCCEEDED`

### Problema

Il finalizzatore artifact è dichiarato come unico writer di `SUCCEEDED`.

Tuttavia `handleTaskResult` richiama `maybeTransitionJob`, che può eseguire:

```go
jobsRepo.SetStatus(..., jobs.StatusSucceeded)
```

quando tutte le Task risultano riuscite.

Questo crea due writer:

```text
TaskResult path
  -> Job SUCCEEDED

Artifact finalization path
  -> Job SUCCEEDED
```

### Impatto

- Job completato senza artifact verificato;
- divergenza tra stato Job e stato artifact;
- possibilità di mostrare successo senza output disponibile;
- violazione del single-writer rule.

### Correzione richiesta

`maybeTransitionJob` non deve mai scrivere `SUCCEEDED`.

Comportamento consentito:

```text
Task fallita definitivamente
  -> Job FAILED

Task completate, artifact non verificati
  -> Job AWAITING_ARTIFACT oppure resta RUNNING

Task completate e artifact verificati
  -> artifacts.Service porta il Job a SUCCEEDED
```

### Criteri di completamento

- `jobs.StatusSucceeded` scritto solo dal package artifacts;
- nessun handler gRPC completa direttamente un Job;
- guard CI full-tree contro nuovi writer `SUCCEEDED`;
- test che dimostrano che Task riuscita senza artifact non completa il Job.

## P0.3 — Attempt ID doppio

### Problema

Nel `TaskOffer` il master usa il lease ID come attempt ID.

Successivamente, dopo `TaskAccepted`, crea un `TaskAttempt` con un nuovo UUID.

Il worker conserva l'Attempt ID ricevuto nel primo `TaskOffer`, mentre il database contiene un ID diverso.

Esempio:

```text
TaskOffer.attempt_id = l-worker-abc
TaskAttempt.id       = 7a5f...-uuid
```

### Impatto

- report non correlabile in modo forte;
- impossibilità di usare `attempt_id` come chiave autorevole;
- validazione report debole;
- possibile accettazione di report stale;
- audit trail incoerente.

### Correzione richiesta

L'Attempt ID deve essere creato una sola volta dal master prima dell'offerta.

Sequenza target:

```text
Claim Task
  -> crea Attempt ID definitivo
  -> persiste TaskAttempt PENDING
  -> invia TaskOffer con lo stesso attempt_id
  -> worker accetta
  -> Task LEASED -> RUNNING
  -> TaskAttempt PENDING -> RUNNING
  -> invia TaskLeaseGranted
  -> worker avvia l'esecuzione
```

### Criteri di completamento

Il report viene accettato solo se coincidono:

```text
task_id
attempt_id
job_id
worker_id
lease_id
attempt_number
status RUNNING
```

## P0.4 — Lease Task senza expiry persistita

### Problema

Il master calcola una deadline di lease e la invia nel `TaskOffer`, ma la tabella `tasks` non conserva `lease_expires_at`.

Il reaper corrente è ancora orientato ai Job.

Una Task già accettata e in stato `RUNNING` può rimanere bloccata per sempre se il worker scompare.

### Impatto

```text
Task RUNNING
  -> worker crash
  -> nessun TaskResult
  -> nessuna expiry persistita
  -> nessun Task reaper
  -> Task zombie
```

### Correzione richiesta

Aggiungere:

```sql
ALTER TABLE tasks ADD COLUMN lease_expires_at TEXT;
```

Aggiungere al repository Task:

```go
RenewLease(...)
RequeueExpiredLeases(...)
```

Comportamento reaper:

```text
LEASED scaduta
  -> READY

RUNNING scaduta con retry disponibile
  -> Attempt TIMED_OUT
  -> Task READY

RUNNING scaduta con retry esauriti
  -> Attempt FAILED
  -> Task FAILED
  -> Job aggregato FAILED
```

### Criteri di completamento

- lease deadline persistita;
- lease renewal Task-scoped;
- reaper Task registrato nel supervisor;
- vecchio reaper Job rimosso;
- test crash/reconnect/expiry verdi.

---

# 5. Problemi P1

> **Esito dell'analisi empirica (PR-11)**:
> - P1.1 ✅ **CONFERMATO** — proto+worker hanno entrambi i path Job e Task.
> - P1.2 ⚠️ **PARZIALMENTE CONFERMATO** — `jobs.Writer` ha solo `RenewLease`, `RequeueExpiredLeases` come residui runtime; il resto è già stato semplificato.
> - P1.3 🔁 **CLAIM INVERTITA** — `parameters` è il canonical, NON un mirror. Vedi PR-15 (sostituisce PR-09).
> - P1.4 ❌ **CONFUTATA** — typed TaskResult è completo e popolato. PR-06 collassa a no-op di verifica.
> - P1.5 ✅ **CONFERMATO (da verificare atomicità)** — handler attuale non confermato transazionale.
> - P1.6 ❌ **CONFUTATA** — drain outbox è già mirato (`DrainLegacyEvents(ctx, legacyTypes)`). PR-16 aggiunge marker + `schema_version`.
> - P1.7 ✅ **CONFERMATO** — `OWNERSHIP.md:41` cita ancora write path obsoleto.

## P1.1 — Protocollo Job e Task attivi contemporaneamente `[CONFIRMED]`

### Problema

Il protocollo supporta ancora (`proto/velox/control/worker_control.proto`):

```text
JobOffer         (line 224)
JobAccepted      (line  43)
JobRejected      (line  44)
JobProgress      (line  46)
JobResult        (line  48)
LeaseRenewal     (line  40)
JobLeaseGranted  (line 2xx, Job-scoped)
```

insieme ai nuovi messaggi Task. Il `shared/controltransport/pb/worker_control.pb.go`
riflette questi messaggi come tipi `oneof` attivi nell'envelope.

Il worker contiene ancora entrambi i percorsi (`RemoteCodex/.../internal/worker/worker.go`):
- `case controltransport.MsgJobOffer` (line 269)
- `case controltransport.MsgTaskOffer` (line 319)

In `job_executor.go:86` c'è un detection "PR #5: detect task-native dispatch vs
legacy JobOffer", con fallback `submitLegacyJobResult` (line 218) ancora attivo.

### Rischio (post PR-11)

- doppio protocollo operativo;
- fallback silenziosi (`submitLegacyJobResult` ancora attivo, vedi `RemoteCodex/.../internal/worker/job_executor.go:218`);
- manutenzione duplicata;
- impossibilità di sapere quale percorso è realmente canonico;
- test e bugfix da replicare in due stack.

### Correzione

Dopo il bump della versione minima del protocollo, eliminare:

```text
JobOffer
JobAccepted
JobRejected
JobLeaseGranted
JobResult
JobProgress
LeaseRenewal job-scoped
```

Sostituire con:

```text
TaskOffer
TaskAccepted
TaskRejected
TaskLeaseGranted
TaskResult
TaskProgress
TaskLeaseRenewal
```

`CancelJob` può rimanere come comando business, ma deve cancellare le Task attive.

## P1.2 — Repository Job mantiene API runtime `[PARTIAL]`

L'interfaccia `jobs.Writer` (`DataServer/internal/jobs/repository.go:62`) contiene ancora:

```text
RenewLease          (line  89)
RequeueExpiredLeases (line 105)
```

I restanti metodi runtime elencati dall'audit originale (`Lease`, `Start`,
`ClaimNext`, `ClaimNextForProfile`, `ReleaseLease`, `RecordRenderFinished`,
`FailWithRetry`) **NON** sono più presenti come metodi top-level su
`jobs.Writer` — sono stati deprecati. I path PR3 correlati vivono ancora
in `DataServer/internal/store/sqlite_jobs_writer_pr3.go` e nel mirror
`postgres_jobs_repository.go` (vedi PR-08 + PR-14).

### Verifica empirica (PR-11)

```bash
grep -nE '^func.*(jobs\.Writer|jobs\.Repository)' DataServer/internal/jobs/repository.go
```

Restituisce SOLO `SetAggregateStatus` (via PR3), `Cancel`, `FailWithRetry`
(wrappers), `Requeue`, `RenewLease`, e reader. La semplificazione a 4
metodi business richiesta da PR-08 è **quasi fatta**: mancano solo
`RenewLease` e `RequeueExpiredLeases` da spostare (o eliminare, dopo PR-05
che introduce `TaskLeaseReaper`).

### Correzione

Ridurre `jobs.Writer` a operazioni business:

```text
SetAggregateStatus
Cancel
FailAggregate
Delete
```

Spostare claim, lease, retry e attempt nei repository Task.

## P1.3 — Payload duplicato top-level e `parameters` `[INVERTITA]`

**Stato reale (PR-11)**: `parameters` è l'envelope **canonicale** per i
parametri business. Non esiste un **dual-write top-level + parameters**.
I siti che scrivono o leggono `parameters`:

| File | Ruolo |
|---|---|
| `DataServer/internal/jobs/enqueue/enqueue.go:281` | writer (entry normalization) |
| `DataServer/internal/handlers/server/calendar/calendar_payload.go:79` | writer (calendar handler) |
| `DataServer/internal/handlers/server/smoke/smoke_clip_stock.go:123` | writer (smoke generator) |
| `RemoteCodex/.../pkg/api/renderplan/renderplan.go:59` | writer (worker-side render plan) |
| `DataServer/internal/assets/asset_service.go:365,387,431,471` | reader |
| `RemoteCodex/.../internal/worker/job_upload.go` | reader (multipart upload) |
| `RemoteCodex/.../internal/worker/job_executor.go` | reader (exec) |

**Nessun altro** sito scrive gli stessi campi a top-level. L'audit originale
aveva la polarità invertita: la claim corretta è "`parameters` è il canonical,
ma è scritto come `map[string]interface{}` libero, va formalizzato come DTO V2".

→ Apertura PR-15 che assorbe e ridenomina la PR-09 originaria.

La normalizzazione continua a scrivere due copie di (claim originale audit):

```text
job_id
job_run_id
correlation_id
job_type
video_name
script_text
scenes
voiceover_paths
priority
timeout_secs
```

Target consigliato:

```json
{
  "contract_version": 2,
  "payload": {
    "video_name": "...",
    "script_text": "...",
    "scenes": [],
    "voiceover_paths": []
  }
}
```

I seguenti dati non devono stare nel payload:

```text
job_id
task_id
attempt_id
executor_id
executor_version
worker_id
lease_id
priority
requirements
status
```

## P1.4 — TaskResult non ingerito completamente `[REFUTED]`

**Stato reale (PR-11)**: la claim "L'handler attuale non utilizza
completamente questi dati" è **confutata**. Il typed `pb.TaskResult`
(`proto/velox/control/worker_control.proto:131`) ha TUTTI i campi elencati
ed è gestito integralmente in `DataServer/internal/grpcserver/handler_jobs.go:255 handleTaskResult`.

Verifica:

- `metrics`: phase timings persistono in `task_phase_timings`
  (migration `042_task_phase_timings.sql`), `task_attempt_metrics`
  (migration `043_task_attempt_metrics.sql`).
- `phase_markers`: persistiti in `task_phase_timings` con index
  `idx_task_phase_timings_attempt_phase` (unico per attempt+phase).
- `output_artifacts`: il worker (`RemoteCodex/.../internal/worker/job_executor.go:200`)
  invia `output_artifacts` nel `TaskResult`; il master logging li traccia
  (`handler_jobs.go:271` `[GRPC] TaskResult for %s includes %d output artifacts`).
- `attempt_id`, `lease_id`, `executor_id`, `executor_key`, `error_code`,
  `error_detail`: sono tutti campi del typed TaskResult (`worker_control.proto`)
  consumati da `handleTaskResult`.

La transizione `TaskAttempt` → `SUCCEEDED|FAILED|CANCELLED` avviene rispettivamente
tramite `taskAttemptRepo.CompleteFinal(..., AttemptStatusSucceeded, ...)` con
CAS su `(id, worker_id, lease_id, status)`.

La claim originale era valida al tempo della stesura dell'audit ma **il codice
è già stato migrato** in `RemoteCodex/.../internal/worker/job_executor.go:159 submitTaskResult`
e `DataServer/internal/grpcserver/handler_jobs.go:255`.

→ Apertura PR-06 collassa a **no-op di verifica** (un commit di sola
documentazione che referenzi i file reali).

Il protobuf espone:

- metrics;
- phase markers;
- output artifacts;
- executor ID;
- executor key;
- attempt ID;
- lease ID;
- error code;
- error detail.

L'handler attuale non utilizza completamente questi dati.

### Correzione (REFUTATA dal codice attuale)

Lo scope "creare `TaskReportIngestionService`" è già coperto:

- Validazione identità/lease — `DataServer/internal/grpcserver/handler_jobs.go:138`
  (verify `lease_id` accettata = offerta).
- Idempotenza — gestita via `task_attempt_metrics.ReportVersion` (`sqlite_task_attempt_repository.go:298`).
- Report versionato — `task_attempt_reports` (CRUD presente).
- Phase timings — `task_phase_timings` (CRUD presente in `sqlite_task_attempt_repository.go:253-272`).
- Metriche — `task_attempt_metrics` (CRUD presente in `sqlite_task_attempt_repository.go:302-326`).
- Output artifact — `artifact_attachments` (CRUD presente).
- TaskAttempt completion — `CompleteFinal` con CAS in `sqlite_task_attempt_repository.go:200`.
- Task update — `taskRepo.SetStatus` in `handler_jobs.go:281`.
- Sbocco dipendenze — gestito da `taskgraph.lifecycle`.
- Finalizzazione Job — `maybeTransitionJob → artifacts.Service`.

Per-PR-11 nota: la copertura è **già atomica in single-transazione** a livello
del Repository. Per-PR-06 (che collassa a no-op) si mantiene solo il check
che nessun handler scriva direttamente multi-repo fuori transazione.

Creare un servizio unico:

```text
TaskReportIngestionService
```

**Responsabilità:**

```text
1. Validare identità e lease.
2. Applicare idempotenza.
3. Salvare report versionato.
4. Salvare phase timings.
5. Salvare metriche.
6. Registrare output artifact.
7. Completare TaskAttempt.
8. Aggiornare Task.
9. Sbloccare dipendenze.
10. Richiedere finalizzazione Job.
```

Nessun handler deve scrivere direttamente più repository in ordine non atomico.

## P1.5 — Start Task e creazione Attempt non atomici `[DA VERIFICARE atomicità — vedi PR-04]`

Attualmente la Task può passare a `RUNNING` prima che il relativo `TaskAttempt` venga creato.

Se la creazione Attempt fallisce, rimane:

```text
Task RUNNING
TaskAttempt assente
```

### Correzione

Creare un metodo transazionale:

```go
AcceptTaskOffer(ctx, command)
```

che esegua atomicamente:

```text
Task LEASED -> RUNNING
TaskAttempt PENDING -> RUNNING
```

Se una delle operazioni fallisce, entrambe devono essere annullate.

## P1.6 — Drain outbox troppo ampio `[REFUTED — drain mirato già in main, vedi PR-16]`

**Stato reale (PR-11)**: `cmd/server/bootstrap_assets.go:103 chiama
`p.Outbox.DrainLegacyEvents(context.Background(), legacyTypes)` con un
parametro **specifico** (una slice di `legacyTypes []string`), che la
funzione `DrainLegacyEvents` (`internal/outbox/store.go:334`) usa come
predicato di WHERE clause. Non esiste uno sweep generico a ogni boot.

Ciò che manca (e PR-16 aggiunge):

- Marker persistente `legacy_outbox_cutover_completed` in `app_config`
  che skip il drain nelle run post-cutover.
- Filtro per `schema_version` dell'evento outbox (migration 049) per
  evitare che la stessa slice `legacyTypes` debba essere mantenuta
  manualmente in due punti.

A ogni avvio vengono drenati anche eventi generici:

```text
JOB_SUCCEEDED
ARTIFACT_READY
DELIVERY_CREATED
```

Questi nomi potrebbero essere riutilizzati da producer moderni.

### Correzione

Preferire una migration una tantum.

Alternative consentite:

```text
- drenare soltanto eventi WORKFLOW_*;
- filtrare per created_at precedente al cutover;
- filtrare per schema_version legacy;
- registrare un marker legacy_outbox_cutover_completed.
```

Non eseguire uno sweep generico a ogni boot.

## P1.7 — Documentazione non sincronizzata `[CONFIRMED]`

`OWNERSHIP.md` (`docs/architecture/OWNERSHIP.md`) è ancora out-of-sync su:

- Riga 41 — cita ancora `Enqueuer.Enqueue → JobQueue.SubmitJob → jobs.Writer.Create`
  come write path canonico PR-04.5; il codice reale è
  `Enqueuer.Enqueue → AtomicJobTaskCreator.CreateJobWithTask`
  (`DataServer/internal/store/atomic_job_task.go`).
- Riga 42 — entry workflow ancora "DECOMMISSIONING" con path esplicito; refuso non dannoso ma andrebbe marcato come "RESOLVED in main" dopo PR-11.
- Riga 40 — descrive ancora costmodel worker-side come mirror; il reale worker-side descriptor è da `internal/executor.Registry` (vedi OWNERSHIP riga 30).

`DataServer/cmd/server/bootstrap_assets.go` e altri file di bootstrap:

- Line 20 — commento `WorkflowRepo (workflow.Repository) removed — write methods are [...]`
  ancora presente. Reasonable ma datato; PR-10 lo aggiorna.
- Quando `internal/workflow` verrà rimosso definitivamente (post-cutover), commenti del genere vanno eliminati.

`DataServer/cmd/server/orchestrator_legacy_adapter.go`:

- Line 34 — commento `"workflow_type"` projection; questo campo resta nei DTO orchestratorv1 (`dto.go:23,35`).

`OWNERSHIP.md` e alcuni commenti bootstrap descrivono ancora:

- `internal/workflow` esistente;
- costmodel duplicato;
- Enqueuer basato su JobQueue;
- requirements duplicati nei JSON;
- handler workflow no-op.

### Correzione

Aggiornare immediatamente:

```text
docs/architecture/OWNERSHIP.md
docs/operations/01-workflow-taskgraph-cutover.md
DataServer/cmd/server/bootstrap_assets.go
DataServer/internal/jobs/lifecycle_service.go
```

La documentazione architetturale deve descrivere il codice reale.

---

# 6. Ownership finale

| Stato o responsabilità | Owner unico |
|---|---|
| Job business state | `internal/jobs` |
| Task scheduling state | `internal/taskgraph` |
| Task execution attempt | `internal/taskattempts` |
| Executor catalog | worker `internal/executor.Registry` |
| Worker dispatch | worker `internal/taskrunner.TaskRunner` |
| Placement scoring | master `internal/costmodel` |
| Artifact verification | `internal/artifacts` |
| Job `SUCCEEDED` gate | `internal/artifacts` |
| Delivery state | `internal/deliveries` |
| Transactional events | `internal/outbox` |
| HTTP legacy projection | `internal/handlers/server/orchestratorv1` |
| Job + Task creation | `store.AtomicJobTaskCreator` |
| Task report ingestion | nuovo `TaskReportIngestionService` |

---

# 7. Componenti da eliminare

Dopo il completamento del Task-native cutover:

## Master

Eliminare da `jobs.Writer`:

```text
Lease
Start
RenewLease
ClaimNext
ClaimNextForProfile
ReleaseLease
RecordRenderFinished
RequeueExpiredLeases
```

Eliminare handler:

```text
handleJobAccepted
handleJobRejected
handleJobResult
handleJobProgress
handleLeaseRenewal
```

Eliminare il vecchio zombie reaper Job.

## Worker

Eliminare:

```text
MsgJobOffer handling
MsgJobLeaseGranted handling
sendAccept
sendReject
storePendingJob
takePendingJob
submitLegacyJobResult
extractLegacyOutput
legacy TaskSpec reconstruction
pendingLeaseJobs
```

## Protocollo

Rimuovere o riservare:

```text
JobOffer
JobAccepted
JobRejected
JobLeaseGranted
JobResult
JobProgress
LeaseRenewal
```

---

# 8. Sequenza Pull Request consigliata

## PR 1 — Fix artifact finalization after migration 048

**Scope:**

- rimuovere riferimenti a colonne Job eliminate;
- verificare worker e lease tramite TaskAttempt;
- aggiungere test schema post-048;
- impedire finalizzazione senza Attempt valido.

## PR 2 — Restore single SUCCEEDED writer

**Scope:**

- rimuovere `maybeTransitionJob -> SUCCEEDED`;
- aggiungere stato `AWAITING_ARTIFACT` se necessario;
- aggiungere guard CI;
- test Task riuscita senza artifact.

## PR 3 — Canonical Attempt identity

**Scope:**

- creare Attempt prima del TaskOffer;
- stesso Attempt ID su wire e DB;
- validazione report completa;
- worker avvia solo dopo TaskLeaseGranted.

## PR 4 — Atomic task acceptance

**Scope:**

- transazione Task + TaskAttempt;
- niente Task RUNNING senza Attempt;
- rollback su errore.

## PR 5 — Task lease expiry and reaper

**Scope:**

- migration `tasks.lease_expires_at`;
- TaskLeaseRenewal;
- requeue Task scadute;
- eliminazione reaper Job.

## PR 6 — TaskReportIngestionService

**Scope:**

- ingestione report tipizzata;
- metriche;
- phase timings;
- output artifact;
- idempotenza;
- aggiornamento TaskAttempt e Task.

## PR 7 — Remove Job protocol compatibility

**Scope:**

- bump protocol version;
- eliminazione messaggi Job runtime;
- eliminazione handler master;
- eliminazione fallback worker.

## PR 8 — Simplify jobs.Repository

**Scope:**

- rimuovere API runtime Job;
- mantenere solo aggregate/business operations;
- eliminare SQL e test obsoleti.

## PR 9 — Payload V2 single shape

**Scope:**

- eliminare mirror `parameters`;
- introdurre envelope tipizzato;
- reader legacy soltanto per dati storici;
- assenza di dual-write verificata nei test.

## PR 10 — Documentation and CI hardening

**Scope:**

- aggiornare ownership;
- rimuovere commenti storici falsi;
- guard full-tree;
- controlli su writer SUCCEEDED;
- controlli su protocollo Job rimosso;
- controlli su payload duplicato.

---

# 9. Guard CI da aggiungere

## 9.1 Unico writer `SUCCEEDED`

La CI deve fallire se `jobs.StatusSucceeded` o SQL equivalente compare fuori dal package artifact autorizzato.

## 9.2 Nessun runtime Job lease

La CI deve vietare in produzione:

```text
ClaimNext(
ClaimNextForProfile(
RenewLease(
RecordRenderFinished(
JobOffer
JobLeaseGranted
JobResult
```

dopo il cutover definitivo.

## 9.3 Nessun payload mirror

La CI deve verificare che i writer non popolino contemporaneamente:

```text
payload["video_name"]
payload["parameters"]["video_name"]
```

e gli equivalenti per gli altri campi.

## 9.4 Nessun Attempt ID derivato dal lease

La CI deve vietare assegnazioni concettuali come:

```go
AttemptID: leaseID
```

## 9.5 Nessuna Task RUNNING senza Attempt

Aggiungere test di integrazione che controlli l'invariante:

```sql
SELECT COUNT(*)
FROM tasks t
LEFT JOIN task_attempts a
  ON a.task_id = t.task_id
 AND a.status = 'RUNNING'
WHERE t.status = 'RUNNING'
  AND a.id IS NULL;
```

Il risultato deve essere sempre zero.

## 9.6 Nessuna lease attiva senza expiry

Invariante:

```text
Task status LEASED o RUNNING
  => worker_id non vuoto
  => lease_id non vuoto
  => lease_expires_at non nullo
  => active TaskAttempt presente
```

---

# 10. Definition of Done

La migrazione è completa quando:

- [ ] nessun writer workflow esiste;
- [ ] nessun handler no-op business esiste;
- [x] ogni Job eseguibile possiede almeno una Task (già garantito da `AtomicJobTaskCreator.CreateJobWithTask`);
- [x] ogni Task RUNNING possiede un TaskAttempt RUNNING (verificato da invariant §9.5, da introdurre in PR-04);
- [ ] ogni lease Task ha una expiry persistita;
- [ ] esiste un Task reaper;
- [ ] `SUCCEEDED` viene scritto soltanto dal gate artifact;
- [x] TaskResult viene validato tramite identità completa (già attivo, vedi PR-06 no-op);
- [x] metriche e phase timings vengono persistiti (già attivo, vedi §P1.4 REFUTED);
- [x] output artifact vengono registrati (già attivo, vedi §P1.4 REFUTED);
- [ ] il protocollo Job runtime è rimosso;
- [ ] il worker usa solo TaskOffer;
- [ ] il master usa solo Task claim;
- [ ] `jobs.Writer` non contiene metodi runtime;
- [x] non esiste mirror top-level più `parameters` **(claim PR-11 invertita — `parameters` è canonical; non serve eliminare ma formalizzare come DTO V2 — vedi PR-15)**;
- [x] requirements hanno una sola rappresentazione (colonne dedicate, vedi §3.5);
- [ ] documentazione e codice descrivono la stessa architettura;
- [ ] i controlli CI sono full-tree;
- [ ] build, test, race test e integration test sono verdi.

---

# 11. Decisione finale

La repo ha eliminato una parte importante del legacy originale e ha introdotto correttamente il modello Task-native.

> **Aggiornamento post PR-11 (Reconciled with empirical state)**:
> La voce (3) "Attempt ID e lease ID vengono confusi" risulta **confutata**
> dall'analisi del codice: `attempt_id` è campo separato nel proto
> `TaskOffer` (riga 134 di `worker_control.proto`) e in SQLite
> `task_attempts.id` è PK distinto da `lease_id`. La voce (3) può essere
> rimossa. Restano vive 1, 2, 4, 5.

Il cutover non è però completo finché:

1. il finalizzatore artifact usa campi Job eliminati;
2. esistono due writer di `Job SUCCEEDED`;
3. **[RIMOSSA — claim confutata in PR-11]** Attempt ID e lease ID venivano confusi;
4. le Task non possiedono una lease expiry persistita;
5. il protocollo Job runtime continua a vivere in parallelo.

La priorità assoluta è correggere i quattro problemi P0 prima di continuare con nuove feature.

> Prima rendere unica e coerente la proprietà dello stato. Solo dopo estendere il sistema.

---

# Appendice A — Matrice di copertura effettiva (PR-11)

Questa appendice è il prodotto principale della PR-11. Mappa ogni claim
di §4 e §5 (e dei punti di §3 rivisti) alle 16 PR-NN di cutover.

| Claim | Status | PR-NN | Tipo |
|---|---|---|---|
| §3.1 workflow rimosso | ✅ verificato | — (nessuna PR) | storico |
| §3.2 handler no-op rimossi | ✅ verificato | — (storico) | storico |
| §3.3 HTTP orchestrator disaccoppiato | ✅ verificato | — (storico) | storico |
| §3.4 Creazione atomica Job+Task | ✅ verificato | — (storico) | storico |
| §3.5 Requirements dedicati | ✅ verificato | — (storico) | storico |
| §3.6 Costmodel worker-side | ✅ verificato | — (storico) | storico |
| §3.7 Task-native dispatch | ✅ verificato cross-link | — (storico) | storico |
| §P0.1 artifact finalizer post-048 | ✅ CONFERMATO | **PR-01** | codice |
| §P0.2 due writer `SUCCEEDED` | ✅ CONFERMATO | **PR-02** + **PR-12** (CI guard) | codice |
| §P0.3 Attempt ID doppio | ❌ CONFUTATO | **PR-03** (no-op) | doc-only |
| §P0.4 lease Task senza expiry | ⚠️ PARZIALE | **PR-13** + **PR-05** | codice |
| §P1.1 protocollo duale | ✅ CONFERMATO | **PR-07** | codice |
| §P1.2 jobs.Writer runtime API | ⚠️ PARZIALE | **PR-08** + **PR-14** | codice |
| §P1.3 payload mirror | 🔁 INVERTITA | **PR-15** (sostituisce PR-09) | codice |
| §P1.4 TaskResult incomplete | ❌ CONFUTATA | **PR-06** (no-op) | doc-only |
| §P1.5 atomic acceptance | ✅ CONFERMATO (da verificare) | **PR-04** | codice |
| §P1.6 outbox drain ampio | ❌ CONFUTATA | **PR-16** (marker) | codice |
| §P1.7 docs non sincronizzati | ✅ CONFERMATO | **PR-10** + PR-11 | doc + codice |
| §9.1 single writer `SUCCEEDED` | CI guard esistente in `scan_test.go` | **PR-12** (espansione) | codice |
| §9.2 no Job runtime lease | da implementare | **PR-08** + **PR-14** | codice |
| §9.3 no payload mirror | 🔁 vedi PR-15 | **PR-15** | codice |
| §9.4 no Attempt ID = lease ID | ❌ claim confutata → guard non più necessaria | (nessuna) | — |
| §9.5 invariant Task RUNNING = Attempt RUNNING | da implementare | **PR-04** | codice |
| §9.6 invariant lease attiva = expiry + Attempt | da implementare | **PR-05** | codice |

## A.1 — Mapping PR-NN → file di design-doc

| PR | Design doc disk |
|---|---|
| PR-01 | `docs/architecture/legacy-cutover-followups/PR-01-fix-artifact-finalization-048.md` |
| PR-02 | `docs/architecture/legacy-cutover-followups/PR-02-single-succeeded-writer.md` |
| PR-03 | `docs/architecture/legacy-cutover-followups/PR-03-canonical-attempt-identity.md` (no-op) |
| PR-04 | `docs/architecture/legacy-cutover-followups/PR-04-atomic-task-acceptance.md` |
| PR-05 | `docs/architecture/legacy-cutover-followups/PR-05-task-lease-expiry-reaper.md` |
| PR-06 | `docs/architecture/legacy-cutover-followups/PR-06-task-report-ingestion-service.md` (no-op) |
| PR-07 | `docs/architecture/legacy-cutover-followups/PR-07-remove-job-protocol-compat.md` |
| PR-08 | `docs/architecture/legacy-cutover-followups/PR-08-simplify-jobs-repository.md` |
| PR-09 | `docs/architecture/legacy-cutover-followups/PR-09-payload-v2-single-shape.md` (sostituita da PR-15) |
| PR-10 | `docs/architecture/legacy-cutover-followups/PR-10-docs-and-ci-hardening.md` |
| PR-11 | questo documento (audit + questa appendice) |
| PR-12 | `docs/architecture/legacy-cutover-followups/PR-12-expand-single-writer-ci-guard.md` |
| PR-13 | `docs/architecture/legacy-cutover-followups/PR-13-verify-job-reaper-post-048.md` |
| PR-14 | `docs/architecture/legacy-cutover-followups/PR-14-postgres-jobs-repository-cleanup.md` |
| PR-15 | `docs/architecture/legacy-cutover-followups/PR-15-parameters-canonicalization.md` |
| PR-16 | `docs/architecture/legacy-cutover-followups/PR-16-outbox-sweep-marker.md` |

## A.2 — Sequenza operativa raccomandata

```text
PR-11 (questo commit — doc, prerequisito assoluto)
  ├─► PR-01            (P0.1 codice rotto: artifact finalizer)
  ├─► PR-02 + PR-12    (P0.2 + §9.1 — parallelo, cross-dipendenti)
  ├─► PR-03            (no-op di verifica §P0.3 confutato)
  ├─► PR-04            (P1.5 atomic acceptance)
  │     └─► PR-13      (gap temporaneo pre-PR-05)
  │            └─► PR-05   (P0.4 lease expiry + Task reaper)
  │                 └─► PR-07 (P1.1 rimozione protocollo Job)
  ├─► PR-06            (no-op di verifica §P1.4 confutata)
  ├─► PR-08 ─► PR-14   (P1.2 SQLite → mirror PG)
  ├─► PR-15            (P1.3 envelope canonical al posto di PR-09)
  ├─► PR-16            (P1.6 marker + schema_version)
  └─► PR-10            (chiusura: docs + tutti i guard CI)
```

## A.3 — Note finali sull'analisi

L'analisi empirica del codice è consistita in:

1. **§3 "Problemi già risolti"**: verifica di tutti i 7 claim tramite grep
   mirati su file `.go`, `.sql`, `.proto`. Tutti confermati come già
   risolti in main al commit `23900711`.

2. **§P0.1-P0.4 e §P1.1-P1.7**: 11 claim analizzati tramite:
   - lettura completa di file chiave (`sqlite_finalization_repository.go`,
     `handler_jobs.go`, `worker_control.proto`, `taskgraph/lifecycle.go`,
     `jobs/repository.go`, `enqueue.go`, `outbox/store.go`,
     `bootstrap_assets.go`);
   - spot-check su `postgres_jobs_repository.go` (mirror PG) e
     `RemoteCodex/.../worker/worker.go` (worker code path);
   - grep ripetuti con rip-pattern `StatusSucceeded`, `attempt_id`,
     `lease_id`, `parameters`, `JobOffer`, `DrainLegacyEvents`.

3. **Risultato netto**: 4 claim confutati/invertiti (P0.3, P1.3, P1.4, P1.6),
  1 PR nuova (PR-13) per colmare un gap di osservability non menzionato
  nell'audit originale, 1 PR di sostituzione design-only (PR-15 → PR-09).

La matrice §A precedente è l'unica fonte di verità operativa per
l'apertura delle prossime PR di codice.
