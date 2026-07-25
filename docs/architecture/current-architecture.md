# Current architecture — Come funziona oggi

**Capitolo del perimetro architetturale Velox** — corrisponde alla **PARTE I** del documento indice [`CURRENT-TO-TARGET-ARCHITECTURE.md`](./CURRENT-TO-TARGET-ARCHITECTURE.md).  
**Stato:** descrittivo dello stato corrente verificabile su `main`.  
**Sezioni:** 6, 7, 8, 9, 10, 11, 12, 13, 15 (la sezione 14 — Supervisor e readiness — è trattata in [`failure-recovery.md`](./failure-recovery.md)).

---

## 6. Ingresso e compilazione Job

Il percorso video usa `shared/contract.JobPayloadV2`.

L'Enqueuer riceve il payload da uno dei tre percorsi di intake canonici:

1. un handler HTTP del flusso master (`POST /api/v1/pipeline-runs/*`);
2. il polling asincrono del `CreatorForwardingRunner` (vedi §12);
3. l'intake HTTP diretto `POST /api/v1/creator/jobs` (handler `creator_push`, autenticato via bearer `VELOX_ADMIN_TOKEN` nello stesso gruppo di route della pipeline) — vedi §12 per il dettaglio dei due percorsi creator.

In tutti i casi l'Enqueuer:

1. risolve voiceover e scene image;
2. normalizza il payload;
3. rimuove alias legacy dalle scritture canoniche;
4. determina identità e metadati;
5. compila `jobs.Job`;
6. compila `taskgraph.TaskSpec`;
7. delega a `AtomicJobTaskCreator`;
8. inserisce Job e primo Task nella stessa transazione.

Per i payload provenienti da un creator, la conversione al DTO tipizzato `RemotePipelineResult` avviene nell'handler (prima della delega all'Enqueuer) e non nell'Enqueuer stesso; vedi `docs/CREATOR-PUSH.md` per il dettaglio del contratto.

Tutti i percorsi di intake creator (asincrono via `CreatorForwardingRunner` e sincrono via `POST /api/v1/creator/jobs`) convergono su `creatorflow.Resolver` e quindi su `AtomicForwardAndEnqueue`; nessun handler introduce un secondo writer sul database (vedi §4.2 di `runtime-invariants.md`).

```mermaid
flowchart TD
    A1[HTTP handler master: POST /pipeline-runs/*] --> B[Enqueuer]
    A2[HTTP creator_push: POST /api/v1/creator/jobs] --> B
    A3[Async: CreatorForwardingRunner] --> B
    B --> C[Asset resolution]
    C --> D[JobPayloadV2 normalization]
    D --> E[Compile Job]
    D --> F[Compile TaskSpec]
    E --> G[AtomicJobTaskCreator]
    F --> G
    G --> H[(SQLite jobs + tasks + task spec)]
```

### Identità normale

Per richieste normali il Job può ricevere un UUID.

### Identità forwarding

Per risultati provenienti da creatorflow:

```text
source_provider
source_job_id
target_executor_id
        ↓
routing.FormatForwardingKey
        ↓
enqueue.DeriveForwardingJobID
```

Webhook duplicati, poller concorrenti e retry post-crash convergono sullo stesso Job.

### Limite corrente

Il percorso video principale è ancora sostanzialmente:

```text
1 Job → 1 Task scene.composite.v1@1
```

Il Task contiene un payload completo che il worker tratta come composizione monolitica. TaskGraph esiste, ma il video standard non è ancora compilato in un vero DAG di Task granulari.

---

## 7. Job, Task e TaskAttempt

### Job

Rappresenta il risultato business richiesto dall'utente.

Stati essenziali:

```text
PENDING
RUNNING
RETRY_WAIT
SUCCEEDED
FAILED
CANCELLED
```

Il Job non deve possedere lease o worker assignment.

### Task

Rappresenta una unità schedulabile e possiede:

- dipendenze;
- stato READY/LEASED/RUNNING/terminal;
- executor ID/version;
- requisiti;
- attempt number;
- revision;
- worker e lease correnti.

### TaskAttempt

Rappresenta un'esecuzione concreta e possiede:

- worker;
- lease;
- risultato;
- metriche;
- timing di fase;
- output;
- motivo tipizzato;
- identità del tentativo vincente.

### Stato attuale

La codebase è migrata verso un modello Task-native:

- i vecchi messaggi Job del protocollo sono rimossi;
- il worker riceve TaskOffer e TaskLeaseGranted;
- i TaskResult sono tipizzati;
- l'ingestion service centralizza la chiusura del tentativo;
- metriche tipizzate e artifact registration sono collegate all'ingestion.

Resta obbligatorio mantenere test full-tree che impediscano nuove mutation laterali.

---

## 8. Placement e dispatch

Il master possiede il cost model.

```mermaid
sequenceDiagram
    participant DB as SQLite
    participant TG as TaskGraph
    participant GRPC as gRPC Master
    participant W as Worker
    participant R as Executor Registry

    TG->>DB: List READY candidates
    TG->>GRPC: candidate TaskSpec
    GRPC->>W: TaskOffer
    W->>R: validate executor ID/version
    W-->>GRPC: TaskAccepted oppure TaskRejected
    GRPC->>DB: atomic claim + lease fencing
    GRPC->>W: TaskLeaseGranted
    W->>R: dispatch Task
```

Il worker non deve selezionare il lavoro tramite switch paralleli. Deve usare il registry.

Il composition root worker oggi:

- costruisce `executor.Registry`;
- costruisce il pipeline runner;
- esegue bootstrap fail-closed del motore C++ e di FFmpeg;
- registra `scene.composite.v1@1`;
- costruisce cache persistente e blob store;
- passa registry, cache e blob al runtime.

Questa è una base corretta.

Gap:

- catalogo executor reale ristretto;
- la pipeline completa passa principalmente da scene composite;
- cost, locality e multi-executor DAG non sono ancora dimostrati E2E;
- la certificazione per hardware class non è chiusa.

---

## 9. Esecuzione worker

```text
TaskLeaseGranted
    ↓
TaskRunner
    ↓
ExecutorRegistry.Resolve(executor_id, version)
    ↓
SceneComposite executor
    ↓
pipeline.Runner
    ↓
video engine C++ / FFmpeg
    ↓
output e metriche
    ↓
TaskResult tipizzato
```

Il worker deve:

- eseguire, non pianificare;
- rispettare il contratto;
- non inventare Task;
- non cambiare il DAG;
- non scegliere un executor alternativo;
- non dichiarare il Job riuscito;
- produrre hash, size, metadati e metriche.

Cache e blob locali sono ottimizzazioni ricostruibili, non autorità business.

---

## 10. Ingestion del TaskResult

Percorso canonico:

```text
gRPC handler
    ↓
TaskReportIngestionService
    ↓
transazione atomica:
    - chiusura TaskAttempt
    - aggiornamento Task
    - metriche tipizzate
    - cache/cost evidence
    - registrazione output
    ↓
completion/finalization
```

L'handler deve soltanto:

- validare protocollo e identità;
- tradurre gli errori in status gRPC;
- delegare al servizio.

Non deve ricreare la sequenza con SQL o repository separati.

---

## 11. Artifact e completion protocol

### DeclareOutputs

Il master:

- valida la FenceTuple;
- crea o riusa `attempt_commit`;
- genera un commit token deterministico HMAC;
- registra le dichiarazioni output;
- restituisce UploadPlan.

### RecordUploadProgress

Aggiorna:

- `last_progress_at`;
- deadline del commit;
- byte caricati.

Gap osservato: il contratto dichiara progress monotono, mentre la mutation corrente assegna il valore ricevuto. Un heartbeat vecchio può regredire `uploaded_bytes`.

Target SQL:

```sql
SET uploaded_bytes = MAX(uploaded_bytes, ?)
```

### CompleteUpload

Verifica:

- stato upload;
- hash worker;
- hash server-side;
- stato artifact;
- conteggio output ready.

Un artifact può diventare READY solo dopo verifica sufficiente.

### CommitAttempt

La transazione finale:

- marca TaskAttempt;
- marca Task;
- marca il commit COMMITTED;
- effettua il roll-up Job secondo il contratto canonico;
- crea delivery;
- inserisce outbox;
- legge CommitResult prima del commit SQL.

### Confine di ownership corrente

Il gate `TestSucceededWriterIsFinalizationOnly` è stato promosso a must-pass e ora passa.

`internal/completion/sqlite_uow.go` è considerato il gateway SQL autorizzato del Coordinator, nella stessa transazione atomica, non un writer business laterale.

La regola da preservare è:

- `artifacts.FinalizeVerified` governa la finalizzazione artifact/job del percorso artifact;
- `Coordinator.CommitAttempt` governa attempt/task/commit e il relativo roll-up atomico;
- nessun handler o runner può aggiungere un terzo percorso;
- nessun Job può diventare SUCCEEDED prima dell'evidenza artifact richiesta.

Resta utile rendere questa distinzione esplicita in ownership e nei test E2E, così l'allowlist del UoW non venga interpretata come permesso per nuovi writer.

---

## 12. Creatorflow e forwarding

Il polling volatile in goroutine è stato sostituito da `creator_forwardings` persistente.

Stati concettuali:

```text
PENDING
POLLING
RETRY_WAIT
READY_TO_FORWARD
FORWARDING
FORWARDED
BLOCKED
FAILED
```

Questi stati si applicano alla riga `creator_forwardings` indipendentemente dal percorso di intake che l'ha generata (vedi "Due percorsi di intake" più sotto).

### Due percorsi di intake, un solo writer

I payload provenienti da macchine Creator possono entrare nel sistema attraverso due percorsi distinti, entrambi destinati al medesimo `creatorflow.Resolver` e quindi allo stesso `AtomicForwardAndEnqueue`:

1. **Polling asincrono** — `CreatorForwardingRunner` interroga il creator remoto, persiste una riga in `creator_forwardings` e, quando il job remoto risulta `completed`, delega al Resolver. Questo è il flusso master-driven descritto sopra.

2. **Push sincrono** — la macchina Creator invia il payload già pronto via `POST /api/v1/creator/jobs` (autenticato tramite bearer `VELOX_ADMIN_TOKEN` nello stesso gruppo di route della pipeline). Il handler `creator_push` converte il payload nel DTO tipizzato `RemotePipelineResult` e lo passa al medesimo `creatorflow.Resolver`. Questo è il flusso Creator-driven, introdotto accanto al precedente per abilitare un intake diretto senza polling master. Il contratto HTTP è documentato in `docs/CREATOR-PUSH.md`.

```mermaid
flowchart TD
    A1[Async: CreatorForwardingRunner] --> R{creatorflow.Resolver}
    A2[HTTP creator_push handler: POST /api/v1/creator/jobs] --> R
    R --> E[AtomicForwardAndEnqueue]
    E --> F[(SQLite: creator_forwardings + jobs + tasks + task_specs)]
```

**Invariante "un solo writer" (riferimento: `runtime-invariants.md §4.2`).** Il push handler `creator_push` non apre transazioni proprie su SQLite e non duplica lo schema di `creator_forwardings`, `jobs` o `tasks`. L'unica transazione che materializza lo stato business è quella aperta da `AtomicForwardAndEnqueue`, lo stesso UoW già utilizzato dal `CreatorForwardingRunner`. I due intake sono quindi due **vie di accesso** alla medesima macchina canonica, non due writer paralleli.

L'identità canonica `(source_provider, source_job_id, target_executor_id)` (vedi `docs/CREATOR-PUSH.md`) produce un `job_id` deterministico: replay, retry post-crash, poller concorrenti e webhook duplicati convergono sullo stesso forwarding e sullo stesso job Velox indipendentemente dal percorso di intake che li ha generati.

### Runner

`CreatorForwardingRunner`:

1. reclama righe;
2. assegna lease;
3. avvia renewal;
4. interroga il creator remoto;
5. persiste failure, retry o risultato;
6. delega al Resolver;
7. crea Job+Task e marca FORWARDED atomicamente.

### Resolver

`creatorflow.Resolver`:

- verifica completezza;
- calcola forwarding key;
- deriva job ID deterministico;
- normalizza payload;
- riscrive URL quando necessario;
- assicura la forwarding row;
- prepara Job e TaskSpec;
- esegue `AtomicForwardAndEnqueue`.

La convergenza è corretta.

### Failure window ancora aperte

- `processLease` non restituisce errore;
- mutation failure possono essere solo loggate;
- `tick` può restituire `nil` senza transizione persistita;
- metriche possono aumentare prima della conferma DB;
- ClaimBatch può superare Concurrency;
- lease reclamate possono attendere il semaphore senza renewal;
- resolver lazy è condiviso tra goroutine;
- fast path "Job esistente" deve garantire repair della forwarding row.

Possibile falso successo:

```text
log = forwarded/retried/failed
metric = incrementata
SQLite = stato precedente
supervisor = runner sano
```

Questo è P0.

---

## 13. Outbox e delivery

```text
Transazione business
    ↓
outbox_event persistito
    ↓
OutboxDispatcher
    ↓
DeliveryRunner
    ↓
Provider Registry
    ↓
delivery terminale o retry durabile
```

Obblighi:

- replay idempotente;
- nessun evento perso;
- nessuna delivery duplicata;
- errori infrastrutturali propagati;
- retry tipizzati;
- backlog e oldest-age osservabili.

Outbox e delivery sono critical perché il server può restare vivo mentre il business flow è fermo.

---

## 15. CI corrente

Sono presenti:

- `make verify`;
- workspace tests;
- routing invariants;
- typed metrics must-pass;
- pre-existing test watchlist promossa a must-pass;
- altri gate architetturali e security.

### Stato aggiornato

I quattro test della vecchia watchlist ora passano deterministicamente:

- `TestSucceededWriterIsFinalizationOnly`;
- `TestBeginUpload_WrongAttemptStatus`;
- `TestUploadCompletedVideo_CanonicalPipeline`;
- `TestGenerateWithImages_UsesCreatorStageWhenConfigured`.

Il gap non è più "correggere quei quattro test". Il gap è:

- rendere il gate required nella branch protection;
- dimostrare una clean `make verify` completa;
- evitare duplicazione di build logic tra workflow;
- rendere CTest e workload E2E obbligatori;
- impedire skip silenziosi;
- pubblicare evidenza di release unica.

Target:

```text
make verify
    ├── formatting
    ├── architecture checks
    ├── migrations
    ├── Go vet/test/race
    ├── C++ configure/build/CTest
    ├── security checks
    ├── real workload E2E
    └── release evidence
```

I workflow devono essere dispatcher sottili.

> La sezione 14 (Supervisor e readiness) è trattata in [`failure-recovery.md`](./failure-recovery.md).
