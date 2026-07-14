# Velox — Architettura attuale, architettura target e piano di stabilizzazione

**Stato:** documento architetturale di raccordo (indice principale)  
**Repository:** `Marcuss-ops/VeloxEditiingg`  
**Branch di riferimento:** `main`  
**Ultima riconciliazione statica:** 3 luglio 2026  
**Ambito:** master `DataServer`, worker `RemoteCodex`, contratti `shared`, persistenza, artifact, forwarding, supervisor, CI e percorso verso il rendering distribuito

> Questo documento è l'**indice principale** del perimetro architetturale Velox. Le sezioni tematiche sono state separate in cinque capitoli dedicati per restare sotto la soglia LOC della documentazione (`docs/metrics/loc-baseline.md §11`):
>
> - [`current-architecture.md`](./current-architecture.md) — **PARTE I**, come funziona oggi (PARTE I, sezioni 6–15)
> - [`target-architecture.md`](./target-architecture.md) — **PARTE II**, architettura target (sezioni 16–21)
> - [`runtime-invariants.md`](./runtime-invariants.md) — principi architetturali non negoziabili (sezione 4.1–4.5)
> - [`failure-recovery.md`](./failure-recovery.md) — supervisor, recovery, progress e conflict budget (sezione 14 + P0-03, P0-04, P1-03)
> - [`distributed-rendering-roadmap.md`](./distributed-rendering-roadmap.md) — piano di scalatura a DAG distribuito (P2, sezione 33)
>
> Una funzionalità è considerata completata soltanto quando esistono codice su `main`, test verdi ed evidenza riproducibile. Commenti, checklist o workflow presenti nel repository non costituiscono da soli prova di completamento.

---

## Indice dei capitoli

| Capitolo | File | Contenuto |
|---|---|---|
| Current architecture | [`current-architecture.md`](./current-architecture.md) | PARTE I: ingresso, Task, placement, esecuzione, ingestion, artifact/completion, creatorflow/forwarding, outbox/delivery, CI |
| Target architecture | [`target-architecture.md`](./target-architecture.md) | PARTE II: flusso end-to-end definitivo, RenderPlan immutabile, multi-Task DAG, scheduler, cache/artifact, recovery target |
| Runtime invariants | [`runtime-invariants.md`](./runtime-invariants.md) | Single source of truth, single writer, registry-first, fail-closed, idempotenza e fencing |
| Failure & recovery | [`failure-recovery.md`](./failure-recovery.md) | Supervisor & readiness (sezione 14), P0-03 supervisor, P0-04 progress/conflict, P1-03 recovery suite |
| Distributed rendering roadmap | [`distributed-rendering-roadmap.md`](./distributed-rendering-roadmap.md) | P2: RenderPlan unico, multi-Task DAG, executor granulari, locality, temporal sharding, soak distribuito |

Le sezioni che seguono (sintesi, obiettivo, fonti, struttura del repository, gap analysis, intervento e regole) costituiscono l'**indice trasversale** e restano in questo file.

---

## 1. Sintesi esecutiva

Velox ha già superato la fase di prototipo monolitico. La codebase corrente contiene:

- master Go con HTTP e gRPC;
- worker Go con executor registry;
- motore C++/FFmpeg;
- stato Job, Task e TaskAttempt persistente;
- creazione atomica Job+Task;
- protocollo Task-native;
- forwarding creator persistente;
- completion protocol con fencing e HMAC;
- artifact, outbox e delivery;
- supervisor con classi di criticità;
- doctor e bootstrap worker;
- cost model master-side;
- cache e blob store worker.

La direzione è corretta. Il sistema, però, non può ancora essere considerato completamente stabilizzato o production-certified.

I gap principali sono:

1. dimostrare una clean baseline completa e required;
2. eliminare ogni false-success nei runner;
3. rendere supervisor e readiness realmente fail-closed;
4. chiudere failure window di forwarding, upload e completion;
5. dimostrare il percorso reale master→worker→artifact→Job con E2E obbligatorio;
6. completare recovery, mTLS, certificazione e soak;
7. trasformare il percorso video da `1 Job → 1 Task monolitico` a `1 Job → RenderPlan → Task DAG`.

La priorità non è aggiungere nuovi layer. La priorità è rendere affidabili, osservabili e recuperabili quelli esistenti.

---

## 2. Obiettivo del sistema

Velox deve essere un runtime **headless, deterministico, server-side e CPU-first** per generare e comporre video tramite un master centrale e worker remoti.

Architettura finale:

```text
Richiesta utente
    ↓
Compiler Registry sul master
    ↓
RenderPlan immutabile e versionato
    ↓
Job persistente
    ↓
Task DAG persistente
    ↓
Scheduler e placement
    ↓
Worker compatibile
    ↓
Executor Registry
    ↓
Motore C++/FFmpeg
    ↓
Artifact intermedi/finali verificati
    ↓
Finalizzazione atomica
    ↓
Outbox e delivery
    ↓
Job SUCCEEDED
```

Velox non deve diventare:

- un editor grafico;
- un renderer browser-based;
- un clone di Premiere, After Effects o Blender;
- un runtime dipendente da GPU;
- una collezione di endpoint con percorsi di esecuzione paralleli;
- un insieme di servizi che scrivono lo stesso stato da punti differenti.

Principio fondamentale:

> **Un solo contratto di esecuzione, un solo proprietario per ogni stato, un solo percorso di mutazione.**

---

## 3. Fonti canoniche e ordine di autorità

Fonti architetturali correnti:

- `README.md` — struttura del repository;
- `docs/architecture/OWNERSHIP.md` — owner e writer canonici;
- `docs/100-percent-plan/00-TARGET-AND-DEFINITION-OF-DONE.md`;
- `docs/100-percent-plan/01-RUNTIME-CONSISTENCY-AND-RECOVERY.md`;
- `docs/100-percent-plan/02-CI-TESTING-AND-RELEASE.md`;
- `docs/100-percent-plan/03-PRODUCTION-OPERATIONS-AND-SECURITY.md`;
- `docs/100-percent-plan/04-DISTRIBUTED-RENDERING-PERFORMANCE-AND-SCALE.md`;
- codice e migrazioni presenti su `main`.

Ordine di autorità in caso di divergenza:

1. schema e vincoli realmente applicati;
2. codice di produzione raggiungibile dal composition root;
3. test di invariante e integrazione;
4. documentazione canonica;
5. commenti e documenti storici.

Se un documento afferma che esiste un solo writer ma il codice permette due mutation path indipendenti, lo stato reale è non conforme fino alla convergenza.

> I principi architetturali non negoziabili (single source of truth, single writer, registry-first, fail-closed, idempotenza e fencing) sono descritti in [`runtime-invariants.md`](./runtime-invariants.md).

---

## 5. Struttura attuale del repository

```text
DataServer/
    cmd/server/                 Composition root master
    internal/jobs/             Job model e lifecycle
    internal/taskgraph/        Task DAG, readiness, lease e revision
    internal/taskattempts/     Tentativi, report e metriche
    internal/grpcserver/       Control plane worker Task-native
    internal/ingest/           Ingestion dei TaskResult
    internal/artifacts/        Upload, verifica e finalizzazione
    internal/completion/       Protocollo di commit degli output
    internal/creatorflow/      Conversione risultati creator in Job Velox
    internal/forwarding/       Polling persistente creator_forwardings
    internal/outbox/           Eventi durabili
    internal/deliveries/       Delivery provider e retry
    internal/store/            SQLite, migrazioni, adapter e BlobStore
    internal/costmodel/        Eligibility e scoring master-side
    internal/registry/         Capability readiness
    internal/metrics/          Metriche runtime e costi
    internal/workers/          Sessione, heartbeat e comandi worker

RemoteCodex/native/worker-agent-go/
    cmd/velox-worker-agent/    Composition root worker
    internal/executor/         Registry degli executor
    internal/taskrunner/       Dispatch Task per executor
    internal/worker/           Stream, heartbeat e runtime
    internal/telemetry/        Stato risorse e readiness
    pkg/video/pipeline/        Pipeline verso engine nativo
    pkg/cache/                 Cache locale persistente
    pkg/blob/                  Artifact blob content-addressed
    pkg/doctor/                Validazioni operatore

RemoteCodex/native/video-engine-cpp/
    engine C++/FFmpeg

shared/
    contratti Go, protobuf, payload e identità condivise
```

La separazione generale è corretta. I problemi principali non derivano dall'assenza totale di moduli, ma dalla necessità di far convergere tutti i percorsi sugli stessi owner e di provare la correttezza nei failure window.

Per il funzionamento corrente end-to-end vedi [`current-architecture.md`](./current-architecture.md).

---

# PARTE III — GAP ANALYSIS

## 22. Mappa sintetica

| Area | Oggi | Target | Gap |
|---|---|---|---|
| Baseline | watchlist must-pass, vari gate | clean full verification required | branch protection ed evidenza completa |
| Job creation | Job+Task atomici | Job+RenderPlan+DAG atomici | compilazione multi-Task |
| Executor | registry + scene composite | più executor reali | catalogo e contract test |
| Forwarding | persistente/deterministico | zero false-success | error propagation e lease batch |
| Completion | fencing/HMAC/UoW | progress e retry coerenti | monotonicità e budget keyed |
| Finalization | invariant writer test verde | proof E2E artifact-before-job | evidenza failure-window |
| Supervisor | classi/readiness | permanent runner fail-loud | nil exit e supervisorDone |
| Capability | registry presente | probe reali | transport placeholder |
| CI | più must-pass gate | make verify canonico | required E2E/CTest e dedupe |
| Recovery | componenti presenti | suite automatizzata | failure injection completa |
| Operations | doctor/bootstrap | worker certificato | mTLS, soak, rollout evidence |
| Scale | TaskGraph/costmodel | DAG/locality/sharding | implementazione e benchmark |

---

# PARTE IV — PIANO DI INTERVENTO

> Le sezioni P0-03, P0-04, P1-03 e P2 sono trattate in dettaglio nei capitoli tematici:
> - P0-03 + P0-04 + P1-03 → [`failure-recovery.md`](./failure-recovery.md)
> - P2 → [`distributed-rendering-roadmap.md`](./distributed-rendering-roadmap.md)
>
> Gli altri item di intervento (P0-01, P0-02, P0-05, P0-06, P1-01, P1-02, P1-04) restano in questo indice trasversale.

## 23. P0-01 — Rendere la baseline realmente obbligatoria

### Stato

I quattro test della watchlist sono verdi e promossi a must-pass.

### Azioni

1. rendere i gate must-pass required nella branch protection;
2. eseguire `make verify-fast` da clean checkout;
3. eseguire `make verify` con Docker e native toolchain;
4. eseguire Go race per tutti i moduli;
5. eseguire CTest e fallire se scopre zero test;
6. verificare architecture, migration, registry, secret e DB-access checks;
7. archiviare summary e durata;
8. impedire neutral/skipped per gate mandatory.

### Accettazione

- zero test noti rossi;
- required checks configurati;
- nessuno skip critico;
- clean verification riproducibile.

---

## 24. P0-02 — Eliminare false-success nel forwarding

### Azioni

1. `processLease` deve restituire `error`;
2. classificare element-scoped, lease-lost e infrastructure;
3. raccogliere errori delle goroutine;
4. propagare errori infrastrutturali da `tick`;
5. incrementare metriche solo dopo CAS riuscito;
6. imporre `effectiveClaimBatch <= Concurrency`;
7. non reclamare lavoro che non può iniziare;
8. iniettare Resolver nel costruttore;
9. eliminare lazy init concorrente;
10. riparare forwarding row nel fast path idempotente;
11. aggiungere failure injection DB.

### Accettazione

- nessun log forwarded senza FORWARDED;
- nessuna metrica terminale senza persistenza;
- DB outage scala al supervisor;
- nessuna lease scade in attesa del semaphore;
- retry converge sullo stesso Job.

---

## 27. P0-05 — Blindare il confine di finalizzazione

### Stato

Il test single-writer è verde e il UoW completion è autorizzato come gateway interno.

### Azioni

1. documentare esplicitamente artifacts vs completion;
2. cercare tutti i writer SUCCEEDED e completed_at;
3. mantenere allowlist minima e motivata;
4. impedire nuovi writer con invariant scan;
5. verificare che Job non preceda artifact READY;
6. test duplicate finalize;
7. test crash prima/dopo blob promotion;
8. test crash prima/dopo DB commit;
9. test losing attempt.

### Accettazione

- nessun terzo writer;
- Job non SUCCEEDED senza artifact richiesto;
- duplicate finalize idempotente;
- un solo final artifact READY.

---

## 28. P0-06 — Workload E2E reale

Fixture minima:

```text
1 Job
1 Task scene.composite.v1@1
1 worker CPU
1 voiceover o clip
1 scena
1 output H.264
```

Sequenza:

1. master con DB/BlobStore temporanei;
2. worker reale;
3. handshake gRPC v3;
4. registry con scene composite;
5. submit API;
6. TaskOffer;
7. TaskAccepted;
8. TaskLeaseGranted;
9. C++/FFmpeg render;
10. upload;
11. hash server-side;
12. artifact READY;
13. attempt SUCCEEDED;
14. Task SUCCEEDED;
15. Job SUCCEEDED;
16. ffprobe;
17. SHA-256;
18. metriche non zero.

Accettazione:

- nessun renderer mock;
- nessun SQL manuale;
- nessuna transizione saltata;
- output/log/DB snapshot archiviati su failure;
- required per cambi runtime.

---

## 29. P1-01 — Restringere le dipendenze ai confini

Usare interfacce consumer-owned:

```go
type ForwardingRepository interface {
    GetBySource(...)
    Insert(...)
    MarkReady(...)
    AtomicForwardAndEnqueue(...)
}

type JobReader interface {
    Get(context.Context, string) (*jobs.Job, error)
}
```

Non creare un framework DI.

Obiettivi:

- business logic non conosce `*SQLiteStore`;
- fake stretti nei test;
- concrete implementation solo nel composition root;
- niente global singleton o service locator.

---

## 30. P1-02 — Consolidare CI

1. target canonici `make test-go`, `test-native`, `test-architecture`, `test-e2e`, `verify`;
2. workflow matrix con job distinti;
3. setup e logica non duplicati;
4. regex test deve matchare almeno un test;
5. CTest deve scoprire almeno un test;
6. gate corretti required;
7. summary unica;
8. artifact di failure archiviati.

---

## 32. P1-04 — Certificazione worker e mTLS

Minimo:

- worker ID stabile;
- certificato dedicato;
- identity-cert mapping;
- no plaintext in staging/prod;
- doctor JSON versionato;
- engine/FFmpeg reali;
- cache/blob/disk writable;
- registry non vuoto;
- canary CPU;
- 24h soak;
- rollout/rollback per digest.

Doctor e bootstrap esistenti sono una base. Il gate finale deve includere sessione master, identity e workload reale.

---

# PARTE V — REGOLE DI IMPLEMENTAZIONE

## 34. Cosa non fare

- Non duplicare owner esistenti.
- Non aggiungere un secondo registry.
- Non aggiungere switch executor accanto al registry.
- Non aggiungere fallback legacy per far passare test.
- Non aggiungere write API di compatibilità.
- Non usare log come sostituto della persistenza.
- Non incrementare metriche prima del commit.
- Non dichiarare ready con probe placeholder.
- Non considerare nil un successo per loop permanenti.
- Non introdurre astrazioni generiche per requisiti futuri.
- Non creare branch o PR per il normale flusso concordato: lavoro e push avvengono direttamente su `main`.

---

## 35. Workflow di modifica sicuro

1. identificare owner;
2. scrivere test che dimostra il bug;
3. cambiare una responsabilità;
4. evitare refactor laterali;
5. eseguire test targeted;
6. eseguire `make verify`;
7. controllare diff e stato;
8. push su `main`;
9. verificare commit remoto e ultimi cinque commit;
10. aggiornare checklist solo con evidenza.

---

## 36. Metriche minime

### Master

- READY/LEASED/RUNNING Task;
- lease expiry;
- stale/duplicate report;
- forwarding queue depth e oldest age;
- forwarding transition failure;
- unreconciled terminal Task;
- outbox pending e oldest;
- delivery retry/failure;
- upload verifying/stuck;
- artifact quarantine;
- conflict budget per key;
- runner state/restart.

### Worker

- session active;
- heartbeat age;
- active Task e slot;
- CPU/RSS/disk/temp;
- cache hit bytes;
- blob bytes;
- render/upload time;
- FFmpeg reason;
- executor rejection;
- certificate lifetime.

### Progetto

- wall-clock;
- critical path;
- worker busy time;
- parallel efficiency;
- cache ratio;
- retry/straggler;
- cost per output minute.

---

## 37. Definition of Done finale

```text
Clean checkout verification = PASS
Go unit/race = PASS
C++ CTest = PASS
Architecture invariants = PASS
Real gRPC workload = PASS
Production-like mTLS workload = PASS

Job = SUCCEEDED
Required Tasks = SUCCEEDED
Winning Attempts = SUCCEEDED
Final Artifact = READY
Final SHA-256 = verified
ffprobe contract = PASS

Master restart = recovered
Worker crash = recovered
Network partition = recovered
Drain/SIGTERM = clean

Lost Jobs = 0
Duplicate READY final artifacts = 0
Orphan terminal Tasks = 0
False-success transitions = 0
Production fallback count = 0

24-hour soak = PASS
Staging and production digest = identical
Rollback by digest = PASS
```

---

## 38. Stato sintetico

### Già presente e nella direzione corretta

- master/worker separati;
- gRPC Task-native;
- Job+Task atomici;
- TaskGraph/TaskAttempt persistenti;
- payload V2;
- executor registry;
- scene composite reale;
- cache/blob worker;
- forwarding persistente;
- job ID deterministico;
- Resolver comune;
- completion protocol;
- HMAC/fencing;
- outbox/delivery;
- supervisor;
- readiness framework;
- doctor/bootstrap;
- cost model master-side;
- watchlist tests promossi a must-pass.

### Parzialmente stabilizzato

- propagation errori runner;
- retry/conflict budget;
- capability readiness;
- recovery automatica;
- CI required checks;
- E2E reale;
- mTLS production-like;
- worker certification;
- metriche complete;
- proof artifact-before-job su tutte le failure window.

### Ancora target

- RenderPlan unico persistito;
- compiler registry completo;
- multi-Task DAG video;
- executor granulari;
- late composition;
- cache distribuita verificata;
- locality-aware scheduling;
- temporal sharding;
- critical path/parallel efficiency;
- soak distribuito certificato.

---

## 39. Conclusione

Velox non necessita di una riscrittura totale.

Il rischio principale è nella distanza tra:

```text
“il codice ha tentato l'operazione”
```

e:

```text
“la transizione è stata persistita,
verificata, osservabile e recuperabile dopo crash”
```

Ordine corretto:

1. rendere obbligatoria la baseline verde;
2. eliminare false-success;
3. blindare supervisor/readiness;
4. correggere retry/progress;
5. provare finalizzazione e artifact;
6. dimostrare E2E;
7. chiudere recovery e certificazione;
8. solo dopo espandere Job in DAG distribuito.

> **Ogni nuova capacità estende il percorso canonico esistente; non ne crea uno parallelo.**
