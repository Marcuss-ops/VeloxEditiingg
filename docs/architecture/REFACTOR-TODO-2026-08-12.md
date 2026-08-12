# Velox — TODO di stabilizzazione e performance

Questa lista è il piano operativo della codebase dopo l'audit strutturale.
L'ordine è intenzionale: prima contratti autorevoli e osservabilità, poi
scaling, quindi ottimizzazione audio. Ogni blocco deve essere piccolo,
reversibile, testato prima/dopo, committato su `main` e pubblicato.

## Regole di avanzamento

- [x] Lavorare su un solo hotspot alla volta.
- [x] Preservare il WIP preesistente in `handler_artifacts.go` e `DataServer/fleetctl`.
- [x] Non introdurre fallback produttivi `nil`, noop o stub nascosti.
- [x] Mantenere `worker_id` immutabile e usare `worker_name` per la vista operativa.
- [x] Per ogni rimozione esportata eseguire `scripts/ci/pre-removal-verify.sh`.
- [ ] Per ogni blocco nuovo aggiungere almeno un test di errore, un test di
      cancellation/race quando applicabile e una prova del contratto HTTP/SQL.
- [ ] Aggiornare la checklist architetturale con commit e comando di verifica.

## Fase A — contratti autorevoli e read model

### A1. Writer/CAS e ownership

- [x] Delivery success: ownership, lease, stato atteso e `RowsAffected`.
- [x] Publication reconciliation: evidence, fase, commit e `RowsAffected`.
- [x] Attempt render-plan stamping: identità e conflitto CAS.
- [x] Attempt ingest projections: versioning, render identity e tracing.
- [x] Worker heartbeat: sessione, worker, revocation e `RowsAffected`.
- [x] Worker command/auth: persistenza verificata prima di emettere token o command.
- [ ] Audit finale di ogni `UPDATE` autorevole fuori dagli helper già verificati.
- [ ] Per ogni aggregate documentare un solo writer principale e i writer
      compatibili ammessi.
- [ ] Aggiungere CAS con generazione/lease dove oggi esiste solo un filtro di stato.
- [ ] Aggiungere test per doppio finalizer, vecchia attempt, vecchia sessione,
      worker riconnesso e lease trasferita.

### A2. Read model e errori

- [x] Snapshot worker, command outbox, revocation, task/attempt/artifact e
      profiling: scan, timestamp, JSON e `rows.Err()` fail-closed.
- [x] Ansible run history: store obbligatorio; errore DB distinto da run assente.
- [x] Ansible inventory: conteggi/list/lookup non degradano a zero o lista vuota.
- [x] Drive token listing: errore `ReadDir` distinto da directory vuota.
- [x] Drive folder cache e master folders: lettura SQLite fallita non serve
      cache vuota/stale e risale all'handler come `503`.
- [ ] Audit dei read model residui che restituiscono `[]`, `{}`, `0` o `false`
      dopo un errore di I/O; classificare ogni eccezione come “assenza valida”
      oppure “guasto”.
- [ ] Rendere uniforme il mapping HTTP: `404` solo per risorsa assente,
      `409` per conflitto, `503` per dipendenza non disponibile, `500` per bug
      interno non classificato.

## Fase B — retry, deadline e idempotenza

### B1. Inventario delle policy

- [x] Remote engine: classificazione typed, cancellation e `Retry-After`.
- [x] Forwarding e delivery: schedule bounded e lease-aware.
- [x] Multipart/asset/chunk: timer cancellabili.
- [x] Social delivery: `Retry-After` tipizzato e idempotency key stabile.
- [x] Remote engine `StartPipeline`: idempotency key fornita dal caller.
- [x] Remote engine script POST: idempotency key deterministica stabile tra retry.
- [ ] Scrivere una tabella canonica per ogni policy con: max attempts,
      errori retryable, backoff, jitter, `Retry-After`, deadline e cancellation.
- [ ] Verificare che `Retries=0` abbia semantica esplicita e uguale alla
      documentazione in ogni client; nessun default implicito non dichiarato.
- [ ] Separare contatori e log per `lease_lost`, `provider_error`,
      `db_error`, `context_canceled` e `permanent_error`.

### B2. Effetti remoti

- [x] Social provider: stessa chiave per ogni replay della delivery.
- [x] Drive provider: marker/idempotency boundary persistente.
- [ ] Verificare forwarding: timeout dopo creazione remota non crea un secondo
      pipeline; la chiave deve essere collegata a job/attempt, non al runner.
- [ ] Verificare ogni POST remoto ritentato e classificare esplicitamente quelli
      che non possono essere ritentati in sicurezza.
- [ ] Aggiungere test con server che applica l'effetto e chiude la connessione
      prima della risposta; il secondo tentativo deve riconciliare lo stesso effetto.
- [ ] Imporre deadline complessiva distinta dal timeout della singola richiesta.

## Fase C — confini e complessità

- [x] `check-db-access.sh` verde sul perimetro attuale.
- [x] Resolver creator/enqueue separati da SQL diretto tramite porte tipizzate.
- [ ] Verificare delivery plan resolver e metric adapters contro il ratchet SQL.
- [ ] Ridurre le funzioni con branching elevato nei path di claim, completion,
      delivery e worker update; estrarre solo decisioni di dominio nominate.
- [ ] Cercare feature envy tra handler, store e service; spostare la logica nel
      modulo proprietario senza creare un nuovo package generico prematuro.
- [ ] Eliminare duplicazioni di parsing per manifest, asset reference e payload;
      una sorgente canonica per ogni contratto.
- [ ] Audit di stato globale mutabile, singleton e registri; consentire solo
      dati read-only o inizializzazione protetta.
- [ ] Audit allocazioni nei loop di render, download, scansione e metriche;
      misurare prima di introdurre pool o riuso buffer.
- [ ] Cercare lookup O(n) ripetuti su liste di worker, asset e destination;
      sostituire con mappe solo dove il profiling dimostra un costo reale.

## Fase D — prefetch deterministico (prima priorità operativa congelata)

- [ ] Misurare il waterfall completo:
      `reservation_created`, `plan_generated`, `plan_sent`, `plan_received`,
      `prefetch_queued`, `download_started`, `asset_ready`, `job_started`.
- [ ] Correggere la reprioritizzazione `JobID` nello scheduler senza ricreare
      downloader o cancellare transfer condivisi.
- [ ] Introdurre generation/version per invalidare work item obsoleti.
- [ ] Passare da resolve sequenziale a queue asset-level bounded e centralizzata.
- [ ] Ordinare la queue con `distance`, `enqueued_at`, `asset_key`.
- [ ] Conservare QoS `FOREGROUND > N+1 > N+2 > N+3`.
- [ ] Applicare admission control per concurrency, byte budget e disk pressure.
- [ ] Verificare cancellation del waiter senza cancellare transfer condivisi.
- [ ] Aggiungere metriche senza label `job_id`, `asset_id` o SHA ad alta cardinalità.
- [ ] Calcolare `ready_lead_ms` per asset prefetchable.
- [ ] Certificare 20 run × 4 worker.
- [ ] Acceptance: 80/80 startup B validi, coverage 100%, zero download foreground,
      zero byte foreground e nessun artifact corrotto.

## Fase E — Fleet/Operations e reconciler

- [ ] Separare persistentemente `desired_digest`, `running_digest`,
      `last_successful_digest` e stato operation.
- [ ] Formalizzare la state machine `REQUESTED → DRAINING → DEPLOYING →
      RESTARTING → WAITING_READY → VERIFYING_DIGEST → SUCCEEDED/FAILED`.
- [ ] Rendere `fleetctl` + Master il percorso normale per config, rollout,
      restart, drain, ready e verifica.
- [ ] Lasciare SSH manuale solo come break-glass documentato.
- [ ] Centralizzare `WorkerDesiredConfig`, versione e hash applicati.
- [ ] Distinguere capability `DISABLED`, `READY`, `MISCONFIGURED` in bootstrap
      e readiness; nessun executor noop produttivo.
- [ ] Completare lo stale-job reconciler con lease, session generation,
      attempt generation e ownership autorevole.
- [ ] Garantire CAS/idempotenza del reconciler e riconciliazione dei child task.
- [ ] Testare rollout fallito, restart interrotto, worker riconnesso,
      operation stale e retry del reconciler.

## Fase F — scaling controllato

- [ ] Congelare workload, fixture, configurazione e versione worker.
- [ ] Eseguire 12 job reali su 4 worker con prefetch attivo e nessun intervento.
- [ ] Raccogliere completati, durata p50/p95/max, queue wait, CPU/RAM,
      disk I/O, cache hit, foreground download, coverage, SQLite latency/
      locks e tmp residuali.
- [ ] Acceptance 12: 12/12 success, artifact validi, zero stale/locked/tmp,
      4/4 worker READY.
- [ ] Ripetere lo stesso identico workload con 16 job.
- [ ] Confrontare delta 12→16: queue wait, esecuzione, RAM, CPU, SQLite,
      coverage e download foreground.
- [ ] Decidere se il limite è throughput reale o semplice accumulo di coda.

## Fase G — profiling render e audio

- [x] Master: compile/canonicalize/hash/persist/total.
- [x] Worker: asset resolution, download, timeline, prepare, mix, mux,
      finalize, SHA e artifact total secondo semantica exclusive/nested.
- [ ] Verificare che ogni job produca automaticamente un breakdown completo
      o un motivo esplicito per ogni fase non disponibile.
- [ ] Salvare fixture permanenti: 5m poche/semplice, 5m molte/complesso,
      10m poche/semplice, 10m molte/complesso.
- [ ] Eseguire almeno 5 run per fixture su ogni worker.
- [ ] Correlare costo con durata video, scene, asset, segmenti audio e hardware.
- [ ] Non ottimizzare audio prima di avere il breakdown reale dei 19 secondi.

## Fase H — AudioProgram e preparazione upstream

- [ ] Estendere il canonical render plan con `AudioProgram`.
- [ ] Usare sample index/timebase razionale, non secondi floating-point come
      sorgente canonica.
- [ ] Includere `AudioProgram` nel canonical JSON e nel plan hash.
- [ ] Rendere il worker esecutore del programma, non interprete della semantica.
- [ ] Testare compile deterministico e SHA equivalente su 4 worker.
- [ ] Benchmark prima/dopo AudioProgram.
- [ ] Solo dopo introdurre audio assets preparati upstream.
- [ ] Derivare la chiave prepared asset da source SHA + transform spec + version.
- [ ] Far passare gli asset preparati da registry, resolver, cache e prefetch canonici.

## Fase I — obiettivi performance

- [ ] Baseline pubblicata: media ~19.3 s, range 15–25 s.
- [ ] Primo obiettivo: p50 < 15 s senza regressione di correctness/determinism.
- [ ] Secondo obiettivo: p50 < 12 s.
- [ ] Obiettivo finale: p50 < 10 s solo dopo evidenza del collo di bottiglia.
- [ ] Dopo gli obiettivi audio, rivalutare RAM L1/NVMe/cache; non ottimizzare
      il cache hit ratio se il baseline mostra già zero miss/download.

## Gate di chiusura

- [ ] `gofmt`, `git diff --check`, test mirati e test di package.
- [ ] `go vet ./...`, `go build ./...`, `go test -count=1 ./...` quando il
      blocco tocca contratti cross-package o rimozioni.
- [ ] `check-architecture.sh`, `check-db-access.sh`, `check-no-legacy.sh` e
      `check-capability-contract.sh` quando il perimetro li coinvolge.
- [ ] Commit atomico con messaggio descrittivo.
- [ ] Push su `main` dopo ogni blocco.
- [ ] Aggiornamento di questa lista e di `REFACTOR-HOTSPOTS-2026-08-11.md`.
