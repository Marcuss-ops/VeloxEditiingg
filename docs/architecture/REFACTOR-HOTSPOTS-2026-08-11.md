# Velox — hotspot e piano di stabilizzazione

Questo documento fotografa la codebase al 2026-08-11. È una checklist
operativa per i prossimi interventi; i test operativi 4/4, 12 job e 16 job
restano volutamente alla fine, dopo la chiusura dei difetti strutturali.

## Vincoli di lavoro

- Un solo blocco alla volta, con test prima/dopo.
- Modifiche atomiche direttamente su `main`, sempre pubblicate.
- Nessun intervento sul WIP preesistente: `DataServer/internal/grpcserver/handler_artifacts.go` e il binario locale `DataServer/fleetctl`.
- Nessun nuovo fallback `nil`, noop o stub nel wiring di produzione.
- `worker_id` resta immutabile; i nomi operativi usano `worker_name`.
- Le rimozioni di simboli o helper cross-package richiedono `scripts/ci/pre-removal-verify.sh`.

## Chiuso in questa stabilizzazione

- Build metadata riallineato a `VERSION.txt`.
- Delivery: gli errori nelle transizioni di fase, retry e finalizzazione non
  vengono più ignorati; risalgono come errori infrastrutturali.
- Worker heartbeat/session: una scrittura SQLite fallita non produce più un
  successo apparente e non aggiorna il read model in memoria; quando è
  presente un `session_id`, il writer verifica anche la coppia
  `session_id`/`worker_id`, `revoked=0` e `RowsAffected()==1`, così una
  sessione sconosciuta, revocata o appartenente a un altro worker non può
  sembrare valida.
- Worker auth/commands: token e `command_id` non vengono più restituiti come
  validi quando la relativa sessione o il comando non sono stati persistiti.
- Render plan: se il piano compilato è presente, il worker verifica che
  `compiled_render_plan_sha256` corrisponda al JSON ricevuto. Il piano v1 resta
  esplicitamente il percorso esecutivo finché il batch executor non è migrato.
- Creator forwarding: il passaggio a `RETRY_WAIT` è protetto da
  `runner_id`, `lease_id` e scadenza del lease.
- Completion/artifact: le transizioni CAS di attempt, task, upload e
  finalizzazione controllano gli effetti (`RowsAffected`) e i fixture del
  protocollo rappresentano ora un tentativo scheduler-owned reale.
- Delivery/smoke/calendar/rollout: gli update terminali o di operation non
  dichiarano più successo quando la persistenza fallisce o aggiorna zero righe;
  gli endpoint riportano anche i worker falliti.
- Store lifecycle e reconciler: i percorsi di `pipeline_runs`, completion,
  artifact, alert, job, forwarding, media-probe, dead-letter, delivery lease,
  M2M e riconciliazione stale non ignorano più gli errori del driver restituiti
  da `RowsAffected()`.
- Job terminalization: `FAILED`/`CANCELLED` non viene committato se history o
  event ledger falliscono; un errore di lettura del retry budget nel reconciler
  stale interrompe l'operazione invece di usare un valore non verificato.
- Retry cancellation: i backoff di remote-engine e multipart publisher usano
  timer stoppabili; anche retry asset/chunk e idle wait del prefetch usano
  timer/ticker riutilizzabili e rispettano la cancellazione.
- Worker retry loops: il bootstrap del protected-assets poller e il retry loop
  del client API usano timer stoppabili invece di creare `time.After` a ogni
  iterazione; i timeout one-shot di processo/stream restano distinti.
- Fleetctl polling: i timer tra i poll del ledger operation, job watch, job
  submit e wait-ready sono cancellabili tramite context, quindi un comando
  interrotto non lascia timer pendenti fino alla scadenza dell’intervallo.
- Social delivery: `429` conserva `Retry-After` negli errori tipizzati fino al
  `ProviderError`; il runner applica quella scadenza quando è valida e usa il
  backoff bounded locale solo in assenza di un header valido.
- Remote engine retry: `Retry-After` non viene più alterato dal jitter locale;
  il jitter resta limitato ai backoff calcolati dal client.
- Worker command outbox: allocazione della sequenza per worker e INSERT sono
  nella stessa transazione; il caso concorrente è coperto da test.
- Calendar/session read models: JSON persistito corrotto e timestamp sessione
  malformato producono errore, non uno stato apparentemente valido.
- Calendar read paths: timestamp persistiti malformati, JSON delle collezioni,
  errori di `Scan` e `rows.Err()` non vengono più ignorati nei read model.
- Worker heartbeat/read models: errori nella lettura dello stato precedente,
  throttling delle metriche e timestamp runtime non vengono più degradati a
  valori vuoti; i formati timestamp legacy validi restano supportati.
- Worker snapshot lists: `ListWorkers` e `ListWorkersByWorkspace` non
  scartano più righe con `Scan`, JSON corrotto o errore di iterazione; il
  read model fallisce esplicitamente invece di restituire una flotta parziale.
- Worker command outbox: payload JSON, timestamp e iterazione delle righe
  vengono validati durante la lettura; una coda corrotta non viene più
  interpretata come coda vuota.
- Worker revocation read model: errori di `Scan` e `rows.Err()` nella lista
  dei worker revocati risalgono al registry invece di produrre una lista
  parziale.
- Legacy job read models: tentativi, artifact, asset, DLQ ed eventi ora
  propagano gli errori di scansione; anche la serializzazione degli eventi
  non viene più ignorata.
- Audit/DLQ read models: timestamp corrotti e metadata JSON invalidi non
  vengono più trasformati in `zero time`, `now` o `{}`; l’endpoint risale con
  errore invece di mostrare una storia operatore alterata.
- Smoke, fleet-operation e worker-metrics read models: timestamp obbligatori
  e payload JSON corrotti interrompono la lettura invece di produrre record
  apparentemente validi.
- Task, resource-sample, validation, upload-chunk e current-task read models:
  gli stessi timestamp persistiti vengono validati al confine store; i campi
  opzionali NULL restano assenza semantica, non errore artificiale.
- Profiling read model: le fasi di un attempt rifiutano scan e timestamp
  corrotti invece di trasformarli in tempi zero, preservando benchmark
  veritieri.
- Metrics phase/segment read model: una riga non scansionabile non viene più
  saltata e `wall_start`/`wall_end` corrotti interrompono la lettura; i
  breakdown di profiling non possono quindi apparire parziali o con tempi
  zero (`0f0be847`).
- Fleet inventory read model: una riga `ansible_hosts` corrotta non viene
  scartata lasciando una registry SSH parziale; il bootstrap riceve l'errore.
- Drive-link read model: JSON e scansioni corrotti risalgono ai resolver; la
  ricerca cartelle distingue finalmente “non trovato” da errore SQLite.
- Job read model: JSON `request/result/slot` corrotti, scan parziali e conteggi
  incompleti ora interrompono la lettura; anche il repository per status non
  salta più righe non decodificabili.
- Attempt read model: timestamp `task_attempts` malformati e righe non
  scansionabili interrompono il percorso invece di diventare tempi zero o
  una lista parziale.
- Benchmark read model: baseline e benchmark run con timestamp corrotti o
  righe non leggibili non entrano più nei risultati come dati incompleti.
- Ansible/Fleet read model: host, run, command JSON e associazioni host
  corrotti non vengono più saltati durante inventory e audit operations.
- Ansible compatibility history/inventory: store obbligatorio, errori di
  lookup/lista/associazione host e conteggi non diventano più liste vuote,
  host mancanti o capability apparentemente sane; gli endpoint usano `503`
  per indisponibilità del datastore e `404` solo per record assenti.
- Drive token listing: errori di accesso alla directory token non diventano
  più `200` con lista vuota; il servizio propaga l'errore e l'handler risponde
  `503`.
- Schema introspection: errori durante `PRAGMA table_info` non vengono più
  trattati come colonne assenti durante il bootstrap SQLite.
- Worker status API: un errore del read model persistito risponde `503`
  invece di degradare a una cache in memoria potenzialmente obsoleta; il
  fallback resta limitato al solo DB vuoto durante il bootstrap.
- Creator idempotency fast-path: un errore nel lookup o nella riparazione del
  forwarding non produce più `enqueue_confirmed=true`; il risultato viene
  restituito solo dopo che la transizione persistita è stata verificata.
- M2M read models: timestamp corrotti nelle chiavi API o nell'audit non
  vengono più convertiti in zero time; la lettura fallisce esplicitamente.
- Deployment/alert read models: timestamp corrotti non vengono più degradati
  a tempi zero; gli update di stato, rollback e touch alert verificano anche
  `RowsAffected` e restituiscono il not-found sentinel quando la riga manca.
- Enqueue Social preflight: il validator non ha più un noop implicito in
  produzione; i piani Drive-only restano compatibili, mentre una destinazione
  Social senza dipendenza configurata viene rifiutata con `ErrNotConfigured`.
- DB pool telemetry: `OpenConnections`, `InUse`, `Idle`, `WaitCount` e
  `WaitDuration` sono esposti senza label per distinguere lock da queueing.
- OpenTelemetry bootstrap: exporter sconosciuto o OTLP senza endpoint viene
  rifiutato al bootstrap; un errore di inizializzazione resta `MISCONFIGURED`
  e porta `/ready` in rosso invece di diventare un noop apparentemente sano
  (`2550eaa7`).
- Bootstrap worker: la lista ffmpeg/ffprobe è una dipendenza esplicita con
  copia difensiva, non più una slice globale esportata e mutabile.
- Worker job lifecycle: i contatori di successo/fallimento usano un
  collector per istanza creato dal costruttore; il global metrics facade resta
  solo compatibilità per fixture legacy costruite manualmente nei test.
- Artifact GC, credential revoke e command delivery verificano ownership e
  righe aggiornate prima di dichiarare completata l'operazione.
- Sessioni worker: `ValidateSession` e `UpdateSessionLastSeen` richiedono una
  riga aggiornata; revoke, snapshot cleanup e scadenza sessioni verificano
  comunque il risultato del driver mantenendo il comportamento idempotente.
- Artifact reconciliation e worker partition recovery controllano il numero
  di righe della transizione prima di confermare `FAILED`, `QUARANTINED` o
  `PARTITIONED`.
- Fleet operation e cleanup DLQ/command/drive-link non ignorano più gli errori
  restituiti da `RowsAffected()`.
- Render-plan stamping: un tentativo inesistente non può più risultare
  aggiornato con successo; il writer restituisce un conflitto CAS.
- Render-plan offer path: il piano viene validato e confrontato con
  `job_id`/`attempt_id` prima di essere persistito o consegnato; nil plan e
  identità incoerenti sono rifiutati fail-closed.
- Render-plan profiling: il log strutturato del Master separa
  `compile_ms`, `canonicalize_ms`, `hash_ms`, `persist_ms` e `total_ms`, e
  registra il motivo dei fallback (`compile_error`, identità, canonicalization
  o persist error) senza creare un compiler parallelo.
- Worker render profiling: il profilo espone ora `compile_plan_ms`, le fasi
  native disponibili (`asset_resolution_ms`, `asset_download_ms`,
  `audio_timeline_compile_ms`, `audio_prepare_ms`, `audio_mix_ms`,
  `mux_ms`) e il blocco artifact con `artifact_sha_ms`,
  `artifact_probe_ms`, `artifact_finalize_ms` e `artifact_total_ms`.
  I tempi SHA/probe provengono dal manifest reale; il tempo AAC resta assente
  quando FFmpeg lo misura insieme al mix, invece di essere stimato.
- Worker timing semantics: i tempi delle singole fasi sono esclusivi rispetto
  alla propria operazione. `artifact_total_ms` parte dopo la fine del render e
  termina quando output e progress receipt hanno manifest pronti; non include
  compile/render e non viene sommato ai tempi nested del motore.
- Atomic ingest/reconciler: un mismatch SHA non viene più registrato in modo
  silenziosamente incompleto; se l'evento audit non persiste, l'ingest viene
  rollbackato. Il reconciler di artifact può promuovere un task solo quando
  l'attempt corrispondente è stato realmente portato a `SUCCEEDED` o è già
  `SUCCEEDED` con identità task/job/worker/lease esatta.
- Creator forwarding: la promozione sincrona a `READY_TO_FORWARD` verifica
  anche che nessun runner abbia acquisito la lease nel frattempo; una race
  restituisce conflitto senza cancellare l’ownership del runner.
- Delivery plan resolver: una istanza SQLite senza database non viene più
  interpretata come “piano assente”; restituisce un errore di configurazione
  distinto da `ErrNoExplicitPlan`, mantenendo separati misconfiguration e
  contratto render-only.
- Attempt ingest projections: versioning, render identity e tracing verificano
  ora `RowsAffected()==1` dopo il CAS principale; un tentativo non più
  raggiungibile non può quindi completare l’ingest lasciando metadati
  parziali senza errore.
- Lease reconciler: quando il jobs repository è configurato, gli errori nella
  lettura del budget o nella finalizzazione del job padre vengono propagati;
  il risultato del reap già committato resta disponibile per il retry.
- Job progress: gli snapshot tardivi di un tentativo precedente o con
  timestamp più vecchio non possono più regredire il read model.
- Render-only: il contatore audio riconosce il contratto esplicito a zero
  destinazioni; i job normali senza delivery plan continuano a fallire chiusi.
- Gate architetturali e gate full-module verdi dopo i fix (`833cf79e`,
  `e4c7154b`, `f64dbae3`, `e1014598`); il polling fleetctl è coperto dal
  test/vet del package `cmd/fleetctl` (`6b2ae939`, `5b1e2fc3`).

## Hotspot da risolvere in ordine

### P0 — Contratti autorevoli e concorrenza

- [x] Propagare gli errori di persistenza della delivery.
- [x] Impedire retry forwarding da lease non più proprietario.
- [x] Rendere fail-closed la persistenza di heartbeat e `last_seen`.
- [x] Rendere fail-closed token session e command queue quando il DB fallisce.
- [x] Rafforzare i writer già toccati: completion, delivery attempt, smoke,
  calendar e rollout command controllano errore, ownership e righe aggiornate.
- [ ] Completare l'audit dei writer non ancora coperti: ogni `UPDATE`
  autorevole deve controllare `RowsAffected`, ownership e generazione. Restano
  soprattutto i writer del runtime volatile e le transizioni publication/
  session che non sono ancora state ricondotte a un unico helper CAS.
- [x] Audit mirato di `MarkDeliverySucceeded`,
  `CompletePublicationAfterReconciliation` e `TaskResult`: i writer
  verificano CAS/ownership, `RowsAffected` e commit; nessun percorso
  dichiara successo dopo una transizione non avvenuta.
- [x] `FinalizeVerified`: il percorso moderno usa CAS su `AttemptID`; il
  percorso legacy è recintato da worker/lease e, sullo schema canonico, da
  `task_attempts`, con replay terminale ammesso solo per la stessa identità.
- [x] Media-probe completion/failure: artifact, parent job, quarantine e
  delivery-plan read ora sono verificati con `RowsAffected`/`rows.Err()` nella
  stessa transazione della lease.
- [x] Deployment ledger e fleet alerts: le transizioni terminali, il flag di
  rollback e il refresh di un alert ACTIVE non dichiarano più successo senza
  una riga autorevole aggiornata; i read model rifiutano timestamp corrotti.
- [x] Rimuovere il fail-open del Social destination preflight: `nil` non
  equivale più a validazione riuscita quando il piano richiede Social.
- [x] Proteggere il caso stale-artifact in cui il commit è presente ma
  l'attempt è già fallito o appartiene a un'altra identità: nessuna
  promozione task/job viene più eseguita in quel caso.
- [ ] Aggiungere test di race/reclaim per: lease scaduto, reconnect worker,
  retry concorrente e doppio finalizer.

### P1 — Render plan e misurabilità

- [x] Hash del compiled plan verificato sul worker.
- [ ] Definire il passaggio di versione che abilita il batch executor a usare
  davvero `segments[]`, `audio[]` e `assets[]`.
- [ ] Solo dopo il passaggio precedente, eliminare la doppia interpretazione
  v1/compiled; fino ad allora il comportamento additive è intenzionale.
- [x] Registrare nel log per ogni attempt il motivo per cui il compiled plan manca:
  compiler assente, compile error, canonicalization error o persist error.
- [x] Separare metriche Master `compile`, `canonicalize`, `hash`, `persist`.
- [x] Esporre il tempo nativo `audio_prepare_ms` (compilazione del piano,
  costruzione del filter graph e comando) nel profilo worker.
- [x] Completare le metriche worker disponibili per `asset resolution`,
  `timeline`, `audio preparation`, `mix`, `mux`, `finalize` e `SHA`. Il tempo
  AAC resta deliberatamente non valorizzato quando il processo FFmpeg espone
  solo il comando combinato mix+encode.
- [x] Documentare la semantica nested/exclusive: il profilo non somma fasi
  sovrapposte; `artifact_total_ms` è un intervallo post-render indipendente.

### P1 — Retry, deadline e idempotenza

- [x] Catalogare e collegare il primo confine provider: remote engine,
  forwarding, delivery, multipart publisher e downloader restano policy
  separate per responsabilità, mentre il Social `Retry-After` attraversa il
  confine in modo tipizzato.
- [ ] Uniformare le policy duplicate: remote engine, forwarding, delivery,
  multipart publisher e downloader.
- [ ] Per ogni policy definire: tentativi totali, errori retryable, massimo
  backoff, jitter, `Retry-After`, deadline e comportamento su cancellation.
- [x] Sostituire i backoff già individuati non cancellabili con timer legati al
  context (remote engine, multipart publisher, asset/chunk worker); resta
  l'audit delle policy.
- [ ] Verificare che ogni retry abbia una chiave idempotente stabile e che un
  retry dopo timeout non generi un secondo effetto remoto.
- [ ] Distinguere sempre `lease lost`, `provider error` e `DB/infrastructure
  error`; non farli confluire nello stesso contatore.

### P1 — Confini I/O e SQL

- [x] Portare `jobs/enqueue/drive_resolution.go` verso un repository di
  lookup read-only; gli errori DB non degradano più silenziosamente a
  “reference originale”.
- [x] Portare `creatorflow/resolver.go` verso repository tipizzati senza
  duplicare la transazione atomica esistente: i constructor SQLite restano
  adapter di composizione, mentre il Resolver riceve `JobLookup`,
  `ForwardingRepository` e `DriveFolderResolver` tramite porte esplicite.
- [ ] Verificare i resolver delivery e gli adapter metrici contro
  `check-db-access.sh`; ogni eccezione deve avere motivazione e owner.
- [x] Rendere esplicita la semantica del retry remoto: `Retries=0` esegue
  un solo tentativo; i valori negativi non ottengono retry impliciti.
- [x] Preservare `retry_budget=0` lungo tutta la catena delivery plan →
  `job_deliveries.max_attempts` → lease → runner; zero ora significa un
  tentativo iniziale senza retry.
- [x] Misurare `sql.DB.Stats()` del Master: `WaitCount` e `WaitDuration`, oltre
  ai contatori di pool, senza label ad alta cardinalità.
- [ ] Mantenere un solo writer per ogni aggregate e CAS esplicito per le
  transizioni concorrenti.
- [x] Rendere `ApplyReconciledDelivery` fail-closed: una proiezione del
  reconciler che non aggiorna alcuna riga ora restituisce `ErrDeliveryNoRow`
  invece di dichiarare successo; il comportamento sui delivery terminali
  esistenti resta idempotente.

### P1 — Worker layering e stato globale

- [x] Rimuovere la dipendenza diretta dei package pubblici worker da package
  `internal`: `pkg/cache` e le pipeline pubbliche passano dalla facciata
  stretta `pkg/observability`, senza duplicare telemetria o tracing.
- [x] Rendere la lista binari ffmpeg una dipendenza di bootstrap immutabile o
  un’opzione esplicita, non una slice globale esportata e mutabile.
- [x] Limitare lo stato globale a registri read-only o inizializzazione; il
  lifecycle dei job riceve un collector metrics per istanza e non condivide
  più i contatori tramite il singleton globale.
- [x] Verificare che un errore di bootstrap non possa lasciare `READY=true`:
  worker `Start` richiede `bootstrap.HardGate`, mentre FleetController e
  DeliveryRunner rifiutano l'avvio senza store durevole.

### P2 — Legacy e documentazione

- [x] Aggiornare `docs/roadmap/README.md`: le directory `refactored/...` non
  esistono più e il documento descrive ancora fasi già completate.
- [ ] Tenere le route 410 solo finché esiste evidenza di traffico e un owner;
  la rimozione richiede audit usage + gate full-module.
- [ ] Rimuovere i dead seam Ansible/remote solo dopo conferma che non siano
  ancora percorsi di break-glass o fixture operative.
- [ ] Eliminare codice commentato e helper inutilizzati solo in commit
  atomici, senza confondere compatibilità documentata con dead code.

### Stato gate al 2026-08-11

- [x] `go vet ./...` e `go build ./...` del modulo Master.
- [x] `go test -count=1 ./...` del modulo Master: tutti i package verdi,
  incluso `internal/store`.
- [x] Test e vet del modulo worker; `check-db-access`, `check-no-legacy`,
  `check-capability-contract` e `check-architecture` verdi.
- [x] `pre-removal-verify.sh` eseguito prima della rimozione della slice globale
  `FFmpegBinaries`; vet, build e test full-module verdi.

## Benchmark finali — non anticipare

1. Certificazione prefetch: 20 run × 4 worker, `B` pronto prima dell’avvio,
   zero download foreground.
2. Fleet/operations: rollout, drain, restart, ready e digest verification
   senza SSH manuale.
3. Scaling: workload identico a 12 job, poi identico a 16 job.
4. Matrice render: 5/10 minuti × poche/molte scene × audio semplice/complesso,
   ripetuta su ogni worker.
5. Profiling e AudioProgram: prima misurare, poi compilare la timeline,
   poi valutare audio preparation upstream.
6. Target performance: p50 `<15s`, poi `<12s`, infine `<10s`, mantenendo
   correttezza, determinismo e SHA artifact equivalenti.

## Gate minimo per ogni blocco

```text
gofmt / lint locale
test package interessati
go vet package interessati
check-architecture.sh
git diff --check
commit atomico
push main
```

Per rimozioni o cambi di contratti cross-package aggiungere sempre:

```bash
bash scripts/ci/pre-removal-verify.sh
```
