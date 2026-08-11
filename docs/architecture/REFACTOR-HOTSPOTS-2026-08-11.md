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
  successo apparente e non aggiorna il read model in memoria.
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
- Worker command outbox: allocazione della sequenza per worker e INSERT sono
  nella stessa transazione; il caso concorrente è coperto da test.
- Calendar/session read models: JSON persistito corrotto e timestamp sessione
  malformato producono errore, non uno stato apparentemente valido.
- Calendar read paths: timestamp persistiti malformati, JSON delle collezioni,
  errori di `Scan` e `rows.Err()` non vengono più ignorati nei read model.
- Worker heartbeat/read models: errori nella lettura dello stato precedente,
  throttling delle metriche e timestamp runtime non vengono più degradati a
  valori vuoti; i formati timestamp legacy validi restano supportati.
- DB pool telemetry: `OpenConnections`, `InUse`, `Idle`, `WaitCount` e
  `WaitDuration` sono esposti senza label per distinguere lock da queueing.
- Bootstrap worker: la lista ffmpeg/ffprobe è una dipendenza esplicita con
  copia difensiva, non più una slice globale esportata e mutabile.
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
- Atomic ingest/reconciler: un mismatch SHA non viene più registrato in modo
  silenziosamente incompleto; se l'evento audit non persiste, l'ingest viene
  rollbackato. Il reconciler di artifact può promuovere un task solo quando
  l'attempt corrispondente è stato realmente portato a `SUCCEEDED` o è già
  `SUCCEEDED` con identità task/job/worker/lease esatta.
- Job progress: gli snapshot tardivi di un tentativo precedente o con
  timestamp più vecchio non possono più regredire il read model.
- Render-only: il contatore audio riconosce il contratto esplicito a zero
  destinazioni; i job normali senza delivery plan continuano a fallire chiusi.
- Gate architetturali e gate full-module verdi dopo i fix (`833cf79e`,
  `e4c7154b`).

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
- [ ] Chiudere l'audit mirato di `MarkDeliverySucceeded`,
  `FinalizeVerified`, `CompletePublicationAfterReconciliation` e `TaskResult`:
  nessun ritorno `nil` dopo un commit non avvenuto.
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
- [ ] Separare metriche Master `compile`, `canonicalize`, `hash`, `persist`.
- [x] Esporre il tempo nativo `audio_prepare_ms` (compilazione del piano,
  costruzione del filter graph e comando) nel profilo worker.
- [ ] Completare le metriche worker `asset resolution`, `timeline`, `mix`,
  `AAC`, `mux`, `finalize`, `SHA`; il mix/AAC resta un unico bucket finché
  il processo FFmpeg non espone una separazione veritiera.
- [ ] Assicurare che la somma dei tempi sia documentata come nested o
  exclusive, evitando breakdown matematicamente incoerenti.

### P1 — Retry, deadline e idempotenza

- [ ] Catalogare le policy duplicate: remote engine, forwarding, delivery,
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
- [ ] Portare `creatorflow/resolver.go` verso repository tipizzati senza
  duplicare la transazione atomica esistente.
- [ ] Verificare i resolver delivery e gli adapter metrici contro
  `check-db-access.sh`; ogni eccezione deve avere motivazione e owner.
- [x] Misurare `sql.DB.Stats()` del Master: `WaitCount` e `WaitDuration`, oltre
  ai contatori di pool, senza label ad alta cardinalità.
- [ ] Mantenere un solo writer per ogni aggregate e CAS esplicito per le
  transizioni concorrenti.

### P1 — Worker layering e stato globale

- [ ] Rimuovere la dipendenza dei package pubblici worker da package `internal`
  (`pkg/cache` → telemetry/trace interno; pipeline pubbliche → trace interno).
- [x] Rendere la lista binari ffmpeg una dipendenza di bootstrap immutabile o
  un’opzione esplicita, non una slice globale esportata e mutabile.
- [ ] Limitare lo stato globale a registri read-only o inizializzazione; ogni
  contatore/observer che deve essere isolabile nei test riceve un’istanza.
- [ ] Verificare che un errore di bootstrap non possa lasciare `READY=true`.

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
