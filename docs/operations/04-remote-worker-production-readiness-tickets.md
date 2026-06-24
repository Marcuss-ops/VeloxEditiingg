# Runbook operativo 04 — Ticket di production readiness per worker remoti

Status: pianificato

Data snapshot: 2026-06-24

Repository: `Marcuss-ops/VeloxEditiingg`

Obiettivo: certificare ogni computer worker remoto prima dell'ingresso nel pool di produzione. Un worker è considerato `PRODUCTION_READY` soltanto quando tutti i ticket obbligatori di questo documento sono chiusi con evidenze riproducibili.

---

## 1. Regole di esecuzione

1. Un ticket corrisponde a un solo problema e, salvo eccezioni documentali, a una sola PR.
2. Non mescolare refactor, nuove feature e test non correlati nello stesso ticket.
3. Prima di iniziare un ticket, partire da `main` aggiornato e cercare il codice già esistente.
4. Non duplicare registry, resolver, sampler, health model, status model o logica di validazione.
5. Ogni nuova capacità deve entrare nel componente canonico già proprietario del dominio.
6. I test devono esercitare il percorso reale quando il ticket riguarda runtime, gRPC, renderer, artifact o recovery.
7. Nessun ticket è chiuso soltanto perché il codice compila.
8. Ogni chiusura deve produrre evidenze verificabili: test, log, metriche, output, query DB o report JSON.
9. Un worker non entra in produzione se un requisito P0 è aperto.
10. La promozione deve essere per singolo worker o per classe hardware omogenea, non per l'intero parco in una sola volta.

---

## 2. Stati dei ticket

- `OPEN`: non iniziato.
- `IN_PROGRESS`: implementazione in corso.
- `BLOCKED`: dipendenza non disponibile.
- `READY_FOR_REVIEW`: codice e test completati.
- `DONE`: merge effettuato, test verdi ed evidenze archiviate.

---

## 3. Definition of Done globale

Un worker remoto è `PRODUCTION_READY` solo quando:

- mTLS è valido e fail-closed;
- la configurazione production passa una validazione completa;
- il motore C++ e l'executor reale partono correttamente;
- cache e blob store sono scrivibili e persistenti;
- il master espone `status=CONNECTED` e `session_active=true`;
- heartbeat, versione protocollo, versione engine e bundle sono coerenti;
- un canary reale mTLS termina con `Job=SUCCEEDED`;
- `TaskAttempt=SUCCEEDED`;
- artifact `READY`, hash verificato e finalizzazione ordinata;
- restart master, crash worker e network partition sono recuperati correttamente;
- drain e SIGTERM non accettano nuovi job e non corrompono output;
- metriche, alert e log operativi sono attivi;
- certificati sono monitorati e ruotabili;
- il worker supera il soak test della propria classe hardware;
- il comando `doctor` restituisce `READY`.

---

# Ticket RW-PROD-001 — Identità worker e mTLS production fail-closed

**Priorità:** P0  
**Stato:** OPEN  
**Dipendenze:** nessuna

## Problema

Un worker non deve potersi registrare con identità ambigua, certificato condiviso, CA errata o fallback plaintext.

## Obiettivo

Ogni worker possiede un'identità stabile e un certificato client dedicato. Il master accetta esclusivamente connessioni mTLS autorizzate nei canali `staging` e `production`.

## Attività

- [ ] Definire un `worker_id` unico, stabile e non riutilizzabile.
- [ ] Generare un certificato client dedicato per ogni worker.
- [ ] Verificare che CN o SAN rispettino il contratto di identità scelto.
- [ ] Impostare `environment=production` sui worker production.
- [ ] Impostare il tripletto completo:
  - [ ] `tls_cert_file`;
  - [ ] `tls_key_file`;
  - [ ] `tls_ca_file`.
- [ ] Rimuovere `allow_insecure_grpc_dev` dalla configurazione production.
- [ ] Impostare permessi chiave privata almeno equivalenti a `0600`.
- [ ] Vietare certificati condivisi tra più worker.
- [ ] Vietare certificati self-signed non appartenenti alla PKI Velox.
- [ ] Vietare fallback automatici verso plaintext.
- [ ] Aggiungere controllo di scadenza minimo di 14 giorni prima dell'ammissione.
- [ ] Registrare fingerprint e seriale del certificato nel report di inventory.

## Criteri di accettazione

- [ ] Certificato valido: registrazione riuscita.
- [ ] Certificato scaduto: registrazione rifiutata.
- [ ] CA errata: registrazione rifiutata.
- [ ] Certificato di un altro worker: registrazione rifiutata.
- [ ] Plaintext contro master TLS: registrazione rifiutata.
- [ ] TLS parziale: worker non avviato.
- [ ] Nessun segreto o chiave privata compare nei log.

## Test obbligatori

- [ ] Test unitari configurazione TLS.
- [ ] Test integrazione handshake valido.
- [ ] Test integrazione wrong CA.
- [ ] Test integrazione bad cert.
- [ ] Test integrazione identity mismatch.
- [ ] Test integrazione plaintext-to-TLS.

## Evidenze richieste

- output `openssl verify`;
- fingerprint certificato;
- log handshake positivo;
- log rifiuto dei casi negativi;
- report JSON per worker.

---

# Ticket RW-PROD-002 — Validazione completa configurazione production

**Priorità:** P0  
**Stato:** OPEN  
**Dipendenze:** RW-PROD-001

## Problema

`--validate-config` controlla la struttura e parte del trasporto, ma non dimostra che renderer, directory, cache, blob store e porte siano realmente utilizzabili.

## Obiettivo

Creare una validazione production completa e fail-fast prima di avviare il loop del worker.

## Attività

- [ ] Validare `worker_id`, `control_grpc_url`, ambiente e protocol version.
- [ ] Validare esistenza e compatibilità cert/key/CA.
- [ ] Validare raggiungibilità DNS del master.
- [ ] Validare scrittura e lettura su:
  - [ ] work directory;
  - [ ] cache directory;
  - [ ] blob directory;
  - [ ] temp directory;
  - [ ] output directory.
- [ ] Validare spazio disco minimo configurabile.
- [ ] Validare che health port e Prometheus port siano libere.
- [ ] Validare disponibilità ed eseguibilità del motore C++.
- [ ] Validare disponibilità FFmpeg/ffprobe.
- [ ] Validare che l'executor registry non sia vuoto.
- [ ] Validare che `scene.composite.v1@1` sia registrato.
- [ ] Restituire errori distinti e machine-readable.
- [ ] Aggiungere modalità `--validate-production` o incorporare il controllo nel futuro comando `doctor`.

## Criteri di accettazione

- [ ] Configurazione completa valida: exit `0`.
- [ ] Directory non scrivibile: exit non-zero.
- [ ] Motore C++ assente: exit non-zero.
- [ ] FFmpeg assente: exit non-zero.
- [ ] Registry executor vuoto: exit non-zero.
- [ ] Porta occupata: exit non-zero.
- [ ] Spazio disco insufficiente: exit non-zero.
- [ ] Output JSON contiene codice errore, componente e rimedio operativo.

## Test obbligatori

- [ ] Test unitari per ogni validatore.
- [ ] Test integrazione con filesystem temporaneo.
- [ ] Test integrazione motore assente.
- [ ] Test integrazione porta occupata.
- [ ] Test integrazione configurazione production valida.

---

# Ticket RW-PROD-003 — Bootstrap runtime ed executor reale

**Priorità:** P0  
**Stato:** OPEN  
**Dipendenze:** RW-PROD-002

## Problema

Un processo vivo non è utile se non possiede un executor realmente eseguibile o se il motore nativo è irraggiungibile.

## Obiettivo

Garantire che il worker annunci soltanto executor realmente inizializzati e operativi.

## Attività

- [ ] Mantenere il registry executor nel composition root canonico.
- [ ] Vietare fallback verso registry vuoto.
- [ ] Eseguire un self-test del motore C++ all'avvio.
- [ ] Eseguire un self-test minimo FFmpeg/ffprobe.
- [ ] Verificare che `scene.composite.v1@1` abbia runner non nullo.
- [ ] Verificare che la directory output dell'executor sia scrivibile.
- [ ] Pubblicizzare capability soltanto dopo bootstrap riuscito.
- [ ] Inserire engine version e bundle version nella Hello.
- [ ] Fallire l'avvio se versione binary, engine e bundle sono incompatibili.

## Criteri di accettazione

- [ ] Worker senza motore C++ non entra nel registry master.
- [ ] Worker con executor non inizializzato non annuncia capability false.
- [ ] Worker valido annuncia `scene.composite.v1@1`.
- [ ] Self-test produce un output minimo verificabile.
- [ ] Nessun percorso Python emergency o fallback viene usato.

## Test obbligatori

- [ ] Test registry descriptor.
- [ ] Test bootstrap engine mancante.
- [ ] Test bootstrap output directory non scrivibile.
- [ ] Test self-render minimo CPU-only.

---

# Ticket RW-PROD-004 — Liveness e readiness worker separate

**Priorità:** P0  
**Stato:** OPEN  
**Dipendenze:** RW-PROD-003

## Problema

L'endpoint `/health` può rispondere `status=ok` anche quando il worker non è registrato o non è pronto a ricevere task.

## Obiettivo

Separare chiaramente processo vivo e worker pronto.

## Attività

- [ ] Aggiungere `/health/live`.
- [ ] Aggiungere `/health/ready`.
- [ ] Mantenere `/health` come adapter temporaneo oppure documentarne la rimozione.
- [ ] `live=true` solo se il processo e il loop principale sono attivi.
- [ ] `ready=true` soltanto se:
  - [ ] sessione gRPC attiva;
  - [ ] registrazione accettata;
  - [ ] executor registry valido;
  - [ ] cache e blob store disponibili;
  - [ ] worker non in drain;
  - [ ] disco sopra soglia critica.
- [ ] Impostare readiness false immediatamente alla disconnessione.
- [ ] Impostare readiness false prima dello shutdown.
- [ ] Restituire motivi machine-readable quando non ready.

## Criteri di accettazione

- [ ] Processo vivo ma master irraggiungibile: live `200`, ready non `200`.
- [ ] Worker connesso e sano: live `200`, ready `200`.
- [ ] Worker draining: live `200`, ready non `200`.
- [ ] Worker senza executor: ready non `200`.
- [ ] Worker con disco critico: ready non `200`.

## Test obbligatori

- [ ] Test transizioni connecting/ready/disconnected/draining.
- [ ] Test HTTP live/ready.
- [ ] Test readiness dopo perdita stream.

---

# Ticket RW-PROD-005 — Stato canonico worker dal master

**Priorità:** P0  
**Stato:** OPEN  
**Dipendenze:** RW-PROD-004

## Problema

L'operatore deve poter distinguere un worker realmente connesso da uno con heartbeat recente ma sessione terminata.

## Obiettivo

Fare di `GET /api/v1/workers` e `GET /api/v1/workers/:id` la fonte operativa canonica.

## Attività

- [ ] Confermare la semantica degli stati:
  - [ ] `CONNECTED`;
  - [ ] `STALE`;
  - [ ] `DISCONNECTED`;
  - [ ] `DRAINING`.
- [ ] Derivare stato da sessione valida più heartbeat.
- [ ] Esporre `session_active`.
- [ ] Esporre heartbeat age.
- [ ] Esporre protocol, engine e bundle version.
- [ ] Esporre executor e task slots.
- [ ] Esporre active tasks e current task.
- [ ] Esporre risorse senza segreti o topology leak.
- [ ] Aggiungere reason code per stato non CONNECTED.
- [ ] Aggiungere endpoint o query per worker class e rollout group.

## Criteri di accettazione

- [ ] Stream chiuso: `session_active=false` senza attendere solo heartbeat timeout.
- [ ] Heartbeat vecchio con sessione attiva: `STALE`.
- [ ] Drain attivo: `DRAINING`.
- [ ] Nessun secret, token, cert path o credential hash nella risposta.

## Test obbligatori

- [ ] Test sessione attiva più heartbeat fresco.
- [ ] Test sessione persa con heartbeat recente.
- [ ] Test stale.
- [ ] Test draining.
- [ ] Test serializzazione e sanitizzazione.

---

# Ticket RW-PROD-006 — Sizing risorse e admission control

**Priorità:** P0  
**Stato:** OPEN  
**Dipendenze:** RW-PROD-005

## Problema

`max_active_jobs` impostato senza misurazioni può causare OOM, swap, disk full o throughput peggiore.

## Obiettivo

Calcolare capacità sicura per ogni classe hardware e impedire overcommit.

## Attività

- [ ] Definire classi hardware ufficiali.
- [ ] Misurare per job piccolo, medio e pesante:
  - [ ] picco RAM;
  - [ ] CPU time;
  - [ ] wall time;
  - [ ] temp bytes;
  - [ ] input/output bytes;
  - [ ] iowait;
  - [ ] network upload;
  - [ ] temperatura e throttling.
- [ ] Definire formula `max_active_jobs` per classe.
- [ ] Riservare almeno 25-30% RAM al sistema.
- [ ] Definire soglia minima disk free.
- [ ] Bloccare nuove task quando il worker supera soglie critiche.
- [ ] Esporre reason code `capacity_full`, `disk_pressure`, `memory_pressure`.
- [ ] Inserire limiti in config centralizzata, non hardcoded.

## Criteri di accettazione

- [ ] Nessun OOM durante test massimo supportato.
- [ ] Nessuna crescita swap continua.
- [ ] `active_tasks` non supera mai `task_slots`.
- [ ] Worker rifiuta nuove task sotto pressione critica.
- [ ] Le soglie sono documentate per classe hardware.

## Test obbligatori

- [ ] Stress test CPU-only.
- [ ] Test memory pressure.
- [ ] Test disk pressure.
- [ ] Test concurrency limiter.

---

# Ticket RW-PROD-007 — Canary mTLS per ogni worker remoto

**Priorità:** P0  
**Stato:** OPEN  
**Dipendenze:** RW-PROD-001, RW-PROD-003, RW-PROD-005

## Problema

Un test cluster-wide può passare anche se uno specifico computer non ha mai eseguito correttamente un job.

## Obiettivo

Eseguire un canary reale e attribuito su ogni worker prima dell'ammissione.

## Attività

- [ ] Definire una fixture CPU-only deterministica e breve.
- [ ] Assicurare che il canary venga assegnato al worker scelto.
- [ ] Registrare worker_id del TaskAttempt.
- [ ] Attraversare il percorso reale:
  - [ ] Hello;
  - [ ] HelloAck;
  - [ ] TaskOffer;
  - [ ] TaskAccepted;
  - [ ] TaskLeaseGranted;
  - [ ] executor;
  - [ ] TaskResult;
  - [ ] artifact upload;
  - [ ] artifact verification;
  - [ ] Job SUCCEEDED.
- [ ] Produrre un report JSON per worker.
- [ ] Rendere il canary rieseguibile on-demand.

## Criteri di accettazione

- [ ] Il TaskAttempt appartiene al worker selezionato.
- [ ] Job `SUCCEEDED`.
- [ ] TaskAttempt `SUCCEEDED`.
- [ ] Artifact `READY`.
- [ ] Output ffprobe valido.
- [ ] Nessun fallback.
- [ ] Metriche worker e master non zero.

## Test obbligatori

- [ ] Canary plaintext solo in ambiente dev.
- [ ] Canary mTLS staging/production-like.
- [ ] Canary su ogni classe hardware.

---

# Ticket RW-PROD-008 — Integrità artifact e finalizzazione

**Priorità:** P0  
**Stato:** OPEN  
**Dipendenze:** RW-PROD-007

## Problema

Un Job non deve diventare `SUCCEEDED` prima che l'artifact sia realmente presente, verificato e finalizzato.

## Obiettivo

Garantire un'unica catena atomica e verificabile tra TaskResult, upload, artifact READY e Job SUCCEEDED.

## Attività

- [ ] Calcolare SHA-256 sul worker.
- [ ] Verificare SHA-256 sul master o blob store.
- [ ] Registrare size, URI, hash, codec e timestamp.
- [ ] Impedire `Job=SUCCEEDED` se artifact non è `READY`.
- [ ] Impedire più artifact finali READY per lo stesso output logico.
- [ ] Rendere finalizzazione idempotente.
- [ ] Gestire upload interrotto.
- [ ] Gestire report tardivo da attempt scaduto.
- [ ] Aggiungere verifica ffprobe rigida per fixture E2E.

## Criteri di accettazione

- [ ] Hash DB uguale al file reale.
- [ ] `jobs.completed_at >= artifacts.verified_at`.
- [ ] Upload corrotto non finalizza il Job.
- [ ] Report duplicato non produce doppio artifact READY.
- [ ] Attempt stale non sovrascrive il vincitore.

## Test obbligatori

- [ ] Upload valido.
- [ ] Hash errato.
- [ ] Upload interrotto.
- [ ] Report duplicato.
- [ ] Report tardivo.

---

# Ticket RW-PROD-009 — Riconnessione dopo restart master

**Priorità:** P0  
**Stato:** OPEN  
**Dipendenze:** RW-PROD-005

## Problema

Un restart del master non deve richiedere intervento manuale sui worker.

## Obiettivo

Garantire riconnessione automatica, nuova sessione valida e nessuna perdita di stato canonico.

## Attività

- [ ] Testare master restart con worker idle.
- [ ] Testare master restart con worker busy.
- [ ] Verificare nuovo transport per ogni sessione.
- [ ] Verificare backoff e jitter.
- [ ] Verificare invalidazione della vecchia sessione.
- [ ] Verificare che readiness diventi false durante il blackout.
- [ ] Verificare che readiness torni true dopo HelloAck.
- [ ] Verificare nessuna registrazione duplicata attiva.

## Criteri di accettazione

- [ ] Riconnessione senza riavvio worker.
- [ ] Un'unica sessione attiva dopo il recovery.
- [ ] Nessun job duplicato.
- [ ] Nessun task perso.
- [ ] Tempo di recovery entro SLO definito.

## Test obbligatori

- [ ] Integration test restart master idle.
- [ ] Integration test restart master durante job.
- [ ] Soak con restart ripetuti.

---

# Ticket RW-PROD-010 — Crash worker, lease expiry e retry

**Priorità:** P0  
**Stato:** OPEN  
**Dipendenze:** RW-PROD-008

## Problema

Un worker terminato durante un job può lasciare task RUNNING, lease bloccate o output parziali.

## Obiettivo

Recuperare automaticamente il lavoro senza doppio completamento.

## Attività

- [ ] Terminare il worker con `SIGKILL` durante render.
- [ ] Verificare mancato rinnovo lease.
- [ ] Verificare transizione attempt precedente a stale/failed secondo contratto.
- [ ] Creare un nuovo attempt canonico.
- [ ] Riassegnare il task a un worker sano.
- [ ] Rifiutare TaskResult del vecchio attempt.
- [ ] Pulire output temporanei orfani.
- [ ] Garantire un solo artifact finale READY.

## Criteri di accettazione

- [ ] Nessuna task resta RUNNING indefinitamente.
- [ ] Nuovo attempt creato una sola volta.
- [ ] Vecchio attempt non può finalizzare.
- [ ] Job termina correttamente sul worker sostitutivo.

## Test obbligatori

- [ ] Crash prima di TaskAccepted.
- [ ] Crash dopo lease grant.
- [ ] Crash durante render.
- [ ] Crash durante upload.

---

# Ticket RW-PROD-011 — Network partition e duplicate suppression

**Priorità:** P0  
**Stato:** OPEN  
**Dipendenze:** RW-PROD-009, RW-PROD-010

## Problema

Una rete instabile può creare sessioni zombie, renew mancati, TaskResult duplicati o split-brain operativo.

## Obiettivo

Gestire partizioni temporanee senza doppie esecuzioni finalizzate.

## Attività

- [ ] Simulare perdita traffico worker -> master.
- [ ] Simulare perdita traffico master -> worker.
- [ ] Simulare latenza elevata e packet loss.
- [ ] Verificare scadenza sessione e lease.
- [ ] Verificare riconnessione con nuova identità sessione.
- [ ] Verificare idempotenza TaskAccepted, renewal e TaskResult.
- [ ] Verificare che due attempt non possano entrambi diventare vincitori.
- [ ] Registrare reason code delle disconnessioni.

## Criteri di accettazione

- [ ] Nessun doppio artifact READY.
- [ ] Nessun doppio Job SUCCEEDED.
- [ ] Nessuna sessione zombie oltre finestra definita.
- [ ] Recovery automatico entro SLO.

## Test obbligatori

- [ ] 60 secondi offline.
- [ ] 120 secondi offline.
- [ ] packet loss controllato.
- [ ] riconnessione durante attempt sostitutivo.

---

# Ticket RW-PROD-012 — Drain, SIGTERM e cancellazione processi

**Priorità:** P0  
**Stato:** OPEN  
**Dipendenze:** RW-PROD-010

## Problema

Il renderer C++ sincrono può non interrompersi immediatamente quando il contesto Go viene cancellato.

## Obiettivo

Rendere manutenzione e deploy prevedibili, senza accettare nuovi job e senza corrompere artifact.

## Attività

- [ ] Definire stato `DRAINING` canonico.
- [ ] Impostare readiness false all'ingresso in drain.
- [ ] Rifiutare nuove TaskOffer durante drain.
- [ ] Attendere job attivi fino a timeout configurabile.
- [ ] Propagare SIGTERM al processo C++.
- [ ] Implementare escalation TERM -> KILL dopo grace period.
- [ ] Pulire temp file dopo kill.
- [ ] Non finalizzare artifact incompleti.
- [ ] Configurare `TimeoutStopSec` coerente con il job massimo.
- [ ] Produrre log strutturati per ogni fase shutdown.

## Criteri di accettazione

- [ ] Nessuna nuova task accettata in drain.
- [ ] Job breve termina pulitamente.
- [ ] Job lungo viene terminato secondo policy.
- [ ] Nessun processo C++ orfano.
- [ ] Nessun artifact parziale READY.
- [ ] Processo esce entro timeout.

## Test obbligatori

- [ ] SIGTERM idle.
- [ ] SIGTERM durante render.
- [ ] SIGTERM durante upload.
- [ ] timeout ed escalation.

---

# Ticket RW-PROD-013 — Metriche, log e alert operativi

**Priorità:** P0  
**Stato:** OPEN  
**Dipendenze:** RW-PROD-005, RW-PROD-006

## Problema

Senza telemetria non è possibile distinguere un worker lento, disconnesso, sotto pressione o in fallback.

## Obiettivo

Definire metriche affidabili, unità corrette e alert azionabili.

## Attività

- [ ] Abilitare health e Prometheus su porte non pubbliche.
- [ ] Proteggere le porte con firewall/VPN/security group.
- [ ] Esportare almeno:
  - [ ] session active;
  - [ ] heartbeat age;
  - [ ] active tasks;
  - [ ] task slots;
  - [ ] CPU utilization;
  - [ ] RSS e memory used;
  - [ ] disk free;
  - [ ] temp bytes;
  - [ ] network RX/TX;
  - [ ] jobs succeeded/failed/timeout;
  - [ ] reconnect count;
  - [ ] lease renewal failures;
  - [ ] artifact upload failures;
  - [ ] fallback count;
  - [ ] Python emergency path count.
- [ ] Correggere tutte le unità, incluso millisecondi vs secondi.
- [ ] Aggiungere alert per:
  - [ ] worker disconnected;
  - [ ] heartbeat stale;
  - [ ] disk pressure;
  - [ ] memory pressure;
  - [ ] fallback > 0;
  - [ ] emergency path > 0;
  - [ ] failure rate oltre soglia;
  - [ ] certificato in scadenza.
- [ ] Correlare log con worker_id, job_id, task_id, attempt_id e session_id.

## Criteri di accettazione

- [ ] Dashboard mostra tutti i worker.
- [ ] Metriche con unità verificate da test.
- [ ] Alert scatta nei test di failure injection.
- [ ] Fallback e Python emergency restano zero in produzione.
- [ ] Nessuna metrica espone segreti.

## Test obbligatori

- [ ] Unit test conversione unità.
- [ ] Test endpoint metrics.
- [ ] Test alert rules.
- [ ] Failure injection con verifica alert.

---

# Ticket RW-PROD-014 — Monitoraggio e rotazione PKI

**Priorità:** P0  
**Stato:** OPEN  
**Dipendenze:** RW-PROD-001

## Problema

Certificati scaduti, illeggibili o non inventariati possono scollegare interi gruppi di worker.

## Obiettivo

Avere monitor fail-closed, rotazione senza downtime e revoca operativa.

## Attività

- [ ] Rendere il monitor certificati fail-closed.
- [ ] Directory assente deve restituire errore.
- [ ] Zero certificati validi deve restituire errore.
- [ ] Certificato illeggibile deve restituire errore.
- [ ] Esporre conteggi ok/warning/critical/expired/invalid.
- [ ] Definire soglie 30/14/7/2 giorni.
- [ ] Definire rotazione certificato worker senza downtime.
- [ ] Supportare overlap vecchio/nuovo certificato durante rollout.
- [ ] Testare revoca immediata.
- [ ] Archiviare seriale, fingerprint, issued_at ed expires_at.
- [ ] Alert automatico e owner operativo.

## Criteri di accettazione

- [ ] Certificato ruotato senza perdita job.
- [ ] Certificato revocato rifiutato.
- [ ] Directory vuota non produce OK.
- [ ] Certificato corrotto non viene ignorato.
- [ ] Alert ricevuto prima della soglia di 14 giorni.

## Test obbligatori

- [ ] Certificato valido.
- [ ] Certificato in warning.
- [ ] Certificato critical.
- [ ] Certificato scaduto.
- [ ] Certificato corrotto.
- [ ] Directory assente.
- [ ] Rotazione live.

---

# Ticket RW-PROD-015 — Soak test e matrice di certificazione hardware

**Priorità:** P0  
**Stato:** OPEN  
**Dipendenze:** RW-PROD-006, RW-PROD-007, RW-PROD-009, RW-PROD-010, RW-PROD-011, RW-PROD-012

## Problema

Un singolo canary non dimostra stabilità nel tempo, sotto carico e su hardware differenti.

## Obiettivo

Certificare ogni classe hardware e ogni worker prima della promotion.

## Attività

- [ ] Creare inventory delle classi hardware.
- [ ] Eseguire almeno 24 ore di soak test per classe.
- [ ] Eseguire almeno 20-50 job rappresentativi per worker o classe.
- [ ] Includere job piccoli, medi e pesanti.
- [ ] Includere cache cold e warm.
- [ ] Includere restart master.
- [ ] Includere restart worker.
- [ ] Includere network partition.
- [ ] Includere drain e SIGTERM.
- [ ] Registrare success rate, p50, p95, p99 e failure reasons.
- [ ] Produrre report firmato per worker.

## Gate numerici minimi

- [ ] job persi = 0;
- [ ] artifact duplicati READY = 0;
- [ ] artifact corrotti = 0;
- [ ] OOM = 0;
- [ ] disk full inattesi = 0;
- [ ] task senza stato terminale = 0;
- [ ] reconnessioni manuali = 0;
- [ ] fallback production = 0;
- [ ] success rate canary = 100%;
- [ ] success rate carico normale >= 99%;
- [ ] active tasks mai oltre slots;
- [ ] nessun throttling termico persistente.

## Criteri di accettazione

- [ ] Ogni classe hardware ha un profilo approvato.
- [ ] Ogni worker ha un report di certificazione.
- [ ] Worker non certificati restano fuori dall'allowlist production.

---

# Ticket RW-PROD-016 — Comando `velox-worker-agent doctor`

**Priorità:** P0  
**Stato:** OPEN  
**Dipendenze:** RW-PROD-001 fino a RW-PROD-015

## Problema

Le verifiche manuali sono frammentate e possono essere eseguite in modo diverso su ogni computer.

## Obiettivo

Fornire un unico comando deterministico che restituisca `READY` o `NOT_READY`.

## Interfaccia target

```bash
velox-worker-agent doctor \
  --production \
  --config /opt/velox/worker_config.json \
  --json
```

## Controlli obbligatori

- [ ] Config e ambiente.
- [ ] Identità worker.
- [ ] Certificato, chiave, CA e scadenza.
- [ ] Permission chiave privata.
- [ ] DNS e reachability master.
- [ ] Handshake mTLS.
- [ ] Registrazione Hello/HelloAck opzionale con modalità canary.
- [ ] Motore C++.
- [ ] FFmpeg e ffprobe.
- [ ] Executor registry.
- [ ] Cache read/write/delete.
- [ ] Blob store read/write/delete.
- [ ] Temp directory.
- [ ] Spazio disco.
- [ ] Porte health/metrics.
- [ ] Risorse CPU/RAM.
- [ ] Versioni protocol/engine/bundle.
- [ ] Canary reale opzionale.
- [ ] Verifica visibility nel master.

## Output JSON target

```json
{
  "worker_id": "worker-01",
  "verdict": "READY",
  "checked_at": "2026-06-24T12:00:00Z",
  "checks": [
    {
      "id": "mtls",
      "status": "PASS",
      "detail": "client certificate accepted"
    }
  ]
}
```

## Criteri di accettazione

- [ ] Exit `0` soltanto con tutti i check obbligatori PASS.
- [ ] Exit non-zero con almeno un check FAIL.
- [ ] Nessun segreto nell'output.
- [ ] Output stabile e versionato.
- [ ] Modalità human-readable e JSON.
- [ ] Testabile senza duplicare la logica del config package, registry o sampler.

## Test obbligatori

- [ ] Doctor completamente verde.
- [ ] Certificato errato.
- [ ] Engine assente.
- [ ] Directory non scrivibile.
- [ ] Master irraggiungibile.
- [ ] Worker non visibile nel master.
- [ ] Canary fallito.

---

# Ticket RW-PROD-017 — Rollout, promotion e rollback worker

**Priorità:** P0  
**Stato:** OPEN  
**Dipendenze:** RW-PROD-015, RW-PROD-016

## Problema

Anche worker corretti possono causare incidente se distribuiti tutti insieme senza canary, drain e rollback.

## Obiettivo

Promuovere worker gradualmente usando gli stessi artifact già verificati.

## Attività

- [ ] Buildare una sola volta l'immagine worker.
- [ ] Identificare immagine con digest e commit SHA.
- [ ] Vietare rebuild tra staging e production.
- [ ] Promuovere prima un worker canary.
- [ ] Eseguire `doctor --production`.
- [ ] Eseguire canary reale mTLS.
- [ ] Osservare metriche per una finestra definita.
- [ ] Promuovere per classe hardware o percentuale.
- [ ] Usare drain prima dell'aggiornamento.
- [ ] Definire rollback allo stesso digest precedente.
- [ ] Conservare compatibilità protocollo durante rollout misto.

## Criteri di accettazione

- [ ] Canary worker verde.
- [ ] Nessun aumento failure rate.
- [ ] Nessun aumento fallback.
- [ ] Rollback completabile senza rebuild.
- [ ] Nessun job perso durante aggiornamento.
- [ ] Report rollout archiviato.

## Test obbligatori

- [ ] Rollout singolo worker.
- [ ] Rollout misto vecchia/nuova versione.
- [ ] Rollback durante idle.
- [ ] Rollback dopo drain.

---

## 4. Ordine di implementazione obbligatorio

1. RW-PROD-001 — Identità e mTLS.
2. RW-PROD-002 — Validazione completa config.
3. RW-PROD-003 — Bootstrap runtime/executor.
4. RW-PROD-004 — Liveness/readiness.
5. RW-PROD-005 — Stato canonico master.
6. RW-PROD-006 — Resource sizing.
7. RW-PROD-007 — Canary per worker.
8. RW-PROD-008 — Artifact integrity.
9. RW-PROD-009 — Restart master.
10. RW-PROD-010 — Crash worker e retry.
11. RW-PROD-011 — Network partition.
12. RW-PROD-012 — Drain e shutdown.
13. RW-PROD-013 — Metriche e alert.
14. RW-PROD-014 — PKI rotation.
15. RW-PROD-015 — Soak e hardware certification.
16. RW-PROD-016 — Worker doctor.
17. RW-PROD-017 — Rollout e rollback.

Non iniziare il ticket successivo finché il precedente non ha test verdi e criteri di accettazione verificati, salvo attività indipendenti esplicitamente approvate.

---

## 5. Scheda finale di certificazione per worker

```text
Worker ID:
Hostname:
Classe hardware:
Versione worker:
Versione engine:
Bundle version:
Protocol version:
Image digest:
Cert fingerprint:
Cert expiry:
Doctor verdict:
Canary job ID:
Canary task ID:
Canary attempt ID:
Artifact ID:
Artifact SHA-256:
Soak start:
Soak end:
Job eseguiti:
Success rate:
Failure count:
Reconnect test:
Worker crash test:
Master restart test:
Network partition test:
Drain test:
Fallback count:
Python emergency count:
Verdetto finale: PRODUCTION_READY | NOT_READY
Approvato da:
Data approvazione:
```

---

## 6. Regola finale di ammissione

Il master deve mantenere fuori dall'allowlist production ogni worker che non possiede una scheda completa con:

```text
Doctor = READY
Canary mTLS = PASS
Artifact integrity = PASS
Recovery suite = PASS
Soak test = PASS
Fallback count = 0
Verdetto = PRODUCTION_READY
```

Nessuna eccezione silenziosa. Ogni deroga deve avere owner, motivazione, scadenza e ticket di rientro.