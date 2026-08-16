# Video pipeline — goal, stato e changelog operativo

## Obiettivo principale

Portare l'assemblaggio video con asset già presenti nella cache a un tempo
target di circa **1 secondo**, usando packet-copy del video quando e solo
quando tutti i segmenti rispettano lo stesso contratto media.

Il target di 1 secondo riguarda:

- risoluzione degli asset già caldi;
- verifica di compatibilità;
- concatenazione/mux video senza decodifica e senza ricodifica;
- scrittura dell'artifact locale.

Non include il tempo di:

- generazione o mixaggio del voiceover;
- ricodifica dell'audio finale quando serve;
- upload dell'artifact su Google Drive;
- attese di scheduling, Master o worker.

Con voiceover e audio originale sovrapposti, l'audio deve continuare a essere
ricostruito/mixato. Il video può restare packet-copy, ma l'audio non può essere
copiato alla cieca se deve essere modificato.

## Contratto di esecuzione desiderato

```text
asset già in cache
        ↓
probe media e verifica compatibilità
        │
        ├── incompatibile → job FAILED prima del render
        │
        └── compatibile
                ↓
        video packet-copy (-c:v copy)
                ↓
        audio mix/encode se richiesto dalla timeline
                ↓
        mux finale
                ↓
        verifica audio/video + ffprobe
                ↓
        upload Drive
```

La regola è fail-closed: non è ammesso un fallback silenzioso a libx264 o a
un render completo quando il contratto packet-copy non è rispettato.

## Criteri di compatibilità video

Tutti i segmenti devono avere parametri compatibili, non soltanto lo stesso
nome di codec:

- codec video;
- profilo e livello;
- width e height;
- pixel format;
- frame rate e timebase;
- GOP chiuso e primo frame keyframe;
- parametri di stream necessari al mux MP4;
- segmenti senza trasformazioni video, filtri, resize, crop, subtitle burn-in
  o altre operazioni che richiedano decodifica.

Se un parametro differisce, il job deve terminare con un errore esplicito,
per esempio `COPY_ONLY_MEDIA_INCOMPATIBLE`, indicando segmento e parametro
non compatibile.

## Stato misurato al 2026-08-16

Il benchmark ripetibile a dieci clip è ora separato in cold e warm run:

| Run | Worker | Totale | Cache | Download | Video | Audio/validazione | Output |
|---|---|---:|---|---:|---|---|---|
| `scriptclip_34c399da-5f91-49a4-8ba1-0d70215349cb` | `host_57_131_20_173` | 20.512 s | 20 hit / 10 miss | 105,336,881 B | `packet_copy`, 0 frame encoded | audio/video presenti, `ffprobe_valid=1` | SHA `560f5003…`, 79,625,950 B |
| `scriptclip_20b468b6-56c4-4e68-a216-fb0845daa517` | `host_57_131_20_173` | 2.834 s | 30 hit / 0 miss | 0 B | `packet_copy`, 0 frame encoded | audio/video presenti, `ffprobe_valid=1` | stesso SHA e size |
| `scriptclip_29dd218a-7e37-43d9-81f3-74f539b7a7a0` | `host_57_129_132_133` | 21.223 s | 20 hit / 10 miss | 105,336,881 B | `packet_copy`, 0 frame encoded | audio/video presenti, `ffprobe_valid=1` | stesso SHA e size |

Il primo run ha completato anche la delivery Drive senza retry, in circa
6.5 s. Il run warm dimostra che il video non viene ricodificato e che gli
asset già caldi non vengono riscaricati. Il canary è risultato
`CONNECTED/HEALTHY/AVAILABLE` sulla nuova immagine `v1.2.34-canonical`.

Il tempo warm è ancora sopra l'obiettivo locale di 1 s: il breakdown osservato
è circa 1.1 s di compile, 0.5 s di render packet-copy e 0.35--0.5 s di
finalizzazione, mentre il totale include scheduling e persistenza Master.
L'upload Drive resta fuori dal target locale.

## Problemi risolti

### 2026-08-10 — fail-closed e contratti di piattaforma

- Corretto il modello delle capability: `DISABLED`, `READY` e
  `MISCONFIGURED`, senza noop o stub produttivi nascosti.
- Stabilizzato il percorso di migrazione e verifica del runtime worker.
- Consolidato il contratto per worker ID immutabile e worker name modificabile.
- Chiusi i residui del gate full-module sulle migrazioni e sulla finalizzazione.

### 2026-08-15 — telemetria e fondamenta

- Consolidata la telemetria su un catalogo unico.
- Ridotte le duplicazioni tra metriche, owner e fasi.
- Preparato il modello per distinguere tempi esclusivi da span annidati.
- Migliorata la leggibilità dei tempi di pipeline senza sommare erroneamente
  fasi sovrapposte.

### 2026-08-16 — runtime Master/Fleet

- Aggiunto il preflight del runtime worker prima del drain.
- Reso il preflight sicuro anche con `worker.env` protetto usando `sudo -n`.
- Corretto il readiness Master quando gRPC push è attivo ma non esiste un
  listener.
- Esposto nel readiness lo stato gRPC e il commit effettivamente in esecuzione.
- Isolate e ruotate correttamente le sessioni Level-D smoke senza revocare la
  sessione gRPC reale del worker.
- Verificata la stabilità dopo restart del Master.

### 2026-08-16 — cache e staging artifact

- Introdotto il blob store content-addressed con SHA completo nel path.
- Separati gli alias logici `asset_key → content_hash` dai blob fisici.
- Abilitata la deduplicazione fisica tra asset con gli stessi byte.
- Introdotta eviction LRU sotto pressione con isteresi high/low watermark.
- Protetti i blob con lease, reservation e protected snapshot.
- Introdotto lo staging artifact su tmpfs con reservation RAM e fallback NVMe.
- Resi immutabili i blob promossi (`0444`).
- Aggiunto il self-healing degli asset cache corrotti preservando l'alias.

### 2026-08-16 — artifact upload e Drive

- Resi idempotenti i retry dei chunk.
- Serializzata la finalizzazione concorrente degli upload.
- Serializzati gli upload foreground e resume sul worker.
- Migliorata la classificazione degli errori di upload.
- Corretto il passaggio dei path dati del Master nel container.
- Selezionata la directory token Drive popolata e verificata.
- Verificato upload Drive completato nella cartella configurata, senza retry.

### 2026-08-16 — telemetria operativa

- Corretto il parsing dei timestamp wall mancanti: valori vuoti non sono più
  trattati come timestamp corrotti.
- I timestamp non vuoti ma malformati continuano correttamente a fallire.
- Verificato che il supervisor non si blocchi su fasi sintetiche prive di
  timestamp.

## Lavori ancora da completare

### 2026-08-16 — packet-copy obbligatorio

- Implementato il probe preventivo e il confronto dei parametri video.
- Un asset incompatibile viene rifiutato prima del render copy-only.
- Il percorso compatibile usa packet-copy e produce `frames_encoded=0`.
- Eliminato il fallback implicito a video re-encode nel percorso copy-only.
- Corretto il mapping telemetry: `packet_copy` valorizza anche
  `final_concat_stream_copy=true`.
- Restano da separare ulteriormente i tempi video, audio, mux e delivery.

### 2026-08-16 — audio corretto

- Mantenere il mix voiceover + audio originale quando previsto dalla timeline.
- Ricodificare solo l'audio quando il mix o la normalizzazione lo richiedono.
- Verificare che ogni scena abbia audio nel segmento corretto e che non venga
  perso l'audio originale del clip.
- Aggiungere test con voiceover, audio clip, ducking e durata non identica.

### 2026-08-16 — intake job da unificare

- Superficie canonica da adottare: `POST /api/v1/jobs` su Velox e
  `POST /api/v1/jobs` sul BFF InstaEdit.
- Oggi esistono ancora adapter di dominio separati: creator, script,
  `/api/v1/jobs/batch`, pipeline-run, calendar enqueue e
  `/api/v1/instaedit/jobs` interno.
- Il passo sicuro è far convergere tutti gli adapter verso un solo submitter
  interno e poi deprecare/rimuovere gli endpoint di creazione duplicati con
  telemetry e gate full-module. Non si devono mantenere dieci logiche di
  enqueue diverse.

### 2026-08-16 — benchmark 1 secondo

- Creare fixture con segmenti realmente compatibili e già caldi.
- Misurare separatamente preflight, packet-copy, audio, mux e Drive.
- Obiettivo: packet-copy locale ≤ 1s in condizioni nominali.
- Obiettivo secondario: nessun download e nessuna ricodifica video.
- Pubblicare p50, p95 e p99; non usare la durata totale del job come unica
  metrica.

### 2026-08-16 — test di rifiuto

- Asset con risoluzione differente → `FAILED` prima del render.
- Asset con FPS/timebase differente → `FAILED` prima del render.
- Asset senza keyframe iniziale o con GOP aperto → `FAILED`.
- Presenza di crop, scale, filtri o sottotitoli → `FAILED` nel percorso copy.
- Verificare che non venga prodotto un artifact parziale o caricato su Drive.

### 2026-08-16 — resilienza ancora da certificare

- Corruzione cache e re-download selettivo.
- Eviction LRU con lease/reservation protetti.
- Retry duplicato di chunk con byte diversi: conservare il chunk originale.
- Retry duplicato di `/complete` dopo risposta persa.
- Restart Master durante upload.
- Restart worker durante render/upload.
- Soak test di 10–20 job consecutivi su più worker.

### 2026-08-16 — build/deploy worker

- `v1.2.34-canonical`: buildx, race test, Cosign, bootstrap e audio canary
  verdi; digest `sha256:12ada4912a680f49a15d464216ba284257980b85435f255c769699448f7cd9a7`.
- La build CI completa ha impiegato circa 2 minuti grazie alla cache GHA di
  Buildx; il precedente comportamento da ore non è più il percorso osservato.
- `v1.2.35-canonical` contiene la correzione telemetry aggiuntiva ed è in
  pubblicazione al momento di questo aggiornamento.

## Definition of Done

Il goal è raggiunto quando tutti i punti seguenti sono veri:

- [ ] Con asset caldi e compatibili, packet-copy video locale ≤ 1s al p95.
- [ ] Il job rifiuta gli asset incompatibili prima di avviare il render.
- [ ] Il video non viene mai ricodificato nel percorso copy-only.
- [ ] L'audio voiceover + clip viene mixato correttamente quando richiesto.
- [ ] L'output contiene sempre uno stream video e uno audio validi.
- [ ] `ffprobe_valid=1` e durata entro la tolleranza contrattuale.
- [ ] Il secondo job identico usa la cache senza download.
- [ ] Drive riceve l'artifact corretto senza retry anomali.
- [ ] Nessun artifact parziale resta dopo commit o fallimento.
- [ ] Telemetria separa correttamente tempi video, audio, mux e delivery.
- [ ] Test di incompatibilità, retry, restart ed eviction sono verdi.

## Nota sulle date

Le date riportate sono date operative del changelog. I commit della tranche
più recente risultano registrati nel repository il 2026-08-16; non vengono
inventate date diverse per singolo commit.
