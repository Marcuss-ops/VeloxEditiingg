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

Invariante di produzione: un assembly SUCCEEDED deve avere
`packet_copy_segments == total_segments`, `rejected_segments == 0`,
`frames_encoded == 0` e `encode_passes == 0` (`concat_mode=mixed_packet`).
Un segmento non copy-safe viene rifiutato (`Reject`) e mai ricodificato.

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

Se un parametro differisce, il job deve terminare con un errore esplicito
`segment_execution_rejected`, indicando il motivo esatto del resolver:
`media signature mismatch: <campo>`, `segment requires a media transform`
oppure `source window is not keyframe-safe for packet copy`.

## Stato misurato al 2026-08-16

Il benchmark ripetibile a dieci clip è ora separato in cold e warm run:

| Run | Worker | Totale | Cache | Download | Video | Audio/validazione | Output |
|---|---|---:|---|---:|---|---|---|
| `scriptclip_34c399da-5f91-49a4-8ba1-0d70215349cb` | `host_57_131_20_173` | 20.512 s | 20 hit / 10 miss | 105,336,881 B | `packet_copy`, 0 frame encoded | audio/video presenti, `ffprobe_valid=1` | SHA `560f5003…`, 79,625,950 B |
| `scriptclip_20b468b6-56c4-4e68-a216-fb0845daa517` | `host_57_131_20_173` | 2.834 s | 30 hit / 0 miss | 0 B | `packet_copy`, 0 frame encoded | audio/video presenti, `ffprobe_valid=1` | stesso SHA e size |
| `scriptclip_29dd218a-7e37-43d9-81f3-74f539b7a7a0` | `host_57_129_132_133` | 21.223 s | 20 hit / 10 miss | 105,336,881 B | `packet_copy`, 0 frame encoded | audio/video presenti, `ffprobe_valid=1` | stesso SHA e size |
| `job_cae61e986f67cc37` — intake canonico caldo | `host_57_129_132_133` | 4.236 s | 30 hit / 0 miss | 0 B | `packet_copy`, 0 frame encoded | audio/video presenti, `ffprobe_valid=1`, `final_concat_stream_copy=true` | stesso SHA e size |

Il primo run ha completato anche la delivery Drive senza retry, in circa
6.5 s. Il run warm dimostra che il video non viene ricodificato e che gli
asset già caldi non vengono riscaricati. Il canary è risultato
`CONNECTED/HEALTHY/AVAILABLE` sulla nuova immagine `v1.2.36-canonical`.

Il tempo warm è ancora sopra l'obiettivo locale di 1 s: il breakdown osservato
è circa 1.1 s di compile, 0.5 s di render packet-copy e 0.35--0.5 s di
finalizzazione, mentre il totale include scheduling e persistenza Master.
L'upload Drive resta fuori dal target locale.

Il test canonico ha anche confermato che il pin verso
`host_57_129_132_133` viene rispettato dal percorso `POST /api/v1/jobs`.
Il percorso script precedente, invece, ha lasciato cadere il pin e ha esposto
un conflitto di spool su `host_57_131_20_173`; questo è un motivo concreto per
convergere gli adapter, non per aggiungere un altro endpoint.

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

### 2026-08-17 — CanonicalJobSubmitter e telemetry intake_source

- Introdotto `creatorflow.CanonicalJobSubmitter` come unico percorso di
  submission Job+Task. Il submitter stampa `IntakeSource` su ogni submission e
  registra la telemetria `pipeline.intake_source_accepted_total{intake_source}`.
- Gli adapter che già passavano dal submitter (canonico `/api/v1/jobs`,
  creator push, batch, instaedit BFF) ora stampano il proprio
  `intake_source`: `canonical`, `creator`, `batch`, `instaedit`.
- Le superfici che accodano direttamente (script `generate-with-images`,
  script ingress `generate`/`jobs/:kind`, pipeline-run) registrano il proprio
  `intake_source`: `script_generate`, `script_kind`, `pipeline_run`.
- La famiglia è registrata sul collector `/metrics` (insieme alla preesistente
  `pipeline_creator_intake_accepted_total`, che prima non era esposta).
- Obiettivo: misurare l'utilizzo degli alias prima di deprecare/rimuovere
  qualsiasi endpoint legacy (gate full-module `pre-removal-verify.sh`).
  Nessun alias è stato rimosso in questa tranche.

### 2026-08-17 — mixed/concat copy-only: SegmentExecutionMode Reject + invariant canary

- `SegmentExecutionMode` ridotto a `PacketCopy / Reject` (rimossi
  `NativeTranscode` e `LegacyFallback`); `resolveSegmentExecution()` è
  fail-closed: legacy, transform, trim non-keyframe-safe e signature
  mismatch producono `Reject` con il reason esatto
  (`media signature mismatch: width`, `segment requires a media transform`,
  `source window is not keyframe-safe for packet copy`, …).
- `RenderEngine::renderMixed` accetta solo `PacketCopy`; ogni `Reject`
  fallisce il job con `segment_execution_rejected` (mai fallback a libx264).
  Rimossa `transcodeMixedSegment()`; il mixed non conosce più
  libx264/preset medium.
- Telemetria mixed: `copy_segments`/`transcode_segments` →
  `packet_copy_segments`/`rejected_segments`; `concat_mode=mixed_packet`.
- Invariant di produzione: un assembly mixed SUCCEEDED deve avere
  `frames_encoded == 0` e `encode_passes == 0`. Enforced dal nuovo canary
  `scripts/ci/worker-mixed-canary.sh` (immutable digest, cablato in
  `.github/workflows/worker-image.yml`): render all-canonical → SUCCEEDED
  con `frames=0`/`encode_passes=0`; scena 720p fuori profilo →
  `segment_execution_rejected` (rc=1, `frames=0`) senza crash del worker.

### 2026-08-17 — soak test 20 job via POST /api/v1/jobs

- Eseguito il soak test con **20 job** tutti attraverso `POST /api/v1/jobs`
  (payload canonico a 10 clip, `job_type=clip.stock.v1`, nessun pin di
  placement — lo scheduler ha distribuito). Script: `ops/jobs/soak_20.sh`
  (mint M2M → 20 POST → poll fino a stato terminale → verdetto aggregato).
- **Risultato: 18/20 SUCCEEDED, 2/20 FAILED.** Cache perfettamente calda su
  tutti i job riusciti: **30 hit / 0 miss / 0 download**, `packet_copy`,
  0 frame encoded, SHA `560f5003…` identico al run canonico, delivery
  `drive-smoke` SUCCEEDED.
- I 2 FAILED sono il **conflitto spool già noto in FASE 7** (CAS conflict
  `expected status=UPLOADING` al `MarkUploaded`) su `host_57_131_20_173` e
  `velox-worker-13197`, entrambi sulla immagine `9e7dcf3c…` (pre-Ensure).
  Il canary `host_57_129_132_133` sulla immagine più recente
  `f4907398…` ha avuto 0 failure (5/5 SUCCEEDED).
- Conferma: il fix `spool.Store.Ensure` (già implementato localmente, non
  ancora deployato) è il rimedio previsto; il rollout agli altri 3 worker
  (FASE 9) è il prerequisito per un soak 20/20 pulito.

### 2026-08-17 — rollout altri 3 worker sul digest canary (FASE 9)

- Eseguito `fleetctl rollout` seriale con `--wait-ready` sugli altri 3 worker
  (`host_57_131_20_173`, `velox-worker-13197`, `velox-worker-523925eb`)
  verso il digest canary `sha256:f4907398…` (l'immagine con 0 failure nel
  soak; i 3 worker erano su `9e7dcf3c…` pre-Ensure).
- 3/3 update SUCCEEDED e READY sul nuovo digest; fleet ora uniforme.
- Verifica post-rollout: **4/4 worker su `f4907398…`**, 4/4 CONNECTED /
  HEALTHY / AVAILABLE / session_active=true, 0 job attivi.
- Nota: `f4907398…` è l'immagine canary che nel soak ha avuto 0 failure
  (5/5 SUCCEEDED), a fronte dei 2 failure spool sull'immagine precedente
  `9e7dcf3c…`. Il codice `spool.Store.Ensure` nel repo resta lavoro locale
  non ancora pubblicato; un prossimo soak 20/20 pulito confermerà la
  risoluzione del conflitto CAS su tutta la fleet.

### 2026-08-17 — re-soak 20 job su fleet uniforme (20/20 SUCCEEDED)

- Dopo il rollout FASE 9 (fleet uniforme su `f4907398…`), re-eseguito il
  soak con 20 job via `POST /api/v1/jobs` (stesso payload canonico a 10
  clip, nessun pin). Script: `ops/jobs/soak_20.sh`.
- **Risultato: 20/20 SUCCEEDED, 0 FAILED, 0 timeout.** Il conflitto spool
  CAS (`expected status=UPLOADING` al MarkUploaded) che aveva prodotto i 2
  failure sul digest `9e7dcf3c…` **non è ricomparso**.
- Cache calda: il primo job (cold-start dopo il restart del rollout) ha
  scaricato i 10 asset unici (20 hit / 10 miss / 10 download); i successivi
  19 job sono tutti **30 hit / 0 miss / 0 download** (hit_ratio=1).
- Su tutti i 20: SHA `560f5003…` identico (determinismo), `packet_copy`,
  0 frame encoded, `final_concat_stream_copy`, delivery `drive-smoke`
  SUCCEEDED.
- Lo scheduler ha distribuito tutti i 20 job sullo stesso worker
  (`velox-worker-523925eb`); fleet finale 4/4 CONNECTED / HEALTHY /
  AVAILABLE / session_active=true, 0 job attivi.

### 2026-08-17 — spool: unica superficie Ensure/GetOrCreate

- Aggiunto `spool.Store.Ensure(ctx, entry) (*SpoolEntry, bool, error)` come
  unica superficie di registrazione del publisher: row inesistente → INSERT
  (`created=true`); row esistente con contenuto compatibile → ritorna la row
  esistente (`created=false`); row esistente con contenuto incompatibile
  (sha256/size diversi) → `ErrIncompatibleSpool`.
- Il publisher (`registerOutputSpool`) ora usa `Ensure` e non gestisce più
  `ErrDuplicateSpool` nei caller: la logica di dedup vive una sola volta nel
  store. Nessuna nuova tabella, nessun secondo spool, nessuna map in-memory.
- Compatibilità definita sul fingerprint di contenuto (sha256 + size_bytes);
  `local_path` non è parte del confronto (può cambiare legittimamente per
  spill/re-render). Una row creata ma mai finalizzata (MarkReady non eseguito)
  è compatibile: il MarkReady del chiamante la completa.
- 6 test obbligatori verdi: `TestEnsure_NewOutputCreatesRow`,
  `TestEnsure_DuplicateRenderingReturnsExisting`,
  `TestEnsure_DuplicateOutputReadyReturnsExisting`,
  `TestEnsure_DuplicateUploadingReturnsExisting`,
  `TestEnsure_AfterRestartReturnsExisting`,
  `TestEnsure_IncompatibleIdentityFails`.

### 2026-08-17 — certificazione recovery artifact (FASE 8)

- Fix `CompleteChunked` idempotente: short-circuit `COMPLETED` (con fencing
  worker/lease/revision/attempt) prima del guard "no chunks", perché la prima
  complete ha già fatto Receive → Finalize → cleanup (row rimosse): un retry
  da risposta persa non deve più cadere nel guard e fallire come 400.
- Refactor `openTestEnvAt` in `service_test.go` per riaprire lo STESSO file
  SQLite (simulazione restart master) mantenendo `setupTestEnv` delegato.
- 3 test master-side verdi (`chunked_recovery_test.go`):
  - `TestChunkedRecovery_DuplicateChunkSucceedsWithoutDuplication` — chunk 0,
    1, 1 (retry), 2 → complete → READY con SHA/size della concatenazione
    esatta, 3 sole row (nessuna duplicazione).
  - `TestChunkedRecovery_DuplicateCompleteReturnsSameArtifact` — secondo
    `/complete` restituisce lo STESSO artifact (COMPLETED short-circuit), non
    un 400; fencing su worker diverso (`ErrTransitionConflict`).
  - `TestChunkedRecovery_MasterRestartDuringUploadResumesAndSucceeds` —
    chunk 0 → close DB → riapri STESSO file → chunk 1 → complete → READY,
    session COMPLETED (chunk/sessione persistono).
- 1 test worker-side verde (`artifact_upload_resume_test.go`):
  `TestResumeArtifactUpload_WorkerRestartPersistsSpoolAndCommits` — spool
  FILE-BACKED (non :memory:), close + riapri lo stesso file, la row UPLOADING
  persiste e il resume loop ri-carica dal path spillato e porta a COMMITTED.
- Invarianti preservati: SHA/fsync/atomic-promotion/fencing/durable-spool non
  toccati; build + vet + test (artifacts, worker, spool) verdi.

### 2026-08-17 — performance <1s: baseline misurata e primo fix finalize

- Baseline reale dal job warm di produzione (`clip.stock.v1`, 180s contenuto,
  cache 30/0/0) via `phase_breakdown` dell'attempt:
  - `compile` **498ms** (target <300ms)
  - `render` **749ms** (target <250ms, packet-copy)
  - `finalize` **250ms** (target <200ms)
  - `asset_wait`/`cache_lookup`/`download`/`encode` tutti 0 (warm).
- I due bottleneck maggiori (`compile` + `render`) sono fasi del **native
  C++ engine** (apertura input + packet mux): il lavoro è già in corso nel
  modulo `video-engine-cpp` (WIP non committato su `render_engine.cpp`,
  `segment_execution.cpp`, `media_packet_pipeline.hpp`) e sui commit
  `d24d337c` (open copy-only mux inputs in parallel) + `31be840b`
  (parallelize artifact manifest + skip ffprobe on sidecar).
- Fix Go-side sul **finalize**: `executors.artifactFromFile` calcolava lo
  SHA-256 con `os.ReadFile` (l'intero artifact in RAM, ~79MB per il job
  canonico, OOM su output da GB) → convertito a **SHA streaming** con buffer
  da 1 MiB, identico pattern di `publisher.streamSHAAndSize`. Digest esatto
  invariato (test di regressione `artifact_from_file_test.go`: hash streaming
  == `sha256.Sum256` su file >1MiB, rifiuto file vuoto/mancante).
- Invarianti preservati: SHA verification intatta, fsync/atomic-promotion/
  fencing/durable-spool non toccati. Build + vet + test taskrunner/worker
  verdi.
- Rimane aperto: il target `<250ms` packet-copy e `<300ms` compile richiedono
  il completamento del WIP engine C++; il `<200ms` finalize dipende dallo SHA
  (~150ms per 79MB, non eliminabile) + il flush/finalize dell'engine.

### 2026-08-17 — tranche performance: misurazione completa submit→commit

- Misurazione sulla fleet v1.2.38 (`e74e2b7c…`), job warm `clip.stock.v1`
  (180s contenuto, cache 30/0/0, worker `host_57_131_20_173`).
- Pipeline completa (job warm, fleet idle):
  - submit→assignment: **~0ms** (`time_to_first_worker_ms=0`, `queue_ms=0`;
    in questa misura il gap `created→started` di 23s era SOLO serializzazione
    dello scheduler dietro il job precedente sullo stesso worker).
  - assignment→compile: **~0ms** (`cache_lookup=0`, `lease_wait=0`, cache
    warm).
  - compile: **776ms** (target <300ms, +476ms)
  - render (packet-copy): **1057ms** (target <250ms, +807ms)
  - finalize: **279ms** (target <200ms, +79ms)
  - commit/delivery `drive-smoke`: upload **20.0s** per 79.6MB (~4 MB/s,
    `total_ms=20.9s`) — il collo di bottiglia NETWORK, fuori dai target
    render-pipeline.
- Somma render-pipeline (compile+render+finalize) = **2112ms** vs target
  **<750ms**; gap 1.36s concentrato in `render` (+807ms) e `compile`
  (+476ms), entrambi fasi del native C++ engine (WIP non committato).
- Job cold-start (stesso payload, 10 download): `compile=864ms`,
  `render=1211ms`, `finalize=346ms`, `download=1171ms`, `cache_lookup=298ms`.
- Conclusione: il `<1s` locale NON è raggiungibile con i soli fix Go-side;
  servono (a) il WIP engine C++ per compile/render e (b) il fix SHA streaming
  `artifactFromFile` (già fatto, non ancora rilasciato) per il margine sul
  finalize. L'upload Drive (20s) è un collo di bottiglia separato da
  profilare a parte.

### 2026-08-17 — engine C++: WIP integrato, build/test verdi e delta compile/render

- Il WIP non committato visto su `render_engine.cpp` / `segment_execution.cpp` /
  `media_packet_pipeline.hpp` risulta **già committato**: `7baf98b5`
  (`feat(engine): enforce copy-only mixed/concat with fail-closed Reject` —
  **correctness**, rimuove il fallback libx264: nessun segmento non copy-safe
  viene mai ricodificato) + `d24d337c` (`perf(engine): open copy-only mux
  inputs in parallel` — il delta perf reale). Nessuna modifica pendente su
  `video-engine-cpp/`.
- **Build + test**: `cmake --build build --parallel` verde (tutti i target,
  `velox_video_engine` contiene le stringhe distintive di `7baf98b5`);
  `ctest` **18/18 pass** (inclusi `render_mixed_tests`, `render_plan_v2_tests`,
  `render_copy_only_zero_intermediates_tests`).
- **Delta misurato** sul benchmark canonico `COPY_ONLY_CANONICAL_5M_V1`
  (24 clip × 375 frame = 300s 1080p, spec digest `8dce9a44…`) con
  `velox-benchmark -runs 5` sull'engine corrente vs baseline Phase-1
  (`be1a56b4`, engine sequenziale):
  - `wall` p50: **333ms → 183ms** (−45%)
  - `engine.packet_mux` (render): **273ms → 139ms** (−49%)
  - `engine.render` (exclusive): **280ms → 145ms** (−48%)
  - `input_reopen_count`: **27 → 2** (il preopen parallelo elimina i
    ri-aprimenti sequenziali)
  - `read_amplification`: **2.40 → 2.07**; `write_amplification`: **1.00x**
    (`mux_bytes_written == final_bytes_written == 5.838.599`, misurato, non
    più il limite sampler 0.05 della Phase-1)
  - Invarianti zero-spawn preservati: `external_process_count=0`,
    `ffmpeg=0`, `ffprobe=0`, `temp_bytes_written=0`, `file_copy_count=0`.
  - Determinismo: **5/5 run** stesso artifact SHA `17930f9c…` (il byte-SHA
    differisce dalla Phase-1 `324a8ecd…` perché il fixture è rigenerato e il
    muxer è cambiato; la determinismo intra-run resta intatto).
- **Nota onesta**: questo isola l'engine. Il `render` di produzione
  (`clip.stock.v1`, 1057ms su v1.2.38) include overhead Go worker/DataServer
  + delivery non presenti nel benchmark locale; il target end-to-end richiede
  il **rilascio** dell'immagine con queste modifiche engine + i fix Go-side
  già fatti (SHA streaming `artifactFromFile`).

### 2026-08-17 — profilo upload Drive: rete, NON re-upload

- Domanda: i 20s/79MB del commit/delivery sono collo di bottiglia di rete o
  re-upload? Misurato sui 20 job del re-soak (`drive-smoke`, artifact
  79.625.950 byte) via `fleetctl job inspect --json` → `deliveries[]`.
- **Verdetto: è rete, zero re-upload.** Su tutti i 20 job:
  `retry_count=0`, `attempt_count=1` → ogni delivery ha caricato l'artifact
  **esattamente una volta** (nessun retry, nessun doppio upload).
- Distribuzione dell'upload reale (`upload_ms`, stesso file 79.6MB):
  - min **4000ms** · p50 **8000ms** · p90 **26000ms** · max **31000ms**
  - throughput equivalente **53 → 159 Mbps** (p50 ~80 Mbps) — la firma di un
    percorso di rete condiviso/variabile, non di un difetto deterministico.
  - il “20s/79MB ≈ 4MB/s” del tranche precedente era **un'osservazione in una
    coda larga**, non il valore tipico (qui p50 upload = 8s).
- Contributo secondario — `queue_ms` (coda delivery): min 310ms · p50 ~8s ·
  max 32s. Causa: il delivery runner è `Concurrency=2`
  (`DefaultRunnerConfig`), quindi i 20 job in burst si accodano dietro il pool
  da 2; `total_ms` p50 ~20s. Non è upload: è attesa di slot.
- Code smell rilevato in `integrations/drive/service_files.go#UploadFile`:
  usa `uploadType=multipart` — **un solo POST monolitico** con l'intero
  79.6MB bufferizzato in RAM (`bytes.Buffer`) prima dell'invio. La
  documentazione Google Drive prescrive `uploadType=resumable` per file
  >5MB (resume su fallimento transitorio + nessun full-buffer). Nota: in
  questo ambiente il multipart a 79.6MB **riesce** (20/20), quindi non è un
  blocco attuale ma un rischio latente (un'interruzione = re-upload integrale
  da zero) + spike di memoria con `Concurrency=2` (~160MB in volo).
- Azioni possibili (non eseguite, da decidere):
  1. migrare `UploadFile` a `uploadType=resumable` (chunked + resume) per
     i file >5MB — elimina il full-buffer e il re-upload integrale su retry;
  2. alzare `Concurrency` delivery (es. 2→4) per assorbire i burst e
     ridurre `queue_ms`, bilanciando la pressione sulle API Drive;
  3. verificare se `drive-smoke` punta davvero alla Drive API reale —
     **VERIFICATO (2026-08-17): SÌ, è la Drive API reale** (vedi sotto).

### 2026-08-17 — `drive-smoke` confermato: punta alla Google Drive API reale

Tracciamento completo del path di delivery:

1. `drive-smoke` è una **row** in `delivery_destinations` (non un endpoint
   né un provider): `destination_id='drive-smoke'`, `provider='google_drive'`,
   `folder_id` reale. "smoke" è solo il nome convenzionale del folder Drive
   usato come target canary.
2. Il runner normalizza `google_drive` → `drive` (`canonicalProviderName` in
   `runner_helpers.go`) e risolve il provider dal registry di produzione
   (`bootstrap_modules.go`), che registra **solo due** provider reali:
   `drive` → `NewDriveProvider(driveMod.Service(), …)` (wrappa
   `internal/integrations/drive.Service`) e `social_gateway`.
3. `DriveProvider.Deliver` → `Service.UploadVideo` → `UploadFile`, e **ogni
   endpoint** del servizio è hardcoded verso l'API Google reale:
   - base `doAPIRequest`: `https://www.googleapis.com/drive/v3`
   - multipart: `https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart`
   - resumable init: `https://www.googleapis.com/upload/drive/v3/files?uploadType=resumable`
   - download: `https://www.googleapis.com/drive/v3/files/{id}?alt=media`
   - OAuth: `https://oauth2.googleapis.com/token` + `https://accounts.google.com/o/oauth2/v2/auth`
   - `FolderLink`: `https://drive.google.com/drive/folders/{id}`
4. **Nessun mock/stub/noop** nel wiring di produzione: i soli
   `stubProvider`/`fakeProvider` sono in `_test.go` (provider_test.go,
   runner_destination_unmapped_test.go). L'unico altro uso di "smoke" è
   `VELOX_SMOKE_DRIVE_FOLDER_ID` (folder reale del Level-D smoke) e il
   `driveUploaderAdapter` del Level-D smoke, che usa anch'esso il
   `integrations/drive.Service` reale.

Conclusione: il multipart >5MB che riesce **non** indica un endpoint smoke —
è la Drive API reale che accetta l'upload (ora comunque migrato a
`uploadType=resumable` per >5MB, vedi sezione delivery).

### 2026-08-17 — telemetria delivery: split upload network vs buffer locale

Per rispondere a "quanto dei ~20s di upload è rete e quanto è lettura/buffer
locale" il path delivery ora misura le due componenti separatamente.

- `drive.UploadResult` espone `network_ms` e `local_buffer_ms`:
  - `network_ms` = tempo negli HTTP round-trip Drive (transfer upload +
    init/status query del protocollo resumable);
  - `local_buffer_ms` = tempo di lettura dell'artifact da disco locale nel
    buffer (multipart `io.Copy`, resumable `ReadAt` per-chunk).
- `DriveProvider.Deliver` propaga i due valori in `Result.ProviderMeta`
  (`upload_network_ms` / `upload_local_buffer_ms`); il runner li rilegge e
  chiama il nuovo `Telemetry.ObserveDeliveryUploadBreakdown`.
- Nuove famiglie Prometheus (OperationalTelemetry):
  `velox_delivery_upload_network_ms` e `velox_delivery_upload_local_buffer_ms`
  (label `provider`). Provider che non misurano lo split (es. social_gateway)
  restano no-op (0/0).
- Test: `TestOperationalTelemetry_UploadBreakdownExportsNetworkAndLocalBuffer`
  (export histogram) e `TestUploadFile_MultipartRecordsNetworkAndLocalBufferSplit`
  (round-trip misurato). Verifica: build+vet+test verdi su
  `integrations/drive`, `deliveries`, `metrics`.

### 2026-08-17 — engine C++ pubblicato (v1.2.39) e delta render in produzione

- L'engine `7baf98b5` (fail-closed Reject) + `d24d337c` (parallel open) è
  **pubblicato e deployato**: v1.2.39 (`worker-v1.2.39-canonical` → digest
  `sha256:2d83c7c5…`) — verificato `git merge-base --is-ancestor`: `7baf98b5`
  è in v1.2.39 ma NON in v1.2.38; `d24d337c` è in entrambi. Fleet **4/4**
  CONNECTED/HEALTHY/AVAILABLE su `2d83c7c5…`, 0 job attivi.
- Misura warm (`clip.stock.v1`, cache 30/0/0, worker `velox-worker-523925eb`,
  `phase_totals` da `job inspect --json`):
  - `compile` **306ms** · `render` **486ms** · `finalize` **179ms**
  - somma render-pipeline **971ms** (target <750ms, gap residuo ~220ms).
- Confronto col baseline v1.2.38 (`host_57_131_20_173`):
  - `compile` 776ms → **306ms** (−61%) · `render` 1057ms → **486ms** (−54%)
    · `finalize` 279ms → **179ms** (−36%).
- **Attribuzione onesta**:
  - `finalize` 279→179ms = **fix SHA streaming `artifactFromFile`**
    (`de5a1822`, in v1.2.39) — reale e atteso.
  - `d24d337c` (parallel open) era **già in v1.2.38**, quindi NON è un delta
    nuovo di v1.2.39; `7baf98b5` è correctness-only (nessun delta perf).
  - Il resto del calo su compile/render è **varianza di misura + worker
    diverso** (il baseline v1.2.38 oscillava già 498→776ms compile e
    749→1057ms render tra due run sullo stesso contenuto). Serve una misura
    ripetuta sullo stesso worker per separare il segnale dal rumore.
- Target vs stato: `finalize <200ms` **raggiunto** (179ms); `compile <300ms`
  quasi (306ms); `render <250ms` ancora sopra (486ms) — il gap resta nella
  fase engine/Go-side non coperta dal benchmark locale (packet_mux puro
  139ms).
- **Finding secondario (cache)**: due job consecutivi sullo stesso worker
  (`velox-worker-523925eb`) hanno dato job1 30/0/0 (warm) e job2 20 hit/10
  miss/10 download (105MB) — il cache **evicina asset tra un job e il
  successivo** (probabile eviction indicizzata post-lease). Da investigare
  come follow-up; non impatta il delta render ma minaccia il "warm"
  sostenuto.

### 2026-08-17 — ri-misura warm post-rollout (conferma riproducibilità)

- Ri-misura richiesta sullo **stesso worker** (`velox-worker-523925eb`) e
  **stesso payload** `clip.stock.v1` per separare il segnale dal rumore
  (job `job_4fc6ec6497a54222`, warm 30/0/0, `phase_breakdown` da
  `job inspect --json`):
  - `compile` **306ms** · `render` **486ms** · `finalize` **179ms**
  - **identici alla prima misura v1.2.39** (306/486/179) → misura
    riproducibile, non varianza. Somma render-pipeline **971ms**.
- Confronto col baseline v1.2.38 (776/1057/279): −61% / −54% / −36%
  **confermato**.
- **Cache eviction ri-confermata**: job2 consecutivo (`job_7f2b6f210cda67ac`)
  = 20 hit/10 miss/10 download (105MB, `download` 7270ms) — il pattern
  "job1 warm 30/0/0 → job2 evicted 20/10/10" è deterministico, non un caso.
  Il `render` del job evicted sale a 507ms (+21ms) per il download
  interleaved; il fix è l'eviction indicizzata (follow-up separato).
- Verdetto target: `finalize <200ms` ✅ (179ms) · `compile <300ms` quasi
  (306ms, −6ms dal target) · `render <250ms` ❌ (486ms, gap engine/Go-side
  non coperto dal benchmark locale packet_mux=139ms).

### 2026-08-17 — read_amplification residuo: chiuso a 2.03x, floor reale ~1.83x

- Domanda: chiudere il `read_amplification` residuo **2.07x** verso il
  target 1.0–1.4x. Misurato su `COPY_ONLY_CANONICAL_5M_V1`
  (`velox-benchmark -runs 7`, engine `19381af5` + fix locali).
- **Due fix C++ applicati** (entrambi eliminano ri-letture ridondanti):
  1. `media_packet_pipeline.cpp`: il gate finale di durata NON ri-legge più
     l'output (`probeMediaInProcess(partial)`) — ora è un check in-memory
     sui timestamp dei packet video già scritti (`written_video_end_us`),
     stesso gate fail-closed `0.08s` del probe precedente.
  2. `render_engine.cpp`: il `hasAudioStream()` ridondante prima di
     `probeFinalAudioMetadata()` è rimosso — l'audio finale (4.9MB) veniva
     probe-letto **due volte**; ora una sola probe guida sia il guard
     has-audio (metadata senza codec) sia la decisione FINAL_AUDIO_COPY.
- **Risultato misurato** (p50 su 7 run, ora **deterministico**):
  - `read_amplification`: **2.07 → 2.03x** (`total_bytes_read`
    12.079.114 → **11.825.478** byte, identico su tutti i 7 run).
  - `input_open_count`: **28 → 26** · `input_reopen_count`: **2 → 1**
    (l'audio è ora aperto una sola volta in probe + una nel preopen).
  - `wall` p50 **152ms** (era 183ms), `packet_mux` invariato ~139ms.
  - Determinismo **invariato**: artifact SHA `17930f9c…` identico; 18/18
    ctest verdi (incluso il tail-extension che copre il gate in-memory).
- **Floor reale ~1.83x — il target 1.0–1.4x NON è raggiungibile su questo
  fixture** (finding onesto, non un limite dell'engine):
  - I 24 clip sono generati con **audio sine ~128kbps incorporato**
    (`color+sine`): 5.83MB di clip contengono solo ~0.65MB di video utile
    (solid-color ~17.6kbps), il resto è audio per-clip che il path copy-only
    **scarta** (`include_audio=false`). L'engine legge comunque l'intero
    file perché l'audio è interleaved con il video nell'mdat.
  - L'audio finale (4.86MB) è ~83% dell'output (5.84MB).
  - Quindi: input letto obbligatorio = 5.83MB clip + 4.86MB audio = 10.69MB
    ÷ output 5.84MB = **floor 1.83x** (anche con lettura perfetta 1x).
  - Il residuo 2.03x → 1.83x (~1.14MB) è `avformat_find_stream_info` +
    probe-buffer sulle clip; marginale e non porta sotto 1.4x.
- **Percorso verso 1.0–1.4x**: rigenerare il fixture con clip **solo-video**
  (source `color`, niente `sine`) → input ≈ 0.65MB clip + 4.86MB audio ≈
  output → `read_amplification` ≈ 1.0x. Alternativa: esprimere il target
  come `read / input_utile` anziché `read / output` per fixture con audio
  per-clip scartato.

### 2026-08-17 — delivery: upload Drive resumable + concurrency 2→4

- **UploadFile → uploadType=resumable per file >5MB** (`service_upload_resumable.go`):
  il multipart monolitico (79.6MB bufferizzati in RAM in un solo POST) è
  sostituito da una sessione resumable con **chunk da 8 MiB** (multiplo di
  256 KiB) e **resume** sul committed offset (`bytes */<total>` → `Range`).
  - Initiate → `POST .../files?uploadType=resumable` con
    `X-Upload-Content-Length`; chunk PUT con `Content-Range`; `308` =
    continua, `200/201` = completo.
  - 5xx/408/429/transport → query status + resume da `committed+1`
    (max 3 tentativi/chunk); 4xx permanente → fail-fast senza retry.
  - File ≤5MB restano su multipart (path estratto in `uploadMultipart`).
  - Test: chunked multi-chunk, resume dopo 500, permanent-failure no-retry
    (`service_upload_resumable_test.go`); build + drive/deliveries verdi.
- **Delivery concurrency 2→4**: `DefaultRunnerConfig().Concurrency`,
  il fallback di `NewDeliveryRunner` e il default `VELOX_DELIVERY_CONCURRENCY`
  passano da 2 a 4 (il forwarding runner era già a 4). Il tick cap già
  `ClaimBatch` a `Concurrency`, quindi il batch per tick passa da 2 a 4:
  un burst da 20 job si distribuisce in ~5 tick invece di ~10.
  - **Misura del nuovo `queue_ms` NON ancora eseguita**: richiede il
    **deploy del master** (il live `cb708fbf` è precedente alla modifica);
    il baseline concurrency=2 era `queue_ms` p50 ~8s / max 32s. Da misurare
    con un burst post-deploy via `fleetctl job inspect --json` →
    `deliveries[]`.

### 2026-08-17 — delivery: ClaimBatch > Concurrency con renewal in attesa (P0-02 emendato)

- **Contesto**: il design P0-02 (`effectiveClaimBatch <= Concurrency`) esiste
  perché una lease reclamata ma in coda al semaphore non veniva rinnovata
  (il renewal parte solo in `processLease`) → poteva scadere prima di
  iniziare. Rimuovere il cap per assorbire burst più grandi richiedeva di
  chiudere quel buco, non di riaprirlo.
- **Modifica (solo delivery runner, `runner.go#tick`)**:
  1. `DefaultRunnerConfig().ClaimBatch` **4 → 20** (Concurrency resta 4).
  2. Rimosso il cap `batch = min(ClaimBatch, Concurrency)`: il tick ora
     reclama fino a `ClaimBatch` in una sola poll.
  3. **Wait-phase renewal**: prima di acquisire il semaphore, ogni lease
     avvia `renewDeliveryLeaseLoop`; il loop si ferma appena lo slot è
     acquisito (`cancelWait(); <-waitDone`) e `processLease` riparte col
     suo renewal → nessun doppio renewal. Se la lease è persa in coda
     (re-claim da un altro runner), `onFailure` cancella il contesto e la
     goroutine abbandona lo slot.
  - La fairness cross-job di `ClaimDeliveries` (`PARTITION BY job_id` +
    `parent_rank`) resta intatta: un batch largo si distribuisce sui
    parent job, non affama un job.
- **Nota di consistenza**: il forwarding runner mantiene il vecchio cap
  `effectiveClaimBatch <= Concurrency` (test
  `TestTick_EffectiveClaimBatch_CappedAtConcurrency` invariato); il cambio
  è volutamente limitato al delivery runner. Follow-up: allineare forwarding
  allo stesso pattern se serve assorbire burst sul canale creator.
- Test: `TestTick_ClaimBatchExceedsConcurrency_AbsorbsBurst` (6 delivery,
  ClaimBatch=6 / Concurrency=2, drain completo senza deadlock, `-race` verde)
  + assert `ClaimBatch > Concurrency` in `TestDefaultRunnerConfig`.

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
- `v1.2.35-canonical` è stato superato da `v1.2.36-canonical` dopo la
  correzione dell'overlay sui raw metrics.
- `v1.2.36-canonical`: digest `sha256:ca08c34a3b65e69bee55f5caa41f1eab80cc0447555a23c2dcf2fc643ee7d5d9`, firma Cosign e
  canary verdi; overlay corretto per `final_concat_stream_copy` sui raw
  metrics già inizializzati dall'executor.

### 2026-08-16 — inventario intake job

Oggi ci sono **9 superfici produttive** che possono creare o accodare job su
Velox, più **1 endpoint smoke operativo**:

1. `POST /api/v1/jobs` — intake canonico M2M.
2. `POST /api/v1/jobs/batch` — batch di job indipendenti.
3. `POST /api/v1/creator/jobs` — creator push.
4. `POST /api/v1/pipeline-runs` — pipeline run durevole.
5. `POST /api/v1/instaedit/jobs` — adapter interno BFF InstaEdit.
6. `POST /api/v1/script/generate-with-images` — script/clip legacy.
7. `POST /api/v1/script/generate` — script ingress.
8. `POST /api/v1/script/jobs/:kind` — script kind dinamico.
9. `POST /api/v1/calendar/events/:id/enqueue` — calendar enqueue.
10. `POST /api/v1/video/smoke-clip-stock` — solo smoke/operazioni.

La superficie pubblica del BFF InstaEdit è già una sola:
`POST /api/v1/jobs`. Il lavoro ancora da fare è rendere tutti gli adapter
Velox thin adapters di un unico `CanonicalJobSubmitter`, deprecare gli alias
con telemetry, migrare i chiamanti noti e solo dopo rimuoverli. Non vanno
rimossi oggi alla cieca: creator, script, calendar e client BFF sono chiamanti
reali e la rimozione richiede il gate `scripts/ci/pre-removal-verify.sh`.

## Definition of Done

Il goal è raggiunto quando tutti i punti seguenti sono veri:

- [ ] Con asset caldi e compatibili, packet-copy video locale ≤ 1s al p95.
- [ ] Il job rifiuta gli asset incompatibili prima di avviare il render.
- [ ] Il video non viene mai ricodificato nel percorso copy-only.
- [ ] Invariante copy-only verificato su ogni assembly SUCCEEDED: `packet_copy_segments == total_segments`, `rejected_segments == 0`, `frames_encoded == 0` e `encode_passes == 0` (`concat_mode=mixed_packet`); un segmento non copy-safe viene rifiutato (`Reject`), mai ricodificato.
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
