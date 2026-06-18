# Velox Architecture — Deploy, Workers e Video Pipeline

## 1. Deploy del Master Server

Il master (`velox-server`) è un server Go con framework Gin, deployato come **systemd service**.

### Installazione

```bash
sudo ./data/deploy/install-server.sh
```

Lo script:
1. Compila il binary Go → `DataServer/bin/velox-server`
2. Crea utente di sistema `velox` (senza login)
3. Deploy in `/opt/velox/current/`
4. Copia service file → `/etc/systemd/system/velox-server.service`
5. Copia env file → `/etc/velox-server.env` (non sovrascrive)
6. Abilita e avvia il servizio

### Runtime

```
/opt/velox/current/
├── DataServer/bin/velox-server    # Binary
└── .velox/
    ├── data/                      # SQLite DB, video, bundle
    └── secrets/                   # Token OAuth YouTube, certificati
```

---

## 2. Deploy dei Worker Remoti

I worker sono **agenti Go** (`worker-agent-go`) che girano su macchine Linux remote. Il deploy avviene tramite **bundle update incrementale** (nessun SSH necessario).

### Flusso Bundle Update

```
Master                              Worker
  │                                    │
  │  POST /api/workers/register  ←────│  Registrazione iniziale
  │  ────→ auth_token per richieste    │
  │                                    │
  │  GET /api/worker/v2/manifest ←────│  Check aggiornamenti
  │  ────→ manifest_v2.json           │  (hash, lista chunk)
  │                                    │
  │  GET /api/worker/v2/chunk/:name ←─│  Scarica chunk mancanti
  │  ────→ binary diff/zip            │
  │                                    │
  │  POST /api/workers/commands ←─────│  Comando: update_code
  │  ────→ worker scarica e riavvia   │
```

### Endpoint Bundle (module.go)

| Endpoint | Metodo | Descrizione |
|----------|--------|-------------|
| `/bundle_manifest.json` | GET | Manifest v1 (compat) |
| `/api/worker/bundle` | GET | Download ZIP completo |
| `/api/worker/v2/manifest` | GET | Manifest v2 (chunk-based) |
| `/api/worker/v2/chunk/:name` | GET | Singolo chunk |
| `/bundle/manifest/generate` | POST | Rigenera manifest (admin) |

### Bundle Files

```
DataServer/internal/handlers/remote/workers/
├── bundle_handlers.go           # HTTP handlers per download
├── bundle_helpers.go            # SHA256, ZIP creation, dir scan
├── bundle_manifest_generate.go  # Genera manifest_v2.json
├── bundle_rebuild.go            # Forza rebuild ZIP via script
└── bundle_v2_handlers.go        # V2 chunk-based download
```

---

## 3. Comunicazione Worker ↔ Master

### 3.1 Registrazione

Quando un worker si avvia, si registra al master:

```
POST /api/workers/register
{
  "worker_id": "worker-abc123",
  "worker_name": "gpu-node-01",
  "api_version": "2.0",
  "code_version": "1.0.0",
  "bundle_version": "20240615",
  "bundle_hash": "sha256:abc...",
  "capabilities": {
    "render_scene_image": true,
    "render_clip_stock": true,
    "ffmpeg": true,
    "cpp_engine": true,
    "max_parallel_jobs": 1,
    "supported_job_types": ["process_video", "render", "process_audio"]
  }
}
```

Il master risponde con un `auth_token` usato per tutte le richieste successive.

### 3.2 Heartbeat

Il worker invia heartbeat periodici per mantenere vivo il suo stato:

| Stato Worker | Intervallo | Scopo |
|-------------|-----------|-------|
| **Idle** | 60s | Keepalive basso |
| **Busy** | 15s | Progress updates frequenti |
| **Error** | 10s | Recovery rapido |

```
POST /api/workers/heartbeat
{
  "worker_id": "worker-abc123",
  "status": "idle|busy|error",
  "current_job": "job-xyz",
  "active_jobs": 0,
  "completed_jobs": 42,
  "failed_jobs": 1
}
```

Il master usa l'heartbeat per:
- Aggiornare `last_seen` del worker
- Rilevare worker morti (timeout)
- Mostrare stato in tempo reale nella dashboard

### 3.3 Polling Comandi

Il worker controlla comandi ogni 30s (configurabile):

```
GET /api/workers/commands?worker_id=worker-abc123
→ [
    {"command": "drain", "timestamp": "..."},
    {"command": "update_code", "timestamp": "..."}
  ]
```

| Comando | Effetto |
|---------|---------|
| `update_code` | Scarica nuovo bundle, si riavvia |
| `restart` | Si riavvia senza update |
| `drain` | Finisce job corrente, non ne accetta di nuovi |
| `cancel_job` | Interrompe job specifico (via context) |
| `reboot_host` | Riavvia l'intera macchina |

Dopo aver eseguito, il worker conferma:
```
POST /api/workers/commands/ack
{"command": "drain", "status": "executed"}
```

### 3.4 SSE (Server-Sent Events)

Per aggiornamenti in tempo reale dalla dashboard admin:
- `GET /api/v1/workers/stream` → SSE stream
- Eventi: `job_status`, `worker_status`, `worker_update`

```
DataServer/internal/handlers/remote/workers/sse.go
```

### 3.5 Autenticazione

Ogni worker riceve un token JWT durante la registrazione. Il token è:
- Validato su ogni richiesta (header `Authorization: Bearer <token>`)
- associato a un `worker_id` specifico
- usato per prevenire impersonificazione

```
TokenManager → distribuisce/valida token
TokenStorage → in-memory, generati durante RegisterV2
```

---

## 4. Ciclo di Vita Completo di un Job

### Fase 1: Job Enqueue (Master)

```
API Handler → BuildSceneImagePayload() → FileQueue.SubmitJob()
                                          │
                                          ▼
                                    SQLite: jobs table
                                    status: "pending"
```

```
DataServer/internal/jobs/enqueue/enqueue.go
DataServer/internal/queue/file_queue.go
```

### Fase 2: Job Dispatch (Master → Worker)

L'orchestrator del master assegna il job al worker disponibile:

```
Orchestrator.Poll() → FileQueue.ClaimNextJob(workerID)
                      │
                      ▼
                status: "pending" → "processing"
                worker_id: "worker-abc123"
```

```
DataServer/internal/queue/orchestrator.go
DataServer/internal/store/store_jobs.go (ClaimNextPendingJob)
```

### Fase 2b: Worker Creator / Computer Creator

In alcuni flussi il job passa prima da un computer creator che prepara o restituisce il payload completo
prima del rendering finale sul worker remoto.

```
Queue job → worker creator / computer creator → ritorno payload completo → worker finale
```

Questo step non sostituisce il worker remoto: lo precede quando il flusso richiede una fase intermedia
di composizione o arricchimento degli asset.

### Fase 3: Job Acquisition (Worker)

Il worker ogni 5s controlla se ci sono job:

```go
// worker_jobs.go
func (w *Worker) jobLoop(ctx context.Context) {
    ticker := time.NewTicker(5 * time.Second)  // Polling ogni 5s
    for {
        job, err := w.pollJob(ctx)  // GET /api/worker/job
        if job != nil {
            w.executeJob(ctx, job)
        }
    }
}
```

### Fase 4: Job Execution (Worker)

```
executeJob()
  │
  ├── concurrencyLimiter.Acquire()    // Max 1 job alla volta
  ├── stato: idle → busy
  ├── runJobTask(job) in base a tipo:
  │   ├── "process_video" → runVideoJob()
  │   ├── "render"        → runRenderJob()
  │   ├── "process_audio" → runAudioJob()
  │   └── "health_check"  → {status: "healthy"}
  │
  ├── executeWorkflowJob()
  │   ├── Crea VideoGenerationWorkflow
  │   ├── Costruisce contract.VideoEngineRequest
  │   ├── Scrive native_video_request.json
  │   └── Lancia C++ engine (exec.CommandContext)
  │
  └── uploadCompletedVideo() → POST /api/video/upload-completed
      └── upload sul canale selezionato / Google Drive
```

### Fase 5: C++ Video Processing

Il binary C++ riceve il JSON e processa il video:

```
velox_video_engine --full-video --request /tmp/velox_workflow_xxx/native_video_request.json
```

**Pipeline C++ (`main.cpp`):**

```
┌─────────────────────────────────────────────┐
│ 1. Scarica asset (immagini, clip, audio)    │
│    file_utils.hpp: HTTP download + GDrive   │
├─────────────────────────────────────────────┤
│ 2. Per ogni scena:                          │
│    ├── scene_image → --build-scene-segment  │
│    │   (immagine → video con Ken Burns)     │
│    │   FFmpeg: zoompan + scale 1920x1080    │
│    │                                        │
│    └── clip_video → --build-clip-segment    │
│        (clip → segmento crop/scale)         │
│        FFmpeg: scale + crop to 1920x1080    │
├─────────────────────────────────────────────┤
│ 3. Concatena segmenti:                      │
│    --concat-segments --list segments.txt    │
│    FFmpeg: concat demuxer                   │
├─────────────────────────────────────────────┤
│ 4. Muxa audio (voiceover):                  │
│    --mux-audio --video final.mp4 \          │
│                --audio voiceover.mp3        │
│    FFmpeg: -c:v copy -c:a aac              │
├─────────────────────────────────────────────┤
│ 5. Output: JSON su stdout + file su disk    │
│    {"success": true, "out": "/path/out.mp4"}│
└─────────────────────────────────────────────┘
```

**Librerie C++ utilizzate:**
- **FFmpeg** (via exec): zoompan, scale, crop, concat, mux audio
- **nlibcurl** (file_utils.hpp): download HTTP/HTTPS, Google Drive
- **nlohmann/json** (json_utils.hpp): parsing/generazione JSON

### Fase 6: Result Submission (Worker → Master)

```
1. C++ esce con codice 0 → success
2. worker.uploadCompletedVideo()
   POST /api/video/upload-completed
   Body: multipart/form-data con il file .mp4
   → Master salva in DataServer/completed_videos/

3. worker.SubmitJobResult()
   POST /api/workers/complete
   {
     "job_id": "job-xyz",
     "worker_id": "worker-abc123",
     "status": "success",
     "output": {
       "output_path": "/path/to/video.mp4",
       "duration": 120.5
     }
   }

4. Master: CompleteJobEnhanced()
   → Aggiorna jobs table: status = "completed"
   → Salva output_path, timestamps
   → Orchestrator prossimo step (se multi-step)
```

```
DataServer/internal/handlers/remote/workers/worker_complete.go
```

---

## 5. Contratto Go ↔ C++

### Strutture condivise (`shared/contract/contract.go`)

```
RenderJobParams (Go) → VideoEngineRequest (JSON) → C++
```

| Campo | Tipo | Descrizione |
|-------|------|-------------|
| `job_id` | string | Identificativo job |
| `video_name` | string | Nome video output |
| `script_text` | string | Testo dello script |
| `scenes` | []SceneRequest | Lista scene con testo, immagine, durata |
| `voiceover_paths` | []string | Percorsi file audio |
| `output_path` | string | Percorso output finale |
| `video_mode` | string | "scene_image" o "clip_stock" |
| `scene_image_paths` | []string | Immagini scaricate per le scene |
| `drive_output_folder` | string | Cartella Google Drive (opzionale) |

### Schema JSON (`shared/contract/contract_test.go`)

I test verificano che i field name JSON corrispondano esattamente alle struct C++ (`video_contract.hpp`).

---

## 6. Sicurezza e Affidabilità

### Autenticazione
- Token JWT per worker registrati
- Admin token per endpoint di amministrazione
- Bearer token o X-Admin-Token header

### Fault Tolerance
- **Zombie Tracker**: rileva job bloccati > N minuti, li rimette in coda
- **Retry automatico**: job falliti vengono ritentati (configurabile)
- **Concurrency Limiter**: previene sovraccarico (max_active_jobs)
- **Context cancellation**: job cancellabili dall'admin
- **Dead Letter Queue**: job che falliscono N volte vanno in DLQ

### Monitoring
- **Prometheus metrics**: job success/failure rate, latenza, worker status
- **Health endpoint**: `GET /health` mostra stato worker registrato
- **SSE stream**: aggiornamenti real-time per dashboard admin
- **Worker logs**: ultimi 300 log + 100 errori inviati con ogni job result

---

## 7. File Chiave

| Componente | Path | Descrizione |
|-----------|------|-------------|
| Master bootstrap | `cmd/server/bootstrap.go` | Avvio server, wiring |
| Routes | `cmd/server/router.go` | Registrazione routes admin |
| Workers module | `modules/workers/module.go` | Routes worker-facing |
| Worker registration | `handlers/remote/workers/worker_registration.go` | RegisterV2, Unregister |
| Heartbeat | `handlers/remote/workers/worker_heartbeat.go` | Heartbeat + status |
| Commands | `handlers/remote/workers/worker_commands.go` | SendCommand, SendCommandBulk |
| Complete job | `handlers/remote/workers/worker_complete.go` | CompleteJobEnhanced |
| Bundle | `handlers/remote/workers/bundle_*.go` | Download/manifest bundle |
| SSE | `handlers/remote/workers/sse.go` | Server-Sent Events broker |
| FileQueue | `queue/file_queue.go` | Coda SQLite con priority |
| Orchestrator | `queue/orchestrator.go` | Event loop multi-step |
| DLQ | `queue/dlq.go` | Dead letter queue |
| Worker agent | `worker-agent-go/internal/worker/worker.go` | Main loop worker |
| Job polling | `worker-agent-go/internal/worker/worker_jobs.go` | Polling ogni 5s |
| Job execution | `worker-agent-go/internal/worker/job_executor.go` | Dispatch per tipo |
| Heartbeat | `worker-agent-go/internal/worker/worker_comms.go` | Heartbeat adaptive |
| Commands | `worker-agent-go/internal/worker/worker_commands.go` | Processamento comandi |
| Go workflow | `worker-agent-go/pkg/video/workflow.go` | Orchestrazione Go→C++ |
| Native engine | `worker-agent-go/pkg/video/native_engine.go` | Lancia binary C++ |
| C++ engine | `video-engine-cpp/src/main.cpp` | Sotto-comandi + dispatcher |
| C++ full video | `video-engine-cpp/src/cmd_full_video.cpp` | Pipeline completa |
| C++ contract | `video-engine-cpp/include/video_contract.hpp` | Strutture JSON↔C++ |
| Go contract | `shared/contract/contract.go` | Strutture Go↔JSON |
| Deploy script | `data/deploy/install-server.sh` | Install master server |

---

## 8. Progress Streaming (FFmpeg → Dashboard)

Il worker mostra progresso reale durante il rendering, non solo "Busy".

### Flusso

```
C++ engine (stderr)           Go worker                    Master
  │                             │                            │
  │ emitProgress(45,3,10,       │                            │
  │   "building_scene")         │                            │
  │ ──→ StderrPipe ───────────→ │                            │
  │                             │  parse JSON lines          │
  │                             │  progressPercent.Store(45) │
  │                             │  progressScene.Store(3)    │
  │                             │  progressTotal.Store(10)   │
  │                             │  progressStage.Store(...)  │
  │                             │                            │
  │                             │  heartbeat()               │
  │                             │ ──→ extra["current_job"] ──→│
  │                             │     progress_percent = 45  │
  │                             │     progress_scene = 3     │
  │                             │     progress_total = 10    │
  │                             │     progress_stage = ...   │
  │                             │                            │
  │                             │                  Dashboard:│
  │                             │  [████░░░░░░] 45%         │
  │                             │  scene 3/10 building_scene │
```

### C++ side

`cmd_full_video.cpp` emette JSON lines su stderr ad ogni step:
- Voiceover download → `{"progress":5,"stage":"voiceover_ready"}`
- Ogni scena/clip → `{"progress":N,"scene":N,"total_scenes":N,"stage":"building_scene"}`
- Concat → `{"progress":85,"stage":"concatenating"}`
- Mux audio → `{"progress":92,"stage":"muxing_audio"}`
- Completato → `{"progress":100,"stage":"completed"}`

### Go side

`native_engine.go` usa `cmd.StderrPipe()` per leggere lo stream in tempo reale.
Ogni riga JSON viene parsata e passata al callback → `Worker.progressPercent` (atomic).

### Heartbeat

I campi progress vengono inclusi nel payload del heartbeat ogni 15s (busy mode):
```json
{
  "current_job": {
    "job_id": "job-xyz",
    "progress_percent": 45,
    "progress_scene": 3,
    "progress_total": 10,
    "progress_stage": "building_scene"
  }
}
```

---

## 9. Dynamic Concurrency

Il worker adatta la concorrenza all'hardware della macchina.

### Calcolo

```
runtime.NumCPU() → 32
detectMaxParallelJobs() → clamp(32/2, min=1, max=8) = 8
```

| Hardware | NumCPU | Max Parallel Jobs |
|----------|--------|-------------------|
| VPS piccola | 2 | 1 |
| VPS media | 4 | 2 |
| Server dedicato | 8 | 4 |
| GPU server | 32 | 8 |

### Registrazione

Il worker informa il master durante `POST /api/workers/register`:
```json
{
  "capabilities": {
    "max_parallel_jobs": 8,
    "cpu_count": 32
  }
}
```

### ConcurrencyLimiter

Il `ConcurrencyLimiter` (semaphore-based) gestisce l'acquisizione slot.
Job ad alta priorità (priority ≥ 3) non vengono mai rifiutati.
Job in coda vengono processati quando uno slot si libera.

---

## 10. Artifact Pipeline (Master-Side)

L'artifact pipeline è il cuore del control plane di Velox. È il **solo percorso** attraverso cui un job può arrivare a `SUCCEEDED`.

### Pipeline Canonica

```
Worker termina rendering
        ↓
  POST /api/v1/video/upload-completed  (o gRPC ArtifactUploaded)
        ↓
  artifacts.Service.BeginUpload  ───  valida auth job/worker/lease
        ↓
  artifacts.Service.Receive     ───  stream byte → master calcola SHA-256
        ↓
  artifacts.Service.Finalize    ───  CAS RECEIVED→FINALIZING
        ↓
  FinalizationRepository.FinalizeVerified
      ├── jobs.status RUNNING → SUCCEEDED
      ├── artifacts.status STAGING → READY
      ├── job_attempts → SUCCEEDED
      ├── outbox: ARTIFACT_READY + JOB_SUCCEEDED
      ├── delivery: job_deliveries PENDING per destinazione
      └── artifact_uploads: FINALIZING → COMPLETED
```

**Regola fondamentale**: un job può diventare `SUCCEEDED` **soltanto** attraverso `FinalizationRepository.FinalizeVerified`. Nessun handler, route amministrativa o repository generico può eseguire `UPDATE jobs SET status = 'SUCCEEDED'`.

### Chunked Upload Persistente

Per file > 50 MB, il worker usa upload chunked resumabile. A differenza della vecchia implementazione (mappa globale in memoria), la nuova architettura è **persistente**:

```
InitChunkedSession → artifact_uploads.CREATED (via BeginUpload)
    ↓
UploadChunk 0..N  → blob staging + artifact_upload_chunks row
    ↓
CompleteChunked   → assembla chunks → Receive (master hash) → Finalize (SUCCEEDED)
```

Il worker invia chunk a:
- `POST /api/v1/video/chunked/init` — inizia sessione
- `POST /api/v1/video/chunked/:job_id/:chunk_index` — carica chunk
- `POST /api/v1/video/chunked/:job_id/complete` — assembla e finalizza

Dopo un riavvio del master, la sessione chunked sopravvive perché tutto lo stato è in SQLite (`artifact_uploads` + `artifact_upload_chunks`).

### Schema Database

```sql
-- artifact_uploads: sessione di upload
CREATE TABLE artifact_uploads (
    upload_id    TEXT PRIMARY KEY,
    artifact_id  TEXT NOT NULL,
    job_id       TEXT NOT NULL,
    status       TEXT NOT NULL CHECK(status IN ('CREATED','UPLOADING','RECEIVED','FINALIZING','COMPLETED','FAILED','EXPIRED')),
    temporary_storage_key TEXT NOT NULL,
    received_size_bytes   INTEGER,
    received_sha256       TEXT,
    created_at   TEXT NOT NULL,
    expires_at   TEXT NOT NULL,
    ...
);

-- artifact_upload_chunks: tracciamento chunk individuali
CREATE TABLE artifact_upload_chunks (
    upload_id    TEXT NOT NULL,
    chunk_index  INTEGER NOT NULL,
    size_bytes   INTEGER NOT NULL,
    sha256       TEXT,
    storage_key  TEXT NOT NULL,
    received_at  TEXT NOT NULL,
    PRIMARY KEY (upload_id, chunk_index),
    FOREIGN KEY (upload_id) REFERENCES artifact_uploads(upload_id)
);
```

---

## 11. Delivery System

Il delivery system gestisce la pubblicazione degli artifact verso destinazioni esterne (YouTube, Google Drive).

### Delivery Plan Resolver

Ogni job può avere un **piano di delivery esplicito** che specifica quali destinazioni devono ricevere l'artifact:

```
job_delivery_plans
- job_id          TEXT (FK → jobs)
- destination_id  TEXT (FK → delivery_destinations)
- enabled         INTEGER
- priority        INTEGER
- metadata_json   TEXT
```

Il resolver segue questo ordine:

1. **Piano per-job**: se esiste una riga in `job_delivery_plans` per il job, usa SOLO quelle destinazioni (con `enabled = 1`)
2. **Fallback globale**: se nessun piano per-job esiste, usa tutte le `delivery_destinations` abilitate (comportamento legacy)

### Delivery Runner

Il `DeliveryRunner` è l'unico writer degli stati delle delivery. Ciclo di vita:

```
PENDING → RUNNING (claim con lease)
             ↓
        SUCCEEDED  |  RETRY_WAIT  |  FAILED  |  BLOCKED_AUTH
```

| Stato | Descrizione |
|-------|-------------|
| PENDING | In attesa di essere processata |
| RUNNING | In upload (lease attivo) |
| SUCCEEDED | Completata con successo |
| RETRY_WAIT | Fallita temporaneamente, riprova con backoff |
| FAILED | Fallita permanentemente (max tentativi raggiunto) |
| BLOCKED_AUTH | Bloccata per errore di autenticazione (intervento operatore) |

I provider (YouTube, Drive) **non toccano mai il database** — chiamano solo API esterne e restituiscono un risultato al runner.

---

## 12. Asset Service

L'`AssetService` è il registry centralizzato per tutti gli asset del sistema (voiceover, scene images, musiche, ecc.).

### Architettura

```
AssetService
  ├── AssetRepository   → DB: asset_id, sha256, storage_key, mime_type
  ├── BlobStore         → filesystem/S3: byte effettivi
  └── ResolverRegistry  → resolver specializzati per tipo
```

### Flusso

```
Richiesta: velox-asset://<sha256>
  ↓
ResolverRegistry.ResolveByInference(ref)  → scarica/seleziona sorgente
  ↓
AssetService.ResolveAndRegister()
  ├── Stream byte → SHA-256 (master calcola)
  ├── BlobStore.PromoteToFinal()  → storage content-addressato
  └── AssetRepository.Insert()    → DB come READY
```

### Worker Asset Serving

I worker scaricano gli asset via:
```
GET /api/v1/worker-assets/:asset_id
```

Il master serve l'asset tramite `AssetRepository.GetByID()` + `BlobStore.ReadFinal()` — il database è la fonte di verità, non il filesystem.

---

## 13. Struttura File (Post-Reorganization)

### DataServer (Go)

```
internal/
├── store/                    # 15 src + 3 test (era 28)
│   ├── sqlite.go             # Core SQLiteStore
│   ├── store_darkeditor.go   # DarkEditor: projects, folders, assets, templates
│   ├── store_jobs.go         # Jobs: CRUD, claim, queries, history, logs
│   ├── store_workers.go      # Workers: CRUD, validations, repository
│   ├── sqlite_youtube.go     # YouTube metrics
│   ├── sqlite_youtube_entities.go  # YouTube channels, groups, cache
│   ├── sqlite_ansible.go     # Ansible hosts, runs
│   ├── sqlite_calendar.go    # Calendar events
│   ├── sqlite_analytics.go   # Analytics cache, stats
│   ├── sqlite_drive_links.go # Google Drive links
│   ├── sqlite_livestream.go  # Livestream CRUD
│   ├── sqlite_queue.go       # Orchestrator queue, DLQ
│   ├── types.go              # Domain types
│   └── legacy_importer.go    # JSON→SQLite migration
├── handlers/remote/workers/  # 22 src (era 25)
│   ├── worker_registration.go
│   ├── worker_heartbeat.go
│   ├── worker_commands.go
│   ├── worker_control.go
│   ├── worker_complete.go
│   ├── worker_update.go      # + helpers merged
│   ├── worker_status.go
│   ├── bundle_*.go
│   ├── sse.go
│   ├── showlog.go
│   ├── validation.go
│   ├── upload_video.go       # + asset_handlers merged
│   ├── youtube_autoupload.go
│   └── workers.go            # + upload_manager merged
├── handlers/server/youtube/  # 23 src (era 26)
│   ├── helpers.go            # + youtube_helpers merged
│   ├── youtube_feed.go       # + youtube_news merged
│   ├── account_handlers.go   # + status_handlers merged
│   ├── creative_ai_cover.go
│   ├── creative_ai_translate.go
│   └── ...
├── integrations/youtube/     # 15 src (era 20)
│   ├── api.go                # api_client + api_video + api_channel
│   ├── storage.go            # storage + storage_channels + storage_groups + storage_cleanup
│   └── ...
└── queue/                    # 14 src (invariato, già organizzato)
```

### RemoteCodex (Go + C++)

```
worker-agent-go/
├── internal/worker/          # 16 src (era 18, split in 2)
│   ├── worker_types.go       # Status, Worker struct, recentLogBuffer
│   ├── worker_init.go        # New(), command dedup
│   ├── worker.go             # Start(), Stop()
│   ├── worker_comms.go       # Heartbeat, register, progress
│   ├── worker_commands.go    # Command polling
│   ├── worker_jobs.go        # Job polling
│   ├── job_executor.go       # Job dispatch + execution
│   ├── job_params.go         # Parameter extraction
│   ├── job_upload.go         # Video upload to master
│   ├── concurrency.go        # ConcurrencyLimiter
│   ├── stage_executor.go     # Stage/chunk execution
│   └── ...
├── pkg/api/                  # 5 src (era 4)
│   ├── client.go             # Core HTTP, retry, circuit breaker
│   ├── client_endpoints.go   # Register, GetJob, Heartbeat, etc.
│   └── ...
└── pkg/video/
    ├── workflow.go           # VideoGenerationWorkflow
    └── native_engine.go      # Lancia C++ con progress streaming

video-engine-cpp/
├── src/
│   ├── main.cpp              # Dispatcher + sotto-comandi semplici (287 LOC)
│   └── cmd_full_video.cpp    # Pipeline completa (310 LOC)
└── include/
    └── video_contract.hpp    # Strutture JSON↔C++
```
