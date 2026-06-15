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
| C++ engine | `video-engine-cpp/src/main.cpp` | Pipeline video FFmpeg |
| C++ contract | `video-engine-cpp/include/video_contract.hpp` | Strutture JSON↔C++ |
| Go contract | `shared/contract/contract.go` | Strutture Go↔JSON |
| Payload | `shared/payload/payload.go` | RenderJobParams |
| Deploy script | `data/deploy/install-server.sh` | Install master server |
| Systemd service | `data/deploy/velox-server.service` | Service file |
| Env template | `data/deploy/velox-server.env` | Configurazione |
