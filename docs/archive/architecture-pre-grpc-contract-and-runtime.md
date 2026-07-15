# Velox Architecture pre-gRPC — Contracts and runtime

> Archived document set: [Part 1 — Deploy, workers and job pipeline](architecture-pre-grpc.md) · **Part 2 — Contracts and runtime** · [Part 3 — Artifacts, delivery and assets](architecture-pre-grpc-artifacts-and-assets.md)

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
| Workers module | `internal/app/workers.go` | Routes worker-facing |
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
