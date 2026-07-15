# Velox Architecture pre-gRPC — Artifacts, delivery and assets

> Archived document set: [Part 1 — Deploy, workers and job pipeline](architecture-pre-grpc.md) · [Part 2 — Contracts and runtime](architecture-pre-grpc-contract-and-runtime.md) · **Part 3 — Artifacts, delivery and assets**

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
