# Velox v1.1.0 — Sistema di Generazione Video Distribuito

![CI](https://github.com/Marcuss-ops/VeloxEditiingg/actions/workflows/ci.yml/badge.svg)

## Panoramica

Velox è un sistema distribuito per la generazione e composizione video. **Master server** (Go/Gin) gestisce coda job e worker remoti; **RemoteCodex** contiene il software installato sui worker per il rendering.

```
┌──────────────────────────────────────────────────┐
│            MASTER SERVER (DataServer)             │
│                                                   │
│  ┌─ Transport Layer ──────────────────────────┐  │
│  │  HTTP (Gin)  ·  gRPC (worker control)      │  │
│  └────────────────────────────────────────────┘  │
│                        │                          │
│  ┌─ Application Services ─────────────────────┐  │
│  │  artifacts.Service  ·  AssetService         │  │
│  │  ChunkedUploadSvc   ·  DeliveryPlanResolver │  │
│  │  LifecycleService   ·  Outbox Dispatcher    │  │
│  └────────────────────────────────────────────┘  │
│                        │                          │
│  ┌─ Persistence ──────────────────────────────┐  │
│  │  SQLiteStore  (jobs/artifacts/workers)      │  │
│  │  BlobStore    (content-addressed storage)   │  │
│  │  Outbox       (transactional event queue)   │  │
│  └────────────────────────────────────────────┘  │
│                        │                          │
│  ┌─ Background Runners ───────────────────────┐  │
│  │  DeliveryRunner   ·  Reconciler             │  │
│  │  OutboxDispatcher ·  Zombie Reaper          │  │
│  └────────────────────────────────────────────┘  │
│                        │                          │
│  ┌─ Integrations ────────────────────────────┐  │
│  │  YouTube · Drive · Ansible · Analytics     │  │
│  │  Calendar · DarkEditor · NVIDIA AI         │  │
│  └────────────────────────────────────────────┘  │
└────────────────────┬─────────────────────────────┘
                     │ gRPC control stream (bidi)
                     │ register, heartbeat, job offers,
                     │ command dispatch, artifact upload
                     ▼
┌──────────────────────────────────────────────────┐
│            WORKER REMOTO (RemoteCodex)            │
│  Worker Agent (Go) ── Video Engine (C++/FFmpeg)   │
│  gRPC stream ──→ push-based job acceptance        │
│  heartbeat 15s/60s ──→ progress streaming         │
│  chunked upload (files > 50MB via resumable)      │
└──────────────────────────────────────────────────┘
```

> **Dettagli completi**: deploy, worker communication, video pipeline, progress streaming → vedi [`docs/archive/architecture-pre-grpc.md`](docs/archive/architecture-pre-grpc.md) (legacy, pre-gRPC reference)

---

## Struttura Repository

```
DataServer/                    # MASTER SERVER (Go)
├── cmd/server/                # main.go, bootstrap.go, router.go
├── internal/
│   ├── handlers/server/       # API REST: youtube/, jobs/, analytics/, calendar/, darkeditor/, drive/
│   ├── handlers/remote/       # Worker API: workers/, ansible/, install/, livestream/
│   ├── integrations/          # Esterni: youtube/, drive/, news/
│   ├── modules/               # Wiring: workers/, youtube/, ansible/, drive/, health/
│   ├── queue/                 # FileQueue, Orchestrator, DLQ, events
│   ├── store/                 # SQLite: store_darkeditor, store_jobs, store_workers, youtube, ansible, ...
│   ├── services/              # Business logic: jobs/, analytics/
│   ├── workers/               # Registry, CommandManager
│   └── config/                # Env vars config
├── data/deploy/               # install-server.sh, systemd, env template
└── bin/                       # velox-server, velox-bundler

RemoteCodex/                   # WORKER REMOTO
├── native/
│   ├── worker-agent-go/       # Go agent
│   │   ├── internal/worker/   # worker_types, worker_init, job_executor, concurrency, ...
│   │   └── pkg/video/         # workflow.go, native_engine.go (launches C++)
│   └── video-engine-cpp/      # C++ engine
│       └── src/               # main.cpp (dispatcher), cmd_full_video.cpp (pipeline)

shared/                        # Libreria condivisa Go
├── contract/                  # Go↔C++ contract types
├── media/                     # ffprobe helpers
└── payload/                   # JSON map utilities
```

---

## Subsistemi

| Subsystem | Descrizione |
|-----------|-------------|
| **Artifact Pipeline** | `BeginUpload → Receive → Finalize` — master calcola hash, verifica, e promuove job a SUCCEEDED in una singola transazione atomica |
| **Chunked Upload** | Upload resumabile persistente per file > 50MB — chunk salvati su disco e tracciati in DB; sopravvive a riavvii del master |
| **Delivery Runner** | Runner background che processa `job_deliveries` PENDING → YouTube/Drive con lease, retry, e classificazione errori |
| **Delivery Plan Resolver** | Piani di delivery espliciti per-job — ogni job può specificare quali destinazioni ricevono l'artifact |
| **Asset Service** | Registry content-addressato (SHA-256) per asset audio/video/immagine — `AssetRepository + BlobStore + ResolverRegistry` |
| **Outbox** | Transactional outbox per eventi di sistema (JOB_SUCCEEDED, ARTIFACT_READY, DELIVERY_CREATED) — garanzia at-least-once |
| **Reconciler** | 4 regole di cleanup per stati interrotti: upload scaduti, blob orfani, artifact QUARANTINED, STAGING bloccati |
| **Job Queue** | FileQueue SQLite, Orchestrator multi-step, DLQ, zombie detection |
| **YouTube Manager** | Upload, channels (OAuth2), groups, AI generation (NVIDIA FLUX/LLaMA), competitor tracking |
| **Analytics** | Dashboard BI: summary, finance, performance, realtime, per-channel/group |
| **Calendar** | Pianificazione video con CRUD eventi, enqueue alla coda job |
| **Dark Editor** | Editor immagini AI (upload, filtro, trasformazione, upscale, generazione) |
| **Google Drive** | Upload video completati, gestione cartelle per gruppi |
| **Ansible Deploy** | Install/update/rollback worker remoti via playbook |
| **Livestream** | YouTube Live stream management |

---

## Variabili d'Ambiente (principali)

| Categoria | Variabili chiave |
|-----------|-----------------|
| **Core** | `VELOX_MASTER_PORT` (8000), `GIN_MODE`, `VELOX_ADMIN_TOKEN` |
| **Storage** | `VELOX_DATA_DIR`, `VELOX_RUNTIME_DIR`, `VELOX_DB_PATH` (sqlite, default at `/var/lib/velox/data/velox.db`) |
| **YouTube** | `VELOX_YOUTUBE_API_KEY`, `VELOX_YOUTUBE_TOKENS_DIR` |
| **Drive** | `VELOX_DRIVE_CLIENT_ID`, `VELOX_DRIVE_CLIENT_SECRET` |
| **NVIDIA** | `VELOX_NVIDIA_API_KEY`, `VELOX_NVIDIA_TEXT_URL` |
| **Worker** | `VELOX_ALLOWED_WORKERS`, `VELOX_WORKER_HEARTBEAT_TIMEOUT` |
| **TLS** | `VELOX_TLS_CERT_FILE`, `VELOX_TLS_KEY_FILE` |
| **S3** | `VELOX_S3_ENDPOINT`, `VELOX_S3_BUCKET`, `VELOX_S3_ACCESS_KEY_ID` |

> Tabella completa: vedi la sezione "Variabili d'Ambiente" nella versione precedente o esegui `go run ./cmd/server --help`.

---

## Quick Start

### Avvio Master Server

```bash
cd DataServer
export VELOX_ADMIN_TOKEN=velox-dev-token
# VELOX_SPA_DIR è OPZIONALE: senza, il master gira in modalità API-only
# Se usi il bundle SPA, punta a una directory che contiene index.html
export VELOX_SPA_DIR=/srv/velox/frontend-velox/build
go run ./cmd/server
# → http://0.0.0.0:8000
```

> **Frontend ora vive (in prospettiva) in un repository separato**.
> La directory `frontend_standalone/` contiene le sorgenti (`web/`,
> `dark_editor/`) ma `web/dist/` non è più committato: il master lo consuma
> via `VELOX_SPA_DIR`. Quando `VELOX_SPA_DIR` non è impostato o punta a una
> directory senza `index.html`, il server parte normalmente e serve solo le
> API; richieste UI ricevono una *landing page* con le istruzioni di
> installazione. Vedi `frontend_standalone/README.md` per la roadmap di
> split.

### Invio Job di Test

```bash
curl -X POST http://localhost:8000/api/script/generate-with-images \
  -H "Authorization: Bearer velox-dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "video_name": "Test Video",
    "voiceover_path": "https://drive.google.com/file/d/.../view",
    "scenes": [
      {"text": "Scena 1", "image_link": "https://drive.google.com/file/d/.../view"},
      {"text": "Scena 2", "image_link": "https://drive.google.com/file/d/.../view"}
    ]
  }'
```

### Verifica Stato

```bash
curl http://localhost:8000/api/v1/jobs/summary \
  -H "Authorization: Bearer velox-dev-token"
```

### Installa Worker Remoto

```bash
cd RemoteCodex/native/worker-agent-go
make build
./bin/velox-worker-agent -master http://<master-ip>:8000 -token <token>
```

---

## Note

- **Go-only mode** è permanente. Python backend rimosso.
- **TLS** via `VELOX_TLS_CERT_FILE` / `VELOX_TLS_KEY_FILE`.
- **S3/MinIO/R2** per storage oggetti.
- **Linting**: `golangci-lint` config in `.golangci.yml` (richiede Go 1.25+).
- **Architecture**: `docs/archive/architecture-pre-grpc.md` — legacy pre-gRPC reference.
