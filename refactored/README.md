# Velox v1.0.6 - Sistema di Generazione Video Distribuito

![CI](https://github.com/Marcuss-ops/VeloxEditiingg/actions/workflows/ci.yml/badge.svg)

## Panoramica

Velox è un sistema distribuito per la generazione e composizione video. È composto da un **master server** (DataServer) che gestisce la coda job e i worker remoti, e da **RemoteCodex** che contiene il software installato sui worker remoti per l'esecuzione effettiva dei job di rendering.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         MASTER SERVER (DataServer)                      │
│  http://0.0.0.0:8000                                                    │
│                                                                         │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────────────────┐ │
│  │ API REST     │  │ Job Queue    │  │ Worker Registry                │ │
│  │ (Gin)        │──│ (SQLite/PG)  │──│ (In-memory + SQLite)          │ │
│  └─────────────┘  └──────────────┘  └────────────────────────────────┘ │
│         │                │                      │                       │
│         │          ┌─────▼──────────┐     ┌─────▼───────────────────┐  │
│         │          │ Orchestrator   │     │ Command Manager          │  │
│         │          │ (DLQ, events)  │     │ (update/restart/drain)   │  │
│         │          └────────────────┘     └─────────────────────────┘  │
│         │                                                              │
│  ┌──────▼────────────────────────────────────────────────────────────┐  │
│  │                        SUBSYSTEMS                                 │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌────────┐ │  │
│  │  │ Dark     │ │ Calendar │ │ Analytics│ │ YouTube  │ │ Drive  │ │  │
│  │  │ Editor   │ │ Events   │ │ Dashboard│ │ Manager  │ │ Upload │ │  │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘ └────────┘ │  │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐           │  │
│  │  │ Pipeline │ │ Auth     │ │ Ansible  │ │ Livestream│           │  │
│  │  │ Script   │ │ (PG)     │ │ Deploy   │ │ YT Live   │           │  │
│  │  └──────────┘ └──────────┘ └──────────┘ └──────────┘           │  │
│  └─────────────────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────────────────┘
         │                              │
         │ POST /api/jobs/get           │ POST /worker/command
         │ POST /api/jobs/result        │ POST /worker/command_ack
         │ POST /api/workers/heartbeat  │ GET  /api/worker/bundle
         ▼                              ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     WORKER REMOTO (RemoteCodex)                         │
│  ┌──────────────────┐  ┌──────────────────┐  ┌─────────────────────┐   │
│  │ Worker Agent (Go) │  │ Video Engine C++  │  │ Systemd Service     │   │
│  │ velox-worker-agent│──│ velox_video_engine│  │ velox-worker.service│   │
│  │ job loop, polling │  │ FFmpeg rendering  │  │                     │   │
│  └──────────────────┘  └──────────────────┘  └─────────────────────┘   │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 1. Struttura del Repository

```
.
├── DataServer/                        # MASTER SERVER (Go)
│   ├── cmd/server/                    # Entrypoint del server
│   │   ├── main.go                    # main() - carica config e avvia server
│   │   ├── bootstrap.go               # Build delle dipendenze (DI)
│   │   └── router.go                  # Tutte le route API
│   ├── internal/
│   │   ├── handlers/
│   │   │   ├── server/                # API REST pubbliche
│   │   │   │   ├── api/               # Route /api/v1/* (api_v1.go, api_v1_native.go)
│   │   │   │   ├── analytics/         # Dashboard BI, analytics (10 file)
│   │   │   │   ├── auth/              # User auth (register, login, sessions)
│   │   │   │   ├── calendar/          # Calendario produzione video (6 file)
│   │   │   │   ├── collaboration/     # Project collaboration (enterprise)
│   │   │   │   ├── darkeditor/        # Dark Editor - editor immagini AI (16 file)
│   │   │   │   ├── diagnostics/       # Data layout diagnostics
│   │   │   │   ├── drive/             # Google Drive handlers
│   │   │   │   ├── groups/            # YouTube group management
│   │   │   │   ├── jobs/              # Job CRUD, submission, normalization
│   │   │   │   ├── master/            # Create-master (multi-title video)
│   │   │   │   ├── pipeline/          # Pipeline generazione script
│   │   │   │   ├── script/            # Script con immagini
│   │   │   │   ├── video/             # Scene video, clip+stock, smoke test
│   │   │   │   └── youtube/           # YouTube upload + management (22 file)
│   │   │   ├── remote/                # API per worker remoti
│   │   │   │   ├── workers/           # Registrazione, heartbeat, bundle
│   │   │   │   ├── ansible/           # Playbook Ansible per deploy
│   │   │   │   ├── install/           # Script installazione worker
│   │   │   │   ├── livestream/        # YouTube Live stream management
│   │   │   │   └── submission/        # Multi-clip submission management
│   │   │   └── web/                   # Web layer
│   │   │       ├── darkeditor/        # Dark Editor web proxy
│   │   │       ├── dashboard/         # Worker dashboard
│   │   │       ├── explorer/          # File explorer
│   │   │       ├── proxy/             # NoRoute handler, compat, landing page
│   │   │       └── spa/               # SPA serving (history fallback)
│   │   ├── config/                    # Config da env vars
│   │   ├── integrations/              # Servizi esterni
│   │   │   ├── drive/                 # Google Drive API
│   │   │   ├── news/                  # News fetching (Google News, NewsAPI, GNews)
│   │   │   └── youtube/               # YouTube Data API v3, OAuth, QuotaManager
│   │   ├── logging/                   # Structured logging, throttling
│   │   ├── queue/                     # Code job
│   │   │   ├── file_queue.go          # FileQueue (SQLite-backed)
│   │   │   ├── orchestrator.go        # Job orchestration
│   │   │   ├── dlq.go                 # Dead letter queue
│   │   │   ├── events.go              # Queue events
│   │   │   └── pipeline_templates.go  # Pipeline templates
│   │   ├── remoteengine/              # Remote script engine client
│   │   ├── services/
│   │   │   ├── analytics/             # Analytics service layer
│   │   │   └── jobs/                  # Job service layer
│   │   ├── state/                     # Application state
│   │   ├── store/                     # Persistenza (33+ file)
│   │   │   ├── sqlite.go              # SQLite store
│   │   │   ├── sqlite_*.go            # Domain-specific SQLite repos
│   │   │   ├── migrations/            # DB migrations
│   │   │   └── interface.go           # Repository interfaces
│   │   ├── workers/                   # Registry, CommandManager, TokenManager
│   │   └── audit/                     # Data layer audit
│   ├── bin/                           # Binary compilati
│   │   ├── velox-server               # Server master
│   │   └── velox-bundler              # Generatore bundle worker
│   └── data/                          # Dati runtime
│       ├── worker_downloads/          # Bundle per worker (worker_code.zip)
│       ├── generated_videos/          # Video generati
│       └── ansible/                   # Playbook Ansible
│
├── RemoteCodex/                       # SOFTWARE WORKER REMOTO
│   └── native/
│       ├── worker-agent-go/           # Worker agent in Go
│       │   ├── cmd/                   # Entrypoints (velox-worker-agent, installer)
│       │   ├── internal/
│       │   │   ├── telemetry/         # Prometheus metrics, GC stats
│       │   │   └── worker/            # Core worker logic (15 file)
│       │   ├── pkg/
│       │   │   ├── api/               # Client HTTP per master (+ renderplan/)
│       │   │   ├── config/            # Worker configuration management
│       │   │   ├── logger/            # Logging
│       │   │   ├── nlp/               # Natural language processing
│       │   │   └── video/             # Pipeline video (workflow, native_engine, fuzzy_match)
│       │   ├── deploy/                # Systemd unit, install/rollback scripts
│       │   ├── Makefile
│       │   └── Dockerfile
│       └── video-engine-cpp/          # Motore video C++ (FFmpeg)
│           ├── include/               # Headers (video_contract.hpp, etc.)
│           ├── src/                   # Source (main.cpp, video_builder.cpp)
│           ├── schemas/               # JSON schema examples
│           └── CMakeLists.txt
│
├── shared/                            # Libreria condivisa Go
│   ├── contract/                      # Go<->C++ contract types
│   ├── media/                         # Multimedia metadata (ffprobe)
│   ├── paths/                         # Filesystem path utilities
│   └── payload/                       # JSON map extraction utilities
│
├── frontend_standalone/               # SPA frontend
│   └── web/dist/                      # Build statica frontend
│
├── docs/                              # Documentazione API
├── scripts/                           # Scripts di deploy
└── VERSION.txt                        # v1.0.6
```

---

## 2. Subsistemi Principali

### 2.1 Job Queue & Orchestrator

Il sistema di coda supporta **SQLite** (default) e **PostgreSQL** (enterprise):

- **FileQueue** - Coda principale con persistenza SQLite
- **StreamsQueue** - Coda per submissions multi-clip
- **DLQ** - Dead letter queue per job falliti permanentemente
- **Orchestrator** - orchestrazione job con pipeline templates
- **Events** - Sistema di eventi per la coda
- **Zombie Detection** - Rilevamento e requeue automatico di job zombie

Stati job: `PENDING` → `PROCESSING` → `COMPLETED` / `ERROR` / `FAILED`

### 2.2 Dark Editor

Editor di immagini web-based con funzionalità AI:

- Upload, filtro, trasformazione, upscaling immagini
- Generazione immagini AI (integrazione NVIDIA FLUX)
- YouTube thumbnail grabber
- Gestione progetti e cartelle (CRUD)
- Export immagini
- Background task processing
- Logging client/server

### 2.3 Calendario Produzione

Sistema di calendario per la pianificazione video:

- CRUD eventi con range date
- Upsert con merge
- Enqueue eventi alla coda job
- ETag caching con `fields=minimal` per query veloci

### 2.4 Analytics Dashboard

Business Intelligence integrato:

- Summary, finance, performance, realtime
- Analytics per canali, gruppi, timeline
- Confronto periodi, export dati
- Health check sistema

### 2.5 YouTube Manager

Gestione completa YouTube:

- **Upload**: Upload video con supporto batch
- **Channels**: Gestione canali con OAuth2
- **Groups**: Organizzazione canali in gruppi
- **AI Generation**: Titoli, descrizioni, tag, traduzioni, cover
- **Competitor Tracking**: Feed, trending, scoperta canali simili
- **Livestream**: Gestione YouTube Live

### 2.6 Google Drive

Integrazione Google Drive:

- Upload video completati
- Gestione cartelle per gruppi
- OAuth2 authentication
- Link sharing

### 2.7 Ansible Deploy

Deploy remoto worker via Ansible:

- Installazione worker
- Update codice
- Rollback
- Test SSH
- Gestione computer

---

## 3. Variabili d'Ambiente

### Core Server

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `VELOX_MASTER_PORT` | `8000` | Porta server |
| `VELOX_STUDIO_PORT` | `5000` | Porta studio |
| `GIN_MODE` | `debug` | Modalità Gin |
| `VELOX_VIDEOS_DIR` | `""` | Directory video completati |

### Data & Storage

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `VELOX_DATA_DIR` | `.velox/data` | Directory dati principale |
| `VELOX_RUNTIME_DIR` | `.velox` | Runtime root |
| `VELOX_DB_DRIVER` | `sqlite3` | Driver DB (`sqlite3`) |
| `VELOX_DB_DSN` | `DataDir/velox.db` | Database connection string |
| `VELOX_DB_MAX_OPEN_CONNS` | `50` | Max connessioni DB aperte |
| `VELOX_DB_MAX_IDLE_CONNS` | `10` | Max connessioni DB idle |
| `VELOX_DB_CONN_MAX_LIFETIME` | `1800` | Max lifetime connessioni (sec) |
| `VELOX_DB_CONN_MAX_IDLE_TIME` | `300` | Max idle time connessioni (sec) |

### S3/MinIO/R2

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `VELOX_S3_ENDPOINT` | `""` | Endpoint S3 |
| `VELOX_S3_REGION` | `us-east-1` | Regione S3 |
| `VELOX_S3_BUCKET` | `""` | Bucket S3 |
| `VELOX_S3_ACCESS_KEY_ID` | `""` | Access key S3 |
| `VELOX_S3_SECRET_ACCESS_KEY` | `""` | Secret key S3 |
| `VELOX_S3_USE_SSL` | `false` | Usa SSL per S3 |

### Worker Management

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `VELOX_ALLOWED_WORKERS` | `""` | Allowlist worker (`*` o `ALL` per tutti) |
| `VELOX_FORCE_SINGLE_WORKER` | `""` | Forza singolo worker per ID/IP |
| `VELOX_MAX_JOB_ATTEMPTS` | `3` | Max tentativi job prima di dead queue |
| `VELOX_WORKER_BUNDLE_DIR` | `""` | Directory bundle worker |
| `VELOX_WORKER_HEARTBEAT_TIMEOUT` | `900` | Timeout heartbeat worker (sec) |
| `VELOX_CODE_VERSION` | `""` | Versione codice (git hash) |

### Server URLs & Proxying

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `MASTER_PUBLIC_URL` | `""` | URL pubblico per worker remoti |
| `VELOX_GRADIO_APP_URL` | `http://127.0.0.1:7860` | URL Gradio standalone UI |

### SPA & Frontend

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `VELOX_SPA_DIR` | `""` | Directory SPA root |
| `VELOX_DARK_EDITOR_DIR` | `""` | Directory Dark Editor |
| `VELOX_DARK_EDITOR_PROXY_URL` | `""` | Proxy URL Dark Editor (Next.js) |

### Admin & Security

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `VELOX_ADMIN_TOKEN` | `""` | Admin bearer token |
| `VELOX_ALLOW_LOCALHOST_MASTER` | `false` | Permetti localhost master URL (dev) |
| `VELOX_TLS_CERT_FILE` | `""` | Path certificato TLS (PEM) |
| `VELOX_TLS_KEY_FILE` | `""` | Path chiave TLS (PEM) |

### Google Drive Integration

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `VELOX_DRIVE_CLIENT_ID` | `""` | Client ID OAuth Drive |
| `VELOX_DRIVE_CLIENT_SECRET` | `""` | Client secret OAuth Drive |
| `VELOX_DRIVE_REDIRECT_URI` | `""` | Redirect URI OAuth Drive |
| `VELOX_DRIVE_TOKENS_DIR` | `""` | Directory token OAuth Drive |

### NVIDIA AI

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `VELOX_NVIDIA_API_KEY` | `""` | API key NVIDIA per FLUX |
| `VELOX_NVIDIA_TEXT_URL` | `""` | Endpoint chat NVIDIA/OpenAI |

### YouTube Integration

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `VELOX_YOUTUBE_API_KEY` | `""` | API key YouTube Data API v3 |
| `VELOX_YOUTUBE_TOKENS_DIR` | `""` | Directory token OAuth YouTube |
| `VELOX_YOUTUBE_POSTING_PATH` | `""` | Root progetto YoutubePosting |
| `VELOX_REMOTE_FALLBACK_URL` | `""` | URL scraper fallback remoto |

### Remote Script Engine

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `VELOX_REMOTE_ENGINE_URL` | `""` | URL remote script engine |
| `VELOX_REMOTE_ENGINE_TOKEN` | `""` | Token auth remote engine |
| `VELOX_REMOTE_ENGINE_TIMEOUT_MS` | `60000` | Timeout remote engine (ms) |
| `VELOX_REMOTE_ENGINE_RETRIES` | `3` | Retry count remote engine |

---

## 4. Endpoint API

### 4.1 Video Creation

| Metodo | Endpoint | Auth | Descrizione |
|--------|----------|------|-------------|
| POST | `/api/script/generate-with-images` | Admin | Video da scene + immagini |
| POST | `/api/v1/script/generate-with-images` | Admin | (alias) |
| POST | `/api/v1/video/create-master` | Admin | Multi-title video |
| POST | `/api/v1/video/create-scenes` | Admin | Scene-based video |
| POST | `/api/v1/video/smoke-clip-stock` | Admin | Smoke test clip+stock |
| POST | `/api/v1/video/upload-completed` | Worker | Notifica upload completato |

### 4.2 Jobs Management

| Metodo | Endpoint | Auth | Descrizione |
|--------|----------|------|-------------|
| GET | `/api/v1/jobs` | Admin | Lista tutti i job |
| POST | `/api/v1/jobs` | Admin | Crea job |
| GET | `/api/v1/jobs/summary` | Admin | Statistiche job |
| GET | `/api/v1/jobs/dashboard` | Admin | Dashboard job |
| GET | `/api/v1/jobs/:id` | Admin | Dettaglio job |
| DELETE | `/api/v1/jobs/:id` | Admin | Elimina job |
| POST | `/api/v1/jobs/:id/retry` | Admin | Riprova job fallito |
| POST | `/api/v1/jobs/bulk_delete` | Admin | Elimina job in massa |

### 4.3 Workers

| Metodo | Endpoint | Auth | Descrizione |
|--------|----------|------|-------------|
| GET | `/api/v1/workers` | Admin | Lista worker |
| GET | `/api/v1/workers/:id/logs` | Admin | Log worker |
| POST | `/api/v1/workers/update_all` | Admin | Update + restart tutti |
| POST | `/api/v1/workers/restart_all` | Admin | Restart tutti worker |
| POST | `/api/v1/workers/send_command_bulk` | Admin | Comando bulk |
| POST | `/api/v1/workers/full_update_linux` | Admin | Update completo Linux |
| POST | `/api/v1/workers/rollout_update` | Admin | Update progressivo (canary) |
| POST | `/api/v1/worker/send_command` | Admin | Comando singolo worker |

### 4.4 Worker Lifecycle (Worker-Token Auth)

| Metodo | Endpoint | Auth | Descrizione |
|--------|----------|------|-------------|
| POST | `/api/workers/register` | Worker | Registrazione worker |
| POST | `/api/workers/unregister` | Worker | Deregistrazione |
| POST | `/api/workers/heartbeat` | Worker | Heartbeat periodico |
| POST | `/api/workers/status` | Worker | Aggiornamento stato |
| GET/POST | `/api/workers/commands` | Worker | Ottiene comandi pending |
| POST | `/api/workers/commands/ack` | Worker | ACK comando |
| GET/POST | `/worker/command` | Worker | Poller endpoint |
| POST | `/worker/command_ack` | Worker | ACK comando |

### 4.5 Worker Admin (Admin Token)

| Metodo | Endpoint | Auth | Descrizione |
|--------|----------|------|-------------|
| POST | `/worker/revoke` | Admin | Revoca worker |
| POST | `/worker/unrevoke` | Admin | Toglie revoca |
| GET | `/worker/revoked` | Admin | Lista worker revocati |
| POST | `/worker/drain` | Admin | Drena worker |
| POST | `/worker/restart` | Admin | Restart worker |
| POST | `/worker/request_update` | Admin | Richiede update worker |

### 4.6 Bundle

| Metodo | Endpoint | Auth | Descrizione |
|--------|----------|------|-------------|
| GET | `/bundle_manifest.json` | Public | Manifest bundle |
| GET | `/api/worker/bundle` | Public | Download bundle |
| GET | `/api/worker/v2/manifest` | Public | Manifest V2 |
| GET | `/api/worker/v2/chunk/:name` | Public | Download chunk |
| GET | `/api/v1/bundle/files` | Admin | Lista file bundle |
| GET | `/api/v1/bundle/info` | Admin | Info bundle |

### 4.7 Dashboard & Analytics

| Metodo | Endpoint | Auth | Descrizione |
|--------|----------|------|-------------|
| GET | `/api/v1/dashboard/summary` | Admin | Riepilogo |
| GET | `/api/v1/dashboard/finance` | Admin | Finanziario |
| GET | `/api/v1/dashboard/performance` | Admin | Performance |
| GET | `/api/v1/dashboard/realtime` | Admin | Realtime |
| GET | `/api/v1/dashboard/channels` | Admin | Canali |
| GET | `/api/v1/dashboard/groups` | Admin | Gruppi |
| GET | `/api/v1/dashboard/timeline` | Admin | Timeline |
| GET | `/api/v1/dashboard/comparison` | Admin | Confronto |
| GET | `/api/v1/dashboard/export` | Admin | Export |
| GET | `/api/v1/dashboard/health` | Admin | Health |
| GET | `/api/v1/analytics/summary` | Admin | Riepilogo analytics |
| GET | `/api/v1/analytics/timeline` | Admin | Timeline analytics |
| GET | `/api/v1/analytics/top-videos` | Admin | Top video |
| GET | `/api/v1/analytics/top-channels` | Admin | Top canali |
| GET | `/api/v1/analytics/top-groups` | Admin | Top gruppi |
| GET | `/api/v1/analytics/realtime` | Admin | Analytics realtime |

### 4.8 YouTube

| Metodo | Endpoint | Auth | Descrizione |
|--------|----------|------|-------------|
| GET | `/api/v1/youtube/tokens/list` | Admin | Lista token |
| GET | `/api/v1/youtube/channels` | Admin | Lista canali |
| POST | `/api/v1/youtube/upload` | Admin | Upload video |
| POST | `/api/v1/youtube/batch-upload` | Admin | Batch upload |
| POST | `/api/v1/youtube/ai/titles` | Admin | AI titoli |
| POST | `/api/v1/youtube/ai/description` | Admin | AI descrizione |
| POST | `/api/v1/youtube/ai/tags` | Admin | AI tag |
| POST | `/api/v1/youtube/ai/translate` | Admin | AI traduzione |
| POST | `/api/v1/youtube/ai/covers` | Admin | AI cover |
| GET | `/api/youtube/manager/feed` | Public | Feed competitor |
| GET | `/api/youtube/manager/trends` | Public | Trends |
| GET | `/api/youtube/manager/discovery` | Public | Scoperta canali |

### 4.9 Calendar

| Metodo | Endpoint | Auth | Descrizione |
|--------|----------|------|-------------|
| GET | `/api/v1/calendar/events` | Public | Lista eventi |
| GET | `/api/v1/calendar/events/range` | Public | Eventi per range |
| POST | `/api/v1/calendar/events` | Public | Crea evento |
| POST | `/api/v1/calendar/events/upsert` | Public | Upsert evento |
| POST | `/api/v1/calendar/events/:id/enqueue` | Public | Accoda evento |
| PUT | `/api/v1/calendar/events/:id` | Public | Aggiorna evento |
| DELETE | `/api/v1/calendar/events/:id` | Public | Elimina evento |

### 4.10 Dark Editor

| Metodo | Endpoint | Auth | Descrizione |
|--------|----------|------|-------------|
| POST | `/dark_editor_v2/upload` | Public | Upload immagine |
| POST | `/dark_editor_v2/process/filter` | Public | Applica filtro |
| POST | `/dark_editor_v2/process/transform` | Public | Trasforma |
| POST | `/dark_editor_v2/export` | Public | Export |
| POST | `/dark_editor_v2/generate` | Public | Genera con AI |
| POST | `/dark_editor_v2/api/upscale` | Public | Upscale |
| GET | `/dark_editor_v2/api/projects` | Public | Lista progetti |
| POST | `/dark_editor_v2/api/projects` | Public | Crea progetto |

### 4.11 Livestream

| Metodo | Endpoint | Auth | Descrizione |
|--------|----------|------|-------------|
| GET | `/api/v1/livestream` | Public | Lista livestream |
| POST | `/api/v1/livestream` | Public | Crea livestream |
| GET | `/api/v1/livestream/status` | Public | Stato livestream |
| POST | `/api/v1/livestream/:id/testing` | Public | Modalità testing |
| POST | `/api/v1/livestream/:id/live` | Public | Vai live |
| POST | `/api/v1/livestream/:id/complete` | Public | Completa livestream |

### 4.12 Ansible

| Metodo | Endpoint | Auth | Descrizione |
|--------|----------|------|-------------|
| GET | `/api/v1/ansible/capabilities` | Admin | Playbook disponibili |
| GET | `/api/v1/ansible/runs` | Admin | Storico esecuzioni |
| POST | `/api/v1/ansible/computers/run_action` | Admin | Esegui azione |
| POST | `/api/v1/ansible/computers/run_shell` | Admin | Comando shell remoto |
| POST | `/api/v1/ansible/computers/test_ssh` | Admin | Test SSH |

### 4.13 Auth (Enterprise)

| Metodo | Endpoint | Auth | Descrizione |
|--------|----------|------|-------------|
| POST | `/api/auth/register` | Public | Registrazione |
| POST | `/api/auth/login` | Public | Login |
| POST | `/api/auth/logout` | Cookie | Logout |
| GET | `/api/auth/me` | Session | Utente corrente |

---

## 5. Quick Start

### Avvio Master Server

```bash
cd DataServer
export VELOX_ADMIN_TOKEN=velox-dev-token
export VELOX_SPA_DIR=../frontend_standalone/web/dist
export MASTER_PUBLIC_URL=http://51.91.11.36:8000
go run ./cmd/server
# Server su http://0.0.0.0:8000
```

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
# Sul worker remoto:
git clone https://github.com/Marcuss-ops/Imageeef.git
cd Imageeef/RemoteCodex/native/worker-agent-go
make build
./bin/velox-worker-agent -master http://<master-ip>:8000 -token <token>
```

---

## 6. Notes

- **Go-only mode** è permanente. Le variabili `GoOnlyMode` e `GoOnlyWhitelist` sono state rimosse.
- **Python backend** non esiste più. La variabile `PythonBackendURL` è stata rimossa.
- **TLS** è supportato via `VELOX_TLS_CERT_FILE` e `VELOX_TLS_KEY_FILE`.
- **S3/MinIO/R2** è supportato per storage oggetti.
