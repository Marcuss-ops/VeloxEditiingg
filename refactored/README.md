# Velox - Sistema di Generazione Video Distribuito

## Panoramica

Velox è un sistema distribuito per la generazione e composizione video. È composto da un **master server** (DataServer) che gestisce la coda job e i worker remoti, e da **RemoteCodex** che contiene il software installato sui worker remoti per l'esecuzione effettiva dei job di rendering.

```
┌─────────────────────────────────────────────────────────────────┐
│                       MASTER SERVER (DataServer)                │
│  localhost:8000                                                  │
│  ┌─────────────┐  ┌──────────────┐  ┌────────────────────────┐ │
│  │ API REST     │  │ File Queue   │  │ Worker Registry         │ │
│  │ (Gin)        │──│ (SQLite)     │──│ (In-memory + SQLite)    │ │
│  └─────────────┘  └──────────────┘  └────────────────────────┘ │
│                          │                      │               │
│                     ┌────▼──────────┐     ┌─────▼───────────┐  │
│                     │ Job Dispacher │     │ Command Manager   │  │
│                     │               │     │ (update/restart)  │  │
│                     └───────────────┘     └─────────────────┘  │
└─────────────────────────────────────────────────────────────────┘
         │                              │
         │ POST /api/jobs/get           │ GET /worker/command
         │ POST /api/jobs/result        │ POST /worker/command_ack
         │ POST /api/workers/heartbeat  │ GET /api/worker/bundle
         ▼                              ▼
┌─────────────────────────────────────────────────────────────────┐
│                    WORKER REMOTO (RemoteCodex)                   │
│  ┌──────────────────┐  ┌──────────────────┐  ┌───────────────┐  │
│  │ Worker Agent (Go) │  │ Video Engine C++  │  │ Systemd       │  │
│  │ velox-worker-agent│──│ velox_video_engine│  │ velox-worker  │  │
│  │ job loop, polling │  │ FFmpeg rendering  │  │ .service      │  │
│  └──────────────────┘  └──────────────────┘  └───────────────┘  │
└─────────────────────────────────────────────────────────────────┘
```

---

## 1. Struttura del Repository

```
refactored/
├── DataServer/                    # MASTER SERVER (Go)
│   ├── cmd/server/                # Entrypoint del server
│   │   ├── main.go                # main() - carica config e avvia server
│   │   ├── bootstrap.go           # Build delle dipendenze (DI)
│   │   ├── router.go              # Tutte le route API
│   ├── internal/
│   │   ├── handlers/
│   │   │   ├── server/            # API REST pubbliche
│   │   │   │   ├── api/api_v1.go  # Route /api/v1/*
│   │   │   │   ├── script/        # Script con immagini
│   │   │   │   ├── video/         # Scene video, clip+stock
│   │   │   │   ├── jobs/          # Gestione job CRUD
│   │   │   │   ├── master/        # Create-master (multi-title)
│   │   │   │   ├── pipeline/      # Pipeline generazione
│   │   │   │   └── calendar/      # Calendario produzione
│   │   │   └── remote/            # API per worker remoti
│   │   │       ├── workers/       # Registrazione, heartbeat, bundle
│   │   │       ├── ansible/       # Playbook Ansible per deploy
│   │   │       └── install/       # Script installazione worker
│   │   ├── queue/                 # Code job (FileQueue, Redis)
│   │   ├── store/                 # Persistenza (SQLite, Postgres)
│   │   ├── services/              # Servizi (jobs, analytics)
│   │   ├── workers/               # Registry, CommandManager
│   │   ├── config/                # Config da env vars
│   │   ├── integrations/          # YouTube, Drive, News
│   │   └── audit/                 # Data layout audit
│   ├── bin/                       # Binary compilati
│   │   ├── velox-server           # Server master
│   │   └── velox-bundler          # Generatore bundle worker
│   └── data/                      # Dati runtime
│       ├── worker_downloads/      # Bundle per worker (worker_code.zip)
│       ├── generated_videos/      # Video generati
│       └── ansible/               # Playbook Ansible
│
├── RemoteCodex/                   # SOFTWARE WORKER REMOTO
│   └── native/
│       ├── worker-agent-go/       # Worker agent in Go
│       └── video-engine-cpp/      # Motore video C++ (FFmpeg)
│
└── frontend_standalone/           # SPA frontend
    └── web/dist/                  # Build statica frontend
```

---

## 2. RemoteCodex - Cosa è e a cosa serve

**RemoteCodex** è il software che viene **installato su ogni worker remoto** (macchine dedicate con GPU) per elaborare i job di generazione video. Contiene due componenti principali:

### 2.1 Worker Agent (Go) — `worker-agent-go/`

Agente Go che gira come **systemd service** (`velox-worker.service`) su ogni worker. Ciclo di vita:

1. **Avvio** → si registra al master (`POST /api/workers/register`)
2. **Polling** → ogni pochi secondi chiede job disponibili (`POST /api/jobs/get`)
3. **Esecuzione** → riceve un job `process_video`, lo esegue:
   - Prepara i parametri (scene, immagini, voiceover)
   - Crea directory temporanea
   - Lancia il **motore C++ nativo** (FFmpeg) per il rendering
   - Upload del video completato
4. **Report** → invia risultato al master (`POST /api/jobs/result`)
5. **Heartbeat** → invia heartbeat periodico (`POST /api/workers/heartbeat`)
6. **Comandi** → controlla comandi in coda (`GET /worker/command`):
   - `update_code` → aggiorna il bundle worker
   - `restart_worker` → restart del servizio
   - `run_smoke_job` → esegui job di test

**Struttura:**
```
worker-agent-go/
├── cmd/
│   ├── velox-worker-agent/    # Entrypoint worker agent
│   └── installer/             # Installer per deploy iniziale
├── internal/worker/           # Core: job loop, executor, stage executor
│   ├── worker.go              # Lifecycle start/stop
│   ├── worker_jobs.go         # Polling e dispatch job
│   ├── job_executor.go        # Esecuzione job (process_video, render, audio)
│   ├── job_upload.go          # Upload video su Drive/S3
│   ├── worker_comms.go        # Heartbeat, register/unregister
│   ├── concurrency.go         # Limitatore concorrenza (max N job paralleli)
│   └── stage_executor.go      # Esecuzione per stage
├── pkg/video/                 # Pipeline video
│   ├── workflow.go            # Orchestrazione generazione video
│   ├── native_engine.go       # Bridge al motore C++ (FFmpeg)
│   └── native_engine.go       # Parsing scene, clip, input
├── pkg/api/                   # Client HTTP per master
│   ├── client.go              # Client con retry e circuit breaker
│   └── api_types.go           # Tipi: Job, JobResult, HeartbeatPayload
├── deploy/                    # Deploy e runtime
│   ├── velox-worker.service   # Systemd unit
│   ├── install-worker.sh      # Script installazione
│   └── rollback-worker.sh     # Script rollback
├── Makefile                   # Build
└── Dockerfile                 # Immagine Docker
```

**Job types supportati:**
| Job Type | Descrizione |
|----------|-------------|
| `process_video` | Composizione video con scene, immagini, voiceover |
| `render` | Pipeline rendering generale |
| `process_audio` | Elaborazione audio standalone |
| `health_check` | Test heartbeat worker |

### 2.2 Video Engine C++ — `video-engine-cpp/`

Motore nativo C++ per composizione video via FFmpeg. Riceve un **payload JSON** con:
- Scene testuali con immagini
- Clip video (intro, stock, end)
- Voiceover paths
- Parametri di output

**Flusso:**
1. Legge il file JSON richiesta (`--request /path/to/payload.json`)
2. Scarica gli asset (immagini, video, audio) da URL remoti (Google Drive, HTTP)
3. Compone segmenti video con FFmpeg (transizioni, zoom, overlay testo)
4. Concatena i segmenti e muxa l'audio finale
5. Produce il file MP4 output

**Build:**
```bash
cd RemoteCodex/native/video-engine-cpp
mkdir -p build && cd build
cmake .. && cmake --build . -j$(nproc)
# Output: ./build/velox_video_engine
```

---

## 3. Endpoint API per la Generazione Video

Tutti gli endpoint protetti richiedono `Authorization: Bearer <admin_token>` (o `X-Admin-Token`).

### 3.1 Script con Immagini (Scene + Voiceover)

Accoda un job video usando scene, immagini e voiceover gia prodotti upstream.

**`POST /api/script/generate-with-images`**
**`POST /api/v1/script/generate-with-images`**

```bash
curl -X POST http://77.93.152.122:8081/api/script/generate-with-images \
  -H "Authorization: Bearer velox-dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "video_name": "Titolo Video",
    "source_text": "Testo dello script...",
    "language": "it",
    "voiceover_path": "https://drive.google.com/file/d/.../view",
    "scenes": [
      {
        "text": "Testo scena 1",
        "image_link": "https://drive.google.com/file/d/.../view",
        "image_links": ["https://..."]
      },
      {
        "text": "Testo scena 2",
        "image_link": "https://drive.google.com/file/d/.../view"
      }
    ]
  }'
```

**Payload:**
| Campo | Tipo | Obbligatorio | Default | Descrizione |
|-------|------|:---:|:-------:|------------|
| `video_name` | string | ✅ | — | Titolo del video |
| `scenes[]` | array | ✅ | — | Scene con `text` + `image_link` |
| `voiceover_path` | string | ✅ | — | URL audio gia pronto (Google Drive, MP3, WAV, M4A) |
| `source_text` | string | ❌ | — | Testo sorgente o metadato per il job |
| `language` | string | ❌ | `it` | Lingua per SRT |
| `drive_output_folder` | string | ❌ | — | Cartella Drive per upload |
| `scene_duration_secs` | number | ❌ | `5` | Durata per scena |
| `total_duration_secs` | number | ❌ | — | Durata totale (sovrascrive scene_duration) |
| `priority` | number | ❌ | `1` | Priorità coda |
| `timeout_secs` | number | ❌ | `3600` | Timeout job |
| `youtube_group` | string | ❌ | — | Gruppo YouTube per upload automatico |

Nota: questo endpoint non genera immagini o voiceover in locale. Le URL devono essere gia disponibili prima della chiamata.

**Risposta (200 OK):**
```json
{
  "ok": true,
  "job_id": "scriptimg_uuid...",
  "status": "PENDING",
  "dispatch_status": "queued_for_workers",
  "video_mode": "scene_image",
  "video_name": "Titolo Video",
  "scene_count": 2,
  "voiceover_count": 1,
  "output_path": "/data/generated_videos/script_with_images/titolo_video.mp4",
  "scene_image_paths": ["https://..."],
  "enqueue": { ... }
}
```

### 3.2 Video da Scene (API V1)

**`POST /api/v1/video/create-scenes`**

Come sopra ma con validazione più rigida (richiede `video_name`, `script_text`, `scenes[]`, `voiceover_paths`). Supporta **proxy mode**: se `submission_mode=draft` e `MasterServerURL` configurato, il job viene inoltrato a un master remoto.

```bash
curl -X POST http://77.93.152.122:8081/api/v1/video/create-scenes \
  -H "Authorization: Bearer velox-dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "video_name": "Test Video",
    "script_text": "Script completo...",
    "voiceover_paths": ["https://..."],
    "scenes": [...]
  }'
```

### 3.3 Create Master (Multi-title)

**`POST /api/v1/video/create-master`**

Endpoint legacy per creazione video con supporto **multi-title** (array `titles[]`). Accetta clip organizzate in slot (start, middle, end, stock).

```bash
curl -X POST http://77.93.152.122:8081/api/v1/video/create-master \
  -H "Authorization: Bearer velox-dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "video_name": "Video Title",
    "start_clips": ["https://..."],
    "middle_clips": ["https://..."],
    "end_clips": ["https://..."],
    "voiceovers": ["https://..."],
    "titles": ["Title 1", "Title 2"]
  }'
```

### 3.4 Smoke Test Clip+Stock

**`POST /api/v1/video/smoke-clip-stock`**

Job minimale per la pipeline clip+stock. Richiede `video_mode`, `intro_clip_paths`, `stock_clip_paths`, `voiceover_paths`.

```bash
curl -X POST http://77.93.152.122:8081/api/v1/video/smoke-clip-stock \
  -H "Authorization: Bearer velox-dev-token" \
  -H "Content-Type: application/json" \
  -d '{
    "video_name": "Smoke Test",
    "script_text": "Test script...",
    "video_mode": "clip_stock",
    "voiceover_paths": ["https://..."],
    "intro_clip_paths": ["https://..."],
    "stock_clip_paths": ["https://..."],
    "output_path": "/tmp/test.mp4"
  }'
```

### 3.5 Upload Video Completato

**`POST /api/v1/video/upload-completed`**

Usato dal worker per notificare il completamento dell'upload del video generato.

---

## 4. Endpoint per Monitoraggio Job

### 4.1 Stato Job

**`GET /api/script/jobs/:job_id`**
**`GET /api/script/jobs/:job_id/full`** (dettaglio completo con history)

```bash
curl http://77.93.152.122:8081/api/script/jobs/scriptimg_uuid \
  -H "Authorization: Bearer velox-dev-token"
```

Risposta:
```json
{
  "ok": true,
  "job_id": "scriptimg_...",
  "status": "COMPLETED",
  "video_name": "Titolo",
  "created_at": "2026-06-11T13:00:49Z",
  "completed_at": "2026-06-11T13:00:58Z",
  "output_path": "/data/generated_videos/.../video.mp4",
  "scene_count": 4,
  "video_mode": "scene_image"
}
```

### 4.2 Lista Job

**`GET /api/v1/jobs`** — Tutti i job
**`GET /api/v1/jobs/:id`** — Singolo job
**`GET /api/v1/jobs/summary`** — Riepilogo statistiche
**`GET /api/v1/jobs/dashboard`** — Dashboard

### 4.3 Gestione Job

| Metodo | Endpoint | Azione |
|--------|----------|--------|
| `DELETE` | `/api/v1/jobs/:id` | Elimina job |
| `POST` | `/api/v1/jobs/:id/retry` | Riprova job fallito |
| `POST` | `/api/v1/jobs/bulk_delete` | Elimina job in massa |

---

## 5. Endpoint Worker Remoto

### 5.1 Worker Lifecycle (usati dal worker agent)

| Metodo | Endpoint | Descrizione |
|--------|----------|-------------|
| `POST` | `/api/workers/register` | Registrazione worker |
| `POST` | `/api/workers/unregister` | Deregistrazione |
| `POST` | `/api/workers/heartbeat` | Heartbeat periodico |
| `POST` | `/api/workers/status` | Aggiornamento stato |
| `GET/POST` | `/api/workers/commands` | Ottiene comandi pending |
| `POST` | `/api/workers/commands/ack` | Acknowledgement comando |
| `GET/POST` | `/worker/command` | Ottiene comandi (worker polling) |
| `POST` | `/worker/command_ack` | ACK comando |

### 5.2 Job Queue (usati dal worker agent)

| Metodo | Endpoint | Descrizione |
|--------|----------|-------------|
| `GET` | `/api/v1/queue/job` | Preleva prossimo job pending |
| `POST` | `/api/v1/queue/start` | Segnala inizio job |
| `POST` | `/api/v1/queue/complete` | Segnala completamento |
| `POST` | `/api/v1/queue/fail` | Segnala fallimento |

### 5.3 Bundle Worker

| Metodo | Endpoint | Descrizione |
|--------|----------|-------------|
| `GET` | `/api/worker/bundle` | Download bundle worker_code.zip |
| `GET` | `/api/worker/v2/manifest` | Manifest V2 bundle |
| `GET` | `/api/worker/v2/chunk/:name` | Download chunk bundle |
| `GET` | `/bundle_manifest.json` | Manifest bundle |
| `GET` | `/api/v1/bundle/files` | Lista file nel bundle |
| `GET` | `/api/v1/bundle/info` | Info bundle |
| `GET` | `/api/v1/bundle/manifest` | Manifest bundle |

### 5.4 Gestione Worker (admin)

| Metodo | Endpoint | Descrizione |
|--------|----------|-------------|
| `GET` | `/api/v1/workers` | Lista worker |
| `GET` | `/api/v1/workers/:id/logs` | Log worker |
| `POST` | `/api/v1/workers/update_all` | Update + restart tutti worker |
| `POST` | `/api/v1/workers/full_update_linux` | Update completo Linux |
| `POST` | `/api/v1/workers/rollout_update` | Update progressivo (canary) |
| `POST` | `/api/v1/workers/restart_all` | Restart tutti worker |
| `POST` | `/api/v1/workers/send_command_bulk` | Comando bulk a worker |
| `POST` | `/api/v1/worker/send_command` | Comando singolo worker |
| `GET` | `/api/v1/workers/update_status` | Stato update |
| `POST` | `/worker/revoke` | Revoca worker |
| `POST` | `/worker/unrevoke` | Toglie revoca |
| `POST` | `/worker/drain` | Drena worker (non accetta nuovi job) |
| `POST` | `/worker/request_update` | Richiede update worker |

### 5.5 Ansible (deploy remoto)

| Metodo | Endpoint | Descrizione |
|--------|----------|-------------|
| `GET` | `/api/v1/admin/ansible/capabilities` | Playbook disponibili |
| `GET` | `/api/v1/admin/ansible/runs` | Storico esecuzioni |
| `POST` | `/api/v1/admin/ansible/computers/run_action` | Esegui azione (update_workers, install_workers, preflight_workers) |
| `POST` | `/api/v1/admin/ansible/computers/run_shell` | Comando shell su host remoto |
| `POST` | `/api/v1/admin/ansible/computers/test_ssh` | Test connessione SSH |

---

## 6. Flusso Completo di Generazione Video

### 6.1 Script con Immagini (scene_image)

```
CLIENT                        MASTER (8000)                  WORKER REMOTO
  │                              │                              │
  │  POST /api/script/           │                              │
  │  generate-with-images        │                              │
  │ ──────────────────────────►  │                              │
  │                              │  1. Normalizza payload       │
  │                              │  2. Salva job in SQLite      │
  │  200 OK {job_id, PENDING}    │     (FileQueue.SubmitJob)    │
  │ ◄──────────────────────────  │                              │
  │                              │                              │
  │                              │  3. Worker polla job         │
  │                              │ ◄── POST /api/jobs/get ──────│
  │                              │                              │
  │                              │  4. Assegna job a worker     │
  │                              │ ──► 200 OK {job params} ────│
  │                              │                              │
  │                              │  5. Worker esegue:           │
  │                              │     a) Prepara scene+immagini│
  │                              │     b) Lancia video-engine   │
  │                              │        C++ (FFmpeg)          │
  │                              │     c) Compone video         │
  │                              │     d) Upload su Drive (se   │
  │                              │        configurato)          │
  │                              │                              │
  │                              │  6. Worker invia risultato   │
  │                              │ ◄── POST /api/jobs/result ──│
  │                              │                              │
  │  GET /api/script/jobs/:id    │                              │
  │ ──────────────────────────►  │  7. Job → COMPLETED          │
  │ ◄── {status: COMPLETED} ────│                              │
```

### 6.2 Modalità Video Supportate

| Modalità | Descrizione | Input Richiesto |
|----------|------------|-----------------|
| `scene_image` | Scene con immagini fisse + voiceover | `scenes[]` con `image_link`, `voiceover_path` |
| `clip_stock` | Clip intro + stock video + voiceover | `intro_clip_paths`, `stock_clip_paths`, `voiceover_paths` |
| `video_to_video` | Trasformazione video → video | `source_media`, parametri trasformazione |
| `image_to_video` | Immagine statica → video animato | `image_link`, durata, zoom |

---

## 7. Configurazione Ambiente

### Variabili d'Ambiente (Master)

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `VELOX_MASTER_PORT` | `8000` | Porta server |
| `VELOX_DATA_DIR` | `.velox/data` | Directory dati |
| `VELOX_ADMIN_TOKEN` | — | Token admin per API protette |
| `VELOX_WORKER_BUNDLE_DIR` | — | Directory bundle worker |
| `VELOX_CODE_VERSION` | — | Versione codice (git hash) |
| `VELOX_ANSIBLE_PLAYBOOK_DIR` | `DataDir/ansible/playbooks` | Playbook Ansible |
| `MASTER_PUBLIC_URL` | — | URL pubblico per worker remoti |
| `VELOX_DB_DSN` | `DataDir/velox.db` | Path database SQLite |

### Variabili d'Ambiente (Worker Agent)

| Variabile | Default | Descrizione |
|-----------|---------|-------------|
| `WORKER_ID` | hostname | ID univoco worker |
| `MASTER_URL` | — | URL del master server |
| `WORKER_TOKEN` | — | Token autenticazione worker |
| `VELOX_VIDEO_ENGINE_CPP_BIN` | — | Path binary video engine C++ |

---

## 8. Bundle Worker

Il **bundle** (`worker_code.zip`) contiene il software eseguibile che viene distribuito ai worker remoti. Include:

- `worker-agent-go` compilato (binary Go)
- `video-engine-cpp` compilato (binary C++/FFmpeg)
- Dipendenze e asset necessari

**Ricostruzione bundle:**
```bash
# Usando il binary velox-bundler
./DataServer/bin/velox-bundler \
  --source /path/to/repo \
  --output /path/to/worker_downloads

# Oppure via API
curl -X POST "http://77.93.152.122:8081/install_worker/force_regenerate_zip?wait=1" \
  -H "Authorization: Bearer velox-dev-token"
```

**Aggiornamento worker remoto:**
```bash
# Via Ansible
ansible-playbook -i inventory.ini update_workers.yml

# Via API
curl -X POST http://77.93.152.122:8081/api/v1/admin/workers/update_all \
  -H "Authorization: Bearer velox-dev-token"
```

---

## 9. Architettura della Coda Job

```
         CLIENT (API)
             │
             ▼
    ┌────────────────┐
    │  FileQueue      │  ← SQLiteStore (velox.db)
    │  (SQLite-backed)│
    └───────┬────────┘
            │
            ▼
    ┌────────────────┐
    │  Job Dispather  │  ← Assegna job PENDING a worker
    └───────┬────────┘
            │
       ┌────┴────┐
       ▼         ▼
   Worker A   Worker B   ...
```

**Stati Job:** `PENDING` → `PROCESSING` → `COMPLETED` / `ERROR` / `FAILED`
- Se un worker fallisce, il job torna `PENDING` (requeue automatico)
- Max tentativi configurabile (`VELOX_MAX_JOB_ATTEMPTS`, default: 3)
- Worker con `drain=true` non riceve nuovi job

---

## 10. Quick Start

### Avvio Master Server
```bash
cd DataServer
export VELOX_ADMIN_TOKEN=velox-dev-token
export MASTER_PUBLIC_URL=http://77.93.152.122:8081
go run ./cmd/server
# Server su http://77.93.152.122:8081
```

### Invio Job di Test
```bash
curl -X POST http://77.93.152.122:8081/api/script/generate-with-images \
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
curl http://77.93.152.122:8081/api/script/jobs/<job_id> \
  -H "Authorization: Bearer velox-dev-token"
```

### Installa Worker Remoto
```bash
# Sul worker remoto:
git clone <repo>
cd RemoteCodex/native/worker-agent-go
make build
./bin/velox-worker-agent -master http://<master-ip>:8000 -token <token>
```
