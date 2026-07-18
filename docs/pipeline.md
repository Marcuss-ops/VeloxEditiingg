# Pipeline di Elaborazione Velox — Panoramica E2E

## 1. Panoramica

La pipeline trasforma un job di rendering in un artefatto finale consegnato
su Google Drive **oppure** su una piattaforma social tramite la **Social
API esterna**.  Velox possiede soltanto: sottomissione → dispatch → esecuzione
→ finalizzazione (con promozione artefatto) → consegna → (opzionale) retry.
Le preoccupazioni di social-platform (OAuth, canali, token, quota, stato di
pubblicazione, scheduling) sono di proprietà della **repo social esterna**;
Velox parla con essa attraverso il `socialclient` (HTTP) o, in fallback,
attraverso il provider di consegna `social_gateway`.

Un singolo job attraversa 6 fasi principali: **sottomissione → dispatch →
esecuzione → finalizzazione (con promozione artefatto) → consegna →
(opzionale) retry**.

```
┌──────────┐   ┌──────────┐   ┌──────────┐   ┌──────────────┐   ┌──────────┐   ┌────────────────┐
│   API    │ → │  Master  │ → │  Worker  │ → │ Finalization │ → │ Delivery │ → │  Drive  /      │
│ Submit   │   │  Enqueue │   │  Render  │   │ + Storage    │   │  Runner  │   │  Social API    │
└──────────┘   └──────────┘   └──────────┘   └──────────────┘   └──────────┘   └────────────────┘
                                                                                (delegated via
                                                                                 socialclient)
```

---

## 2. Fase 1 — Sottomissione Job

### Endpoint
`POST /api/jobs/:kind`  
Handler: `DataServer/internal/handlers/server/jobs/handler.go:30`

### Flusso
1. Il client POSTa un JSON con payload specifico del job kind
2. L'ingress registry (`DataServer/internal/jobs/ingress/registry.go:11`) risolve il builder per `kind`
3. `Enqueuer.PrepareJobAndTask()` normalizza il payload, risolve asset esterni (voiceover, scene-images), e chiama `CreateJobWithTask()` che in una singola transazione atomica inserisce:
   - `jobs` — il record job con stato `PENDING`
   - `tasks` — il relativo task con stato `READY`
   - `task_specs` — la specifica immutabile del task (executor_id, payload_json...)
   - `task_requirements` — i requisiti di capabilities per il placement worker

### Job Kind Registrati
Ogni job kind è registrato in `DataServer/cmd/server/bootstrap_modules.go` via `ingress.Registry`.  
Esempio: `scriptclip` (Jackie Chan documentary) → executor `scene.composite.v1`.

---

## 3. Fase 2 — Dispatch Master → Worker

### Task Claiming
- Il worker contatta il master via gRPC
- Il master chiama `ClaimNextReadyTask()` su `taskgraph/repository.go:43`:
  ```sql
  UPDATE tasks SET status='RUNNING', leased_by=?, lease_id=?, ...
  WHERE status='READY' ... RETURNING *
  ```
- **Placement**: `DataServer/internal/placement/matcher.go` verifica che il worker abbia le capabilities richieste dal task

### Lease
- Il task ha un lease temporale (`lease_expires_at`)
- Se il lease scade (worker crash), un altro worker può reclamarlo
- `attempt_count` incrementale: dopo `max_attempts` tentativi, il task va in `FAILED`

---

## 4. Fase 3 — Esecuzione Worker

### Worker Agent
`RemoteCodex/native/worker-agent-go/cmd/velox-worker-agent/main.go`

### TaskRunner Lifecycle
`RemoteCodex/native/worker-agent-go/internal/taskrunner/runner.go:37`
1. **Acquire** — ottiene il task dal master
2. **Execute** — avvia l'executor specifico (es. `scene.composite.v1`)
3. **Report** — notifica il completamento (successo/fallimento)
4. **Finalize** — carica l'artefatto sul master via `BeginUpload` + `Receive` + `Finalize`
5. **Cleanup** — pulizia risorse locali

### Executor: `scene.composite.v1`
File: `RemoteCodex/native/worker-agent-go/internal/taskrunner/executors/scene_composite.go`
- ID: `scene.composite.v1`
- Esegue compositing video + voiceover
- Il risultato è un file `.f4v` o `.mp4`
- `resolveOutputPath()` oggi **ignora intenzionalmente** `payload.output_path` e sintetizza sempre un path locale worker-side `outputBase/<jobID>.mp4`.
- `PayloadOutputPath()` espone comunque il valore originale del master per usi metadata/upload target, non come render path locale.

### Upload Artefatto
Il worker carica il file generato sul master in 3 fasi:
1. `BeginUpload` — master alloca uno staging path e restituisce un upload session ID
2. `Receive` — master riceve i bytes in streaming, calcolando SHA-256 e size on-the-fly, scrivendo nello **staging** (`/opt/velox/current/.velox/data/staging/`)
3. `Finalize` — master verifica hash/size, determina MIME type, promuove a **final storage**

---

## 5. Fase 4 — Finalizzazione & Storage

### Promozione Artefatto (Staging → Final)

File: `DataServer/internal/artifacts/storage.go:86`

`PromoteToCanonical(blobStore, stagingPath, sha256Hex, extension)`:
1. Calcola `CanonicalStorageKey(sha256Hex, ext)` → `"artifacts/sha256/<primi-2>/<sha256>.<ext>"` (content-addressable)
2. Calcola `FinalStorageKey(blobStore, sha256Hex, ext)` → unisce finalDir + relKey
3. Crea la directory finale se non esiste
4. Crea un file temporaneo **nella stessa directory** del target (stesso filesystem → rename atomico)
5. Copia i bytes dallo staging al temporaneo
6. `flush → fsync → close`
7. `rename(2)` atomico del temporaneo al path finale
8. `fsync` della directory padre (best-effort)

**Ritorna**: la `relKey` (es. `"artifacts/sha256/e5/e5f2c2355....f4v"`) che viene salvata nel DB come `storage_key`.

### Transizione Finale (Succeeded)

File: `DataServer/internal/artifacts/sqlite_finalization_repository.go`

`FinalizeVerified()` esegue una singola transazione SQL atomica:
1. Verifica `artifact_uploads.status = FINALIZING` e la catena di identità (`worker_id`, `lease_id`, `attempt_number`)
2. Job `RUNNING | AWAITING_ARTIFACT → SUCCEEDED`
3. Artefatto `STAGING → READY`
4. **Inserisce righe in `job_deliveries`** — una per ogni destinazione dal delivery plan
5. `artifact_uploads FINALIZING → COMPLETED`

### Invariante chiave: Blob BEFORE SQL
La promozione del file avviene **prima** della transazione SQL.  
Se la SQL fallisce (rollback), rimane un blob orfano sul filesystem (il reconciler lo pulirà).  
Mai il contrario: non esiste `artifacts.READY` senza file su disco.

### Storage a indirizzamento contenuto
`CanonicalStorageKey(sha256, ext)`:
- Stesso hash → stessa chiave (dedup naturale)
- Struttura: `artifacts/sha256/<aa>/<sha256>.<ext>`
- La directory a 2 caratteri (`e5/`) evita milioni di file in una singola directory

### FilesystemBlobStore
File: `DataServer/internal/store/blobstore.go:52`

```go
type FilesystemBlobStore struct {
    stagingDir string  // es. /opt/velox/current/.velox/data/staging
    finalDir   string  // es. /opt/velox/current/.velox/data/storage
}
```

| Metodo | Input | Output |
|--------|-------|--------|
| `StagingPath(jobID, artifactID, ext)` | identità job/artefatto | path assoluto in staging |
| `FinalPath(jobID, artifactID, ext)` | identità job/artefatto | path assoluto in final |
| `PromoteToFinal(staging, final)` | staging + final path | move atomico via `os.Rename` |
| `ReadFinal(storageKey)` | storage_key **relativo** | file handle aperto (path assoluto risolto internamente) |
| `FinalDir()` | — | restituisce `finalDir` assoluto |

#### Bug Risolto: `ReadFinal`

**Prima** (v1.1.0):
```go
func (b *FilesystemBlobStore) ReadFinal(storageKey string) (*os.File, error) {
    f, err := os.Open(filepath.Clean(storageKey))
    // Apre il path RELATIVO — non funziona se CWD ≠ finalDir
```

**Dopo** (v1.2.0):
```go
func (b *FilesystemBlobStore) ReadFinal(storageKey string) (*os.File, error) {
    fullPath := filepath.Join(b.finalDir, filepath.Clean(storageKey))
    f, err := os.Open(fullPath)
```

Il `storageKey` memorizzato nel DB è sempre relativo (`artifacts/sha256/e5/...`).  
Senza `filepath.Join`, `os.Open` cercava il file relativo alla CWD invece che dentro `finalDir`.

---

## 6. Fase 5 — Delivery Runner

### Architettura

File: `DataServer/internal/deliveries/runner.go`

```
Run() loop (ticker ogni 5s)
  └─ tick()
       └─ ClaimDeliveries (UPDATE+RETURNING atomico)
            └─ Per ogni DeliveryLease reclamato (max Concurrency=2)
                 └─ processLease()
                      ├─ Risolve provider via Registry (social_gateway | drive)
                      ├─ Hydrata Destination + Artifact dal DB
                      ├─ Avvia renewDeliveryLeaseLoop (heartbeat ogni lease/3 ≈ 100s)
                      ├─ Provider.Deliver()
                      │    ├─ ✅ Successo → MarkDeliverySucceeded
                      │    ├─ ❌ PERMANENT → MarkDeliveryFailed (FAILED terminale)
                      │    ├─ 🔒 AUTH → MarkDeliveryBlockedAuth (BLOCKED_AUTH, intervento manuale)
                      │    ├─ ⏳ RATE_LIMIT → MarkDeliveryRetry (con backoff)
                      │    └─ ⚠️ TRANSIENT (default) → MarkDeliveryRetry (con backoff)
                      └─ Ferma heartbeat
```

### Configurazione Default Runner

| Parametro | Default | Descrizione |
|-----------|---------|-------------|
| `PollInterval` | 5s | Frequenza di polling sul DB |
| `LeaseDuration` | 5m | Durata lease per ogni delivery |
| `MaxAttempts` | 5 | Tentativi massimi (sovrascritto per-delivery da `retry_budget`) |
| `Concurrency` | 2 | Delivery simultanee massime |
| `BackoffSchedule` | `[30s, 2m, 10m, 30m]` | Backoff tra retry progressivi |

### Claim Deliveries

File: `DataServer/internal/store/store_deliveries_lease.go:31`

Query atomica `UPDATE ... RETURNING *` che seleziona righe `job_deliveries` con:
- `status = 'PENDING'`, oppure
- `status = 'RETRY_WAIT'` con `next_attempt_at ≤ NOW()`, oppure
- `status = 'RUNNING'` con `lease_expires_at < NOW()` (zombie reclamation)

Ogni delivery reclamata riceve un `lease_id` univoco (`dl_<uuid>`) che serve per:
- Heartbeat periodico (rinnovo lease)
- CAS (Compare-And-Swap) nelle operazioni `MarkDelivery*` — evita conflitti tra runner

### Classificazione Errori

File: `DataServer/internal/deliveries/provider.go:72`

| Sentinella | Classe | Azione Runner |
|------------|--------|---------------|
| `ErrProviderPermanent` | `PERMANENT` | FAILED terminale (nessun retry) |
| `ErrProviderNotConfigured` | `PERMANENT` | FAILED terminale |
| `ErrProviderAuth` | `AUTH` | BLOCKED_AUTH (intervento operatore) |
| `ErrProviderRateLimit` | `RATE_LIMIT` | Retry con backoff (se <= maxAttempts) |
| `default` (qualsiasi altro) | `TRANSIENT` | Retry con backoff (se <= maxAttempts) |

### Supervisione

File: `DataServer/cmd/server/background_supervisor.go`

Il `delivery-runner` è registrato come `ClassCritical`:
- Riavvio automatico con backoff esponenziale (1s → 30s max)
- Se esaurisce i retry critici (se configurati), il master esce con errore non-zero → k8s riavvia il pod

---

## 7. Approfondimento Provider — Drive

### DriveProvider

File: `DataServer/internal/deliveries/providers/drive.go:18`

```go
type DriveProvider struct {
    service   *integrationsDrive.Service
    blobStore store.BlobStore
}
```

### Deliver Flow
1. Prende `artifact.StorageKey` (relativo dal DB, es. `"artifacts/sha256/e5/..."`)
2. Se `blobStore` è configurato:
   - `blobStore.ReadFinal(storageKey)` — verifica esistenza file (ora risolve il path assoluto)
   - **Se fallisce** → fallback a `artifact.LocalPath` (legacy), altrimenti `ErrProviderPermanent`
   - **Se OK** → chiude il file, risolve `filePath` a path assoluto con `filepath.Join(blobStore.FinalDir(), storageKey)`
3. Chiama `service.UploadVideo(ctx, filePath, artifact.ID, destination.FolderID, deliveryID)`
4. Drive crea/sceglie una sottocartella col nome del progetto, carica il file via multipart upload

### Drive Service

File: `DataServer/internal/integrations/drive/service_files.go:20`

- `UploadFile()`: multipart upload REST verso Google Drive API
- Stampa `deliveryID` come proprietà del file Drive (`velox_delivery_id`) per tracciabilità
- `UploadVideo()`: wrapper che prima crea/riusa una cartella progetto, poi chiama `UploadFile`

### Drive Auth

File: `DataServer/internal/integrations/drive/auth.go:20`

- `TokenManager` carica/salva token OAuth2 come file JSON in `data/drive_tokens/`
- `LoadToken(destinationID)` → legge `<dataDir>/drive_tokens/<destinationID>.json`
- `SaveToken(destinationID, token)` → scrive file JSON
- Refresh automatico del token tramite `RefreshToken` quando scade
- Config: `VELOX_DRIVE_CLIENT_ID` e `VELOX_DRIVE_CLIENT_SECRET` in `/etc/velox-server.env`

#### Bug Risolto: filePath relativo non risolto per upload

**Prima** (v1.1.0):
```go
f, _ := d.blobStore.ReadFinal(filePath)  // verificava con path relativo
f.Close()
// filePath era ANCORA relativo quando passato a UploadVideo!
uploadRes, _ := d.service.UploadVideo(ctx, filePath, ...)
```

**Dopo** (v1.2.0):
```go
f, _ := d.blobStore.ReadFinal(filePath)
f.Close()
filePath = filepath.Join(d.blobStore.FinalDir(), filePath)  // ← risolve ad assoluto
uploadRes, _ := d.service.UploadVideo(ctx, filePath, ...)
```

---

## 8. Approfondimento Provider — Social API (delivery delegata)

### SocialGatewayProvider

File: `DataServer/internal/deliveries/providers/social_gateway.go:17`

`SocialGatewayProvider` è l'adapter **piattaforma-agnostico** che delega
l'intera pubblicazione a un servizio Social API esterno.  Velox NON
conosce OAuth, canali, token, quota, stato di pubblicazione, scheduling:
invia solo metadati dell'artefatto al Social API e riceve un
`social_delivery_id` che persiste nella riga `job_deliveries`.

Struttura:
1. Costruisce il payload (`socialclient.DeliverArtifactRequest`)
2. POST a `${SOCIAL_API_URL}/internal/v1/deliveries`
3. Riceve `{ social_delivery_id, status }` e ritorna `deliveries.Result`

Riferimenti incrociati: §9 (delivery plan), §14 (`SOCIAL_API_URL`,
`SOCIAL_API_TOKEN`).

### socialclient

File: `DataServer/internal/socialclient/` (nuovo package)

Tutta la logica HTTP `POST /internal/v1/deliveries` vive qui
(`client.go`, `config.go`, `requests.go`).  Il
`SocialGatewayProvider` è un thin adapter che chiama
`socialclient.New(cfg).DeliverArtifact(...)` e mappa la risposta su
`deliveries.Result`.

### Auth & Quota

- NESSUNA auth Google/YouTube diretta in Velox.
- NESSUNA tabella `youtube_oauth` (rimossa con migration 090).
- NESSUNA tabella `youtube_quota_usage` (di competenza del servizio
  Social API esterno).
- Il bearer verso la Social API stessa è `SOCIAL_API_TOKEN` (env).

---

## 9. Delivery Plan & Destinazioni

### Delivery Plan per Job

File: `DataServer/internal/deliveries/plan_resolver.go:28`

**Production mode** (`GlobalFallback=false`):
- Richiede un piano esplicito in `job_delivery_plans` per ogni job
- Nessun piano → `ErrNoExplicitPlan` → nessuna delivery generata
- Il piano definisce: `destination_id`, `priority`, `retry_budget`
- L'operatore crea il piano via SQL o tool admin

### Destinazioni

Tabella `delivery_destinations`:
| Colonna | Descrizione |
|---------|-------------|
| `destination_id` | Identificativo univoco (es. `comedy_test`) |
| `provider` | `drive` o `social_gateway` (chiave di **routing** interna a Velox — **NON** social platform) |
| `folder_id` | Cartella Drive di destinazione (usato solo da `provider=drive`; ignorato da `provider=social_gateway`) |
| `external_destination_id` | Identificativo **opaco** risolto server-side dalla Social API in (platform, account, channel, language, credentials). **Post PR-15.13** (Residuo 3 closure) + **PR-15.14** (Residuo 4 closure, migration 092 rename): Velox non conosce né propaga più le 3 colonne pre-closure (account_id, channel_id, language) come campi verso `social_repo`. L'`external_destination_id` è canonico post-migration 092 (precedentemente `social_destination_id`). Vuoto/whitespace-only al claim time del runner → fail-closed con codice `DESTINATION_UNMAPPED` (vedi `runner.go:499-500`). |
| `name` | Nome descrittivo della destinazione |
| `enabled` | Se la destinazione è attiva |
| `configuration_json` | Blob JSON opaco Velox-internal. NON propagato verso `social_repo`. Eventuali sub-key platform-shaped legacy (`$.platform`, `$.account_id`, `$.channel_id`, `$.language`) restano qui solo per audit-trail — vedi `docs/SOCIAL_API_MIGRATION_RUNBOOK.md` §2. Per nuova intent platform-shaped, vedi il campo wire `metadata map[string]any` su `socialclient.DeliverArtifactRequest` (`DataServer/internal/socialclient/requests.go`). |

### Retry Budget

- `job_delivery_plans.retry_budget` → stampato su `job_deliveries.max_attempts` all'inserimento
- Il runner legge `max_attempts` al claim e sovrascrive `cfg.MaxAttempts` per quella delivery
- Backoff schedule: 1° retry a 30s, 2° a 2m, 3° a 10m, 4+ a 30m
- Dopo `max_attempts` tentativi → FAILED terminale

---

## 10. SQL Tables

### Job & Task
| Tabella | Ruolo |
|---------|-------|
| `jobs` | Record job, status machine (`PENDING → SUCCEEDED/FAILED`) |
| `tasks` | Task atomico, executor_id, lease, attempt_count |
| `task_specs` | Payload immutabile del task |
| `task_requirements` | Capabilities richieste dal task |
| `job_attempts` | Log per-tentativo (worker, lease, errore) |

### Artifact
| Tabella | Ruolo |
|---------|-------|
| `artifacts` | Metadati artefatto (storage_key, sha256, size, mime, status) |
| `artifact_uploads` | Sessione di upload (lease, expected hash/size, received hash/size) |

### Delivery
| Tabella | Ruolo |
|---------|-------|
| `job_deliveries` | Record delivery (status, lease_id, attempt_count, max_attempts, idempotency_key) |
| `delivery_destinations` | Schema **post-PR-15.13**: `provider`, `folder_id`, `external_destination_id`, `name`, `enabled`, `configuration_json`, `created_at`, `updated_at`. Evoluzione: migration `091_opaque_destination.sql` ha DROPPATO 3 colonne pre-closure (account_id, channel_id, language) (Residuo 2); migration `092_rename_social_to_external_destination_id.sql` ha rinominato `social_destination_id` → `external_destination_id` (Residuo 4). **Nessun campo propagato verso `social_repo` ricade sulle 3 colonne pre-closure (account_id, channel_id, language)** — il contratto wire è esclusivamente `external_destination_id` (opaco) + `metadata` (opaque pass-through). Riferimento incrociato: CHANGELOG.md §15.13 (Residuo 3 closure — `socialclient.DeliverArtifactRequest` typed boundary). |
| `job_delivery_plans` | Piano per-job (destination, priority, retry_budget) |
| `delivery_attempts` | Audit log per-tentativo (status, errore, timestamp) |

---

## 11. Directory Layout Rilevante

```
DataServer/
├── cmd/server/
│   ├── main.go                           # Entry point
│   ├── bootstrap.go                      # Assembly componenti
│   ├── bootstrap_modules.go              # Registrazione moduli (socialclient, Drive, Enqueuer, DeliveryRunner)
│   ├── bootstrap_persistence.go          # Init DB + BlobStore
│   └── background_supervisor.go          # Ciclo vita runner (delivery-runner ClassCritical)
├── internal/
│   ├── jobs/
│   │   ├── enqueue/enqueue.go            # PrepareJobAndTask + CreateJobWithTask
│   │   └── ingress/registry.go           # Registry builder per job kind
│   ├── taskgraph/
│   │   ├── repository.go                 # ClaimNextReadyTask
│   │   └── lifecycle.go                  # Gestione lease task
│   ├── artifacts/
│   │   ├── service.go                    # BeginUpload / Receive / Finalize
│   │   ├── service_receive.go            # Streaming upload worker → master
│   │   ├── service_finalize.go           # Promozione staging → final
│   │   ├── storage.go                    # CanonicalStorageKey / FinalStorageKey / PromoteToCanonical
│   │   └── sqlite_finalization_repository.go  # FinalizeVerified (transazione atomica SUCCEEDED)
│   ├── store/
│   │   ├── blobstore.go                  # Interfaccia BlobStore + FilesystemBlobStore + NopBlobStore
│   │   └── store_deliveries_lease.go     # ClaimDeliveries + MarkDelivery* + renewDeliveryLease
│   ├── deliveries/
│   │   ├── runner.go                     # DeliveryRunner (Run, tick, processLease)
│   │   ├── provider.go                   # Interfaccia Provider + error sentinelle
│   │   ├── registry.go                   # Provider registry
│   │   ├── plan_resolver.go              # DeliveryPlanResolver (per-job o global fallback)
│   │   ├── providers/
│   │   │   ├── drive.go                  # DriveProvider (Deliver)
│   │   │   ├── social_gateway.go         # SocialGatewayProvider (delega via socialclient)
│   │   │   ├── s3.go                     # S3Provider (skeleton)
│   │   │   └── localexport.go            # LocalExportProvider (skeleton)
│   │   ├── socialclient/
│   │   │   ├── client.go                 # HTTP client: POST /internal/v1/deliveries
│   │   │   ├── config.go                 # SOCIAL_API_URL / TOKEN / TIMEOUT / RETRIES
│   │   │   └── requests.go               # DeliverArtifactRequest/Response typed contract
│   ├── integrations/
│   │   └── drive/
│   │       ├── service.go                # NewService, getToken
│   │       ├── service_files.go          # UploadFile, UploadVideo, DownloadFile
│   │       └── auth.go                   # TokenManager (carica/salva/refresh token)
│   └── handlers/server/jobs/handler.go   # POST /api/jobs/:kind handler
└── cmd/server/router.go                  # Router wiring

RemoteCodex/native/worker-agent-go/
└── internal/taskrunner/
    ├── runner.go                          # TaskRunner lifecycle
    └── executors/scene_composite.go       # scene.composite.v1 executor

ops/jobs/
├── submit_jackie_chan_doc_voiceover_clips.sh    # Script sottomissione job
└── jackie_chan_doc_voiceover.generate-from-clips.json  # Payload job Jackie Chan
```

---

## 12. Cronologia Bug Risolti

| Bug | File | Sintomo | Fix |
|-----|------|---------|-----|
| Ambiguità su `output_path` tra master e worker | `scene_composite.go` | Rischio di trattare un path master-side come render path locale nel worker | Il worker ora ignora intenzionalmente `payload.output_path` e rende sempre in `outputBase/<jobID>.mp4`; il valore originale resta disponibile via `PayloadOutputPath()` |
| `ReadFinal` non risolveva path assoluto | `blobstore.go:114` | Delivery falliva con `no such file or directory` su storage_key relativo | Aggiunto `filepath.Join(b.finalDir, filepath.Clean(storageKey))` |
| Provider usava storage_key relativo per upload | `drive.go:71` | Drive riceveva path relativo → `failed to open file` | Aggiunto `filePath = filepath.Join(d.blobStore.FinalDir(), filePath)` dopo ReadFinal |
| Drive token mancanti | `drive/auth.go` | `PROVIDER_NOT_CONFIGURED` | Aggiunti `VELOX_DRIVE_CLIENT_ID` e `VELOX_DRIVE_CLIENT_SECRET` a `/etc/velox-server.env` |

---

## 13. Comandi Utili

### Sottomissione Job
```bash
./ops/jobs/submit_jackie_chan_doc_voiceover_clips.sh
```

### Verifica Stato
```bash
# Delivery
sqlite3 /opt/velox/current/.velox/data/velox.db \
  "SELECT delivery_id, status, attempt_count, last_error_code, remote_id, remote_url \
   FROM job_deliveries WHERE delivery_id = '<DELIVERY_ID>';"

# Job
sqlite3 /opt/velox/current/.velox/data/velox.db \
  "SELECT job_id, status, video_name FROM jobs WHERE job_id = '<JOB_ID>';"

# Artifact
sqlite3 /opt/velox/current/.velox/data/velox.db \
  "SELECT id, status, type, storage_key, sha256, size_bytes, mime_type \
   FROM artifacts \
   WHERE job_id = '<JOB_ID>';"
```

### Reset Delivery per Retry
```bash
sqlite3 /opt/velox/current/.velox/data/velox.db "
UPDATE job_deliveries
SET status = 'PENDING',
    attempt_count = 0,
    last_error_code = NULL,
    last_error_message = NULL,
    next_attempt_at = datetime('now'),
    updated_at = datetime('now')
WHERE delivery_id = '<DELIVERY_ID>';"
```

### Log Delivery Runner
```bash
sudo journalctl -u velox-server --since "5 minutes ago" --no-pager |
  grep -iE "delivery|DRIVE|SOCIAL|blobstore|ReadFinal|Upload|socialclient"
```

### Ricostruzione Worker
```bash
cd `<repo-root>` && make agent
# Il binario viene prodotto in:
#   RemoteCodex/native/worker-agent-go/bin/velox-worker-agent
```

### Ricostruzione Master
```bash
cd `<repo-root>/DataServer` && go build -o /tmp/velox-server ./cmd/server
```

---

## 14. Variabili d'Ambiente

### Master (`/etc/velox-server.env`)
| Variabile | Valore Default | Descrizione |
|-----------|----------------|-------------|
| `VELOX_RUNTIME_DIR` | `/opt/velox/current/.velox` | Root runtime |
| `VELOX_DATA_DIR` | `$VELOX_RUNTIME_DIR/data` | Dati (DB, storage) |
| `VELOX_STORAGE_DIR` | `$VELOX_DATA_DIR/storage` | Final artifact storage |
| `VELOX_STAGING_DIR` | `$VELOX_DATA_DIR/staging` | Staging temporaneo upload |
| `VELOX_DRIVE_CLIENT_ID` | — | Client ID OAuth Google Drive |
| `VELOX_DRIVE_CLIENT_SECRET` | — | Client Secret OAuth Google Drive |
| `SOCIAL_API_URL` | — | Base URL del Social API esterno (es. `https://social.example.com`). Letto da `socialclient/internal/socialclient/config.go::ConfigFromEnv()`. |
| `SOCIAL_API_TOKEN` | — | Bearer inviato come `Authorization: Bearer <token>` verso la Social API. **SEGRETO**: popolato da `vault_velox_social_api_token` nel vault ansible. |
| `SOCIAL_API_TIMEOUT_MS` | `30000` | Timeout singola chiamata HTTP verso la Social API (default 30s). |
| `SOCIAL_API_RETRIES` | `3` | Hint di retry (Velox-side BackoffSchedule è canonico). |
| `SOCIAL_CALLBACK_BASE_URL` | — | URL pubblicamente raggiungibile di Velox, usato per costruire `download_url` e `callback_url` delle delivery. |
| `SOCIAL_ARTIFACT_PUBLIC_URL` | — | (forward-looking) CDN pubblico per fetch artefatti quando non si usa il meccanismo callback. |
| `SOCIAL_WEBHOOK_SECRET` | — | (forward-looking, **SEGRETO**) HMAC per i callback `social_repo` → Velox. Da `vault_velox_social_webhook_secret`. |

### Worker (`/etc/velox-worker.env`)
| Variabile | Descrizione |
|-----------|-------------|
| `VELOX_MASTER_URL` | URL del master gRPC |
| `VELOX_RUNTIME_DIR` | Root worker runtime |
