# Velox — Future Improvements

> Catalogo di feature, ottimizzazioni e migliorie future per la render farm
> Velox (repository `VeloxEditiingg`). Riferimenti canonici:
> `ROADMAP.md` (stato), `docs/100-percent-plan/` (piano verso il 100%),
> `docs/architecture/` (architettura target e distributed rendering).
>
> Legenda priorità: 🔴 alta · 🟡 media · 🟢 bassa · ⚪ infra/manutenzione.

---

## 1. 🧠 Scheduling & Placement

### 🟡 Warm-cache affinità (base già su main)
- [x] Worker advertise `asset_cache_keys` (hello + heartbeat, max 2048)
- [x] Master estrae `RequiredAssetKeys` dal payload del task
- [x] Tie-break warm-cache a parità di priorità nel matcher
- [ ] **Telemetria affinità** — metriche su candidati scelti caldi vs freddi,
      download evitati stimati (KB), hit-rate per asset
- [ ] **Dashboard dedicata** — `dashboards/warm-cache-affinity.json` con i
      nuovi KPI di risparmio
- [ ] **Cache key deterministica** — riusare chiavi canoniche per la
      deduplicazione inter-job (lega a P2 distributed rendering)
- [ ] **Policy di priorità calibrabile** — rendere il bonus warm-cache un
      parametro (peso) configurabile invece di un tie-break fisso

### 🟡 DAG & scala (P2)
- [ ] RenderPlan schema + compiler registry + persistenza plan
- [ ] Multi-Task DAG + executor granulari
- [ ] Intermediate artifact contract + locality scoring
- [ ] Temporal sharding + benchmark CPU + soak distribuito
- [ ] **`PlacementScorer` unico** — oggi il matcher fa tie-break
      `priority DESC → cachedAssetCount DESC → created_at FIFO`, che è un
      confronto per *numero* di file e non per byte: 5 file da 100 KB pesano
      come un asset da 2 GB. Evolvere `RequiredAssetKeys []string` verso una
      forma canonica con `SizeBytes` per pesare
      `estimated_transfer_saved_bytes` / `expected_download_ms_saved`, e solo
      dopo valutare il passaggio da "worker → miglior task" a
      "task × worker → miglior coppia". La `priority` resta un boundary
      HARD: la locality decide solo DENTRO la stessa priorità.

---

## 2. 💾 Asset acquisition & cache

### 🟡 Cache worker
- [x] Lease **many-to-many** (`cached_asset_leases`) — un asset condiviso
      tra più job è protetto finché l'ultimo lease è attivo
- [x] `DeleteIfUnleased` atomico — chiude la race List→Delete
- [x] Cleanup **fail-safe** — nessuna snapshot valida = nessuna cancellazione
      (`ErrSnapshotUnavailable`)
- [x] Singleflight sui download cold-cache (`assetDownloads`) — certificato:
      25 richieste logiche su 12 asset collassano in **12 transfer fisici** +
      13 waiter coalesced (`TestManager_25Requests12AssetsSingleFlight`).
      **Semantica metrica:** `duplicate_download_bytes` = byte EVITATI dal
      dedupe, non byte duplicati fisicamente. L'acceptance è il lato fisico
      (`physical upstream transfers == asset unici`, byte fisici == una copia
      per asset); un valore `> 0` è atteso e sano.
- [x] Migrazione legacy: `active_job_id` → `cached_asset_leases` a `Open()`
- [ ] **Prefetch identity persistita** — oggi `future_asset_hydration.go`
      risolve Drive/URI → SHA256+size e azzera `DeferredJobs` (Deferred è uno
      stato temporaneo di risoluzione, NON un secondo sistema: nessun
      `DeferredCache`/`DeferredDownloader`/`DeferredScheduler`). Il passo
      successivo è persistere l'identità scoperta nell'Asset Catalog, così un
      altro worker non riscarica l'intero file solo per riscoprire lo SHA e
      `velox-drive://...` resta un *source locator* e non una identity.
- [ ] **Test di migrazione esplicito** — fixture DB con schema vecchio +
      lease in flight, verifica `INSERT OR IGNORE` e coerenza `active_job_id`
- [ ] **Eviction con priorità asset** — LRU per gruppo (clips vs stock vs VO)
      invece del solo `last_used_at`
- [ ] **Cleanup policy per quota disco** — percentuale target invece di soli
      vincoli temporali

### 🟢 Download & verifica
- [ ] **Resume/retry con backoff** esponenziale per download interrotti
- [ ] **Verifica SHA-256 a campione** durante l'idle (self-healing del cache)

---

## 3. 🔐 Auth & sicurezza

### 🟡 Consolidamento
- [x] **`internal/auth/workerauthz`** — implementato: la decisione di
      allowlist è unica (`workerauthz.New`) e consumata sia dall'handler HTTP
      `handlers/remote/workers/lifecycle` (403) sia da
      `grpcserver/authorizer.go` (`PermissionDenied`), quindi HTTP e gRPC non
      possono divergere. Nota identità: `worker_id` è IMMUTABILE (principal
      mTLS/OpenBao) e `worker_name` è l'unico campo mutabile/display
      (AGENTS.md §5).
- [ ] **CI guard allowlist** — `scripts/ci/check-worker-allowlist-coverage.sh`
      che fallisce se il CSV `VELOX_ALLOWED_WORKERS` perde riferimenti
- [ ] **Rotazione credenziali worker** semplificata (runbook + script)

### 🟢 Hardening
- [ ] **jti replay protection** sul control JWT BFF (blacklist layer)
- [ ] **Rate limiting** su login/checkout API lato InstaEdit
- [ ] **Security audit automatico** (secrets scanning, npm/go audit)

---

## 4. 📦 Delivery & integrazioni

### 🟡 Delivery
- [ ] **Provider aggiuntivi** oltre Drive (S3, GCS) via registry esistente
- [ ] **Retry budget per piano** già propagato — estendere con retry
      categorizzati per errore (BLOCKED_AUTH / TARGET_NOT_AVAILABLE)
- [ ] **Webhook outbound** per stato delivery (notifiche a sistemi esterni)

### 🟢 Observabilità delivery
- [ ] Dashboard `delivery-*` per tassi di successo per destinazione
- [ ] SLO per latenza end-to-end (enqueue → artifact → delivery)

---

## 5. 📊 Telemetria & ops

### 🟡 Metriche
- [ ] Metriche warm-cache (hit-rate, download evitati)
- [ ] Metriche di affinità placement (candidati caldi vs freddi scelti)
- [ ] Metriche cleanuploop (row ispezionate, skipped per causa)

### ⚪ Infra
- [ ] **Backup automatico SQLite** (retention 30gg) + restore verificato —
      requisito non implementato; lo scaffolding irraggiungibile rimosso nel
      call-site proof del 2026-08-10. Reintrodurre solo con owner
      platform/operations, entrypoint runtime reale e restore verificabile.
- [ ] **Staging environment** speculare (DB + master + worker + canary)
- [ ] **Log centralizzato** (aggregazione multi-host)
- [ ] **Alert su drop conversioni / failure rate** (webhook Slack esistente)

---

## 6. 🧪 Testing & CI

### 🟡 Coverage
- [ ] **E2E publishing flow** automatizzato in CI (non solo runbook)
- [ ] **Load test** (k6) su enqueue + delivery con N job concorrenti
- [ ] **Visual regression** per i dashboard (se applicabile)

### ⚪ Qualità
- [ ] `make verify` esteso con i nuovi guard CI (warm-cache, manifest,
      allowlist)
- [ ] **Dependency audit** settimanale automatizzato
- [ ] **API documentation** OpenAPI estesa a tutte le route master
      (oggi solo creator-push + manifest)

---

## 7. 🏗 Refactor & tech debt

### 🟡 Consolidamenti
- [x] Unificare HTTP+gRPC worker allowlist (`internal/auth/workerauthz`)
- [x] **Unificare l'autorità del profilo video** — un solo owner della
      compatibilità (`shared/contract.CanonicalVideoProfileV1`) + una sola
      policy di qualità (`shared/contract.PreparationQualityPolicy`); il
      trimmer deriva ogni argomento ffmpeg dal profilo e il manifest registra
      `canonical_profile_id`. Rimossi `VideoNormalization` (30 fps) e
      `normalization_version`. Rete di sicurezza:
      `TestVideoPreparationUsesCanonicalStreamProfile` + regola 14 di
      `scripts/ci/check-architecture.sh`.
- [ ] Ridurre duplicazione scope contract (possibile codice generato da un
      singolo schema condiviso cross-repo)
- [ ] `FrontendModule` residui nei template env — ripassata finale
      (`VELOX_SPA_DIR`, `VELOX_GRADIO_APP_URL` in `deploy/`)
- [ ] **`OutputSink` extraction (zero-disk, step 1)** — oggi
      `media_packet_muxer.cpp` parla di `PacketOutputSink` ma implementa
      soltanto un file sink (`open(partial)` → `pwrite` → `fsync` →
      `publishAtomic`). Primo refactor a comportamento INVARIATO:
      `OutputSink` (astratto) └── `FileOutputSink`; poi
      `OutputSinkResolver` └── `FileOutputSink` | `StreamingOutputSink`
      (fMP4 append-only). Niente catene `if streaming … if file …` sparse nel
      muxer. Step 2: `AVIO` → bounded pipe/socketpair → uploader Go esistente
      (master-stream/multipart/retry/journal restano in Go, non si riscrivono
      in C++), con `FileOutputSink` come fallback per debug/failure e per il
      MP4 classico.
- [x] **Batch intake: dal dispatch HTTP al core canonico (P1)** —
      `job_submit_core.go` è il core transport-neutral dell'intake canonico
      (validation → risoluzione identity → `CanonicalJobSubmitter` → envelope
      tipizzato). `POST /jobs` ne è un adapter sottile; `POST /jobs/batch` lo
      chiama per item e proietta l'envelope TIPIZZATO nel risultato dell'item.
      Eliminati: il replay di `SubmitJob()` su una `gin.Context` clonata, il
      `batchResponseCapture` (gin.ResponseWriter sintetico) e il round-trip
      JSON per item. La attribuzione dell'intake source è esplicita
      (`canonical` vs `batch`), non più trasportata nel context; rimossi
      `SetIntakeSource`/`IntakeSourceFromContext`. Rete di sicurezza: regola 16
      di `scripts/ci/check-architecture.sh` (vieta `SubmitJob()(` nel file batch
      e qualunque scrittura su `gin.Context` nel core).
- [x] **Bug latente chiuso: stock pool perso sui batch item** —
      `SubmitScene.StockAssets` è internal-only (`json:"-"`) e il vecchio path
      batch ri-serializzava l'item già normalizzato, perdendo il pool derivato
      da `spec.scenes[].stock` (array); la ri-normalizzazione nel handler
      replayato non poteva ricrearlo perché le scene erano già presenti. Ora
      un batch item raggiunge il worker con lo stesso `scene.stock[]` della
      richiesta singola, pinnato da
      `TestSubmitJobE2E_BatchAndSingleProjectTheSameStockPool`.
- [ ] **Batch intake: preprocessing condiviso (P2)** — oggi ogni item rifà la
      propria proiezione/risoluzione. Il passo successivo abilitato dal core
      condiviso è risolvere gli asset UNA volta per envelope e riusare i
      manifest canonici sui 100 item, invece di ripetere il preprocessing per
      item.

### 🟢 Pulizia
- [ ] Audit LOC metrics (ri-baseline con `docs/metrics/loc-baseline.md §12`)
- [ ] Riuso helper `sortedAssetKeys` / extractor dove duplicato
