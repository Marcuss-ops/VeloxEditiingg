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

---

## 2. 💾 Asset acquisition & cache

### 🟡 Cache worker
- [x] Lease **many-to-many** (`cached_asset_leases`) — un asset condiviso
      tra più job è protetto finché l'ultimo lease è attivo
- [x] `DeleteIfUnleased` atomico — chiude la race List→Delete
- [x] Cleanup **fail-safe** — nessuna snapshot valida = nessuna cancellazione
      (`ErrSnapshotUnavailable`)
- [x] Singleflight sui download cold-cache (`assetDownloads`)
- [x] Migrazione legacy: `active_job_id` → `cached_asset_leases` a `Open()`
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
- [ ] **`internal/auth/workerauthz`** — unificare HTTP 403 + gRPC
      `PermissionDenied` allowlist in un unico package (oggi byte-for-byte
      duplicati, intenzionalmente)
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
- [ ] Unificare HTTP+gRPC worker allowlist (`internal/auth/workerauthz`)
- [ ] Ridurre duplicazione scope contract (possibile codice generato da un
      singolo schema condiviso cross-repo)
- [ ] `FrontendModule` residui nei template env — ripassata finale
      (`VELOX_SPA_DIR`, `VELOX_GRADIO_APP_URL` in `deploy/`)

### 🟢 Pulizia
- [ ] Audit LOC metrics (ri-baseline con `docs/metrics/loc-baseline.md §12`)
- [ ] Riuso helper `sortedAssetKeys` / extractor dove duplicato
