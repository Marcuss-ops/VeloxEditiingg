# Velox — Roadmap di Stato

> **Stato aggiornato:** Agosto 2026 — separazione architetturale completata.
> Documento di stato del progetto `VeloxEditiingg` (render farm). La roadmap
> tecnica di dettaglio vive in `docs/100-percent-plan/` (piano verso il 100%)
> e in `docs/roadmap/` (passi tecnici numerati 01–15).

---

## Architettura target — COMPLETATA ✅

Il sistema è separato in **tre repository** con responsabilità non sovrapposte:

```text
VeloxFrontend       → interfaccia editor (UI)
InstaeditLogin      → utenti, workspace, progetti editor, sessioni e BFF/proxy
VeloxEditiingg      → render farm: job intake, workers, scheduling, rendering,
                      artifact verification, delivery, telemetria operativa
```

### Confine architetturale (contratto stabile)

- `VeloxEditiingg` **non contiene** più alcun codice dell'editor:
  - zero import Go `darkeditor`
  - zero route `/api/darkeditor`, `/dark_editor_v2`, `/api/v1/instaedit/editor`
  - zero proxy Dark Editor, zero config/env (`VELOX_DARK_EDITOR_*`, `NVIDIA*`)
  - zero tabelle `dark_editor_*` nello schema finale (migrazione `128`)
  - zero scope `editor.*` / `youtube.session.publish` nel contratto JWT
- `VeloxEditiingg` **non serve più UI**: `FrontendModule`, SPA fallback e static
  file serving rimossi dal master (headless).
- L'editor parla con Velox **solo** attraverso il BFF InstaEdit:

```text
InstaeditLogin BFF  ──►  Velox jobs/workers/assets
                          GET  /api/v1/instaedit/jobs            jobs.read
                          POST /api/v1/instaedit/jobs            jobs.write
                          GET  /api/v1/instaedit/jobs/:id        jobs.read
                          POST /api/v1/instaedit/jobs/:id/cancel jobs.write
                          GET  /api/v1/instaedit/jobs/:id/deliveries  jobs.read
                          GET  /api/v1/instaedit/workers         workers.read
                          GET  /api/v1/instaedit/workers/:id     workers.read
                          GET  /api/v1/instaedit/assets/:id      assets.read
```

Il vocabolario scope (5 valori wire: `jobs.read`, `jobs.write`, `workers.read`,
`assets.read`, `assets.write`) è **duplicato e allineato** tra
`InstaeditLogin/internal/veloxcontract/contract.go` (mint) e
`VeloxEditiingg/internal/instaeditauth/scopes.go` (verifica) — un drift
produrrebbe 403 alla prima chiamata BFF.

---

## Stato per area funzionale

| Area | Stato | Note |
|---|---|---|
| Job intake (`/api/v1/jobs`, creator push, manifest) | ✅ Attiva | `POST /api/v1/creator/jobs`, `velox.render-manifest.v1`, single-writer invariant |
| InstaEdit BFF (`/api/v1/instaedit/*`) | ✅ Attiva | JWT control-plane, scope per operazione, 403 enriched |
| Worker management (gRPC control plane) | ✅ Attiva | registry, sessioni, heartbeat, allowlist, mTLS |
| Scheduling & placement | ✅ Attiva | warm-cache placement awareness (preferisce worker con asset già in cache) |
| Asset acquisition / cache | ✅ Attiva | `velox-asset://` canonico + legacy Drive fallback, lease many-to-many |
| Rendering | ✅ Attiva | worker agent Go + engine C++/FFmpeg |
| Artifact verification & finalization | ✅ Attiva | SHA-256, retry budget, reconciler |
| Delivery orchestration | ✅ Attiva | provider registry (Drive, Social API esterna) |
| Telemetria operativa | ✅ Attiva | metrics, dashboards, audit, SLO |
| **Dark Editor** | ✅ **RIMOSSO** | separazione completata (vedi sotto) |
| **UI/SPA serving** | ✅ **RIMOSSO** | master headless, UI in VeloxFrontend |

---

## Storia recente (commit di riferimento su `main`)

### Chiusura piano Dark Editor (Committ 1–6)

| Fase | Commit | Contenuto |
|---|---|---|
| Commit 1 — runtime HTTP | `ce3412d7` | rimozione `/api/darkeditor`, `/api/v1/instaedit/editor`, proxy `/dark_editor_v2`, `DarkHandler`, test negativo route |
| Commit 2 — package | `6a36ae6a` | eliminato `internal/handlers/server/darkeditor/` |
| Commit 3 — persistence | `6a36ae6a` | eliminati `store_darkeditor_*.go` e tipi editor |
| Commit 4 — config/NVIDIA/scope | `62feefd4`, `32b60129` (+ InstaeditLogin `dc3ddd7`) | rimozione `NVIDIAConfig`, `VELOX_DARK_EDITOR_*`; migrazione scope `editor.*` → `jobs/workers/assets.*` |
| Commit 5 — migrazione | `3d31fcf7`, `eb59c7d2`, `8792d03c` | `128_drop_dark_editor_domain.sql` (6 DROP TABLE), `129_ensure_task_specs.sql`, retired/legacy checksum runner |
| Commit 6 — script/docs/CI | `8bc002bc`, `b7f5d96b`, `3cab8507`, `ab55f465` | audit YouTube, blacklist `check-no-legacy.sh`, changelog, LOC metrics |

Gate finale: **zero** residui (import, route, proxy, config, env, logging, store,
tabelle, scope) verificati con scansioni `rg`, `go list -deps`, suite race e
query `sqlite_master` su DB nuovo (0 tabelle `dark_editor%`).

### Warm-cache placement awareness (Agosto 2026)

| Commit | Contenuto |
|---|---|
| `6f649c8f` | `assetref.ExtractAssetKeys` — extractor canonico (`velox-asset://` + fallback Drive) |
| `b7da853c` | master: placement preferisce worker con asset caldi a parità di priorità; `CachedAssetKeys` da heartbeat/hello |
| `d3d7b8b7` | worker: lease many-to-many (`cached_asset_leases`), singleflight download, cleanup fail-safe, advertisement `asset_cache_keys` |
| `4a6ddcd1` | master headless: rimozione `FrontendModule`, SPA/static serving, `FrontendConfig` |

---

## Prossime fasi (in ordine di priorità)

1. **100% plan** — seguire i cinque documenti in `docs/100-percent-plan/`
   (runtime/CI, recovery, operazioni/security, distributed rendering) e
   riconciliare ogni voce completata con `main`.
2. **P2 distributed rendering** — `docs/architecture/distributed-rendering-roadmap.md`
   (RenderPlan, DAG, cache key deterministica, locality scoring, sharding).
3. **Warm-cache affinità** — telemetria dedicata (dashboards) per misurare il
   risparmio di download dagli asset caldi e tarare il tie-break di placement.
4. **Consolidamento auth** — eventuale `internal/auth/workerauthz` per
   unificare le allowlist HTTP+gRPC.

### Esplicitamente fuori scope di VeloxEditiingg

- Editor UI, progetti editor, sessioni utente → `InstaeditLogin`
- Frontend editor (Dark Editor / VeloxFrontend) → `VeloxFrontend`
- Social publishing UI/sessioni → `InstaeditLogin` (BFF) + Social API esterna
