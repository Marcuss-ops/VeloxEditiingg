# 🚀 Velox — Deploy Checklist

> **Stato:** architettura separata (Velox=render farm, InstaeditLogin=editor/BFF,
> VeloxFrontend=UI). Riferimenti: `README.md`, `deploy/velox-server.env.example`,
> `deploy/runtime/worker.env.example`, `docs/worker_deployment.md`.
>
> Questa checklist copre il deploy del **master Velox** (`VeloxEditiingg`).
> L'editor (UI + BFF + utenti) si deploya separatamente da `InstaeditLogin` /
> `VeloxFrontend` e consuma Velox via `/api/v1/instaedit/*`.

---

## 0. Prerequisiti architetturali

- [ ] `VeloxEditiingg` su `main` aggiornato (ultimo commit di riferimento)
- [ ] **Nessun residuo editor nel master**: verifica rapida
      ```bash
      rg -n '/api/darkeditor|/dark_editor_v2|/api/v1/instaedit/editor' DataServer deploy scripts || echo OK
      ```
- [ ] **Nessuna UI servita dal master**: master headless, niente
      `VELOX_SPA_DIR` / `VELOX_GRADIO_APP_URL` / `FrontendModule`
- [ ] Schema DB: **migrazioni 128+ applicate** (nessuna tabella `dark_editor_*`)
      ```bash
      sqlite3 /path/to/velox.db \
        'SELECT COUNT(*) FROM sqlite_master WHERE type="table" AND lower(name) LIKE "dark_editor%";'
      # → 0
      ```

---

## 1. Secret & env master

File: `deploy/velox-server.env.example` → `/etc/velox-server.env` sul master.
Placeholder `CHANGE_ME_*` mai copiati in produzione.

### Obbligatori
| Variabile | Note |
|---|---|
| `VELOX_ADMIN_TOKEN` | admin routes + creator push |
| `INSTAEDIT_CONTROL_JWT_SECRET` | ≥32 byte, **condiviso con InstaeditLogin** (minter) — un mismatch = 401/403 su tutte le chiamate BFF |
| `VELOX_ALLOWED_WORKERS` | allowlist worker (no `*` in produzione) |
| `VELOX_WORKER_*` / credenziali worker | registry + credential validation |
| `ALERT_WEBHOOK_URL` | Slack webhook per monitoring |

### Da NON impostare più
- ❌ `VELOX_JOB_MASTER_URL` (proxy Drive legacy rimosso; le route `/api/drive/*` sono servite dal modulo Drive canonico)
- ❌ `VELOX_DARK_EDITOR_DIR`, `VELOX_DARK_EDITOR_PROXY_URL` (rimossi)
- ❌ `VELOX_NVIDIA_API_KEY`, `VELOX_NVIDIA_TEXT_URL` (rimossi)
- ❌ `VELOX_SPA_DIR`, `VELOX_GRADIO_APP_URL` (UI non più servita dal master)

---

## 2. Contratto BFF (prima del deploy del master)

Il segreto JWT e gli scope devono essere allineati con InstaeditLogin:

```text
InstaeditLogin  ──mint──►  JWT (iss=instaedit, aud=velox, workspace_id, scopes)
VeloxEditiingg  ──verify─►  iss/aud/exp/workspace_id + scope per route
```

| Chiamata BFF | Scope richiesto (Velox) | Scope mintato (InstaeditLogin) |
|---|---|---|
| ListJobs / GetJob / ListJobDeliveries | `jobs.read` | `jobs.read` |
| CreateJob / CancelJob | `jobs.write` | `jobs.write` |
| ListWorkers / GetWorker | `workers.read` | `workers.read` |
| GetAsset | `assets.read` | `assets.read` |

- [ ] Verifica parità scope:
      ```bash
      rg 'ScopeVelox' InstaeditLogin/internal/veloxcontract/contract.go
      rg 'Scope' VeloxEditiingg/DataServer/internal/instaeditauth/scopes.go
      ```
- [ ] `INSTAEDIT_CONTROL_JWT_SECRET` identico sui due lati

---

## 3. Database

- [ ] Migrazioni SQLite applicate in ordine (runner fail-closed su checksum
      sconosciuti; la 001/040/044 storica è tollerata via retired checksums)
- [ ] Schema finale: assenti `dark_editor_*`; presenti `jobs`, `artifacts`,
      `job_deliveries`, `calendar_events`
- [ ] Backup pre-deploy + test di restore
- [ ] (Se upgrade da DB con Dark Editor) la 128 è **irreversibile** — dati
      editor non recuperabili dal master

---

## 4. Worker fleet

Vedi `docs/worker_deployment.md` — "Minimum Remote Worker Configuration".

- [ ] Ogni worker: `VELOX_WORKER_ID` in allowlist, `VELOX_GRPC_MASTER_URL`,
      `VELOX_WORKER_SECRET`, TLS PEM (14gg residui minimi, 0600), backend render
- [ ] Worker image aggiornata (nuova logica cache/lease many-to-many)
- [ ] `worker.env.example` senza riferimenti `dark_editor`
- [ ] Smoke Level-D (lease→ffmpeg→artifact→delivery) su almeno 1 worker

---

## 5. Sequenza di deploy consigliata

```text
1. InstaeditLogin BFF  (nuovo vocabolario scope — se non già live)
2. Master Velox       (migrazioni 128/129 + nuove route instaedit)
3. Workers            (nuova immagine worker-agent-go)
4. VeloxFrontend      (UI, senza path /dark_editor_v2 verso il master)
```

> ⚠️ Se il master viene aggiornato prima del BFF, il BFF vecchio (scope
> `editor.*`) riceve **403 insufficient scope** — fail-fast voluto. Deploy
> BFF→master nello stesso rollout.

---

## 6. Smoke test post-deploy

```bash
# Master health + readiness
curl -sS -H "Authorization: Bearer ${VELOX_ADMIN_TOKEN}" "${VELOX_MASTER_URL}/health/ready"

# Canary locale (se configurato)
bash deploy/runtime/submit-canary-local.sh

# BFF: ListJobs con JWT mintato dal BFF (deve rispondere 200)
curl -sS -H "Authorization: Bearer <bff-issued-token>" \
  "${VELOX_MASTER_URL}/api/v1/instaedit/jobs" | jq

# 403 enriched atteso con scope mancante (verifica diagnostica)
curl -sS -H "Authorization: Bearer <token-senza-jobs.write>" \
  -X POST "${VELOX_MASTER_URL}/api/v1/instaedit/jobs" | jq
# → error=insufficient scope, required_scopes=[jobs.write], presented_scopes=[...]

# Route editor assenti
curl -sS -o /dev/null -w '%{http_code}\n' "${VELOX_MASTER_URL}/api/darkeditor/dark_editor_v2"
# → 404
```

---

## 7. Rollback

- [ ] `git revert` dell'ultimo commit + push (re-deploy automatico) oppure
      promote del penultimo deploy verde
- [ ] Env vars: `npx vercel env rm <KEY>` solo per la UI/BFF (master su VPS)
- [ ] DB: migrazioni SQLite non retrocedono; per il master usare snapshot/
      backup pre-deploy (la 128 è forward-only)

---

## 8. Verifica finale

```bash
cd VeloxEditiingg
git status -sb
git log --oneline -5

# Gate residui Dark Editor (definizione di done)
rg -n -i 'dark[ _-]?editor|darkeditor|DARK_EDITOR|VELOX_DARK_EDITOR|CodeDarkEditor' \
  --glob '!CHANGELOG.md' --glob '!docs/CHANGELOG.md' --glob '!docs/metrics/**' \
  --glob '!docs/adr/**' --glob '!docs/archive/**' --glob '!scripts/ci/check-no-legacy.sh' \
  --glob '!DataServer/internal/store/migrations/**' --glob '!deploy/scripts/audit-no-youtube-residuals.sh' \
  . || echo "ZERO RESIDUI (ok)"
```
