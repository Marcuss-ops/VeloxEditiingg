# RW-PROD-017 — Rollout, promotion e rollback worker

**Priorità:** P0
**Dipendenze:** RW-PROD-015, RW-PROD-016
**Stato attuale:** `DataServer/internal/handlers/remote/ansible/deploy.go:45` `buildDeployPlan` con `canary_percent` + batch. `scripts/bump-version-and-deploy.sh:127` chiama `deploy_workers` con canary. `deploy/playbooks/rollback.yml` esiste. **Gap**: integrare `doctor --production` + canary reale come gate obbligatorio prima della promozione, versionamento immagine digest, anti-rebuild tra staging/prod.

---

## 1. Pain points

1. **Nessun gate `doctor` automatico prima del rollout.** `bump-version-and-deploy.sh` esegue deploy diretto.
2. **Anti-rebuild non enforced.** Stesso digest? `deploy/runtime/compose.yml:19` accetta `VELOX_WORKER_IMAGE=...@sha256:...`. Nessun check `build_hash == last_deployed` su master.
3. **Rollback playbook** esiste ma non è testato integration.
4. **Rollout mixed (vecchia/nuova immagine) live** non documentato con rischi.

---

## 2. Soluzione

1. **Gate obbligatorio `doctor`:**
   - `scripts/bump-version-and-deploy.sh` aggiungere step `velox-worker-agent doctor --production --json` su host canary → exit 0 obbligatorio.
   - In caso fail: abort, no promozione.

2. **Image digest versioning:**
   - `deploy/runtime/worker.env.example` aggiungere `VELOX_WORKER_IMAGE=ghcr.io/marcuss-ops/velox-worker@sha256:digest`.
   - `RemoteCodex/BUILD_INFO.json` deve contenere digest usato.
   - Master `worker_image` campo in DB tabella `worker_deploys` con `digest`, `commit_sha`, `rollout_started_at`, `promoted_at`.

3. **Ansible playbook `promote-canary.yml`:**
   - Sequence: identify canary host → run `doctor --production` → run `canary.sh` (RW-PROD-007) → observe metrics → promote by class/percentage.
   - Failure: rollback to previous digest.

4. **Rollback testable:**
   - `tests/e2e/rollback/` E2E che promuove image v2, osserva fallimento (e.g., fake `engine missing`), esegue rollback automatico a v1.

5. **Vietare rebuild:**
   - In CI: `make build-worker TAG=vX` produce uno specifico digest. CI verifica che `BUNDLE_HASH.txt` sia lo stesso durante staging e production build (debounce con label).

---

## 3. Azioni concrete

| # | File:line | Azione |
|---|-----------|--------|
| A1 | `scripts/bump-version-and-deploy.sh` | Aggiungere step `doctor --json` su canary host, fail-fast. |
| A2 | `deploy/runtime/worker.env.example` | Aggiungere `VELOX_WORKER_IMAGE_DIGEST` env. |
| A3 | `DataServer/internal/store/migrations/` (nuovo) | Tabella `worker_deploys` (digest, commit, started_at, promoted_at). |
| A4 | `deploy/playbooks/promote-canary.yml` (nuovo) | Playbook orchestrazione canary → canary job → promote. |
| A5 | `tests/e2e/rollback/run.sh` (nuovo) | E2E test rollback. |
| A6 | `scripts/check-no-rebuild.sh` (nuovo) | Diff build artifact stub vs staged prod build. |
| A7 | `DataServer/internal/handlers/remote/ansible/deploy.go` | Aggiungere `WorkerImageDigest` field. |
| A8 | `DataServer/internal/handlers/server/api/rollouts/` (nuovo) | Endpoint `GET /api/v1/rollouts` per inspect. |
| A9 | `docs/operations/03-build-deploy-and-ci-hardening.md` | Sezione "Rollout: anti-rebuild + canary gate". |

---

## 4. Criteri di accettazione

- [ ] Canary worker verde (`doctor = READY`, canary = PASS).
- [ ] Nessun aumento failure rate.
- [ ] Nessun aumento fallback / emergency.
- [ ] Rollback completabile **senza rebuild** (riuso stesso digest precedente).
- [ ] Nessun job perso durante aggiornamento.
- [ ] Report rollout archiviato (digest, commit, canary metrics p95, fail rate).
- [ ] Nessun rebuild tra staging e production (digest identico o fail-fast).

---

## 5. Test obbligatori

- `TestPromote_DependsOnDoctorPass`.
- `TestRollback_RestoresToLastDigest`.
- `TestAntiRebuild_DifferentDigestBetweenStages_Fails`.
- `TestRolloutMixed_OldAndNewLive_NoJobLoss`.
- `TestCanaryFailure_AutoRollback`.

---

## 6. Evidenze

- `rollout-${ID}-${TS}.json` con digest, commit, canary metrics, fail rate, durations.
- `worker_deploys` DB rows populate on every promotion.
- HTTP endpoint `GET /api/v1/rollouts` consultabile.
- Dashboard `rollouts.json` con timeline + status.
