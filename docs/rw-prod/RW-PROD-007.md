# RW-PROD-007 — Canary mTLS per ogni worker remoto

**Priorità:** P0
**Dipendenze:** RW-PROD-001, RW-PROD-003, RW-PROD-005
**Stato attuale:** `tests/e2e/workload-mtls/run.sh` esiste ed è verde in CI (`e2e-workload-mtls.yml`). Copre il percorso reale (Hello → HelloAck → TaskOffer → TaskAccepted → TaskLeaseGranted → executor → TaskResult → artifact → Job=SUCCEEDED) e impone SHA-256 obbligatorio. **Gap**: non è rilanciabile on-demand per **un singolo worker specifico**, e non c'è un report JSON per worker in uscita (lo script fa fail/pass test ma non pubblica metriche).

---

## 1. Pain points

1. **Canary non è invocabile per singolo worker.**
   `workload-mtls/run.sh` esegue un singolo Docker compose (master + 2 worker). Non permette di puntare ad un host fisico già esistente con il proprio worker-agent in piedi.
2. **No report JSON per worker.**
   Output binario PASS/FAIL. Le evidenze richieste (worker_id, task_id, attempt_id, artifact sha, metrica master) non sono serializzate in `report.json`.
3. **Fixture CPU-only deterministica coperta?** Il canary usa una fixture E2E workload (`silent.mp3` + immagini). Specifica del ticket chiede "fixture CPU-only deterministica e breve" — verificare se è idempotente (FFmpeg cache determinism).
4. **NO fallback path:** la spec è già rispettata (PR `e2e-workload-mtls.yml` line 9: "NO insecure fallback"). Conferma con test.

---

## 2. Soluzione

1. **`velox-worker-agent canary --target HOST [--json]` (nuovo comando):**
   - Lato master (`DataServer/internal/handlers/remote/canary/`), endpoint `POST /api/v1/workers/:worker_id/canary` che:
     - Risolve `worker_id` → sessione attiva.
     - Scheduala un task "canary" (executor `canary.v1` con payload `{duration:1,fps:1,scene:"black 1s"}`).
     - Attende terminal status (≤ 60s).
     - Ritorna JSON: `{worker_id, job_id, task_id, attempt_id, artifact_id, artifact_sha256, status, error?, metrics_before, metrics_after}`.

2. **Fixture canary**:
   - Definire `RemoteCodex/.../internal/executor/canary/canary.go` con executor `canary.black-1s@1`.
   - Output: file MP4 1s nero con hash SHA-256 noto e committed in `tests/fixtures/canary_v1_baseline.sha256`.

3. **`scripts/run-canary.sh`**:
   - Wrapper che prende `WORKER_ID`, chiama `canary --target`, salva `report-${WORKER_ID}-$(date).json`, exit 0 solo se `status=SUCCEEDED, attempt=SUCCEEDED, artifact=READY, hash=battle`.
   - Integrato in `deploy/scripts/apply-local-worker-config.sh` come post-deploy step.

4. **`doctor` integration**: vedi RW-PROD-016 — il canary è uno dei check opzionali di `doctor`.

---

## 3. Azioni concrete

| # | File:line | Azione |
|---|-----------|--------|
| A1 | `RemoteCodex/.../internal/worker/canary/canary.go` (nuovo) | Executor `canary.black-1s@1` registrato via flag build (`VELOX_CANARY_EXECUTOR=true`). |
| A2 | `RemoteCodex/.../cmd/velox-worker-agent/main.go` (nuovo) | `canaryCmd` subcommand. |
| A3 | `DataServer/internal/handlers/remote/canary/` (nuovo) | Endpoint `POST /api/v1/workers/:worker_id/canary`. |
| A4 | `tests/e2e/canary/` (nuovo) | `run.sh` che esegue canary su 1 worker, verifica JSON shape. |
| A5 | `tests/fixtures/canary_v1_baseline.sha256` (nuovo) | SHA-256 baseline committato; golden test in CI. |
| A6 | `scripts/run-canary.sh` (nuovo) | Wrapper invocabile on-demand, exit codes documentati. |
| A7 | `deploy/scripts/apply-local-worker-config.sh` | Dopo applicazione config, eseguire `run-canary.sh` (gate). |
| A8 | `DataServer/cmd/server/bootstrap.go` | Wiring handler canary solo se `VELOX_ENABLE_CANARY_ENDPOINT=true`. |
| A9 | `docs/operations/03-build-deploy-and-ci-hardening.md` | Sezione "Canary gate prima della promotion" con esempi. |

---

## 4. Criteri di accettazione

- [ ] `run-canary.sh worker-01 --json` termina in ≤ 60s su worker sano.
- [ ] TaskAttempt associato al worker selezionato (verificato da `SELECT task_attempts.worker_id` ).
- [ ] Job `SUCCEEDED`, TaskAttempt `SUCCEEDED`, artifact `READY`, hash matches baseline.
- [ ] Metriche master `velox_compute_seconds_total{outcome="useful"}` incrementate di 1.
- [ ] Su worker con engine assente → Job `FAILED` con `error_code=engine_missing` e run-canary exit 5 ≠ 0.
- [ ] Su worker revocato → 403.

---

## 5. Test obbligatori

- `TestCanary_HealthyWorker` — end-to-end, JSON shape.
- `TestCanary_EngneMissing` — Job FAILED.
- `TestCanary_StaleWorker_NoNewTask` — 409 stale.
- `TestCanary_BaselineHashStable` — golden vector.
- `TestCanary_FixtureIdempotent` — rieseguire N volte → stesso hash.
- `TestRunCanary_BashArgs` — script bash in CI su compose.

---

## 6. Evidenze

- `report-${WORKER_ID}-${TS}.json` in `ops/canary-reports/`.
- Archive mensile su S3 del bucket canary (se configurato).
- Log master con `event=canary.assigned` `event=canary.succeeded` (campi strutturati).
