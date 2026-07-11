# RW-PROD-012 — Drain, SIGTERM e cancellazione processi

**Priorità:** P0
**Dipendenze:** RW-PROD-010
**Stato attuale:** Master `deploy/velox-server.service` `TimeoutStopSec=60`. Worker `data/ansible/playbooks/tasks/normalize_worker_systemd.yml:347` `TimeoutStopSec=35` — incoerente. `RemoteCodex/.../pkg/video/services/native/render_client.go:135` fa `syscall.Kill(-pid, SIGTERM)` al C++. `internal/worker/worker.go` `runSession` su `stopChan` o `ctx.Done()` imposta `ConnDraining`. **Gap**: escalation TERM→KILL, cleanup temp post-kill, garantire che artifact incompleti non siano marcati READY.

---

## 1. Pain points

1. **TERM→KILL escalation assente.** `render_client.go:135` invia SIGTERM ma se il processo C++ non termina nel grace period, non c'è escalation SIGKILL.
2. **Cleanup temp post-kill non documentato.** Se escalation uccide il C++ durante scrittura, `.tmp` rimangono.
3. **`TimeoutStopSec` incoerente (master 60 vs worker 35).** Da uniformare.
4. **`/health/ready` deve diventare false appena drain avviato** (vedi RW-PROD-004).
5. **`msg.LicenseRevoked` non gestito su drain** — se il master revoca mentre stiamo drainando.

---

## 2. Soluzione

1. **`render_client.go` escalation:**
   - Timer `KillGracePeriod = 20s` configurabile.
   - Trascorso: `syscall.Kill(-pid, SIGKILL)` + log `escalation_kill_fired reason=timeout`.

2. **`runSession` post-escalation:**
   - `w.cleanupOrphanedTemp()` (già RW-PROD-010 punto 3).
   - Non finalizzare artifact se `state.staged == true && SHA == ""`.

3. **`TimeoutStopSec` uniforme:**
   - Master: `TimeoutStopSec=120` (imposta default + bundle runtime).
   - Worker: `TimeoutStopSec=120` (corrispondente al massimo grace + escalation).
   - In `data/ansible/playbooks/tasks/normalize_worker_systemd.yml:347` cambiare 35 → 120.

4. **Drain state machine:**
   ```text
   CONNECTING → CONNECTED → DRAINING → STOPPED
                ↓ (trigger MsgDrain o SIGTERM)
                ConnDraining, /health/ready=503
   ```

5. **Reason code `state=stopping`:**
   - Su SIGTERM, strutturato `{worker_id, job_id, task_id, attempt_id}` in tutti i log.

---

## 3. Azioni concrete

| # | File:line | Azione |
|---|-----------|--------|
| A1 | `RemoteCodex/.../pkg/video/services/native/render_client.go:135` | Wrappa `Kill` in timer; escalation KILL dopo `KillGracePeriod`. |
| A2 | `RemoteCodex/.../internal/worker/worker.go` | Aggiungere `cleanupOrphanedTemp`. |
| A3 | `data/ansible/playbooks/tasks/normalize_worker_systemd.yml:347` | `TimeoutStopSec=120`. |
| A4 | `deploy/velox-server.service` | `TimeoutStopSec=120`. |
| A5 | `RemoteCodex/.../internal/worker/worker.go` `runSession` su stopChan | Già imposta ConnDraining + StatusStopped; assicurarsi transizione `/health/ready` 503. |
| A6 | `DataServer/internal/workers/registry_register.go` `revokePriorSessions` | Continuare a invocare quando drain. |
| A7 | `dashboard/draining.json` | Pannello count worker in DRAINING + drain duration. |
| A8 | `docs/operations/03-build-deploy-and-ci-hardening.md` | Sezione "Worker SIGTERM lifecycle" con tempi. |

---

## 4. Criteri di accettazione

- [ ] Nessuna nuova task accettata in drain (`sendTaskRejected(draining)`).
- [ ] Job breve termina pulitamente prima di escalation.
- [ ] Job lungo terminato secondo policy (SIGKILL dopo 20s).
- [ ] Nessun processo C++ orfano (`pgrep velox-render-cpp` = 0 post-stop).
- [ ] Nessun artifact parziale READY.
- [ ] Processo esce entro `TimeoutStopSec`.

---

## 5. Test obbligatori

- `TestDrain_Idle_SIGTERM_ExitsClean`.
- `TestDrain_Running_SIGTERM_GracefulStop`.
- `TestDrain_Running_SIGKILL_WorkerEscalates`.
- `TestDrain_DuringUpload_NoPartialReady`.
- `TestEscalation_KillFired_Logs`.

---

## 6. Evidenze

- Log `event=shutdown.escalation.kill pid=... reason=timeout`.
- Report `tests/e2e/drain/last-shutdown.json` con timeline SIGTERM → exit.
- Grafana: `velox_worker_drain_seconds` p95/p99.
