# RW-PROD-010 — Crash worker, lease expiry e retry

**Priorità:** P0
**Dipendenze:** RW-PROD-008
**Stato attuale:** `DataServer/internal/taskgraph/reaper.go` `TaskLeaseReaper` esegue sweep su task scaduti. `internal/store/sqlite_task_renew_test.go` copre 3 rejection path (`stale revision`, `wrong lease`, `empty identity`). `handler_workers.go:173` lease 30 min. **Gap**: nessun test SIGKILL reale su worker con render in flight.

---

## 1. Pain points

1. **No SIGKILL integration test.** I test attuali usano `taskgraph.Lifecycle` in-process. Manca test end-to-end che invia un job, manda SIGKILL al worker, verifica che:
   - `tasks.lease_expires_at` scade.
   - Il reaper crea nuovo `attempt` (canonical-attempt-identity).
   - Vecchio attempt status diventa `EXPIRED` o `STALE`.
   - Nuovo worker riuscirà a eseguire.
2. **Orphaned temp sui worker** dopo SIGKILL senza cleanup.
3. **Cleanup temp non documentato** post-crash.

---

## 2. Soluzione

1. **`tests/e2e/worker-crash/run.sh` (nuovo):**
   - Boot master + 2 worker.
   - Submit job lungo (es. scena di 60s).
   - Aspetta `RUNNING` state su worker A.
   - `docker kill -s SIGKILL workerA`.
   - Verifica: master reaper pianifica re-assegnazione al worker B, terminal OK.

2. **Cleanup temp post-mortem:**
   - `RemoteCodex/.../pkg/video/services/native/render_client.go` all'avvio lista `.tmp` in output dir, rimuove.

3. **Marker retry reason:**
   - `task_attempts.failure_reason` valorizzato: `worker_crash` per SIGKILL/recovery, vs `lease_expired` per timeout.

---

## 3. Azioni concrete

| # | File:line | Azione |
|---|-----------|--------|
| A1 | `tests/e2e/worker-crash/run.sh` (nuovo) | E2E test SIGKILL su worker busy. |
| A2 | `DataServer/internal/taskgraph/reaper.go` | Log reason `worker_crash_detected` quando lease reaped E task era RUNNING. |
| A3 | `RemoteCodex/native/worker-agent-go/internal/taskrunner/runner.go` (boot step, prima della goroutine di Run) | `cleanOrphanedTemp(outputDir)` per rimuovere `*.tmp` rimasti da SIGKILL/crash precedenti. |
| A4 | `DataServer/internal/store/store_attempts.go` | Aggiungere `failure_reason` se assente; valorizzare `worker_crash`. |
| A5 | `RemoteCodex/.../pkg/video/services/native/render_client.go` | Lista + rm `*.tmp` nella outputDir al Start (duplicato difensivo di A3 su due livelli). |
| A6 | `tests/e2e/worker-crash/assert.sh` | Assertions: 1 attempt nuovo, vecchio EXPIRED, no partial READY. |

---

## 4. Criteri di accettazione

- [ ] Nessuna task resta RUNNING indefinitamente dopo SIGKILL.
- [ ] Nuovo attempt creato una sola volta (no doppio).
- [ ] Vecchio attempt status terminal (EXPIRED/TIMED_OUT) — non può finalizzare.
- [ ] Job termina sul worker sostitutivo.
- [ ] Nessun artifact parziale READY.

---

## 5. Test obbligatori

- `TestWorkerCrash_PreTaskAccepted` (worker muore prima di accettare).
- `TestWorkerCrash_PostLeaseGrant` (in-flight).
- `TestWorkerCrash_DuringRender` (`render_client` mantiene lock su outputDir).
- `TestWorkerCrash_DuringUpload` (chunked upload parziale).

---

## 6. Evidenze

- E2E log con timeline: t0 submit, t1 RUNNING, t2 SIGKILL, t3 reaper job, t4 leased to worker B, t5 SUCCEEDED.
- Cleanup temp loggato `cleanup_orphaned_temp_files count=N`.
- Report JSON `tests/e2e/worker-crash/last-run.json` con i tempi.
