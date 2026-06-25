# RW-PROD-009 — Riconnessione dopo restart master

**Priorità:** P0
**Dipendenze:** RW-PROD-005
**Stato attuale:** `RemoteCodex/.../internal/worker/worker.go:38` — fresh transport per ogni session attempt, backoff crescente, connection-level vs application-level distinction. `docs/roadmap/04-re-registration-loop.md` documenta il comportamento. **Gap**: nessun test integrato che fa restart master con worker busy e verifica che la vecchia sessione sia invalidata sul master e readiness transitorio false.

---

## 1. Pain points

1. **Vecchia sessione non invalidata esplicitamente.** Quando il master riavvia, le righe `worker_sessions` con `revoked=0` restano tali finché `CleanupExpiredSessions` (24h, `internal/store/store_worker_control.go:335`) non le raccoglie. Il worker reinvia Hello, ottiene nuova `session_id`, ma sul master possono coesistere due session attive per lo stesso worker per alcuni secondi.
2. **Readiness transitorio durante blackout.** `internal/workers/worker.go` imposta `ConnDisconnected`, ma `/health/ready` (RW-PROD-004) deve riflettere la transizione. Verificare.
3. **Backoff senza jitter deterministico.** `worker.go:79` usa `rand.Float64()` — non deterministico, accettabile ma test deve mockare.
4. **Nessun test integrato di restart master.** Da scrivere.

---

## 2. Soluzione

1. **Invalidazione sessione precedente all'handshake successivo:**
   - Lato master, durante `Hello` processing, `UPDATE worker_sessions SET revoked=1, revoked_at=now WHERE worker_id=? AND session_id != ? AND revoked=0` (in-tx).
   - Aggiungere `DataServer/internal/workers/registry_register.go` settare la chiusura.

2. **`READY=false` durante blackout:**
   - Su master side: ready endpoint già gestisce con `booted` (master-side). Il "worker visible on master" check è gestito da RW-PROD-005 (`HasAtLeastOneLive`).

3. **Test integrato in `tests/e2e/recovery-master-restart/run.sh`:**
   - Boot master + worker, assegna job.
   - `docker restart master` o `kill -9` del processo master.
   - Verifica che il worker ri-connetta, il master abbia UNA sola sessione attiva, e nessun job duplicato.

4. **Soglie recovery entro SLO:**
   - Definire `RecoverySLO = 30s` (master restart + worker reconn). Misurare in CI loop.

---

## 3. Azioni concrete

| # | File:line | Azione |
|---|-----------|--------|
| A1 | `DataServer/internal/workers/registry_register.go` (o dove si gestisce `Hello`) | Aggiungere `revokePriorSessions(tx, workerID, except sessionID)` al commit del nuovo sessione. |
| A2 | `tests/e2e/recovery-master-restart/` (nuovo) | Script che fa restart master durante job in flight. |
| A3 | `tests/e2e/recovery-master-restart/assert.sh` | Asserzioni: 1 session attiva, no orphan commands, TaskAttempt transitioned. |
| A4 | `scripts/ci/golden-e2e.sh` | Aggiungere suite E2E master-restart. |
| A5 | `DataServer/internal/store/store_worker_control.go` `CleanupExpiredSessions` | Valutare se accorciare il window (24h → 5min) per pulizia sessioni stale post-restart. |
| A6 | `RemoteCodex/.../internal/worker/worker.go` `runSession` | Su sessionCtx.Done() immediato → `SetHealthRegistered(false)`. |
| A7 | `RemoteCodex/.../docs/roadmap/04-re-registration-loop.md` | Aggiornare con il nuovo invariant. |
| A8 | `dashboards/worker-recovery.json` (nuovo) | Pannello Recovery Time p95/p99 + Restart count. |

---

## 4. Criteri di accettazione

- [ ] Riconnessione senza riavvio worker.
- [ ] Un'unica sessione attiva dopo recovery (entro 30s).
- [ ] Nessun job duplicato.
- [ ] Nessun task perso (acknowledged ma mai finalizzato).
- [ ] Tempo di recovery entro RecoverySLO (30s p95) misurato in CI.

---

## 5. Test obbligatori

- `TestMasterRestart_IdleWorker_Reconnects`.
- `TestMasterRestart_BusyWorker_RecreatesSession`.
- `TestMasterRestart_DoubleSession_InvalidatedOnHello`.
- E2E soak: 10 restart master consecutivi, recovery time <= SLO ogni volta.

---

## 6. Evidenze

- Log master `event=session.revoke.previous worker_id=... session_id=...`.
- Timeseries `velox_worker_recovery_seconds`, `velox_worker_double_session_total{status="revoked"}`.
- Report `tests/e2e/recovery-master-restart/last-run.json`.
