# RW-PROD-011 — Network partition e duplicate suppression

**Priorità:** P0
**Dipendenze:** RW-PROD-009, RW-PROD-010
**Stato attuale:** Idempotenza via `(task_id, worker_id, lease_id)` tuple (canonical attempt contract). `taskgraph` reaper mantiene cleanup. **Gap**: nessun test E2E di 60s/120s offline, packet loss controllato, race tra reconn e attempt sostitutivo.

---

## 1. Pain points

1. **Nessun test "60 sec offline" / "120 sec offline".** Le simulazioni di partizione richiedono `tc` o firewall rule; non c'è script E2E.
2. **Reason code disconnessioni non uniforme.** `internal/logging/codes.go` ha `CodeMasterURLFallback` ma non un `CodePartitionDetected`.
3. **Race: reconn durante attempt sostitutivo.** Due attempt attivi dopo reconn veloce? Da verificare sequenza: worker vecchio attempt, master reaper fa X, worker reconnect offerta su stesso task → CAS deve rifiutare.

---

## 2. Soluzione

1. **`tests/e2e/network-partition/` (nuovo):**
   - `partition-offline.sh` — usa `iptables -A OUTPUT -d master_ip -j DROP` per 60s e 120s.
   - Verifica: nessun doppio artifact READY, nessun doppio Job SUCCEEDED, nessuna sessione zombie oltre 120s, reconn automatico.

2. **Reason code partition:**
   - `internal/logging/codes.go`: `CodePartitionDetected = "VELOX_W001"`, `CodeIdempotentReplay = "VELOX_W002"`.

3. **Race protection:**
   - `taskgraph.LifecycleService.Lease` deve fare refresh `attempt_number` se CAS fail per `ErrTransitionConflict` + recente `ExpireLease`.

---

## 3. Azioni concrete

| # | File:line | Azione |
|---|-----------|--------|
| A1 | `tests/e2e/network-partition/partition-offline.sh` (nuovo) | Wrapper `iptables` su compose network. |
| A2 | `tests/e2e/network-partition/assert.sh` | Assert NoDoubleArtifact, NoDoubleJob, AutomaticRecovery. |
| A3 | `DataServer/internal/logging/codes.go` | Aggiungere `CodePartitionDetected`, `CodeIdempotentReplay`. |
| A4 | `DataServer/internal/taskgraph/lifecycle.go` `Lease` | Su `ErrTransitionConflict` verificare se lease era expired di recente, retry. |
| A5 | `dashboards/network-resilience.json` | Pannello: reconn sotto partizione, session zombie count. |

---

## 4. Criteri di accettazione

- [ ] Nessun doppio artifact READY post-partition.
- [ ] Nessun doppio Job SUCCEEDED.
- [ ] Nessuna sessione zombie oltre finestra (5min).
- [ ] Recovery automatico entro SLO (30s dopo riconnessione).

---

## 5. Test obbligatori

- `TestNetworkPartition_60sOffline` — E2E.
- `TestNetworkPartition_120sOffline`.
- `TestNetworkPartition_PacketLossControlato`.
- `TestIdempotentReplay_TaskAccepted` (worker reinvia stesso `MsgTaskAccepted` per stesso `task_id`).
- `TestRace_DoubleAttemptDuringReconnect` — due worker provano stesso task.

---

## 6. Evidenze

- Log master `event=partition.detected worker_id=... duration_sec=...`.
- Report `tests/e2e/network-partition/last-run.json`.
- Timeseries `velox_worker_session_zombie_count`, `velox_worker_partition_total`.
