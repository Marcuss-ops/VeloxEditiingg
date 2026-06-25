# RW-PROD-005 — Stato canonico worker dal master

**Priorità:** P0
**Dipendenze:** RW-PROD-004
**Stato attuale:** `DataServer/internal/workers/registry_query.go:38` `ConnectionStatus()` definisce i 4 stati (CONNECTED / STALE / DISCONNECTED / DRAINING) e DRAINING wins. `DataServer/internal/handlers/server/api/workers_handler.go` sanitizza (no creds/cert paths/topology leaks). Quasi tutto in piedi. Da completare: esposizione di `protocol_version`/`engine_version`/`bundle_version`/`current_task`/`task_slots`/`active_tasks` nel payload; reason code per `STALE`; endpoint/filter per `worker_class` e `rollout_group`.

---

## 1. Pain points

1. **`SessionActive` + `ConnectionStatus` sono già calcolati** ma altri campi (`engine_version`, `bundle_version`, `current_task`) non sono diffusi in modo uniforme nella risposta. `docs/api/workers.md:41-100` descrive lo schema, ma in pratica il handler può droppare alcuni campi se assenti nel DB.

2. **`reason` non esposto.** Quando `status != CONNECTED` manca un campo stabile che indica perché (es. `disconnected_session`, `heartbeat_stale`, `drain`). Operatori guardano solo `status` e non hanno drill-down.

3. **`worker_class` e `rollout_group` non filitrabili.** `Inventory` Ansible con `worker_class`/group esiste (`deploy/inventory/production.ini.example`) ma non c'è modo di interrogare `GET /api/v1/workers?class=cpu-xlarge`.

4. **`current_task` non presente** — spec chiede di esporre `current_task` (id + executor) sui worker connessi che hanno almeno 1 task RUNNING.

5. **Sanitization robusta** — da verificare sotto fuzz (PII, IPv6, percorsi assoluti nei campi pubblici).

---

## 2. Soluzione

Estensione di `DataServer/internal/handlers/server/api/workers_handler.go` con:

1. **`WorkerResponse` arricchito**:
   ```go
   type WorkerResponse struct {
       WorkerID         string                 `json:"worker_id"`
       Status           string                 `json:"status"`           // CONNECTED | ...
       Reason           string                 `json:"reason,omitempty"` // detached_session | heartbeat_stale | drain | ...
       SessionActive    bool                   `json:"session_active"`
       HeartbeatAgeSec  int64                  `json:"heartbeat_age_sec"`
       ProtocolVersion  string                 `json:"protocol_version,omitempty"`
       EngineVersion    string                 `json:"engine_version,omitempty"`
       BundleVersion    string                 `json:"bundle_version,omitempty"`
       BundleHash       string                 `json:"bundle_hash,omitempty"`
       Executors        []ExecutorSummary      `json:"executors,omitempty"`
       TaskSlots        int32                  `json:"task_slots"`
       ActiveTasks      int32                  `json:"active_tasks"`
       CurrentTask      *TaskSummary           `json:"current_task,omitempty"`
       Class            string                 `json:"worker_class,omitempty"`
       RolloutGroup     string                 `json:"rollout_group,omitempty"`
       HostSummary      HostSummary            `json:"host,omitempty"` // NO IPs / NO secrets
   }
   ```

2. **`Reason` derivation** in `ConnectionStatusForInfo` (estensione):
   - `drain=true` → `reason="drain"` (precedence 1).
   - `!sessionActive` → `reason="detached_session"`.
   - `lastHB unparseable|very-old` → `reason="heartbeat_stale"`.
   - `active && fresh` → `reason=""`.

3. **Filtri nuovi**:
   - `GET /api/v1/workers?class=cpu-xlarge&status=CONNECTED&rollout_group=canary-2026q3`.

4. **`CurrentTask` lookup**: join con `task_attempts WHERE status='RUNNING' AND worker_id=?`.

5. **Fuzz test** di sanitization (vedi A6).

---

## 3. Azioni concrete

| # | File:line | Azione |
|---|-----------|--------|
| A1 | `DataServer/internal/handlers/server/api/workers_handler.go` `WorkerResponse` | Aggiungere campi `Reason`, `ProtocolVersion`, `EngineVersion`, `BundleVersion`, `BundleHash`, `Executors`, `TaskSlots`, `ActiveTasks`, `CurrentTask`, `Class`, `RolloutGroup`, `HostSummary`. |
| A2 | `DataServer/internal/workers/registry_query.go` `ConnectionStatus` | Aggiungere secondo ritorno `reason string` o refactor in `ConnectionStatusDetailed`. |
| A3 | `DataServer/internal/handlers/server/api/workers_handler.go` | Query param parsing: `?class=&status=&rollout_group=&needs_executor=` con validazione. |
| A4 | `DataServer/internal/handlers/server/api/workers_handler.go` | `LoadCurrentTask(workerID)` join `task_attempts JOIN tasks`. |
| A5 | `DataServer/internal/store/store_workers.go` | Aggiungere `Class` + `RolloutGroup` (campi DB + migrazione). |
| A6 | `DataServer/internal/handlers/server/api/workers_handler_test.go` | Test fuzz su HostSummary (`IP 127.0.0.1`, percorsi `/etc/passwd`, secret `abc…`). |
| A7 | `DataServer/internal/handlers/server/api/workers_handler.go` | Migration test su DB-level filter (`?class=` non ritorna cross-class se filter attivo). |
| A8 | `docs/api/workers.md` | Aggiornare schema canonico con nuovi campi e tabella `Reason`. |
| A9 | `RemoteCodex/.../internal/worker/worker.go` `buildHello` | Includere `worker_class` e `rollout_group` nel payload Hello. |

---

## 4. Criteri di accettazione

- [ ] Stream chiuso → `session_active=false` + `reason="detached_session"` senza aspettare solo 30s.
- [ ] Heartbeat vecchio con sessione attiva → `STALE` + `reason="heartbeat_stale"`.
- [ ] Drain attivo → `DRAINING` + `reason="drain"` (DRAINING wins).
- [ ] Nessun secret, token, cert path o credential hash nella risposta — test fuzz passa.
- [ ] `GET /api/v1/workers?class=cpu-xlarge&status=CONNECTED` filtra correttamente.
- [ ] `current_task` valorizzato per ogni worker con task RUNNING.

---

## 5. Test obbligatori

- `TestWorkerResponse_Sanitization` — secret, IP, percorsi sono filtrati.
- `TestWorkerResponse_SessionDetached_ReasonSet`.
- `TestWorkerResponse_HeartbeatStale_ReasonSet`.
- `TestWorkerResponse_Drain_ReasonSet`.
- `TestFilterByClass`, `TestFilterByStatus`, `TestFilterByRolloutGroup`.
- `TestCurrentTask_PopulatedWhenActive`.

---

## 6. Evidenze

- Snapshot `GET /api/v1/workers` filtrato per classe vs intero fleet.
- Diff payload prima/dopo (campi nuovi visibili solo se popolati).
- Log master con `reason` ogni volta che pubblica `status` non-CONNECTED.
