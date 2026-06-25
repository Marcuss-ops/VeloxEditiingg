# RW-PROD-006 — Sizing risorse e admission control

**Priorità:** P0
**Dipendenze:** RW-PROD-005
**Stato attuale:** `DataServer/internal/costmodel/cost.go` ha i gate transitori (draining / offline / at_capacity) e i 4 campi requirement → capability. `RemoteCodex/.../internal/worker/concurrency/concurrency.go` (PR-3.5) implementa `MaxActiveJobs` runtime con SetMaxActiveJobs. Mancano: reason-code specifici `capacity_full`/`disk_pressure`/`memory_pressure`, formula `max_active_jobs` per classe hardware, soglia disk free minima, logica di blocco nuove task sotto pressione.

---

## 1. Pain points

1. **Reason-code generico.** `cost.go:106` ha `"worker at capacity"`, ma si vuole `capacity_full` distinto (saturazione slot) da `memory_pressure` (RSS > X) e `disk_pressure` (disk free < Y).
2. **`max_active_jobs` non è derivato da misurazioni.** Il valore è impostato in `deploy/runtime/worker_config.example.json:21` a `1` hardcoded. Serve formula per classe HW (`cpu-small`, `cpu-medium`, `cpu-xlarge`) basata su misurazioni reali.
3. **Nessuna reservation 25-30% RAM/SWAP.** Worker può saturare il proprio RAM e iniziare swap.
4. **Nessun block nuove task sotto pressione.** Il sampler (`internal/telemetry/resource_sampler.go`) raccoglie RAM/Disk ma non è agganciato a un "deny new offers" gate.
5. **`active_tasks` può superare `task_slots`** se `Offer` arriva mentre `Sample.Concurrency.Acquire` non è ancora avvenuto.

---

## 2. Soluzione

1. **Classi hardware ufficiali**:
   - Definire in `RemoteCodex/.../pkg/config/config.go` enum `WorkerClass`:
     - `cpu-small` (2 vCPU, 4 GiB RAM)
     - `cpu-medium` (4 vCPU, 8 GiB RAM)
     - `cpu-xlarge` (8 vCPU, 16 GiB RAM)
   - Mapping da inventario Ansible (`deploy/inventory/production.ini.example`).

2. **Formula `max_active_jobs`**:
   - `max_active_jobs = max(1, floor((RAM_total * MB - RAM_reserved_MB - peak_render_RSS_MB) / peak_render_RSS_MB * 0.7))`
   - I valori storici da `resource_sampler.go` (peak RSS, current active).
   - `MAP` in `pkg/sizing/classes.go`.

3. **Reason code aggiuntivi in `costmodel.Score`**:
   - `capacity_full` (`active_tasks >= task_slots`)
   - `memory_pressure` (`worker.MemoryUsedBytes > 0.75 * worker.RAMBytes`)
   - `disk_pressure` (`worker.DiskFreeBytes < cfg.MinDiskFreeMB * 1MB`)

4. **Logic gate lato master** (PR-04.7):
   - `GetSchedulableWorkers` calcola anche i tre nuovi gate; ritorna vuoto + log warning quando operator triggers.

5. **Lato worker**:
   - `concurrency.SetMaxActiveJobs` viene chiamato al boot con la formula + ri-calcolato ad ogni `MsgConfigurationUpdate`.
   - `SetMaxActiveJobs(0)` se `memory_pressure || disk_pressure` (immediato).

---

## 3. Azioni concrete

| # | File:line | Azione |
|---|-----------|--------|
| A1 | `RemoteCodex/.../pkg/sizing/classes.go` (nuovo) | Enum `WorkerClass`, tabella mapping, `MaxActiveJobsFor(class, ramBytes, peakRSSBytes) int`. |
| A2 | `RemoteCodex/.../internal/worker/worker.go:31` | Al boot, calcolare `effectiveMaxJobs = sizing.MaxActiveJobsFor(class, ...)`; log + `cfg.MaxActiveJobs = effectiveMaxJobs`. |
| A3 | `DataServer/internal/costmodel/cost.go` `Score` | Aggiungere tre check dopo `MaxParallel`. Introdurre struttura `PressureState` per distinguerli. |
| A4 | `DataServer/internal/costmodel/worker_profile.go` | Aggiungere campi `RAMBytes`, `MemoryUsedBytes`, `DiskFreeBytes`, `MemoryPressure bool`, `DiskPressure bool`. |
| A5 | `RemoteCodex/.../internal/telemetry/resource_sampler.go` | Pubblicare `Latest().MemoryPressure` e `Latest().DiskPressure` aggiornati ad ogni tick. |
| A6 | `RemoteCodex/.../internal/worker/worker.go` receiveLoop `MsgConfigurationUpdate` | Al ricevimento `memory_pressure`, applicare `SetMaxActiveJobs(0)`. |
| A7 | `pkg/config/config.go` | Aggiungere `MinDiskFreeMB int` (default 512) + `WorkerClass string`. |
| A8 | `deploy/runtime/worker_config.example.json` | Documentare `worker_class` e `min_disk_free_mb` (non più solo `max_active_jobs`). |
| A9 | `scripts/benchmark-classes.sh` (nuovo) | Esegue 50 job piccoli+medi+pesanti per classe, registra peak RSS/iowait/wall-time, scrive YAML per `pkg/sizing/classes.go`. |
| A10 | `scripts/ci/check-architecture.sh` | Aggiungere guard: nessun hardcoded `max_active_jobs=2` nei test fixtures senza una `worker_class` equivalente. |

---

## 4. Criteri di accettazione

- [ ] Nessun OOM durante test massimo supportato per classe.
- [ ] Nessuna crescita swap continua (`velox_worker_swap_bytes` flat).
- [ ] `active_tasks` non supera mai `task_slots` (verificato via Prometheus query).
- [ ] Worker rifiuta nuove task su memory_pressure o disk_pressure con reason code visibile in `/api/v1/workers`.
- [ ] Soglie documentate per classe in `deploy/runtime/worker_config.example.json` + tabella `docs/sizing.md`.

---

## 5. Test obbligatori

- `TestCostMemoryPressure_ReasonSet`.
- `TestCostDiskPressure_ReasonSet`.
- `TestCostCapacityFull_ReasonSet` (distinto da at_capacity).
- `TestSizingCpuSmall` (RAM=4GiB, peakRSS=1.5GiB → max_jobs=1).
- `TestSizingCpuXlarge` (RAM=16GiB, peakRSS=2GiB → max_jobs=4-5).
- `TestWorkerMemoryPressure_RejectsNewTasks` (mock sampler, verifica SetMaxActiveJobs(0)).
- `TestBenchmarkClasses` (CI nightly, non required-pass).

---

## 6. Evidenze

- Output YAML `pkg/sizing/classes.go` generato da `benchmark-classes.sh` → commit.
- Timeseries Grafana dashboard `dashboards/sizing.json` con `velox_worker_rss_bytes`, `velox_worker_swap_bytes`, `velox_worker_disk_free_bytes`, `velox_cost_total_per_output_minute{worker_class="..."}`.
- Report post-benchmark firmato per classe HW.
