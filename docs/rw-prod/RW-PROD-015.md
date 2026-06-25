# RW-PROD-015 — Soak test e matrice di certificazione hardware

**Priorità:** P0
**Dipendenze:** RW-PROD-006, RW-PROD-007, RW-PROD-009, RW-PROD-010, RW-PROD-011, RW-PROD-012
**Stato attuale:** `tests/e2e/soak-partition/test_recovery.sh` esiste (recovery-oriented), nessun soak duraturo 24h per classe. **Gap**: matrice hardware certificazione, firma digitale report, finestra soak non documentata, integrazione con `scripts/benchmark-classes.sh` (RW-PROD-006).

---

## 1. Pain points

1. **Nessun soak 24h per classe.** Repository ha test brevi (<5min) e "soak-partition" che fa recovery, non durata.
2. **Nessun firmato output.** Report JSON firmato digitalmente per worker? Attualmente solo JSON testuale.
3. **Matrice HW non codificata.** Inventory Ansible ha `worker_class` ma non tabella classe → spec / RAM / swap / disk.
4. **Gate numerici del runbook (§15):** job persi = 0; doppio READY = 0; OOM = 0; etc. — non esiste script `verify-soak-gates`.
5. **Inclusione mancanti scenari:** cache cold/warm, restart master, restart worker, network partition, drain, SIGTERM — non sono tutti.

---

## 2. Soluzione

1. **`scripts/soak-run.sh`**:
   - Per ogni classe registrata (via `inventory`):
     - 24h di carico rappresentativo (job piccoli 40%, medi 40%, pesanti 20%).
     - Cold start + warm cache phases.
     - Restart master × 3 random all'interno.
     - Restart worker × 1.
     - Partition 60s × 1.
     - Drain × 1.
   - Output JSON `report-worker-${WORKER_ID}-${TS}.json`.

2. **`scripts/verify-soak-gates.sh`**:
   - Legge report JSON, applica gate numerici (vedi sotto), exit 0 solo se tutti soddisfatti.

3. **Firma report:**
   - SHA-256 del report + `gpg --sign` (pubky disponibile in vault).
   - Conservazione in `ops/soak-reports/`.

4. **Matrice hardware in `deploy/inventory/hardware_matrix.yml`:**
   ```yaml
   classes:
     cpu-small: {min_ram_gb:4, max_active:1, recommended_swap_gb:2}
     cpu-medium: {min_ram_gb:8, max_active:2, recommended_swap_gb:4}
     cpu-xlarge: {min_ram_gb:16, max_active:4, recommended_swap_gb:8}
   ```

5. **Output standardizzato:**
   ```json
   {"worker_id","class","soak_start","soak_end","jobs_total","jobs_succeeded","jobs_failed","p50_render_s","p95_render_s","p99_render_s","oom_count","disk_full_count","tasks_stuck_count","reconnect_count","fallback_count","python_emergency_count","verdict"}
   ```

---

## 3. Azioni concrete

| # | File:line | Azione |
|---|-----------|--------|
| A1 | `scripts/soak-run.sh` (nuovo) | Driver soak per worker. |
| A2 | `scripts/verify-soak-gates.sh` (nuovo) | Gate check su report JSON. |
| A3 | `deploy/inventory/hardware_matrix.yml` (nuovo) | Tabella classi. |
| A4 | `docs/operations/03-build-deploy-and-ci-hardening.md` | Sezione "Soak test workflow" con gate. |
| A5 | `tests/e2e/soak/` (nuovo) | Job fixtures (piccoli/medi/pesanti). |
| A6 | `tests/e2e/soak/fixtures/` | 3 fixture deterministic. |
| A7 | `data/ansible/playbooks/tasks/run_soak.yml` (nuovo) | Ansible playbook che lancia soak per host. |
| A8 | `scripts/soak-aggregate.sh` (nuovo) | Aggrega più worker report. |

---

## 4. Gate numerici minimi

- [ ] job persi = 0
- [ ] artifact doppi READY = 0
- [ ] artifact corrotti = 0
- [ ] OOM = 0
- [ ] disk full inattesi = 0
- [ ] task senza stato terminale = 0
- [ ] reconnessioni manuali = 0
- [ ] fallback production = 0
- [ ] python_emergency = 0
- [ ] success rate canary = 100%
- [ ] success rate carico normale ≥ 99%
- [ ] active tasks mai oltre slots
- [ ] nessun throttling termico persistente

---

## 5. Test obbligatori

- `TestSoak_SmallClass_24h_GatesPass` (su stage hardware surrogato).
- `TestSoak_RestartMaster_NoLostJob`.
- `TestSoak_Partition60s_AutoRecovery`.
- `TestVerifyGates_JSONValid` (`verify-soak-gates.sh` exit codes).
- `TestAggregate_MultiWorkerOK`.

---

## 6. Evidenze

- `report-worker-${ID}-${TS}.json` firmato GPG.
- `ops/soak-reports/index.json` aggregato.
- Dashboard Grafana `dashboards/soak-status.json`.
- Report di riepilogo mensile a `sre@velox.io`.
