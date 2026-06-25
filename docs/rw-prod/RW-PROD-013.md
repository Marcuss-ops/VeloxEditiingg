# RW-PROD-013 — Metriche, log e alert operativi

**Priorità:** P0
**Dipendenze:** RW-PROD-005, RW-PROD-006
**Stato attuale:** `RemoteCodex/.../internal/telemetry/metrics_types.go:151` espone `velox_fallback_count_total` + `velox_python_emergency_path_total`. `DataServer/internal/metrics/collector.go` ha metriche per worker ma unità miste (alcuni ms interpretati come s). `alerts/spec-14-compute-outcomes.yml` alert parziali. `docs/operations/PR-6-pki-rotation-runbook.md:215` `alert-cert-expiry.sh` **TODO non versionato**. **Gap**: unit test conversione unità, alert cert expiry e fallback/emergency paths, correlazione `worker_id + job_id + task_id + attempt_id + session_id` su ogni log, protezione porte Prometheus/health.

---

## 1. Pain points

1. **Unit audit non automatizzato.** Metriche eterogenee: `velox_worker_heartbeat_age_seconds` (s) bene, ma campioni interni come `velox_worker_temp_bytes` (byte sui worker) arrivano in JSON al master senza unit conversion.
2. **`alert-cert-expiry.sh` non esiste.** Esiste solo `monitor-expiry.sh` (RW-PROD-014 farà fail-closed), ma manca il wrapper che pubblica su Alertmanager.
3. **Correlazione log:** `pkg/logger` ha `LogRegisterFailed/CertRejected/SignalReceived`, ma non un campo universale `slog.With("worker_id",...)` per garantire propagation su tutti i log.
4. **Porte health/metrics pubbliche.** `deploy/runtime/compose.yml` ha `network_mode: host` o pubblica porte; da vincolare con firewall/VPN.

---

## 2. Soluzione

1. **Unit audit Prometheus (`scripts/audit-prom-units.sh`):**
   - Parsa `prometheus/` `dashboards/` `alerts/` per nomi metrici.
   - Confronto con mapping canonico in `docs/metrics-units.md`.
   - Diff → CI failure.

2. **`scripts/alert-cert-expiry.sh`:**
   - Wrapper per monitor-expiry `--json`, evaluation soglie 14/7/2 giorni, pubblica su `/api/v1/alerts` master.
   - Owner: `security.sre`.

3. **Correlazione log obbligatoria:**
   - `pkg/logger.LogCtx(ctx)` con `worker_id`, `job_id`, `task_id`, `attempt_id`, `session_id` propagato via context.
   - Verifica su test che ogni log in `pkg/logger` includa almeno il correlation set.

4. **Porte protezione:**
   - `deploy/runtime/compose.yml` mountare `network_mode` o usare `expose` invece di `ports`.
   - Aggiungere playbook Ansible `firewall-open-metrics.yml`.

5. **Alert sentinella:**
   - `VeloxFallbackEverUsed` (YAML in alerts/spec-14-compute-outcomes.yml estensione).
   - `VeloxPythonEmergencyEverUsed`.
   - `VeloxCertExpiringSoon` (30/14/7/2 giorni).
   - `VeloxDiskPressure` (vedi RW-PROD-006).
   - `VeloxMemoryPressure`.

---

## 3. Azioni concrete

| # | File:line | Azione |
|---|-----------|--------|
| A1 | `scripts/audit-prom-units.sh` (nuovo) | Scan metriche, diff con `docs/metrics-units.md`. |
| A2 | `scripts/alert-cert-expiry.sh` (nuovo) | Wrapper per `/api/v1/alerts`. |
| A3 | `docs/metrics-units.md` (nuovo) | Tabella canonica: `velox_worker_*_bytes` byte, `velox_worker_*_seconds` seconds. |
| A4 | `pkg/logger` | Funzione `LogCtx(ctx)` con propagation strutturata. |
| A5 | `deploy/runtime/compose.yml` | `network_mode: bridge` + `expose` invece di `ports`. |
| A6 | `deploy/playbooks/firewall-open-metrics.yml` (nuovo) | Aprire solo health/metrics verso VPN/mesh. |
| A7 | `alerts/spec-14-compute-outcomes.yml` | Aggiungere alert fallback/emergency/cert/disk/memory. |
| A8 | `tests/e2e/metrics-units/` (nuovo) | Test unit audit. |

---

## 4. Criteri di accettazione

- [ ] Dashboard mostra tutti i worker (verified via plugin).
- [ ] Metriche con unità verificate da test (audit passa).
- [ ] Alert scatta in failure injection test (`velox_fallback_count_total > 0`).
- [ ] Fallback ed Emergency path restano 0 in produzione (alert computato).
- [ ] Nessuna metrica espone secret (fuzz test).
- [ ] Porte health/Prometheus solo su VPN/firewall.

---

## 5. Test obbligatori

- `TestAuditUnits_MatchesCanonical`.
- `TestAlert_FallbackEver_Fires`.
- `TestAlert_EmergencyEver_Fires`.
- `TestAlert_CertExpiring_Day14`.
- `TestAlert_DiskPressure`.
- `TestLogCtx_PropagatesWorkerID`.

---

## 6. Evidenze

- Run `audit-prom-units.sh` report JSON.
- Simulate cert expiry in `tests/e2e/grpc-control-plane/certs/generate-dev-pki.sh --days 13`, scenario alert.
- Dashboard screenshot.
- Grafana alert incident simulation log.
