# RW-PROD: Production Readiness for Remote Workers

> Data snapshot: 2026-06-24
> Repo: `VeloxLEgit` (Marcuss-ops / VeloxEditiingg)
> Scopo: certificare ogni worker remoto prima dell'ingresso nel pool di produzione (`PRODUCTION_READY`).

Questa cartella contiene l'analisi, i pain points, e le azioni concrete per i 17 ticket `RW-PROD-001` … `RW-PROD-017` definiti nel runbook operativo 04.

---

## Indice della cartella

| Ticket                                    | File                                              | Priorità |
|-------------------------------------------|---------------------------------------------------|----------|
| [RW-PROD-001](./RW-PROD-001.md) mTLS fail-closed | mTLS, identità, cert fingerprint          | P0       |
| [RW-PROD-002](./RW-PROD-002.md) Validate-config production | validazione runtime completa      | P0       |
| [RW-PROD-003](./RW-PROD-003.md) Bootstrap runtime/executor | engine + registry reale              | P0       |
| [RW-PROD-004](./RW-PROD-004.md) `/health/live` + `/health/ready` | liveness vs readiness                 | P0       |
| [RW-PROD-005](./RW-PROD-005.md) Worker status canonico | `GET /api/v1/workers`                   | P0       |
| [RW-PROD-006](./RW-PROD-006.md) Resource sizing / admission | `max_active_jobs`, pressure          | P0       |
| [RW-PROD-007](./RW-PROD-007.md) Canary mTLS per worker | canary reale attribuito                | P0       |
| [RW-PROD-008](./RW-PROD-008.md) Artifact integrity | CAS unico STAGING→READY + SHA-256      | P0       |
| [RW-PROD-009](./RW-PROD-009.md) Restart master recovery | reconn automatico                      | P0       |
| [RW-PROD-010](./RW-PROD-010.md) Crash worker e retry | lease reaper                           | P0       |
| [RW-PROD-011](./RW-PROD-011.md) Network partition | dedup lease/attempt                    | P0       |
| [RW-PROD-012](./RW-PROD-012.md) Drain + SIGTERM | escalation TERM→KILL                   | P0       |
| [RW-PROD-013](./RW-PROD-013.md) Metriche + alert | unità, log correlati                    | P0       |
| [RW-PROD-014](./RW-PROD-014.md) PKI rotation monitor | fail-closed su assente/non valido      | P0       |
| [RW-PROD-015](./RW-PROD-015.md) Soak per classe HW | 24h soak / report firmato              | P0       |
| [RW-PROD-016](./RW-PROD-016.md) Worker doctor | `velox-worker-agent doctor --production` | P0       |
| [RW-PROD-017](./RW-PROD-017.md) Rollout / rollback | canary + drain                        | P0       |
| [ACTIONS](./ACTIONS.md)                   | Checklist trasversale di azioni           | —        |

---

## Mappa "stato attuale" (dall'analisi della repo)

| # | Componente | Stato attuale (in repo) | Gap vs ticket |
|---|------------|------------------------|---------------|
| 1 | Validazione TLS worker | `pkg/config/config.go` rifiuta mix `tls_*` + `allow_insecure`, rifiuta `insecure + production`, verifica esistenza cert + pair cert/key. | ✅ tripletta. Mancano: scadenza minima 14gg, permessi 0600, fingerprint, CN/SAN, plaintext-vs-TLS server-side hard reject. |
| 2 | `--validate-config` worker | `cmd/velox-worker-agent/main.go:117` — *transport check only*. | ❌ manca: dirs write, FFmpeg, engine binary, executor registry, scene.composite.v1@1, disk free, porte libere. |
| 3 | Bootstrap runtime | `cmd/velox-worker-agent/main.go:281` — fail-closed se `video.NewPipelineRunner` fallisce. `internal/worker/worker.go:35` warn su registry vuoto. | ⚠️ engine CPed engine bin reachable ma nessun self-test di rendering, nessun check FFmpeg/ffprobe. |
| 4 | Health endpoints | Master: `/health`, `/ready` (`internal/app/health.go`). Worker: solo `/health` (`internal/telemetry/health.go`). | ❌ worker manca `/health/live` e `/health/ready`. |
| 5 | Stato worker dal master | `internal/workers/registry_query.go:38` — `ConnectionStatus` con 4 stati, DRAINING wins. `handlers/server/api/workers_handler.go` — payload sanitizzato (no secret/credential/path). | ✅ quasi completo. Verificare esposizione `protocol_version`, `engine_version`, `bundle_version`, `current_task` nel payload attuale. |
| 6 | Admission + sizing | `internal/costmodel/cost.go:106` reasons: `draining/offline/at capacity`. `RemoteCodex/.../internal/worker/concurrency/concurrency.go` — `MaxActiveJobs` runtime. | ⚠️ mancano reason-code specifici `capacity_full`, `disk_pressure`, `memory_pressure`; formula per classe HW non documentata. |
| 7 | Canary mTLS | `tests/e2e/workload-mtls/run.sh` esiste ed è verde in CI (`e2e-workload-mtls.yml`). | ⚠️ non è rilanciabile on-demand per un singolo worker arbitrario. |
| 8 | Artifact integrity | `internal/artifacts/sqlite_finalization_repository.go:163` — `FinalizeVerified` in **single tx** con CAS su jobs/artifacts/attempt/artifact_uploads. `internal/ingest/service.go:62` — SHA-256 worker hint vs master-computed. | ✅ catena corretta. Verificare: late-report da attempt stale respinto, no doppio `READY` (UNIQUE(storage_provider,storage_key) presente), `jobs.completed_at >= artifacts.verified_at` (scan_test.go). |
| 9 | Reconnection master restart | `internal/worker/worker.go:38` — fresh transport per attempt, backoff + jitter. | ✅ già presente (PR-04 re-registration loop). Verificare test integrato. |
| 10 | Lease expiry / retry | `internal/taskgraph/reaper.go` (`TaskLeaseReaper`), `internal/store/sqlite_task_renew_test.go` (3 rejection path test). `internal/grpcserver/handler_workers.go:173` lease 30 min. | ✅ robust. Da verificare copertura con crash test SIGKILL reale. |
| 11 | Network partition / duplicate suppression | Idempotenza via `(task_id,worker_id,lease_id)` tuple. `task_attempts` reaper. | ⚠️ manca un test di 60s/120s offline dedicato (PR-05 follow-up lo cita). |
| 12 | Drain + SIGTERM | Master: `deploy/velox-server.service` `TimeoutStopSec=60`. Worker: `data/ansible/playbooks/tasks/normalize_worker_systemd.yml:347` `TimeoutStopSec=35`. `pkg/video/services/native/render_client.go:135` fa `syscall.Kill(-pid, SIGTERM)` al C++. | ⚠️ escalation TERM→KILL + cleanup temp post-kill **non** presente nel worker. Timeout incoerente (60 vs 35). |
| 13 | Permission chiave privata | Non controllato a livello Go, non controllato a livello script. | ❌ manca enforcement. |
| 14 | Metriche / alert | `internal/telemetry/metrics_types.go:151` espone `velox_fallback_count_total`, `velox_python_emergency_path_total`. `alerts/spec-14-compute-outcomes.yml` — alert su compute-outcomes. `docs/operations/PR-6-pki-rotation-runbook.md:215` — `alert-cert-expiry.sh` **TODO non versionato**. | ⚠️ non tutte le soglie RW-PROD-013 hanno alert; correlazione `worker_id + job_id + task_id + attempt_id + session_id` da verificare. |
| 15 | Cert expiry monitor | `deploy/certs/monitor-expiry.sh` — emite exit 0/1/2/3 in base al peggior stato **se trova almeno un cert**. Directory assente → 0 certificati → `worst_exit=0` ⇒ exit 0 (KO). | ❌ fail-closed mancante: deve uscire non-zero su directory assente, su zero cert validi, su cert illeggibile. |
| 16 | Soak test | `tests/e2e/soak-partition/test_recovery.sh` esiste. | ⚠️ non sembra essere un 24h per classe. Da definire matrice e firma risultati. |
| 17 | `doctor` command | Non esiste (flag disponibili: `--version`, `--generate-config`, `--validate-config`). | ❌ da implementare ex novo. |
| 18 | Rollout / rollback | `DataServer/internal/handlers/remote/ansible/deploy.go:45` `buildDeployPlan` con canary_percent. `scripts/bump-version-and-deploy.sh:127` chiama `deploy_workers` con canary. `deploy/playbooks/rollback.yml` esiste. | ⚠️ manca integrazione esplicita con `doctor --production` prima della promozione; manca versioning dell'image digest nel rollout plan. |

---

## Sequenza di implementazione (dal runbook §4)

1. RW-PROD-001 → 2. 002 → 3. 003 → 4. 004 → 5. 005 → 6. 006 → 7. 007 →
8. 008 → 9. 009 → 10. 010 → 11. 011 → 12. 012 → 13. 013 → 14. 014 →
15. 015 → 16. 016 → 17. 017.

Non cominciare il successivo finché il precedente non ha test verdi, evidenze archiviate e criteri di accettazione verificati.

---

## Regola di ammissione per worker

Per entrare in allowlist production:

```
Doctor = READY
Canary mTLS = PASS
Artifact integrity = PASS
Recovery suite = PASS
Soak test = PASS
Fallback count = 0
Python emergency count = 0
Verdetto = PRODUCTION_READY
```

Nessuna eccezione silenziosa. Ogni deroga deve avere owner, motivazione, scadenza e ticket di rientro (vedi scheda finale in ogni MD).
