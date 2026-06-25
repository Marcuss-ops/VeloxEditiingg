# ACTIONS — Checklist trasversale per chiusura ticket RW-PROD

Ogni azione elenca `File:line → Azione`. Lo stato "owner" è indicativo — i singoli MD contengono il razionale e i criteri di accettazione.

> Convenzione: gli ID `RW-PROD-NNN-Ax` richiamano le azioni nei singoli MD (RW-PROD-001.md, …).

---

## Blocco A — Identità, mTLS, validate-config (RW-PROD 001..003)

| ID | File:line | Azione | Ticket | Owner |
|----|------------|--------|--------|-------|
| 001-A1 | `RemoteCodex/.../pkg/config/config.go` (`Validate()`, 245-265) | Aggiungere parse `notAfter` e rifiuto <14gg | 001 | sec.platform |
| 001-A2 | `RemoteCodex/.../pkg/config/config.go Validate` | Enforce key perm `0600` (warn in dev) | 001 | sec.platform |
| 001-A3 | `RemoteCodex/.../pkg/logger` | `LogCertRejected(workerID, fingerprint, serial, reason)` | 001 | sec.platform |
| 001-A4 | `RemoteCodex/.../pkg/config/config.go` post-normalize | Check regex worker_id shape | 001 | sec.platform |
| 001-A5 | `DataServer/internal/grpcserver/bootstrap_grpc.go` | Env `VELOX_GRPC_REQUIRE_TLS=true` → panic se TLS assente | 001 | sec.platform |
| 001-A6 | `DataServer/internal/grpcserver/authorizer_test.go` | `TestServer_PlaintextRejectedWhenTLSRequired` | 001 | sec.platform |
| 001-A7 | `scripts/check-share-cert.sh` (nuovo) | Diff cert+key tra host Ansible | 001 | sec.platform |
| 001-A8 | `deploy/scripts/apply-local-worker-config.sh` | Salva `LAST_CERT_HASH`+`LAST_CERT_SERIAL` | 001 | sec.platform |
| 001-A9 | `scripts/gen-production-pki.sh` | CN == `worker_id` enforced | 001 | sec.platform |
| 002-A1 | `RemoteCodex/.../pkg/doctor/validator.go` (nuovo) | Interface `Validator { Name, Run }` | 002 | sre |
| 002-A3 | `RemoteCodex/.../cmd/velox-worker-agent/main.go` | `--validate-config` delega a `doctor.Run(cfg).Exit()` | 002 | sre |
| 002-A4 | `pkg/config/config.go` | Aggiungere `MinDiskFreeMB`, `VideoEngineCppBin`, `OutputDir`, `TempDir` | 002 | sre |
| 002-A5..A7 | `pkg/doctor/{engine,ffmpeg,registry}.go` | Implementare i 10 sotto-validatori | 002 | sre |
| 003-A1 | `RemoteCodex/.../pkg/bootstrap/` (nuovo) | `bootstrap.Run(ctx, cfg) error` | 003 | video+infra |
| 003-A2 | `pkg/bootstrap/self_render.go` | Self-render frame 1×1 nero | 003 | video |
| 003-A3 | `pkg/bootstrap/ffmpeg.go` | Esegui ffprobe -version, parse major | 003 | video |
| 003-A4 | `pkg/bootstrap/output_dir.go` | Mkdir + write + remove | 003 | video |
| 003-A5 | `cmd/velox-worker-agent/main.go` (~234) | Wire `bootstrap.Run` subito dopo `pipelineRunner` istanziato | 003 | video |
| 003-A6 | `internal/worker/worker.go` | Spostare `SetHealthRegistered(true)` dopo bootstrap OK | 003 | video |
| 003-A8 | `pkg/bundle/` (nuovo) | `BundleHashMatches` | 003 | infra |

---

## Blocco B — Health, stato canonico, sizing (RW-PROD 004..006)

| ID | File:line | Azione | Ticket | Owner |
|----|------------|--------|--------|-------|
| 004-A1..A4 | `RemoteCodex/.../internal/telemetry/health.go` | Aggiungere `/health/live`, `/health/ready`, `ReadySnapshot` | 004 | sre |
| 004-A5 | `deploy/runtime/compose.yml` | `healthcheck.test` su `/health/ready` | 004 | infra |
| 004-A7 | `DataServer/internal/workers/registry_query.go` | `HasAtLeastOneLive(ctx) bool` | 004 | sre |
| 004-A8 | `DataServer/cmd/server/bootstrap.go` | Wire readiness check "workers_at_least_one_live" | 004 | sre |
| 005-A1 | `DataServer/internal/handlers/server/api/workers_handler.go` | Estendere `WorkerResponse` | 005 | api-team |
| 005-A2 | `internal/workers/registry_query.go` `ConnectionStatus` | Aggiungere `reason` | 005 | api-team |
| 005-A3 | `handlers/server/api/workers_handler.go` | Query params `?class=&status=&rollout_group=` | 005 | api-team |
| 005-A4 | `handlers/server/api/workers_handler.go` | `LoadCurrentTask` | 005 | api-team |
| 005-A5 | `internal/store/store_workers.go` | Campi `Class`, `RolloutGroup` + migrazione | 005 | api-team |
| 006-A1 | `pkg/sizing/classes.go` (nuovo) | Enum + tabella mapping | 006 | sre+arch |
| 006-A2 | `internal/worker/worker.go` | Boot `effectiveMaxJobs = sizing.MaxActiveJobsFor(class, ...)` | 006 | sre |
| 006-A3 | `costmodel/cost.go` | Aggiungere `capacity_full` / `memory_pressure` / `disk_pressure` | 006 | arch |
| 006-A4 | `costmodel/worker_profile.go` | Campi `MemoryUsedBytes`, `DiskFreeBytes` | 006 | arch |
| 006-A5 | `internal/telemetry/resource_sampler.go` | Pubblicare `MemoryPressure`, `DiskPressure` | 006 | video+infra |

---

## Blocco C — Canary, artifact integrity, restart (RW-PROD 007..009)

| ID | File:line | Azione | Ticket | Owner |
|----|------------|--------|--------|-------|
| 007-A1 | `RemoteCodex/.../internal/worker/canary/canary.go` (nuovo) | Executor `canary.black-1s@1` | 007 | video+infra |
| 007-A2 | `RemoteCodex/.../cmd/velox-worker-agent/main.go` | `canaryCmd` subcommand | 007 | infra |
| 007-A3 | `DataServer/internal/handlers/remote/canary/` (nuovo) | Endpoint `POST /api/v1/workers/:worker_id/canary` | 007 | api-team |
| 007-A5 | `tests/fixtures/canary_v1_baseline.sha256` | Baseline SHA committato | 007 | video+qa |
| 007-A6 | `scripts/run-canary.sh` (nuovo) | Wrapper CLI invocabile | 007 | sre |
| 008-A1 | `DataServer/internal/artifacts/service_finalize.go` | `Finalize` idempotente su upload COMPLETED | 008 | artifact-team |
| 008-A2 | `DataServer/internal/artifacts/sqlite_finalization_repository.go` | Test `TestFinalize_LateReportRejected` | 008 | artifact-team |
| 008-A3 | `DataServer/internal/artifacts/service_receive.go` | Test hash + size mismatch | 008 | artifact-team |
| 008-A4 | `DataServer/internal/artifacts/service_finalize.go` | `ffprobe` post-finalize (env opt) | 008 | video+artifact |
| 009-A1 | `DataServer/internal/workers/registry_register.go` | `revokePriorSessions(tx, workerID, except sessionID)` | 009 | sre |
| 009-A2 | `tests/e2e/recovery-master-restart/` (nuovo) | Script restart master | 009 | sre+qa |
| 009-A6 | `internal/worker/worker.go` runSession | Su done → `SetHealthRegistered(false)` | 009 | sre |

---

## Blocco D — Crash, partition, drain, metrics (RW-PROD 010..013)

| ID | File:line | Azione | Ticket | Owner |
|----|------------|--------|--------|-------|
| 010-A1 | `tests/e2e/worker-crash/run.sh` (nuovo) | SIGKILL E2E | 010 | sre+qa |
| 010-A2 | `taskgraph/reaper.go` | Reason `worker_crash_detected` | 010 | taskgraph-team |
| 010-A3 | `intern...taskrunner/runner.go` | 010 | infra |
| 011-A1 | `tests/e2e/network-partition/` (nuovo) | Wrappers iptables | 011 | sre+qa |
| 011-A3 | `DataServer/internal/logging/codes.go` | Aggiungere `CodePartitionDetected`, `CodeIdempotentReplay` | 011 | logging |
| 011-A4 | `taskgraph/lifecycle.go` | Lease on stale expired → refresh | 011 | taskgraph-team |
| 012-A1 | `pkg/video/services/native/render_client.go:135` | Escalation TERM→KILL dopo `KillGracePeriod` | 012 | video |
| 012-A3 | `data/ansible/playbooks/tasks/normalize_worker_systemd.yml:347` | `TimeoutStopSec=120` (uniformare) | 012 | infra |
| 012-A4 | `deploy/velox-server.service` | `TimeoutStopSec=120` | 012 | infra |
| 013-A1 | `scripts/audit-prom-units.sh` (nuovo) | Audit unità metriche | 013 | sre |
| 013-A2 | `scripts/alert-cert-expiry.sh` (nuovo) | Wrapper pubblicazione alert | 013 | sre |
| 013-A3 | `docs/metrics-units.md` (nuovo) | Tabella canonica unità | 013 | sre |
| 013-A4 | `pkg/logger` | `LogCtx(ctx)` propagation | 013 | logging |
| 013-A5 | `deploy/runtime/compose.yml` | `network_mode: bridge` + `expose` | 013 | infra |
| 013-A7 | `alerts/spec-14-compute-outcomes.yml` | Aggiungere alert fallback/emergency/cert/disk/memory | 013 | sre |

---

## Blocco E — PKI rotation, soak, doctor, rollout (RW-PROD 014..017)

| ID | File:line | Azione | Ticket | Owner |
|----|------------|--------|--------|-------|
| 014-A1 | `deploy/certs/monitor-expiry.sh` | Fail-closed dir assente + zero cert + cert illeggibile | 014 | sec.platform |
| 014-A3 | `docs/operations/PR-6-pki-rotation-runbook.md` | Sezione "Rotate worker without downtime" | 014 | sec.platform |
| 014-A4 | `DataServer/internal/grpcserver/authorizer.go` | Allowlist multi-cert durante overlap | 014 | sec.platform |
| 014-A5 | `deploy/certs/revocation.sh` (nuovo) | Revoca automatica via `revoked/` | 014 | sec.platform |
| 014-A6 | `DataServer/internal/store/store_worker_control.go` | Tabella `cert_revocations` | 014 | sec.platform |
| 015-A1 | `scripts/soak-run.sh` (nuovo) | Driver soak 24h per classe | 015 | sre+qa |
| 015-A2 | `scripts/verify-soak-gates.sh` (nuovo) | Gate numerici | 015 | sre+qa |
| 015-A3 | `deploy/inventory/hardware_matrix.yml` (nuovo) | Tabella classi HW | 015 | infra+arch |
| 015-A7 | `data/ansible/playbooks/tasks/run_soak.yml` (nuovo) | Playbook Ansible | 015 | sre |
| 016-A1 | `cmd/velox-worker-agent/main.go` | Nuovo subcommand `doctor` | 016 | infra+qa |
| 016-A2 | `pkg/doctor/` (nuovo) | Package validatori | 016 | infra |
| 016-A3 | `pkg/doctor/handshake.go` | Dial master + Hello | 016 | infra |
| 016-A4 | `pkg/doctor/visibility.go` | HTTP GET master `/api/v1/workers/:id` | 016 | infra |
| 016-A7 | `deploy/scripts/apply-local-worker-config.sh` | Aggiungere `doctor --json` gate | 016 | infra |
| 017-A1 | `scripts/bump-version-and-deploy.sh` | Gate `doctor` su canary host | 017 | sre |
| 017-A3 | `DataServer/internal/store/migrations/` | Tabella `worker_deploys` | 017 | infra |
| 017-A4 | `deploy/playbooks/promote-canary.yml` (nuovo) | Playbook orchestrazione | 017 | sre |
| 017-A5 | `tests/e2e/rollback/run.sh` (nuovo) | E2E test rollback | 017 | sre+qa |
| 017-A6 | `scripts/check-no-rebuild.sh` (nuovo) | Anti-rebuild CI guard | 017 | ci |

---

## Sequenza raccomandata (dal runbook §4)

> Non cominciare il ticket successivo finché il precedente non ha test verdi.

```
001 → 002 → 003 → 004 → 005 → 006 → 007 → 008 → 009 → 010 → 011 → 012 → 013 → 014 → 015 → 016 → 017
```

---

## Scheda finale di certificazione (per ogni worker)

```
Worker ID:           _____________
Hostname:            _____________
Classe hardware:     _____________   (cpu-small | cpu-medium | cpu-xlarge)
Versione worker:     _____________   (= VERSION.txt)
Versione engine:     _____________   (= cfg.EngineVersion)
Bundle version:      _____________   (= VERSION.txt == config.BundleVersion)
Bundle hash:         _____________   (= BUNDLE_HASH.txt)
Protocol version:    v3
Image digest:        _____________   (@sha256:…)
Cert fingerprint:    _____________   (master card)
Cert expiry:         _____________
Cert serial:         _____________
Doctor verdict:      PASS | FAIL  (RW-PROD-016)
Canary job ID:       _____________  (RW-PROD-007)
Canary task ID:      _____________
Canary attempt ID:   _____________
Artifact ID:         _____________
Artifact SHA-256:    _____________
Soak start:          _____________  (RW-PROD-015)
Soak end:            _____________
Job eseguiti:        _____________
Success rate:        _____________
Failure count:       _____________
Reconnect test:      PASS | FAIL  (RW-PROD-009)
Worker crash test:   PASS | FAIL  (RW-PROD-010)
Master restart test: PASS | FAIL  (RW-PROD-009)
Network partition:   PASS | FAIL  (RW-PROD-011)
Drain test:          PASS | FAIL  (RW-PROD-012)
Fallback count:      0
Python emergency:    0
VERDETTO finale:     PRODUCTION_READY | NOT_READY
Approvato da:        _____________
Data approvazione:   _____________
```

---

## Definizione di "fatto"

Un ticket è `DONE` solo quando:

- [ ] Tutte le azioni `Ax` del singolo MD sono completate.
- [ ] Test obbligatori (sezione 5) verdi in CI.
- [ ] Evidenze (sezione 6) archiviate in `ops/`.
- [ ] Acceptance criteria (sezione 4) verificati.
- [ ] Cronologia review: 1 reviewer + 1 approvatore.
- [ ] Report `git diff` allegato alla PR.

Un worker è `PRODUCTION_READY` solo quando TUTTI i ticket P0 sono `DONE` e la scheda finale è completamente compilata e firmata.
