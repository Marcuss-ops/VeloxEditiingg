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
| 014-A1 | `doctor --production` | Fail-closed dir assente + zero cert + cert illeggibile | 014 | sec.platform |
| 014-A3 | `docs/operations/PR-6-pki-rotation-runbook.md` | Sezione "Rotate worker without downtime" | 014 | sec.platform |
| 014-A4 | `DataServer/internal/grpcserver/authorizer.go` | Allowlist multi-cert durante overlap | 014 | sec.platform |
| 014-A5 | `deploy/certs/revocation.sh` (nuovo) | Revoca automatica via `revoked/` | 014 | sec.platform |
| 014-A6 | `DataServer/internal/store/store_worker_control.go` | Tabella `cert_revocations` | 014 | sec.platform |
| 015-A1 | `scripts/soak-run.sh` (nuovo) | Driver soak 24h per classe | 015 | sre+qa |
| 015-A2 | `scripts/verify-soak-gates.sh` (nuovo) | Gate numerici | 015 | sre+qa |
| 015-A3 | `deploy/inventory/hardware_matrix.yml` (nuovo) | Tabella classi HW | 015 | infra+arch |
| 015-A7 | `data/ansible/playbooks/tasks/run_soak.yml` (nuovo) | Playbook Ansible | 015 | sre |
| 016-A1 | `RemoteCodex/native/worker-agent-go/cmd/velox-worker-agent/main.go` | `doctor --production [--json]` | 016 | infra+qa |
| 016-A2 | `RemoteCodex/native/worker-agent-go/pkg/doctor/` | Production validators fail-closed | 016 | infra |
| 016-A3 | `pkg/doctor/handshake.go` | Dial master + Hello | 016 | infra |
| 016-A4 | `pkg/doctor/visibility.go` | HTTP GET master `/api/v1/workers/:id` | 016 | infra |
| 016-A7 | `deploy/scripts/apply-local-worker-config.sh` | Aggiungere `doctor --json` gate | 016 | infra |
| 017-A1 | `scripts/bump-version-and-deploy.sh` | Compatibility wrapper delega esclusivamente a `fleetctl` | 017 | sre |
| 017-A3 | `DataServer/internal/store/migrations/` | Tabella `worker_deploys` | 017 | infra |
| 017-A4 | `DataServer/data/ansible/playbooks/` | Rollout Ansible ritirato/fail-closed | 017 | sre |
| 017-A5 | `tests/e2e/rollback/run.sh` (nuovo) | E2E test rollback | 017 | sre+qa |
| 017-A6 | `scripts/check-no-rebuild.sh` (nuovo) | Anti-rebuild CI guard | 017 | ci |

---

## Blocco F — Audit dead code, wrapper e compatibility shim

Questa sezione traccia i risultati dell’audit legacy del 2026-08-10. Le
rimozioni sono separate dai guardrail: un simbolo non viene eliminato solo
perché contiene `legacy`, `compat` o `fallback`; prima devono essere verificati
caller, reflection, registry, payload reali e confini di modulo.

Gli ID `LEGACY-*` sono un’estensione locale del tracker per questo audit e
non sostituiscono gli ID `RW-PROD-NNN-Ax` dei ticket di rollout. Le priorità
incrociano impatto/regressione, complessità della superficie e probabilità di
modifica; `P0` indica un guardrail da non perdere, non necessariamente un
lavoro di rimozione immediata.

| ID | File/simbolo | Azione | Stato | Decisione | Owner | Priorità | Rischio | Criterio di completamento |
|----|--------------|--------|-------|-----------|-------|----------|--------|---------------------------|
| LEGACY-A1 | `DataServer/internal/socialclient/targets.go:152` `ListPublishingTargets`; test in `targets_test.go` | Migrare i test da `ListPublishingTargets` a `ListPublishingCatalog`, poi rimuovere il wrapper non usato in produzione | DA FARE | RIMUOVERE DOPO INVENTARIO | social-platform | P1 | Medio: possibile riferimento test o consumer interno non rilevato | `git grep -n 'ListPublishingTargets' -- '*.go'` mostra solo dichiarazione/test aggiornato; `go test ./internal/socialclient/...`; `scripts/ci/pre-removal-verify.sh` verde |
| LEGACY-A2 | `DataServer/internal/socialclient/targets.go:27` `PublishingTargetCatalogRequest` | Sostituire l’alias con `PublishingCatalogRequest` e rimuovere il tipo compatibile | DA FARE | RIMUOVERE DOPO INVENTARIO | social-platform | P1 | Basso: alias interno senza comportamento proprio | `git grep -n 'PublishingTargetCatalogRequest' -- '*.go'` non trova riferimenti; wire shape invariata; test socialclient, build e `scripts/ci/pre-removal-verify.sh` verdi |
| LEGACY-A3 | `DataServer/internal/socialclient/targets.go:95` `Groups`; `DataServer/internal/publishing/resolver_normalize.go:54` | Verificare `Groups` rispetto a `ResolvedGroups`, raccogliere evidenza sui payload reali e definire il sunset | IN ATTESA EVIDENZA | MANTENERE FINO A DECISIONE | social-platform + api-team | P2 | Alto: rimozione prematura può perdere gruppi provenienti da upstream legacy | Entro il riesame del 2026-08-17, report upstream/telemetria indica finestra e soglia; test di rifiuto/assenza del vecchio campo; decisione e data sunset approvate |
| LEGACY-A4 | `RemoteCodex/native/worker-agent-go/pkg/video/entity_association.go:21,219` `audioFilePath` | Valutare la rimozione graduale del parametro ignorato, verificando interfacce e consumer esterni | IN ATTESA INVENTARIO | MANTENERE FINO A MIGRAZIONE | worker-video | P3 | Alto: modifica della firma può rompere consumer del package `pkg/` | Entro il riesame del 2026-08-17, `git grep` production/test + inventario consumer esterni sono allegati al ticket; owner approva nuova firma; migrazione e test completati oppure shim con sunset |
| LEGACY-A5 | `RemoteCodex/native/worker-agent-go/pkg/resilience/circuitbreaker.go:175` `GetState`; `RemoteCodex/native/worker-agent-go/pkg/api/circuit_breaker.go` | Valutare il wrapper senza rimuoverlo finché esiste superficie consumabile esterna | GUARDRAIL CONFERMATO | MANTENERE | worker-platform | P2 | Alto: API breaking change per consumer del worker-agent | `State()` è canonico nei nuovi caller; inventario consumer esterni e policy di versioning sono registrati; eventuale deprecazione ha owner, data e note di migrazione |
| LEGACY-A6 | `shared/compatibility/registry.go`; `scripts/ci/check-compatibility-alias-registry.sh` | Mantenere registry e observer degli alias come guardrail runtime/CI | GUARDRAIL CONFERMATO | MANTENERE | architecture + ci | P0 | Alto: rimozione elimina controllo anti-regressione e visibilità sugli alias | Test registry e check CI passano; ogni alias ha owner, motivazione e scadenza; nessun nuovo alias passa senza registrazione |
| LEGACY-A7 | `DataServer/internal/metrics/collector_http.go:33` `RouteSurfaceLegacy`; `DataServer/internal/metrics/collector_attempts.go` | Mantenere fallback metrici e classificazione legacy finché servono per osservare schema/route precedenti e pianificare il cutover | GUARDRAIL CONFERMATO | MANTENERE FINO A SOGLIA ZERO | observability | P1 | Alto: perdita di telemetria prima della rimozione delle superfici legacy | Metriche prodotte e consultabili nel report observability; route legacy classificabili; rimozione solo dopo soglia zero documentata per una finestra approvata |
| LEGACY-A8 | `RemoteCodex/native/worker-agent-go/pkg/bootstrap/view.go` `RunnerView`; `RemoteCodex/native/worker-agent-go/pkg/video/pipeline_runner.go` `pipelineAdapter` | Conservare gli adapter come confini di test/iniezione, non classificarli come dead code per il solo fatto che siano wrapper | GUARDRAIL CONFERMATO | MANTENERE | worker-video | P1 | Medio: rimozione riaccoppia core, I/O e test | Test unitari usano le interfacce; ogni adapter ha consumer o ruolo nel composition root; review architetturale conferma assenza di duplicati canonici |
| LEGACY-A9 | `DataServer/internal/socialclient/config.go` e `config_test.go` | Mantenere documentazione e configurazione sugli env canonici `SOCIAL_API_URL`, `SOCIAL_API_TOKEN`, `SOCIAL_API_TIMEOUT_MS`, `SOCIAL_CALLBACK_BASE_URL`, mantenendo il comportamento fail-closed per nomi deprecati | CHIUSO | MANTENERE FAIL-CLOSED | social-platform + docs | P2 | Basso: il fallback legacy resta intenzionalmente rifiutato | Cleanup documentale `71c7ee58`; `TestConfigFromEnv` verde; `bash scripts/ci/check-no-legacy.sh` verde; root changelog e runbook canonici preservati |
| LEGACY-A10 | `scripts/ci/check-no-legacy.sh`; `DataServer/internal/app/workers_namespace_test.go` | Conservare check CI e test sulle route rimosse come guardrail contro la reintroduzione di endpoint e simboli eliminati | GUARDRAIL CONFERMATO | MANTENERE | ci + api-team | P1 | Alto: senza guardia possono ricomparire percorsi duplicati o obsoleti | Check CI eseguito nel gate; test dimostrano che le route legacy non sono montate; eccezioni nominate con owner e scadenza |

### Regole di chiusura del blocco legacy

- `DA FARE` richiede diff piccolo, caller inventory aggiornato, test mirati e
  `scripts/ci/pre-removal-verify.sh` prima della rimozione di simboli esportati
  o helper cross-package.
- `IN ATTESA EVIDENZA` e `IN ATTESA INVENTARIO` non autorizzano modifiche al
  codice: prima servono dati runtime, consumer esterni o approvazione del
  proprietario del contratto.
- `GUARDRAIL CONFERMATO` indica una decisione architetturale di mantenimento;
  va comunque riesaminata alla scadenza o soglia indicata, non duplicata.
- Un’azione è completata solo quando il criterio della riga è verificato e
  l’evidenza (test, query o report) è collegata al commit o al runbook
  operativo.

### Evidenza verificata — audit YAGNI e gate pre-removal (2026-08-10)

- **Retry Social/API:** `SOCIAL_API_RETRIES` e il retry config del
  `socialclient` non sono letti dal runtime; i retry effettivi restano di
  competenza del delivery runner e dei budget per-job/delivery (`MaxRetries`,
  `RetryBudget`, `MaxAttempts`). La distinzione evita di dichiarare rimosso
  il `MaxRetries` di dominio, che è ancora attivo.
- **S3Provider:** rimosso perché non configurato e senza caller dimostrati nel
  commit `7c684b2a` (`refactor(deliveries): remove unused social retries and S3 stub`);
  i test del registry verificano il comportamento fail-closed.
- **`internal/backup`:** call-site proof senza import, caller, bootstrap,
  scheduler, CLI o job operativi (`9ecdbef5`); scaffolding rimosso in
  `b0f0d209` (`refactor(backup): remove unreachable backup scaffolding`). Le
  procedure backup/restore restano documentate nel runbook operativo e non
  implicano che esista un helper runtime.
- **Gate di rimozione:** `scripts/ci/pre-removal-verify.sh` verificato verde
  nelle tre fasi `go vet ./...`, `go build ./...` e
  `go test -count=1 ./...`. Le correzioni atomiche di evidenza sono:
  `efa76929` (import test store), `e2b300cb` (wiring phase executor nel test
  delivery) e `2fe840d8` (fixture worker canonico). Il cleanup documentale
  degli alias Social è `71c7ee58`.
- I file dirty concorrenti presenti nel worktree non sono inclusi nelle
  evidenze o nei commit di questo tracker.

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
