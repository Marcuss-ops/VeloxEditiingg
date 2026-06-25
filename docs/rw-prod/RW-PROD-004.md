# RW-PROD-004 — Liveness e readiness worker separate

**Priorità:** P0
**Dipendenze:** RW-PROD-003
**Stato attuale:** Worker espone solo `GET /health` (`RemoteCodex/.../internal/telemetry/health.go`). Master espone `/health`, `/api/health`, `/ready`, `/api/ready` (`DataServer/internal/app/health.go`). Mancano sul worker gli endpoint separati `/health/live` e `/health/ready`.

---

## 1. Pain points

1. **Worker `/health` ritorna 200 anche se non registrato** se solo il processo è vivo. Kubernetes (o un monitor equivalente) non distingue "processo vivo" da "worker può ricevere task".
2. **Worker `/health` non distingue drain / executor assente / disco critico.** Tutto confluisce in `status=ok` con `registered=false` — un operatore che legge solo `status` non vede la differenza.
3. **Master `/ready` non ha check "session_active verso almeno 1 worker"** per orchestrare blue/green in ambienti shadow.
4. **Tipico pattern `/health/live` (k8s liveness probe) vs `/health/ready` (readiness)** non standardizzato sul worker. Le cartelle `deploy/runtime/compose.yml` hanno solo una `healthcheck:` su `/health` — insufficiente.

---

## 2. Soluzione

Aggiungere due handler paralleli a `/health` (mantenuto come adapter temporaneo, da rimuovere in release futura):

- **`GET /health/live`** → 200 sempre se processo e loop principale attivi. Body: `{"status":"alive","worker_id":"...","uptime_sec":...}`.
- **`GET /health/ready`** → 200 solo se TUTTE le condizioni:
  1. Sessione gRPC attiva (`registered=true && session_active`).
  2. Registrazione accettata (`registered=true`).
  3. Executor registry valido (`len(executorRegistry.Descriptors()) >= 1`).
  4. Cache + blob store disponibili (`localCache != nil && blobs != nil`).
  5. `drainMode == false`.
  6. Disk free > soglia critica configurabile.
  7. Bootstrap OK (vedi RW-PROD-003).

  Quando non pronto, body con motivi machine-readable:
  ```json
  {
    "status":"not_ready",
    "reasons":["drain_mode","disk_free_below_threshold"],
    "detail":{"disk_free_bytes":12345,"threshold_bytes":104857600}
  }
  ```

- **`GET /health`** (legacy, transitorio): ritorna `200` solo se `/health/ready` è `200`, altrimenti `503`. Deprecato ma mantenuto per retro-compat. Log un warning per indicare "deprecation, use /health/live or /health/ready".

Sul master mantenere gli endpoint già presenti e aggiungere un check readiness "at least 1 worker live":
- `AddReadinessCheck("workers_at_least_one_live", r.Registry.HasAtLeastOneLive)` — ritorna true se ≥1 worker in `GetActiveWorkers(...)`.

---

## 3. Azioni concrete

| # | File:line | Azione |
|---|-----------|--------|
| A1 | `RemoteCodex/.../internal/telemetry/health.go` | Aggiungere handler `mux.HandleFunc("/health/live", liveHandler)` e `mux.HandleFunc("/health/ready", readyHandler)`. |
| A2 | `RemoteCodex/.../internal/telemetry/health.go` | Introdurre `atomic.Pointer[ReadySnapshot]` aggiornato da `worker.Start`/`Stop`/`processCommand`. |
| A3 | `RemoteCodex/.../internal/telemetry/ready.go` (nuovo) | `ReadySnapshot{Registered, DrainMode, Bootstrapped, Executors, CacheReady, BlobReady, DiskFree, Reasons[]string}`. |
| A4 | `RemoteCodex/.../internal/worker/worker.go` | Hook su `setConnState(ConnReady)` → `ReadySnapshot.Registered=true`; su `MsgDrain` → `DrainMode=true`; post-bootstrap → `Bootstrapped=true`. |
| A5 | `deploy/runtime/compose.yml` | Aggiungere `healthcheck.test: ["CMD","/usr/bin/curl","-fsS","http://localhost:HEALTH_PORT/health/ready"]` con `start_period`, `interval`. |
| A6 | `RemoteCodex/.../internal/telemetry/health_test.go` | Test transizioni: CONNECTING→CONNECTED→DRAINING kill / disk free sotto soglia. |
| A7 | `DataServer/internal/workers/registry_query.go` | Aggiungere `HasAtLeastOneLive(ctx) bool` (count `GetActiveWorkers(30s)` ≥ 1). |
| A8 | `DataServer/cmd/server/bootstrap.go` | `AddReadinessCheck("workers_at_least_one_live", h.Registry.HasAtLeastOneLive)` (solo se env `VELOX_REQUIRE_LIVE_WORKERS=true`). |
| A9 | `deploy/runtime/README.md` + `RemoteCodex/.../deploy/install-worker.sh` | Aggiornare systemd unit (`ExecStart=... --ready-endpoint /health/ready`). |

---

## 4. Criteri di accettazione

- [ ] **Processo vivo ma master irraggiungibile**: `/health/live` → 200, `/health/ready` → 503 + reason `not_registered`.
- [ ] **Worker connesso e sano**: live 200, ready 200.
- [ ] **Worker draining** (ricevuto `MsgDrain`): live 200, ready 503, reason `drain_mode`.
- [ ] **Executor vuoto / motore assente**: ready 503, reason `executors.empty`.
- [ ] **Disk free sotto soglia**: ready 503, reason `disk.critical`.
- [ ] Body JSON sempre con `reasons[]` quando non ready.

---

## 5. Test obbligatori

- `Telemetry/Health/LiveAlive` — solo processo, no sessione gRPC.
- `Telemetry/Health/ReadyAfterHello` — Hello success.
- `Telemetry/Health/ReadyDuringDrain` — `MsgDrain` ricevuto.
- `Telemetry/Health/ReadyNoExecutors` — registry vuoto.
- `Telemetry/Health/ReadyUnderDiskPressure` — mock disk free basso.

---

## 6. Evidenze

- Log strutturato `health.ready.ready=true|false` con motivi in formato key=value.
- Metriche Prometheus `velox_worker_ready{worker_id="..."}` 0/1 (vedi RW-PROD-013).
- Report `scripts/dump-ready-states.sh` che itera tutti i worker via master `/api/v1/workers`.
