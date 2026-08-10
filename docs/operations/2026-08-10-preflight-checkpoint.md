# Pre-flight checkpoint — 2026-08-10 (before any recovery action)

> Snapshot captured live on **2026-08-10 ~20:22 UTC** from the production
> master (`vps-334f342f`). This is the STARTING checkpoint for the
> certification run: no worker was restarted, no rollout was triggered,
> no flag was forced. Facts below come from the master REST admin API,
> the master journal, the fleet SSH surface (`ssh-check`), and worker
> journals over SSH. No secrets are contained in this document.

## Master

| Item | Value |
| --- | --- |
| Host | `vps-334f342f` |
| systemd unit | `velox-server.service` — `active (running)` |
| Container | `velox-server` — `Up 6 minutes (healthy)` |
| Running image | `ghcr.io/marcuss-ops/velox-server@sha256:3e66c5b63756de6d45d32ac3f814de2c4044aeed4f1e6aac38054df189d4d53a` |
| Expected digest | `3e66c5b6…` — **MATCH ✅** |
| `GET /health` | `200` `{"status":"healthy"}` |
| `GET /health/ready` | `200` `{"status":"ready","checks":9,"capabilities":{"level-d-smoke":"READY","opsalerts":"DISABLED"}}` |
| SSH master → workers (`ssh-check`) | **4/4 READY** (ssh=4, hostkey=4, sudo=4; key `/etc/velox/ssh/id_ed25519_velox`) |

The master was restarted shortly before this snapshot (container "Up 6
minutes", last gRPC outage window on workers ≈ 20:10–20:15) — this is the
deployment that carries the bound-credential auth fix
(`fix(auth): accept bound grpc worker credentials`, `29b880b2`).

## Fleet — worker cards (master admin API)

| Worker | connection | health | digest (running == target) | digest_state | active jobs |
| --- | --- | --- | --- | --- | --- |
| `host_57_129_132_133` (canary) | CONNECTED | **DRAINING** | `33795a3a736e…` == target | FAILED | 0 |
| `host_57_131_20_173` | CONNECTED | HEALTHY | `90ffddd60f77…` == target | PENDING | 0 |
| `velox-worker-13197` | CONNECTED | HEALTHY | `90ffddd60f77…` == target | PENDING | 0 |
| `velox-worker-523925eb` | CONNECTED | HEALTHY | `90ffddd60f77…` == target | PENDING | 0 |

### Readiness detail — all 4 workers (identical, status `ok`, no reasons)

```json
{
  "blob_ready": true,
  "bootstrapped": true,
  "cache_protection_ready": true,
  "cache_ready": true,
  "drain_mode": false,
  "executors_count": 5,
  "registered": true,
  "status": "ok"
}
```

- `protected_snapshot_age_seconds`: 39 (canary) / 38 / 39 / 19 — **fresh
  snapshots on every worker at capture time**.
- `fleetctl doctor --production`: **all PASS** (identity / connection /
  readiness / digest) for all 4 workers.

## protected-assets / circuit-breaker recovery evidence

The poller path that was blocked (`[CIRCUIT_BREAKER] Request rejected -
circuit is open (endpoint: /api/v1/agent/cache/protected-assets)`) has
**recovered autonomously** — no worker restart, no flag bypass:

| Worker | CB rejections (window) | Last rejection | After last rejection |
| --- | --- | --- | --- |
| `host_57_129_132_133` | 16 188 (24 h) | **2026-08-10 20:15:28** | none |
| `host_57_131_20_173` | 80 (6 h) | **2026-08-10 20:15:41** | none |
| `velox-worker-13197` | 79 (6 h) | **2026-08-10 20:14:54** | none |
| `velox-worker-523925eb` | 78 (6 h) | **2026-08-10 20:14:35** | none |

- The last rejections on all workers coincide with the master downtime /
  restart window (20:10–20:15). From ~20:16 the master journal shows the
  pollers succeeding continuously:

  ```text
  2026/08/10 20:20:11 GET /api/v1/agent/cache/protected-assets 200 627.456µs
  2026/08/10 20:20:24 GET /api/v1/agent/cache/protected-assets 200 364.702µs
  2026/08/10 20:20:28 GET /api/v1/agent/cache/protected-assets 200 424.841µs
  … (every ~7–13 s, all 200)
  ```

- Worker logs do not emit success lines for the poll (only WARN on
  rejection); the success signal is the master 200 log + the fresh
  `protected_snapshot_age_seconds` on each readiness card.
- Circuit-breaker config in force: failure threshold 5, success threshold
  3, half-open timeout 60 s (`CircuitBreakerTimeoutSecs` default) — the
  observed recovery is consistent with an automatic OPEN → CLOSED
  transition once the master started answering 200.

## Findings for the run

1. **protected-assets pipeline is healthy again** — 4/4
   `cache_protection_ready=true` with fresh snapshots. The auth fix on the
   master is effective. ✅
2. **Canary `host_57_129_132_133` is intentionally DRAINING** (isolated
   for the certification). It must be resumed via the canonical path when
   the run is ready; readiness itself is `ok`.
3. **Fleet digest is NOT uniform**: canary `33795a3a…`, other three
   `90ffddd6…` (each matches its own target). The serial rollout must
   unify the fleet onto one digest.
4. **`digest_state` differs**: `FAILED` (canary) vs `PENDING` (others) —
   to be revisited during the rollout; not a readiness blocker.
5. Master downtime window 20:10–20:15 caused transient worker
   registration failures (`dial tcp 51.91.11.36:9000: connect: connection
   refused`); all workers reconnected after restart.
6. **Pre-existing uncommitted WIP** in the repo working tree (not part of
   this commit, left untouched): `DataServer/cmd/server/bootstrap_wiring.go`,
   `DataServer/internal/config/config_bootstrap.go`,
   `DataServer/internal/config/config_types.go`,
   `DataServer/internal/fleet/resume_executor.go`,
   `DataServer/internal/fleet/smoke_health_runner.go`,
   `deploy/openbao/scripts/resolve-master-env.sh`,
   `scripts/operator/deploy-production.sh`.

## Commands used (token values never printed)

```bash
# wrapper (sources .velox/production.env, refuses to echo secrets)
source scripts/operator/with-production-env.sh
curl -sS -w '\nHTTP:%{http_code}\n' "$VELOX_MASTER_URL/health"
curl -sS -w '\nHTTP:%{http_code}\n' "$VELOX_MASTER_URL/health/ready"

# fleet state
scripts/fleetctl status
scripts/fleetctl doctor --production
scripts/fleetctl inspect <worker_id>          # per worker
scripts/fleetctl ssh-check

# master digest / units (local, sudo)
sudo docker inspect --format '{{.Config.Image}}' velox-server
sudo journalctl -u velox-server --since '6 hours ago' | grep -i protected-assets

# worker journals (SSH from master; key + known_hosts copied to /tmp with 0600)
ssh -i /tmp/k1 -o UserKnownHostsFile=/tmp/kh1 pierone@<host> \
  'sudo -n journalctl -u velox-worker.service --since "6 hours ago" --no-pager | grep -c CIRCUIT_BREAKER'
```

## Canary recovery verification — plan step 2 (2026-08-10 ~20:26 UTC)

**Outcome: autonomous recovery CONFIRMED — no worker restart performed.**
The planned restart condition ("if after the timeout it keeps rejecting")
was never met.

Evidence collected after the last rejection at 20:15:28:

| Signal | Value |
| --- | --- |
| `CIRCUIT_BREAKER` lines after 20:15:28 | **0** (the 5 counted at the boundary are all at 20:15:28 itself) |
| Master `protected-assets 200` (last 15 min) | **93** — poller succeeding continuously |
| Worker `/health` (127.0.0.1:8081) | `{"status":"ok","worker_id":"host_57_129_132_133","registered":true,"uptime_sec":2237}` |
| Worker `/health/ready` (127.0.0.1:8081) | `status:ok` — `cache_protection_ready:true, cache_ready:true, bootstrapped:true, blob_ready:true, registered:true, executors_count:5, protected_snapshot_age_seconds:41` |
| Worker container | `velox-worker` — `Up 37 minutes (healthy)` (no restart in this window) |
| Master worker card | health DRAINING (isolation), active_jobs 0, snapshot age 39s |
| Cache cleanup loop | no longer skipped: `[CACHE_CLEANUP] inspected=0 removed=0 skipped_protected=0 skipped_leased=0` (previously `no valid protection snapshot, skipping cleanup`) |

Interpretation: the circuit breaker made the autonomous transition within
~1 minute of the master returning to service (~20:16), consistent with the
60 s half-open timeout (failure threshold 5, success threshold 3). The
poller resumed, snapshots are accepted and fresh (39–41 s), and the
`cache_protection_ready` readiness gate is open. Cache and assets were
touched by nothing: no restart, no wipe, no prefetch.

### Observation — one pre-existing failed job on the canary (not a blocker)

At 20:17:39 (during the smoke window, before this verification) a job ran
on the canary and failed: `job_cf30b525118fbd21`, error `common asset
resolver: scenes_json[0].clip.url: failed to download velox asset
13df6d1ef…: asset not found`, attempt_count 1, artifacts 0, deliveries 0.
It is terminal (active_jobs now 0) and unrelated to the circuit-breaker
recovery; it points at the smoke-asset pickup gap addressed by
`fix(fleet): configure production smoke asset` (`88430140`).

## Poller + auth verification — plan step 3 (2026-08-10 ~20:28 UTC)

**All three requested checks are green.**

### 1. Real request with 200 (master journal, process `481275`)

Transition window 20:15:00–20:15:58 on the production master:

```text
20:15:09 GET /api/v1/agent/cache/protected-assets 200  8.88032ms
20:15:17 GET /api/v1/agent/cache/protected-assets 401  265.069µs   ← only 401 in window (last pre-fix rejection)
20:15:25 GET /api/v1/agent/cache/protected-assets 200  722.347µs   ← auth accepted from here on
20:15:27 GET /api/v1/agent/cache/protected-assets 200  974.112µs
20:15:28 GET /api/v1/agent/cache/protected-assets 200  834.419µs
20:15:35 GET /api/v1/agent/cache/protected-assets 200  638.277µs
20:15:54 GET /api/v1/agent/cache/protected-assets 200  396.23µs
20:15:58 GET /api/v1/agent/cache/protected-assets 200  6.403187ms
```

24 h totals: **1899 requests, 1897 × 200, exactly 2 × 401** (19:54:50 on the
old process `430787`, 20:15:17 on the new process) — i.e. zero non-200
responses after 20:15:25. The poll is still flowing at capture time
(20:28:05 / 20:28:11 / 20:28:24, all 200).

### 2. No `[CIRCUIT_BREAKER]` after recovery

Worker journal (`velox-worker.service`, canary): **0** `CIRCUIT_BREAKER`
lines after `2026-08-10 20:15:29` (the pre-flight counted 5 lines all at
the 20:15:28 boundary second itself).

### 3. Master auth accepts the bound credential (code + runtime)

- Route: `r.GET("/api/v1/agent/cache/protected-assets",
  m.protectedAssetsAuth, m.protectedAssetsHandler.Snapshot())`
  (`DataServer/internal/app/workers.go:247`).
- `protectedAssetsAuth = api.WorkerOrAdminAuthMiddleware(cfg, tokenMgr)`
  (`workers.go:76`). The middleware (`api_v1.go`) accepts the persistent
  `Hello.credential_hash` ONLY when bound to `X-Worker-ID` and validated
  against the credential row via `TokenManager.ValidateWorkerCredentialToken`
  (commit `29b880b2`).
- Runtime proof: the continuous 200s from 20:15:25 mean the bound
  credential is accepted; the single 401 at 20:15:17 is the last
  pre-fix/edge rejection, after which the circuit closed.

### “Snapshot accepted” — worker-side equivalent evidence

The worker poller intentionally logs nothing on success (design invariant
in `protected_assets_poller.go`: “The poller does NOT log internally”;
success surfaces through telemetry via `MarkCacheProtectionReady(true)`).
The observable equivalents on the canary at capture time:

```json
{"status":"ok","detail":{"blob_ready":true,"bootstrapped":true,
 "cache_protection_ready":true,"cache_ready":true,
 "protected_snapshot_age_seconds":55,"registered":true}}
```

plus the cleanup loop running with `skipped_protected=0` (a valid
snapshot exists — it is no longer skipped with “no valid protection
snapshot”). A fresh `protected_snapshot_age_seconds` can only be produced
by a 200 poll whose snapshot was applied.

## Next steps (per the certification plan)

1. ✅ Canary recovery confirmed (this document) — no restart needed.
2. Resume `host_57_129_132_133` from DRAINING when the run starts
   (canonical `fleetctl resume`).
3. Serial rollout of the remaining 3 workers onto the canonical digest
   (one at a time, `wait-ready` between, never 4 together).
4. Gate pre-job: 4/4 CONNECTED, 4/4 HEALTHY, 4/4 READY, same digest,
   0 active jobs, 4/4 protection ready, 0 downloads at boot.
5. Onda 1 (4 pinned comic jobs) → deep verification → Onda 2 (warm cache)
   → negative copy-only → final report.
