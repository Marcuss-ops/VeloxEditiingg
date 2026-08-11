# fleetctl — Velox fleet-operator unified CLI

`fleetctl` is the canonical operator-facing CLI for the
**Step 15/15 fleet-operator rollout**. It is a single Go binary
installed on the Master that wraps the Master REST API
primitives (Step 1/15 + Step 6/15 + Step 10/15 + Step 12/15 +
Step 13/15) and stdout-greppable, scriptable, exit-coded.

This document is intentionally minimal per the design
lock-in: each section is a 1-screen operator reference.

---

## 1. Install

The canonical install location is `/opt/velox/bin/fleetctl`
with a `/usr/local/bin/fleetctl` symlink so the binary is in
the operator's default `$PATH`:

```bash
sudo install -m 0755 /opt/velox/current/bin/fleetctl /opt/velox/bin/fleetctl
sudo ln -sf /opt/velox/bin/fleetctl /usr/local/bin/fleetctl
fleetctl --version          # → fleetctl v1.0.0 (Step 15/15)
```

There is no system package (`deb`/`rpm`) installer in
atomic Step 15/15 — operators drop the build artifact on the
Master directly.

---

## 2. Auth

Token resolution precedence (top wins):

| Precedence | Source | Notes |
|---:|---|---|
| 1 | `$TOKEN_FILE` | chmod-600 file containing `VELOX_ADMIN_TOKEN=...` |
| 2 | `$VELOX_ADMIN_TOKEN` env var | trimmed; no leading/trailing whitespace |
| 3 | `/opt/velox/secrets/admin-token` | canonical Master-side file when supported by the installed client |

Master URL resolution:

| Precedence | Source | Notes |
|---:|---|---|
| 1 | `$VELOX_MASTER_URL` env var | Master REST base URL |
| 2 | (none) | the shell entrypoint uses its documented local default when unset |

The canonical Master port is **8000** (REST + admin auth).
The gRPC control plane on **:9000** is NOT wrapped by fleetctl —
the worker's gRPC handshake is direct-to-broker and not an
operator surface.

---

## 3. Sub-Commands

`fleetctl` is the operator facade for the Master REST API. Production worker
updates, rollbacks and restarts are API operations; operators do not invoke
Ansible, SSH, Docker, or systemd mutations directly. The shell entrypoint in
`scripts/fleetctl` is the repository's canonical command reference; this file
summarizes the same Master-side contract.

The supported commands are `status`, `inspect`, `drain`, `resume`,
`quarantine`, `restart`, `update`, `rollback`, `operations`, and `smoke`.
The Go binary also exposes read-only observability commands `job` and
`doctor`; these do not require SSH, Docker, or SQLite access on a worker.

### 3.1 `scripts/fleetctl status`

```bash
VELOX_MASTER_URL=https://velox.example.com:8000 scripts/fleetctl status
```

Lists every worker registered with the Master + a table-shaped
WorkerCard summary (one row per worker):

```
WORKER_ID                       STATUS      HEALTH     JOBS      EXECUTOR@VERSION         LAST_SMOKE
---------                       ------      ------     ----      --------------         ----------
velox-worker-13197              CONNECTED   HEALTHY    0/1       scene.composite.v1@1    SUCCEEDED
velox-worker-523925eb           CONNECTED   DEGRADED   1/1       scene.composite.v1@1    FAILED
…
```

### 3.2 `scripts/fleetctl inspect <worker_id> [--json]`

```bash
VELOX_MASTER_URL=https://velox.example.com:8000 scripts/fleetctl inspect velox-worker-13197
VELOX_MASTER_URL=https://velox.example.com:8000 scripts/fleetctl inspect --json velox-worker-13197
```

Fetches the WorkerCard for one worker from
`GET /api/v1/admin/workers/{id}` and renders it in **two canonical
operator sections** — the image state and the rollout history are
deliberately separate views, so an old FAILED rollout never makes a
worker whose running digest matches the target look unhealthy:

```
worker_id:    velox-worker-13197
worker_name:  velox-worker-01
status:       CONNECTED
health:       HEALTHY
executor:     scene.composite.v1@1
jobs:         0/1

IMAGE
  running_digest = ghcr.io/acme/velox-worker@sha256:337d…
  target_digest  = ghcr.io/acme/velox-worker@sha256:337d…
  digest_match   = true

LAST UPDATE OPERATION
  status       = SUCCEEDED
  reason       =
  operation_id = op_9f2c…
  type         = update
  started_at   = 2026-08-09T10:12:00Z
  finished_at  = 2026-08-09T10:17:00Z
```

- **IMAGE** — real-time image state (`running_digest` vs
  `target_digest` vs `digest_match`); no operation history.
- **LAST UPDATE OPERATION** — the last rollout ledger row
  (`status`/`reason`, `operation_id`, `type`, started/finished
  timestamps). A worker with no ledger row prints
  `(no update operation on record)`.

`--json` prints the **full WorkerCard as indented JSON** — the
machine-readable contract consumed by scripts such as
`ops/align-worker-digest.sh` and `ops/canary-worker-rollout.sh`.
The two views are nested under `image_state` and `operation_state`;
legacy flat keys (`image_digest`, `running_digest`, `target_digest`)
remain for compatibility.

### 3.3 `scripts/fleetctl drain <worker_id>`

```bash
VELOX_MASTER_URL=https://velox.example.com:8000 scripts/fleetctl drain velox-worker-13197
# optional reason is the second positional argument:
# scripts/fleetctl drain velox-worker-13197 "manual drain for cert rotation"
```

Sets the worker's `WorkerInfo.Drain = true` via the Step 6/15
mutation endpoint, then polls the `fleet_operations` ledger
until terminal status. Default wait budget: **10 min**.

### 3.4 `scripts/fleetctl quarantine <worker_id>`

```bash
scripts/fleetctl quarantine velox-worker-13197 "asset failures"
```

Quarantines a worker through the authenticated Master operation path.

### 3.5 `scripts/fleetctl restart <worker_id>`

```bash
scripts/fleetctl restart velox-worker-13197 "after config change"
```

Publishes the restart operation; the Master owns the worker-side action.

### 3.6 `scripts/fleetctl worker-config set <worker_id>`

```bash
scripts/fleetctl worker-config set velox-worker-13197 \
  --audio-mix-strategy optimized --audio-mix-profile 0
```

The command publishes an audited Master operation. The Master invokes only
the allowlisted root-owned `velox-worker-set-config` helper over SSH; the
helper updates `worker.env` atomically, restarts the canonical service, waits
for `/health/ready`, and rolls back the file if readiness does not recover.
Operators must not edit `worker.env` or run direct SSH mutation commands.

### 3.7 `scripts/fleetctl operations [worker_id] [status]`

```bash
scripts/fleetctl operations velox-worker-13197
```

Reads the `fleet_operations` audit ledger; it does not mutate the worker.

### 3.8 `scripts/fleetctl update <worker_id> <ghcr-image@sha256:digest>`

```bash
IMAGE=ghcr.io/<owner>/velox-worker@sha256:<64-lowercase-hex>
scripts/fleetctl update velox-worker-13197 "$IMAGE" "canary rollout"
```

Drives the Step 9/15 image update cascade end-to-end via the
Master REST API (cosign verify + forward + health + rolling
commit). The `--digest` flag is validated **client-side**
before any HTTP call — a typo or mutable ref (`:latest`,
`:main`, `:stable`) returns `fleetctl: image-invalid:` with
exit code 7, no Master call. Default wait budget: **30 min**.

### 3.8 `scripts/fleetctl smoke <worker_id>`

```bash
scripts/fleetctl smoke velox-worker-13197
```

Issues a Step 12/15 Level-D smoke (lease → download asset →
ffmpeg → Drive delivery) and polls until terminal. Smoke
`asset_id`, `render_plan`, and `timeout_sec` use the canonical
canary defaults (`asset-canary-001` + a 5-second ffmpeg passthrough
+ 600 s). Default wait budget: **12 min**.

### 3.9 `scripts/fleetctl resume <worker_id>`

```bash
scripts/fleetctl resume velox-worker-13197 "after cert rotation"
```

Clears a previous drain / quarantine via the Step 6/15
`/resume` mutation. Default wait budget: **5 min**.

### 3.10 `scripts/fleetctl rollback <worker_id> <ghcr-image@sha256:digest>`

```bash
IMAGE=ghcr.io/<owner>/velox-worker@sha256:<previous-64-lowercase-hex>
scripts/fleetctl rollback velox-worker-13197 "$IMAGE" "image pinned incorrect"
```

Drives the Step 9/15 rollback cascade (`previous_digest`
re-issued, cosign re-verified, container restarted). Default
wait budget: **30 min**.

### 3.11 `fleetctl job inspect <job_id>`

```bash
fleetctl job inspect job_10b41a3c469bd84d
```

Returns one JSON read model containing lifecycle, attempts, phase timing,
typed resource/cache metrics, per-scene segment telemetry, artifacts,
deliveries, and persisted execution events.

### 3.12 `fleetctl job metrics <job_id>`

```bash
fleetctl job metrics job_10b41a3c469bd84d
```

Prints the execution and cache metrics projection, including FFmpeg, frame,
phase, and cache hit/miss counters.

### 3.13 `fleetctl job watch <job_id>`

```bash
fleetctl job watch job_10b41a3c469bd84d
```

Polls the Master event timeline and prints newly observed events until the
job reaches `SUCCEEDED`, `FAILED`, or `CANCELLED`. `docker logs` remains a
break-glass diagnostic only.

### 3.14 `fleetctl doctor --production`

```bash
fleetctl doctor --production
```

Runs fail-closed checks for immutable worker identity, connection,
worker-reported readiness, and desired/running image digest. Missing
readiness telemetry is `UNKNOWN` and makes the production doctor unhealthy.
`worker_id` remains the mTLS identity; `worker_name` is display-only.

---

## 4. Exit-Code Matrix

Shell scripts (`set -e`, `&&`, `||`) can pattern-match on `$?`
without parsing stderr:

| Code | Constant | Meaning |
|---:|---|---|
| 0 | `ExitOK` | Operation completed (sync read or polled to `SUCCEEDED`) |
| 1 | `ExitUnexpected` | Transport / decode / HTTP 5xx / unknown failure |
| 2 | `ExitMisuse` | Missing arg / bad flag / missing token / 401 / 403 |
| 4 | `ExitWorkerNotFound` | Master returned 404 for `/admin/workers/{id}` |
| 5 | `ExitLeaseUnavailable` | Master returned 409 (operation in-flight) |
| 6 | `ExitSmokeFailed` | Smoke operation polled to `FAILED` |
| 7 | `ExitImageInvalid` | `--digest` rejected by client-side validator OR Master |
| 8 | `ExitRollbackFailed` | Rollback operation polled to `FAILED/ROLLBACK` |

Examples:

```bash
scripts/fleetctl smoke worker-13197 || rc=$?
case $rc in
  0) echo "smoke OK" ;;
  6) echo "smoke failed — check audit row error_message" ;;
  *) echo "fleetctl exited $rc" ;;
esac
```

---

## 5. Audit + Telemetry

- Every sub-command that posts to the Master emits a row in
  the `fleet_operations` ledger with the operator-readable
  `--reason` text. The dashboard queries this ledger via
  the Step 7/15 audit endpoints (to be added in a follow-up).
- `fleetctl update` / `fleetctl smoke` / `fleetctl rollback`
  produce a row in `deployment_records` / `smoke_runs` with
  duration_ms stamps — these feed the Step 13/15 snapshot
  rollup (5-minute tick) read by the dashboard's per-worker
  `/api/v1/admin/workers/{id}/metrics`.

---

## 6. Architecture (Stack Diagram)

```
┌────────────────────────┐
│ Operator's terminal    │
│  $ fleetctl status …   │
└──────────┬─────────────┘
           │ Authorization: Bearer <token>
           │ (HTTPS, :8000)
           ▼
┌────────────────────────────────────────┐
│ Master REST :8000                      │
│  Step 1,6,10,12,13 (audit, drain/…)    │
└──────────┬─────────────────────────────┘
           │ FleetController tick
           │  Step 4/15 (5min scheduler +
           │  executor lifecycle)
           ▼
┌────────────────────────────────────────┐
│ FleetController → UpdateExecutor        │
│  drain → cosign → pull → activate-image│
│  → health → smoke → Drive              │
│  WorkerNodeRegistry → SSH → helper     │
└────────────────────────────────────────┘
```

`fleetctl` talks only to the Master REST API; the Master resolves the
`WorkerNodeRegistry` and its `ansible_hosts` connectivity row, then
`UpdateExecutor` uses SSH and `velox-worker-activate-image`. The CLI never
invokes a host-side playbook. A complete rollout is:

```bash
# Step 1: HTTP-driven image update + smoke
scripts/fleetctl update worker-13197 --digest "$DIGEST" || exit $?
scripts/fleetctl smoke  worker-13197                       || exit $?
```

---

## 7. Examples (1-shot Operators)

```bash
# Canary drain + resume:
scripts/fleetctl drain  worker-523925eb && \
  scripts/fleetctl smoke worker-523925eb && \
  scripts/fleetctl resume worker-523925eb

# Image digest update (manifold):
IMAGE=ghcr.io/<owner>/velox-worker@sha256:<64-lowercase-hex>
for w in worker-57_129 worker-57_131 worker-13197 worker-523925; do
  echo "=== $w ==="
  scripts/fleetctl update "$w" "$IMAGE" "serial rollout" || { echo "FAIL on $w"; break; }
done

# Smoke-everything:
for w in $(scripts/fleetctl status | awk 'NR>2 {print $1}' | grep -v WORKER_ID); do
  scripts/fleetctl smoke "$w" || echo "smoke failed on $w"
done
```

---

## 8. FAQ

**Q: Where does restart run?**
A: `scripts/fleetctl restart <worker_id> [reason]` publishes the authenticated
Master operation. The Master owns the worker-side SSH/systemd action; operators
must not bypass the operation ledger with direct host mutation. Logs and
low-level inspection remain diagnostic commands documented in
`docs/operations/fleetctl.md`.

**Q: Why not Cobra / spf13?**
A: AGENTS.md + the 0-import search across the repo show no
existing cobra usage. The stdlib `flag.NewFlagSet` per
sub-command gives clean routing with zero new dependencies.
A follow-up commit can introduce Cobra if request-completion
shell / config-file / global-flag ergonomics become
operationally meaningful.

**Q: Where does the Master URL come from?**
A: Set `$VELOX_MASTER_URL` to the Master REST base URL. The shell entrypoint
uses `http://127.0.0.1:8000` when it is unset; authentication still requires
`VELOX_ADMIN_TOKEN` or `TOKEN_FILE`.

**Q: How does `--wait` work?**
A: The atomic Step 15/15 ships hard-coded wait budgets per
sub-command (see §3). A future commit may add `--wait=<duration>`
override; today the operator must match the rule "expect up
to 12 min for smokes, 30 min for updates / rollbacks".

---

## 9. Layered Commitments (what ships NOW vs LATER)

| Surface | Status | Where |
|---|---|---|
| Master API commands (status/inspect/drain/resume/quarantine/restart/worker-config/update/rollback/operations/smoke) | ✅ canonical | `scripts/fleetctl` |
| Token resolution (`$TOKEN_FILE` / `$VELOX_ADMIN_TOKEN`) | ✅ canonical | `scripts/fleetctl` |
| Exit-code matrix | ✅ atomic | exit_codes.go |
| Pinned GHCR digest validation | ✅ canonical | `scripts/fleetctl` |
| Operation polling (5 s interval, kind-specific deadlines) | ✅ atomic | polling.go |
| Per-sub-command pretty-printing | ✅ atomic | handlers.go |
| `restart` / `worker-config` | ✅ Master API operation | `scripts/fleetctl restart`, `scripts/fleetctl worker-config set` |
| `logs` | ⏳ diagnostic-only follow-up | host-side inspection, not a rollout mutation |
| Cobra CLI parser (vs stdlib flag) | ⏳ future | follow-up if operational payoff |
| Auditing `fleetctl <sub>` invocations into the operation row's actor log | ⏳ future | Step 4/15 ledger extension |

The canonical fleet surface is `scripts/fleetctl` (Master REST API):
`FleetController → WorkerNodeRegistry → SSH → root-owned helper`. Host-side
mutation is owned by the Master and must not be performed as an operator
bypass.
