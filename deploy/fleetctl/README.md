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
| 1 | `--token-file=PATH` | chmod-600 file (group/other rejected by client) |
| 2 | `$VELOX_ADMIN_TOKEN` env var | trimmed; no leading/trailing whitespace |
| 3 | `/opt/velox/secrets/admin-token` | canonical Master-side file |

Master URL resolution:

| Precedence | Source | Notes |
|---:|---|---|
| 1 | `--master=https://HOST:8000` | required flag |
| 2 | `$VELOX_MASTER_URL` env var | fallback |
| 3 | (none) | surfaced as `fleetctl: misuse: master URL required` |

The canonical Master port is **8000** (REST + admin auth).
The gRPC control plane on **:9000** is NOT wrapped by fleetctl —
the worker's gRPC handshake is direct-to-broker and not an
operator surface.

---

## 3. Sub-Commands

`fleetctl` exposes exactly the seven sub-commands listed in the
Step 15/15 spec. Restart + logs are intentionally NOT in this
set; operators who need them continue to use the Step 11/15
[fleet-* Ansible playbooks](../playbooks/) (the binary does
NOT wrap ansible-playbook invocations in atomic Step 15+).

### 3.1 `fleetctl status`

```bash
fleetctl --master=https://velox.example.com:8000 status
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

### 3.2 `fleetctl inspect <worker_id>`

```bash
fleetctl --master=https://velox.example.com:8000 inspect velox-worker-13197
```

Prints the full WorkerCard as indented JSON (status, health,
session_active, executor, executor_version, image_digest,
software_version, desired_version, last_heartbeat_at,
active_jobs/max, deployment_state, last_smoke_status/at,
last_restart_at).

### 3.3 `fleetctl drain <worker_id>`

```bash
fleetctl --master=https://velox.example.com:8000 drain velox-worker-13197
# optional: --reason="manual drain for cert rotation"
```

Sets the worker's `WorkerInfo.Drain = true` via the Step 6/15
mutation endpoint, then polls the `fleet_operations` ledger
until terminal status. Default wait budget: **10 min**.

### 3.4 `fleetctl update <worker_id> --digest sha256:...`

```bash
DIGEST=sha256:$(curl -s https://registry/foo/bar/manifests/latest | jq -r .digest)
fleetctl --master=https://velox.example.com:8000 \
  update velox-worker-13197 --digest "$DIGEST"
```

Drives the Step 9/15 image update cascade end-to-end via the
Master REST API (cosign verify + forward + health + rolling
commit). The `--digest` flag is validated **client-side**
before any HTTP call — a typo or mobile ref (`:latest`,
`:main`, `:stable`) returns `fleetctl: image-invalid:` with
exit code 7, no Master call. Default wait budget: **30 min**.

### 3.5 `fleetctl smoke <worker_id>`

```bash
fleetctl --master=https://velox.example.com:8000 smoke velox-worker-13197
```

Issues a Step 12/15 Level-D smoke (lease → download asset →
ffmpeg → Drive delivery) and polls until terminal. Smoke
`asset_id`, `render_plan`, and `timeout_sec` use the canonical
canary defaults (`asset-canary-001` + a 5-second ffmpeg passthrough
+ 600 s). Default wait budget: **12 min**.

### 3.6 `fleetctl resume <worker_id>`

```bash
fleetctl --master=https://velox.example.com:8000 \
  resume velox-worker-13197 --reason="after cert rotation"
```

Clears a previous drain / quarantine via the Step 6/15
`/resume` mutation. Default wait budget: **5 min**.

### 3.7 `fleetctl rollback <worker_id>`

```bash
fleetctl --master=https://velox.example.com:8000 \
  rollback velox-worker-13197 --reason="image pinned incorrect"
```

Drives the Step 9/15 rollback cascade (`previous_digest`
re-issued, cosign re-verified, container restarted). Default
wait budget: **30 min**.

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
fleetctl smoke worker-13197 || rc=$?
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
┌────────────────────────┐  ┌─────────────────────────┐
│ Master REST :8000      │  │ Ansible Master node     │
│  Step 1,6,10,12,13     │  │  Step 11/15 playbooks   │
│  (audit, drain/smoke…) │  │  fleet-restart / logs   │
└──────────┬─────────────┘  └──────────┬──────────────┘
           │                            │
           ▼                            ▼
   ┌─────────────────────────────┐  ┌───────────────────┐
   │ FleetController tick         │  │ SSH direct on     │
   │  Step 4/15 (5min scheduler +  │  │ the worker host   │
   │  executor lifecycle)         │  │ (docker compose,  │
   └─────────────────────────────┘  │ healthcheck loop) │
                                    └───────────────────┘
```

`fleetctl` does NOT wrap the ansible side. Operators running
both sides concurrently do:

```bash
# Step 1: HTTP-driven image update + smoke
fleetctl update worker-13197 --digest=$DIGEST || exit $?
fleetctl smoke  worker-13197                    || exit $?
# Step 2 (optional, for transient cleanup):
ansible-playbook -i deploy/ansible/inventory.ini \
                 deploy/playbooks/fleet-restart.yml
```

---

## 7. Examples (1-shot Operators)

```bash
# Canary drain + resume:
fleetctl drain  worker-523925eb && \
  fleetctl smoke worker-523925eb && \
  fleetctl resume worker-523925eb

# Image digest update (manifold):
DIGEST=sha256:...
for w in worker-57_129 worker-57_131 worker-13197 worker-523925; do
  echo "=== $w ==="
  fleetctl update "$w" --digest "$DIGEST" || { echo "FAIL on $w"; break; }
done

# Smoke-everything:
for w in $(fleetctl status | awk 'NR>2 {print $1}' | grep -v WORKER_ID); do
  fleetctl smoke "$w" || echo "smoke failed on $w"
done
```

---

## 8. FAQ

**Q: Why no `restart` / `logs` sub-command?**
A: They're not in the Step 15/15 spec. Operators continue to
use the Step 11/15 Ansible playbooks (`fleet-restart.yml`,
`fleet-logs.yml`). Adding them to fleetctl would require
either shelling out to `ansible-playbook` from inside the Go
binary OR wrapping SSH directly — both are larger scope than
the atomic Step 15/15 commit.

**Q: Why not Cobra / spf13?**
A: AGENTS.md + the 0-import search across the repo show no
existing cobra usage. The stdlib `flag.NewFlagSet` per
sub-command gives clean routing with zero new dependencies.
A follow-up commit can introduce Cobra if request-completion
shell / config-file / global-flag ergonomics become
operationally meaningful.

**Q: Where does the `--master` flag come from?**
A: `--master=https://HOST:8000` flag, or `$VELOX_MASTER_URL`
env var. If both are absent the binary surfaces
`fleetctl: misuse: master URL required` (exit 2) before any
HTTP call.

**Q: How does `--wait` work?**
A: The atomic Step 15/15 ships hard-coded wait budgets per
sub-command (see §3). A future commit may add `--wait=<duration>`
override; today the operator must match the rule "expect up
to 12 min for smokes, 30 min for updates / rollbacks".

---

## 9. Layered Commitments (what ships NOW vs LATER)

| Surface | Status | Where |
|---|---|---|
| 7 listed sub-commands (status/inspect/drain/update/smoke/resume/rollback) | ✅ atomic Step 15/15 | this binary |
| Token resolution (`--token-file` / `$VELOX_ADMIN_TOKEN` / canonical file) | ✅ atomic | client.go + auth.go |
| Exit-code matrix | ✅ atomic | exit_codes.go |
| `--digest sha256:64-hex` client-side validator | ✅ atomic | digest.go |
| Operation polling (5 s interval, kind-specific deadlines) | ✅ atomic | polling.go |
| Per-sub-command pretty-printing | ✅ atomic | handlers.go |
| `restart` / `logs` sub-commands via ansible | ⏳ future | Step 11/15 Ansible playbooks (intentional decoupling) |
| Cobra CLI parser (vs stdlib flag) | ⏳ future | follow-up if operational payoff |
| Auditing `fleetctl <sub>` invocations into the operation row's actor log | ⏳ future | Step 4/15 ledger extension |

For unchanged parts, the Step 11/15 [fleet-* playbooks](../playbooks/)
remain the canonical surface — `fleetctl` and the playbooks
are complementary, not overlapping.
