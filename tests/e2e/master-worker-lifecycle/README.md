# Master–Worker Lifecycle E2E

This suite exercises the real Velox master and the real `dev-hello-client`
against an isolated SQLite database. It is intentionally separate from the
full workload E2E: no video engine or external delivery provider is needed to
prove the control-plane lifecycle.

## Coverage

The orchestrator validates:

1. **Bootstrap fail-fast + clean startup** — first starts `velox-server` with an
   intentionally unsupported database driver and asserts a bounded non-zero
   exit before readiness; it then starts the real master with a fresh migrated
   SQLite database and waits for `/health/ready`.
2. **Registration + collision** — first calls the canonical
   `POST /api/v1/workers/register` endpoint to persist the worker identity,
   then starts `DataServer/cmd/dev-hello-client`, completes the typed gRPC
   `Hello`/`HelloAck` handshake, and verifies the worker read model is
   `CONNECTED`. A second real gRPC client using the same `WorkerID` and a
   different credential must fail with typed `AlreadyExists`, create no second
   active session, and leave the first session active.
3. **Heartbeat** — the client sends real typed heartbeats on a short interval;
   the master exposes the worker as connected.
4. **Persisted operation** — posts an authenticated admin drain mutation,
   polls `GET /api/v1/admin/operations/:operation_id`, and cross-checks the
   terminal `SUCCEEDED` row directly in `fleet_operations`.
5. **Heartbeat loss** — freezes the real client with `SIGSTOP`; after the
   isolated E2E thresholds elapse, the worker becomes `STALE` or
   `DISCONNECTED` while remaining registered, and the canonical API connection
   state is no longer live. Scheduler admission is covered by the focused Go
   regression `TestGetSchedulableWorkers_ExcludesStaleHeartbeat`.
6. **Master restart** — terminates and restarts only the master against the
   same SQLite file; the previously completed operation remains readable via
   both API and database.
7. **Reconnection** — releases and replaces the stopped client without
   re-registering over HTTP, then verifies the same `WorkerID` is `CONNECTED`
   with exactly one active session. The completed drain state remains visible
   through the read model.

The test never uses `sudo`, firewall changes, production paths, or a production
DB. `SIGSTOP` is the deterministic local equivalent of a network/heartbeat
blackout and is released during cleanup. Required host tools are `go`, `curl`,
`jq`, `sqlite3`, `python3`, `setsid`, `timeout`, `sha256sum`, and `awk`.

## Run

From the repository root:

```bash
make e2e-master-worker
```

Direct invocation:

```bash
bash tests/e2e/master-worker-lifecycle/run.sh
```

The script builds `velox-server`, `dev-hello-client`, and the SQLite fixture
seeder into the workdir when binaries are not supplied. The seeder is built
once and run with a 120-second timeout, so dependency compilation or migration
problems fail with a preserved `seed.log` instead of hanging the suite. By
default evidence is retained at:

```text
/tmp/velox-e2e-master-worker-lifecycle/
├── bin/
├── data/velox.db
├── logs/master.log
├── logs/worker.log
├── master.env
├── seed.log
└── operation.id
```

Set `E2E_KEEP_WORKDIR=0` to remove this directory after the run.

## Configuration

Useful overrides:

| Variable | Default | Purpose |
|---|---|---|
| `E2E_WORKDIR` | `/tmp/velox-e2e-master-worker-lifecycle` | Isolated evidence/runtime root |
| `E2E_MASTER_PORT` | dynamic | Master HTTP port |
| `E2E_GRPC_PORT` | dynamic | Master gRPC port |
| `E2E_ADMIN_TOKEN` | `e2e-master-worker-admin` | Admin bearer used by the test |
| `E2E_WORKER_ID` | `e2e-master-worker-1` | Canonical worker identity |
| `E2E_WORKER_SECRET` | `e2e-lifecycle-secret` | Dev client credential input |
| `E2E_HEARTBEAT_INTERVAL` | `1s` | Synthetic client heartbeat cadence |
| `E2E_HEARTBEAT_WINDOW` | `30m` | Bounded lifetime of the synthetic worker process |
| `E2E_STALE_WAIT_SECONDS` | `180` | Wait budget after `SIGSTOP`; canonical read-model stale threshold is 150s |
| `E2E_OPERATION_TIMEOUT` | `30` | Operation terminal-state budget |
| `E2E_KEEP_WORKDIR` | `1` | Preserve evidence (`0` removes it) |
| `VELOX_SERVER_BIN` | workdir binary | Use a pre-built master |
| `VELOX_DEV_HELLO_BIN` | workdir binary | Use a pre-built dev client |
| `VELOX_SEED_BIN` | workdir binary | Use a pre-built SQLite fixture seeder |

The read model derives status from the canonical store thresholds: `STALE` at
150 seconds without a fresh heartbeat and `DISCONNECTED` at 300 seconds. The
suite waits 180 seconds by default, so it verifies the first transition without
waiting for the five-minute partition threshold. The synthetic client now stays
alive with a bounded 30-minute heartbeat window, allowing the SIGSTOP and
restart/reconnect phases to exercise a real session. Override
`E2E_STALE_WAIT_SECONDS` only when the deployed read-model contract is known to
use a different threshold.

## Failure diagnosis

Inspect:

```bash
cat /tmp/velox-e2e-master-worker-lifecycle/logs/master.log
tail -100 /tmp/velox-e2e-master-worker-lifecycle/logs/worker.log
sqlite3 /tmp/velox-e2e-master-worker-lifecycle/data/velox.db \
  'SELECT operation_id, worker_id, op, status, error_message FROM fleet_operations;'
sqlite3 /tmp/velox-e2e-master-worker-lifecycle/data/velox.db \
  'SELECT worker_id, session_id, status, revoked FROM worker_sessions;'
```

A failure after master restart should be investigated against the preserved
SQLite file, not repaired manually. The suite is designed to make persisted
state and process logs available for post-mortem analysis.

The collision probe temporarily rotates the credential hash only inside the
isolated test database, starts a second real gRPC client, and restores the
original hash before reconnect testing. No production credential or secret is
written to the repository.
