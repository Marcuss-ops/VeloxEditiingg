# PR 3 — gRPC Control-Plane E2E Matrix

End-to-end test for the worker→master gRPC control plane. Exercises the **6-case matrix** of TLS / plaintext combinations that the master’s mTLS handshake must handle correctly, anchored on the contract documented in [`docs/operations/PR-1-migration.md`](../../../docs/operations/PR-1-migration.md).

## What this validates

After PR 1 (`codex/grpc-config-single-source`) the worker-agent refuses to start unless either **(a)** a full mTLS triple is on disk, **or (b)** `allow_insecure_grpc_dev=true` is paired with `environment != "production"`. PR 3 proves the **master-side** counterpart: every combination of master TLS state × worker TLS state resolves to the correct accept/reject verdict.

## Layout

```
tests/e2e/grpc-control-plane/
├── README.md                       (this file)
├── compose.yml                     docker compose v2 reference (manual alt)
├── run.sh                          host-native orchestrator (the matrix entrypoint)
├── assert.sh                       assertion helpers (sourced by run.sh)
├── certs/
│   └── generate-dev-pki.sh         openssl-driven CA + leaf generator (7d / 1d default TTL)
└── configs/
    ├── master.env.example          master env template (run.sh sed-patches per case)
    ├── worker-tls.json             mTLS worker config (env=production, fixed ports)
    └── worker-plaintext.json       dev-insecure worker config (env=dev per PR 1 recipe)
```

`make e2e-grpc` at the repo root builds the binaries once (if missing), then runs `run.sh` over the 6-case matrix.

## Quick start

```bash
# from the repo root
make e2e-grpc                                          # full matrix (~90s once built)
E2E_WORKDIR=/tmp/velox-e2e-grpc make e2e-grpc          # custom workdir
E2E_CLEAN=1 make e2e-grpc                              # wipe workdir on exit
bash tests/e2e/grpc-control-plane/run.sh               # direct invocation
```

## 6-case matrix

| # | Case | Master TLS state | Worker TLS state | Expected verdict |
| - | ---- | ---------------- | ---------------- | ---------------- |
| 1 | plaintext accept | no TLS, `VELOX_GRPC_ALLOW_INSECURE_DEV=true`, allowlist includes worker | `allow_insecure_grpc_dev=true`, `environment=dev` | **PASS** — handshake succeeds, worker registers |
| 2 | TLS accept | full mTLS triple, no insecure bypass, allowlist=worker | matching mTLS triple, `environment=production` | **PASS** — handshake succeeds, worker registers |
| 3 | bad-cert reject | full mTLS triple, allowlist=worker | mTLS triple where leaf is from a separate CA | **PASS** (worker must be REJECTED) — handshake fails at client-cert verification |
| 4 | wrong-CA reject | full mTLS triple from CA-A | full mTLS triple from CA-B | **PASS** (worker must be REJECTED) — `unknown certificate authority` |
| 5 | plaintext-vs-TLS reject | full mTLS triple, no insecure bypass | plaintext (`allow_insecure=true`) | **PASS** (worker must be REJECTED) — server requires TLS |
| 6 | parallel one-accept-one-reject | full mTLS triple, allowlist=`good,bad` | one worker with valid mTLS, one with bad CA | **PASS** — only the good worker registers |

The “PASS” verdict for rejection cases (3-6) means: the **worker** exits non-zero / shows a handshake-failure marker, and the master log shows the matching reject marker (the operator-pointing `PermissionDenied`/`Unauthenticated`/handshake-fail tokens). It is **not** a confirmation of false-negative accept.

## Detection mechanism

run.sh captures per-case artifacts to `$WORKDIR/cases/case-N/`:

```
$WORKDIR/
├── bin/                            built velox-server + velox-worker-agent
├── pki/
│   ├── case-1-…/                   (empty — no PKI for plaintext case)
│   ├── case-2-tls-accept/          full CA+server+worker (7d / 1d TTL)
│   ├── case-3-bad-cert-reject/     master-ca PKI; worker-bad subdir
│   ├── case-3-bad-cert-reject-worker-bad/  rogue CA + worker leaf
│   ├── case-4-wrong-ca-reject-ca-a/        master CA pool (CA-A)
│   ├── case-4-wrong-ca-reject-ca-b/        worker's CA (CA-B)
│   └── case-6-parallel…-good / -bad        case-6 dual-PKI
├── cases/
│   ├── case-1-plaintext-accept/{master.env, master.log, worker-config.json, worker-…log}
│   ├── case-2-tls-accept/…
│   └── …
└── cases-…/                        # one per case, mirrors the per-case settled state
```

assert.sh reads `master.log` and `worker-…log` from each case dir, looking for the markers documented above. The check is purely log-/curl-based — no internal Go state — so the same assertions will work against any Velox release that emits the documented log lines.

## Environment variables (customize the run)

| Variable | Default | Notes |
| -------- | ------- | ----- |
| `E2E_WORKDIR` | `/tmp/velox-e2e-grpc` | certs, logs, dbs, binaries. Preserved on exit unless `E2E_CLEAN=1`. |
| `DATASERVER_ROOT` | `$ROOT/../../DataServer` | source root for `velox-server` build |
| `WORKERAGENT_ROOT` | `$ROOT/../../RemoteCodex/native/worker-agent-go` | source root for `velox-worker-agent` build |
| `VELOX_SERVER_BIN` | `$WORKDIR/bin/velox-server` (built on-demand) | pre-built path skips rebuild |
| `VELOX_WORKER_BIN` | `$WORKDIR/bin/velox-worker-agent` (built on-demand) | pre-built path skips rebuild |
| `E2E_CLEAN` | `0` | `=1` wipes `$WORKDIR` on EXIT |

## Cleanup

`run.sh` registers `trap on_exit EXIT`, plus `INT` and `TERM` handlers that walk `$CHILD_PIDS` and escalate `TERM` → `KILL` after 1s. The workdir is preserved on exit so the operator can grep `master.log` and `worker-…log` for the actual reason a case failed; this is what makes a 6-case failure actionable post-fact.

## Compose reference (operators only)

`compose.yml` is a **REFERENCE** — it is not invoked by `run.sh`. Operators who prefer to drive the workers from containers (matching `/tmp/dev-2workers/` behaviour from PR 1) can use:

```bash
# pseudo flow only; run.sh is the test entrypoint
docker compose build worker-agent
docker compose up worker-tls-valid
```

…which binds worker containers to a host-native master on `localhost:50051`. `network_mode: host` is required (a bridge network adds a NAT hop that breaks the mTLS SAN=DNS:localhost match).

## Troubleshooting

**Master never becomes ready** → grep `$WORKDIR/<case>/master.log` for the `velox-server` banner and any earlier bootstrap failure. The Pre-flight binaries (DataServer/workflow `make verify-fast`) will surface compile errors before the matrix starts.

**Case 1 (plaintext) shows worker FATAL handshake errors** → PR 1 safe-by-default is intentionally strict: a worker without a full mTLS triple in `environment=production` is refused. The case-1 fixture sets `"environment": "dev"`, which is the canonical “dev-only plaintext” recipe (see [PR 1 migration §Path B](../../../docs/operations/PR-1-migration.md)).

**Case 4 (wrong-CA) shows the worker registering** → the master is NOT enforcing `tls.RequireAndVerifyClientCert`. PR 4 (`codex/worker-ops-read-model`) adds the missing assertion on the worker’s cert chain. Until then, the case is good evidence that **the worker side correctly reports a handshake failure** but is not yet a guarantee the master side is rejecting.

**Case 6 (parallel) shows both workers rejected** → likely a master TLS config bug: the allowlist contains both worker IDs, and the master’s CA pool only trusts the good PKI. If you see both rejected, grep `master.log` to confirm whether the handshake-fail message is server-side (correct) or registration-time (regression in PR 4 territory).

## Out of scope (separate PRs)

- **PR 4** `codex/worker-ops-read-model` — `GET /api/v1/workers` is the canonical acceptance probe for cases 1/2/6; until it ships, the registry assertion is log-based.
- **PR 6** `codex/pki-rotation-runbook` — the 7d / 1d TTL defaults in `generate-dev-pki.sh` are dev-only; production rotates differently (root CA → intermediate → leaf, with shorter leaf TTL).
