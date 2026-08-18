# Worker Deployment Guide

> **Production release path:** GitHub Actions publishes one signed GHCR image;
> FleetController activates its immutable digest. Ansible deployment actions
> are retired. `prepare-host.sh` remains the one-time bootstrap/convergence
> tool for a host.

## Overview

Velox production workers have one release path: GitHub Actions builds and
signs once, then Master/FleetController rolls out the immutable `@sha256:`
digest serially. Host bootstrap is separate from release activation.

The deployment pipeline handles:
1. SSH connectivity and preflight checks
2. exact-digest image activation
3. systemd service setup (single source of truth)
4. Watchdog and auto-update timers

## Worker Naming Convention

Workers have two identities: immutable `worker_id` (mTLS/OpenBao principal)
and mutable operator-facing `worker_name`. Inventory/host aliases are
connectivity data in `ansible_hosts`; they never become the certificate-bound
worker ID. The Compose container name is always `velox-worker`.

## Bootstrap and compatibility files

> The old server-side Ansible deployment API is retired and fail-closed. Do
> not use it for releases; inventory remains a connectivity substrate for
> `WorkerNodeRegistry`.

### normalize_worker_systemd.yml
One-time bootstrap cleanup/convergence for a host. It does not build an image;
release activation remains FleetController-owned.

```bash
# Run only during host bootstrap with the prepared immutable image variables.
```

Actions:
- Stop/disable all `velox-worker-*.service` units
- Unmask the canonical service
- Remove stale unit files and override directories
- Write the canonical worker config while preserving the certificate-bound
  `VELOX_WORKER_ID`
- Write the canonical `/etc/systemd/system/velox-worker.service`
- daemon-reload → enable → start
- Verify service status and heartbeat on master

### update_workers.yml — retired guard
This file exists only to fail closed for stale operators. It cannot download a
bundle, build Docker, or restart a worker. Use `fleetctl update`.

`fleetctl ssh-check` is the canonical connectivity check. A restart is owned
by the FleetController activation cascade, not by a standalone rollout.

## Worker Compatibility Check

When a worker requests a job via `ClaimNextJob`, the master validates:

1. **Protocol version** — must match `DefaultWorkerProtocolVersion` (`v3`)
2. **Capabilities** — worker must report non-empty capabilities
3. **Supported job types** — if the job type is specified, worker must list it in `capabilities.supported_job_types`

If any check fails, the job is rejected with a descriptive reason.

## Minimum Remote Worker Configuration

A remote worker MUST have all five configuration items below to register
and execute jobs. Each item is a single-valued contract — drift between
the local worker config and the master's allowlist breaks the handshake
with `HTTP 403 worker_not_allowed` on `POST /api/v1/agent/register`,
which is the canonical operator-visible rejection path (see
`DataServer/internal/handlers/remote/workers/lifecycle/{handler,registration}.go`
+ `DataServer/internal/grpcserver/{authorizer,handler_stream}.go` for the
parallel HTTP 403 and gRPC `PermissionDenied` paths; both must agree on
the allowlist decision; they differ only in status-code surface).

### 1. `VELOX_WORKER_ID`

The unique worker identifier (canonical regex
`^[a-z][a-z0-9-]{2,62}$`; auto-derived host aliases are lowercased,
dots replaced with `_`, leading `host_` stripped). The value MUST appear
in the master's `VELOX_ALLOWED_WORKERS` CSV — otherwise the master
returns HTTP 403 on `/api/v1/workers/register` and the worker is
silently excluded from placement.

```bash
VELOX_WORKER_ID=velox-render-1
# Auto-derived from IP: 57.129.132.133 → host_57_129_132_133
```

### 2. `VELOX_GRPC_MASTER_URL`

The master gRPC control-plane endpoint (host:port, no scheme). Required
by the gRPC-push architecture; the worker opens a bidirectional stream
to this endpoint and authenticates via mTLS.

```bash
VELOX_GRPC_MASTER_URL=master.example.com:9000
# Local dev:
VELOX_GRPC_MASTER_URL=127.0.0.1:51851
```

### 3. Worker credential

Set via `VELOX_WORKER_SECRET`. The secret is combined with `worker_id`
to derive the credential_hash that the master validates against the
`worker_credentials` table (SHA-256 of `worker_id:secret`). A worker
that sends no credential on `/api/v1/workers/register` skips credential
validation (backward compatibility); supplying a WRONG credential to a
KNOWN worker returns HTTP 401 (impersonation signal).

```bash
VELOX_WORKER_SECRET=$(openssl rand -hex 32)
```

### 4. TLS for the gRPC control plane

Mandatory except in dev. The three PEM files form the worker's mTLS
identity. All three are required together; partial configuration is
rejected by `worker-config-validate` (see `pkg/config`).

| Env var | File | Purpose |
|---------|------|---------|
| `VELOX_GRPC_TLS_CERT_FILE` | worker cert (PEM) | Leaf cert, validated against CA; 14-day minimum residual validity (RW-PROD-001 A1) |
| `VELOX_GRPC_TLS_KEY_FILE` | worker key (PEM) | Private key; must be `0600` in production (RW-PROD-001 A2) |
| `VELOX_GRPC_TLS_CA_FILE` | CA cert (PEM) | CA that signed the master's cert; verifies server identity |

```bash
bash scripts/gen-worker-certs.sh /etc/velox/certs velox-render-1
export VELOX_GRPC_TLS_CERT_FILE=/etc/velox/certs/worker.crt
export VELOX_GRPC_TLS_KEY_FILE=/etc/velox/certs/worker.key
export VELOX_GRPC_TLS_CA_FILE=/etc/velox/certs/ca.crt
```

### 5. Render backend + max active jobs

| Env var | Default | Purpose |
|---------|---------|---------|
| `VELOX_RENDER_BACKEND` | `native` | Selects the rendering backend (`native`, `chronon3d`, …) |
| `VELOX_VIDEO_ENGINE_CPP_BIN` | `velox-render-cpp` | Path to the C++ video render binary (resolved via `exec.LookPath`) |
| `VELOX_MAX_ACTIVE_JOBS` | `1` | Concurrent active jobs per worker (currently bound via `worker_config.json` as `max_active_jobs`; `VELOX_MAX_ACTIVE_JOBS` env-var binding is on the roadmap) |

### 6. Asset download (chunked + prefetch)

Per-asset parallel chunked download is opt-in. When enabled, an asset at/above
the threshold is split into N parallel `Range: bytes=start-end` requests; the
single-stream resumable path remains the fallback when the upstream ignores
Range.

| Env var | Default | Purpose |
|---------|---------|---------|
| `VELOX_ASSET_DOWNLOAD_CONCURRENCY` | `4` | Simultaneous byte transfers across assets |
| `VELOX_ASSET_CHUNKED_DOWNLOAD` | off | Enables per-asset parallel chunked download |
| `VELOX_ASSET_CHUNKED_DOWNLOAD_THRESHOLD_BYTES` | `67108864` (64 MiB) | Minimum asset size that triggers chunking |
| `VELOX_ASSET_CHUNKED_DOWNLOAD_CONCURRENCY` | `4` | Parallel chunk connections per chunked asset |

### Failure modes (operator-visible)

| Misconfiguration | Master's response |
|------------------|-------------------|
| `worker_id` not in `VELOX_ALLOWED_WORKERS` | **HTTP 403 `worker_not_allowed`** on `POST /api/v1/agent/register` (this is the canonical "not on the fleet" rejection path) |
| Cert expired or about to expire (<14d) | Master rejects at gRPC handshake (`FailedPrecondition`) |
| Key permissions > `0600` (production) | Production boot fails closed; non-prod records a non-fatal `weak_permissions_warn` |
| `VELOX_GRPC_TLS_*` partial (missing one of three) | `worker-config-validate` rejects with explicit missing-field list |
| `VELOX_GRPC_MASTER_URL` missing | Transport factory refuses to start |
| Bundle hash mismatch | Master returns gRPC `FailedPrecondition` at handshake |

The HTTP 403 `worker_not_allowed` path is the FIRST signal an operator
sees when a new worker is misconfigured — long before any gRPC stream
is opened. See `DataServer/internal/handlers/remote/workers/lifecycle/worker_registration_test.go`
for the regression tests.

## Manifest Bundle

The master auto-generates `manifest_v2.json` at startup containing:
- `version`, `code_version`, `bundle_version`
- `build_hash` (SHA256 of the bundle zip)
- `protocol_version`, `engine_version`
- `platform`, `arch`, `timestamp`

Workers can verify their bundle hash against the master's manifest to detect version drift.

## Systemd Service Structure

Each worker gets a single canonical unit:
```
/etc/systemd/system/velox-worker-<alias>.service
/etc/systemd/system/velox-worker-<alias>.service.d/  (overrides)
/etc/velox-worker.env
```

Supporting services:
- `velox-worker-watchdog.service` + `.timer` — restarts stopped workers every 5min
- `velox-auto-update.service` + `.timer` — OS + bundle update every 12h

## Runtime writable dirs (proposal) — TODO

**Status**: proposal only — not yet activated. Do not wire into
`canonical_worker_runtime.yml` until the worker image no longer writes
mutable state under `/app/RemoteCodex/...` at runtime.

**Problem**: today the worker container mounts
`/var/lib/velox/workers/<host>/assets_cache` over
`/app/RemoteCodex/assets_cache` because the image stores mutable state
under the read-only `/app/RemoteCodex` tree. Any state the worker
writes outside an explicit volume mount fails with
`Read-only file system`; any state inside `/app/RemoteCodex/...` is
either a volume mount (and therefore OK) or a silent violation of the
`/app:ro` contract.

**Proposal**: move all mutable runtime state out of `/app/RemoteCodex`
into three explicit subdirs of the host's writable runtime tree, and
set `WorkingDirectory=` in the systemd unit so the worker always
starts from a known-good cwd.

**New writable tree** (host side, mirrors container `/var/lib/velox-worker/`):
```
/var/lib/velox/workers/<host>/
├── cache/       # engine caches (was: /app/RemoteCodex/assets_cache)
├── sessions/    # interactive session state (new)
└── scratch/     # transient worker scratch (new)
```

**Systemd unit change** in `canonical_worker_runtime.yml`:
```ini
[Service]
WorkingDirectory=/var/lib/velox/workers/<host>
ExecStart=...
  -v /var/lib/velox/workers/<host>/cache:/var/lib/velox-worker/cache \
  -v /var/lib/velox/workers/<host>/sessions:/var/lib/velox-worker/sessions \
  -v /var/lib/velox/workers/<host>/scratch:/var/lib/velox-worker/scratch \
  ...
```

**Provisioning playbook**: `tasks/provision_velox_writable_dirs.yml`
— creates the three subdirs with `ansible.builtin.file`
`state=directory`, owner UID `10001` (velox user), mode `0775`.

**Pre-requisites before activation**:
1. Update the worker Dockerfile + image to NOT write to `/app/RemoteCodex/...` at runtime.
2. Re-direct engine code paths to use `/var/lib/velox-worker/{cache,sessions,scratch}`.
3. Add `WorkingDirectory=/var/lib/velox/workers/<host>` to the unit.
4. Include `tasks/provision_velox_writable_dirs.yml` from `canonical_worker_runtime.yml` BEFORE the unit write.
5. Update `cleanup_worker.yml` to also remove `cache/`, `sessions/`, `scratch/`.

## Production command boundaries

Diagnostic commands include `scripts/fleetctl status`, `scripts/fleetctl
inspect`, `scripts/fleetctl operations`, `preflight_workers.yml`,
`systemctl status`, `docker ps`, `docker inspect`, and a local
`/health/ready` probe. `fleetctl smoke` is an explicit mutating smoke
operation, not a read-only health check.

Do not use direct `ssh docker pull`, mutable image tags, manual
`docker compose up`, manual `systemctl restart`, or hand-edited worker env
files as a production rollout. Use the bridge or the FleetController path in
`docs/operations/worker-rollout-paths.md`.

## Adding a New Worker

1. Add the worker to the `ansible_hosts` table (`WorkerNodeRegistry` — the
   single inventory source) via `POST /api/v1/admin/ansible/computers` or
   SQLite, with `worker_id` set to `host_<sanitized_ip>`, plus `ansible_host`
   and `ansible_user`.
2. Provision the OpenBao AppRole material + worker credential on the host:
   `scripts/operator/provision-worker-openbao.sh --worker <id> --ssh-host <ip> --ssh-user <user>`.
3. Install the worker runtime on the host (`prepare-host.sh` /
   `velox-worker-activate-image`).
4. Verify heartbeat on master: `curl http://MASTER/api/v1/workers/status`

## Troubleshooting

### Worker not starting
```bash
systemctl status velox-worker-<alias>
journalctl -u velox-worker-<alias> -n 100
```

### Worker masked
```bash
systemctl unmask velox-worker-<alias>
systemctl daemon-reload
systemctl enable --now velox-worker-<alias>
```

### Bundle hash mismatch
Worker reports `bundle_hash mismatch` — update the worker bundle:
```bash
# Master-side convergence (inventory from the WorkerNodeRegistry DB):
#   POST /api/v1/admin/ansible/computers/run_action  action=update
```

### Protocol version mismatch
Worker reports `protocol_version mismatch` — update the worker agent binary to match `DefaultWorkerProtocolVersion`.
