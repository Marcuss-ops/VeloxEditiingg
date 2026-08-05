# Worker Deployment Guide

> **Choose the rollout path first:**
> `docs/operations/worker-rollout-paths.md` is the authoritative map. The
> bundle-based Ansible flow below is a bridge for old workers; the immutable
> GHCR/FleetController flow is the definitive target. Do not combine them.

## Overview

Velox workers are deployed via two explicitly separated paths:

1. **Bridge:** Ansible downloads the current bundle and may build the worker
   image locally on an old host while it is normalized.
2. **Definitive target:** GitHub Actions builds one signed GHCR image, and the
   Master/FleetController rolls out its immutable `@sha256:` digest serially.

The deployment pipeline handles:
1. SSH connectivity and preflight checks
2. Bundle download and Docker image build
3. systemd service setup (single source of truth)
4. Watchdog and auto-update timers

## Worker Naming Convention

Workers are identified by a sanitized inventory alias:
- IP `57.129.132.133` → alias `host_57_129_132_133`
- IP `51.222.204.158` → worker `velox-worker-523925eb` (hostname `vps-523925eb`)
- The alias becomes both the Ansible `inventory_hostname` and the `worker_id`

## Playbooks

### install_workers.yml
Full installation: preflight → directory setup → Docker build → systemd → start.

```bash
ansible-playbook -i inventory.ini install_workers.yml \
  -e "master_url=http://MASTER:8000"
```

### normalize_worker_systemd.yml
Cleans up old/masked services and writes a single canonical unit per worker.

```bash
ansible-playbook -i inventory.ini normalize_worker_systemd.yml \
  -e "master_url=http://MASTER:8000"
```

Actions:
- Stop/disable all `velox-worker-*.service` units
- Unmask the canonical service
- Remove stale unit files and override directories
- Write `/etc/velox-worker.env` with correct `WORKER_NAME`, `VELOX_WORKER_ID`
- Write `/etc/systemd/system/velox-worker-<alias>.service`
- daemon-reload → enable → start
- Verify service status and heartbeat on master

### update_workers.yml — legacy bridge only
Downloads the latest bundle from the Master, rebuilds the Docker image locally
on the worker, and re-applies systemd setup. Use only to normalize or migrate
an old worker; it does not certify that all workers run identical image bytes.

```bash
ansible-playbook -i "$INVENTORY" \
  DataServer/data/ansible/playbooks/update_workers.yml \
  --limit <inventory_host> \
  -e "master_url=$MASTER_URL"
```

`$INVENTORY` must be an operator-local inventory. Do not use an `*.example`
template as production input and do not run this playbook fleet-wide.

### restart_workers.yml
Simple restart of existing worker services.

```bash
ansible-playbook -i inventory.ini restart_workers.yml
```

### preflight_workers.yml — diagnostic only
Read-only checks: SSH, disk, OS, commands, Docker, service status. A passing
preflight is not a deployment or release certification.

```bash
ansible-playbook -i "$INVENTORY" \
  DataServer/data/ansible/playbooks/preflight_workers.yml \
  --limit <inventory_host>
```

## Worker Compatibility Check

When a worker requests a job via `ClaimNextJob`, the master validates:

1. **Protocol version** — must match `DefaultWorkerProtocolVersion` (`2026-06-worker-v1`)
2. **Capabilities** — worker must report non-empty capabilities
3. **Supported job types** — if the job type is specified, worker must list it in `capabilities.supported_job_types`

If any check fails, the job is rejected with a descriptive reason.

## Minimum Remote Worker Configuration

A remote worker MUST have all five configuration items below to register
and execute jobs. Each item is a single-valued contract — drift between
the local worker config and the master's allowlist breaks the handshake
with `HTTP 403 worker_not_allowed` on `POST /api/v1/workers/register`,
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

### Failure modes (operator-visible)

| Misconfiguration | Master's response |
|------------------|-------------------|
| `worker_id` not in `VELOX_ALLOWED_WORKERS` | **HTTP 403 `worker_not_allowed`** on `POST /api/v1/workers/register` (this is the canonical "not on the fleet" rejection path) |
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

1. Add the worker to `ansible_hosts` table (via SQLite or the `/api/v1/ansible/computers` API) with `worker_id` set to `host_<sanitized_ip>`
2. Add to `inventory.ini` with `ansible_host=<ip>` and `ansible_user`
3. Run `normalize_worker_systemd.yml`
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
ansible-playbook -i inventory.ini update_workers.yml -e "master_url=http://MASTER:8000"
```

### Protocol version mismatch
Worker reports `protocol_version mismatch` — update the worker agent binary to match `DefaultWorkerProtocolVersion`.
