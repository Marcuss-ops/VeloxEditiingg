# Worker Deployment Guide

## Overview

Velox workers are deployed via Ansible playbooks from the master server. The deployment pipeline handles:
1. SSH connectivity and preflight checks
2. Bundle download and Docker image build
3. systemd service setup (single source of truth)
4. Watchdog and auto-update timers

## Worker Naming Convention

Workers are identified by a sanitized inventory alias:
- IP `57.129.132.133` → alias `host_57_129_132_133`
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

### update_workers.yml
Downloads latest bundle from master, rebuilds Docker image, re-applies systemd setup.

```bash
ansible-playbook -i inventory.ini update_workers.yml \
  -e "master_url=http://MASTER:8000"
```

### restart_workers.yml
Simple restart of existing worker services.

```bash
ansible-playbook -i inventory.ini restart_workers.yml
```

### preflight_workers.yml
Read-only checks: SSH, disk, OS, commands, Docker, service status.

```bash
ansible-playbook -i inventory.ini preflight_workers.yml
```

## Worker Compatibility Check

When a worker requests a job via `ClaimNextJob`, the master validates:

1. **Protocol version** — must match `DefaultWorkerProtocolVersion` (`2026-06-worker-v1`)
2. **Capabilities** — worker must report non-empty capabilities
3. **Supported job types** — if the job type is specified, worker must list it in `capabilities.supported_job_types`

If any check fails, the job is rejected with a descriptive reason.

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
