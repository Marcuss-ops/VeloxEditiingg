# Velox

Distributed video generation and composition system.

## Repository layout

```
DataServer/                # Master server (Go/Gin + gRPC)
RemoteCodex/               # Worker agent (Go) + video engine (C++/FFmpeg)
shared/                    # Shared Go lib (already promoted to root)
deploy/                    # Install scripts, systemd unit, env templates, full Ansible suite (canonical)
├── install-server.sh      # sudo ./deploy/install-server.sh
├── velox-server.service   # systemd unit
├── velox-server.env.example  # copy to /etc/velox-server.env
├── ansible.cfg            # canonical ansible config for the suite below
├── requirements.yml       # ansible collection requirements
├── group_vars/            # all.yml + vault.yml.example (operator-fill only)
├── inventory/             # production.ini.example (NEVER commit production.ini itself)
├── playbooks/             # bootstrap-ssh.yml, deploy-master-config.yml, rollback.yml
├── runtime/               # per-host worker.env.example, compose.yml, prepare-host.sh
├── scripts/               # validate-jinja-render.py, apply-local-worker-config.sh
└── templates/             # velox-server.env.j2 (renders /etc/velox-server.env)
docs/                      # ADRs, deploy notes, post-PR operator runbook, archived architecture
.github/workflows/         # CI + worker-image + master-image release pipelines
frontend_standalone/       # SPA frontend (VELOX_SPA_DIR)
VERSION.txt                # Single source of version truth
```

## Placeholder contract (canonical)

Every IP, hostname, worker ID, and credential in versioned files is
a `CHANGE_ME_*` placeholder. Production secrets live ONLY in:

- `deploy/group_vars/vault.yml` (ansible-vault encrypted, never committed)
- `deploy/inventory/production.ini` (excluded from git, **/production.ini)
- `/etc/velox-server.env` on the master (excluded, `**/.env.production`)
- `/etc/velox-worker/worker.env` per host (excluded)

If anything in this README, the canonical templates, or the
operator runbook references a `CHANGE_ME_*` token, it is meant to
be replaced before deploy — NOT copied verbatim. The runtime
guarantees on the worker allowlist (non-empty, no wildcard,
unique, no fixed fleet size) live in
`DataServer/internal/config/workers_validator.go`:
`ValidateProductionWorkers`. Operators may scale the fleet up or
down freely; only the shape of the allowlist is enforced.

The runtime grep at
`docs/post-pr-two-worker-hardening.md` describes the historic
two-worker operator runbook — that file is archived and remains in
the repo only so operators who landed on a post-merge clone with
the two-worker topology can still pass the existing greps. The
**current canonical rule** is fleet-size-unbounded, validated by
`ValidateProductionWorkers`.

## Documentation

ADRs, deployment notes, and architecture references live in [`docs/`](docs/).