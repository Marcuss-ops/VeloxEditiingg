# Velox

Distributed video generation and composition system.

## Repository layout

```
DataServer/                     # Master server (Go/Gin + gRPC)
├── cmd/server/                 # Entrypoint: main.go, bootstrap.go (composition root), router.go
├── internal/
│   ├── app/                    # Module composition (ansible, drive, frontend, health, livestream,
│   │                           #   workers, youtube, module, registry)
│   ├── artifacts/              # Artifact service (upload, finalization, storage, chunked uploads)
│   ├── assets/                 # Asset registry, resolvers, service, store
│   ├── audit/                  # Data-layer audit (file existence, naming, primary files)
│   ├── config/                 # Env-based configuration loading and validation
│   ├── creatorflow/            # Creator flow orchestration service
│   ├── deliveries/             # Delivery runner + providers (drive, youtube, s3, localexport)
│   ├── grpcserver/             # gRPC handler (jobs, workers, artifacts, recovery, auth)
│   ├── handlers/
│   │   ├── remote/             # ansible, livestream, workers (assets, lifecycle, management,
│   │   │                       #   sse, uploads, validation)
│   │   ├── server/             # api, audit, calendar, darkeditor, drive, groups, health,
│   │   │                       #   pipeline, script, smoke, youtube
│   │   └── web/                # explorer, proxy, spa
│   ├── identity/               # Identity/ID generator
│   ├── integrations/           # drive (auth, files), youtube (api, channels, oauth, uploads)
│   ├── jobs/                   # Job model, repository, enqueue, status, transitions, views
│   ├── logging/                # Structured logging
│   ├── outbox/                 # Outbox event store, dispatcher, registry
│   ├── platform/               # clock, database (sqlite/postgres handle), retry
│   ├── queue/                  # File queue, lifecycle service, job status
│   ├── remoteengine/           # Remote engine gRPC client
│   ├── secrets/                # AES-GCM encryptor
│   ├── services/               # drive, workflow_events, youtube
│   ├── store/                  # SQLite stores, Postgres adapters, migrations, contracts, blobstore
│   ├── workers/                # Worker registry, commands, auth, heartbeat
│   └── workflow/               # Workflow repository, steps, migrate
RemoteCodex/                     # Worker agent (Go) + video engine (C++/FFmpeg)
shared/                          # Shared Go lib (identity, contract, controltransport, validation, media)
deploy/                          # Install scripts, systemd unit, env templates, Ansible suite
├── install-server.sh            # sudo ./deploy/install-server.sh
├── velox-server.service         # systemd unit
├── velox-server.env.example     # copy to /etc/velox-server.env
├── ansible.cfg                  # canonical ansible config for the suite below
├── requirements.yml             # ansible collection requirements
├── group_vars/                  # all.yml + vault.yml.example (operator-fill only)
├── inventory/                   # production.ini.example (NEVER commit production.ini itself)
├── playbooks/                   # bootstrap-ssh.yml, deploy-master-config.yml, rollback.yml
├── runtime/                     # per-host worker.env.example, compose.yml, prepare-host.sh
├── scripts/                     # validate-jinja-render.py, apply-local-worker-config.sh
└── templates/                   # velox-server.env.j2 (renders /etc/velox-server.env)
docs/                            # ADRs, architecture ownership, roadmap, deployment notes
.github/workflows/               # CI + worker-image + master-image release pipelines
scripts/                         # CI checks (architecture, migrations, secrets, etc.)
VERSION.txt                      # Single source of version truth
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

The implementation plan for task DAGs, reusable precompositions, worker registries, rendering metrics, cost-aware scheduling, and temporal sharding lives in [`docs/architecture/distributed-rendering/`](docs/architecture/distributed-rendering/README.md).
