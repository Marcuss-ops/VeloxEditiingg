# Velox

Distributed video generation and composition system.

## Repository layout

```text
DataServer/                     # Master server (Go/Gin + gRPC)
├── cmd/server/                 # Entrypoint and composition root
├── internal/
│   ├── app/                    # Module composition and route registration
│   ├── artifacts/              # Upload, verification, finalization and reconciliation
│   ├── assets/                 # Asset registry, resolvers, service and store
│   ├── audit/                  # Data-layer audit
│   ├── config/                 # Canonical configuration loading and validation
│   ├── costmodel/              # Master-owned placement requirements and scoring
│   ├── creatorflow/            # Creator-flow orchestration
│   ├── deliveries/             # Delivery runner and provider registry
│   ├── grpcserver/             # Worker control plane and Task protocol
│   ├── handlers/               # HTTP and remote-worker handlers
│   ├── identity/               # Canonical identity generation
│   ├── ingest/                 # Task report ingestion
│   ├── integrations/           # Drive and YouTube integrations
│   ├── jobs/                   # Job model, repository and lifecycle
│   ├── logging/                # Structured logging
│   ├── metrics/                # Runtime and cost metrics
│   ├── observability/          # Read-only Task and attempt diagnostics
│   ├── outbox/                 # Outbox store, dispatcher and registry
│   ├── platform/               # Clock, database and retry primitives
│   ├── remoteengine/           # Remote engine client
│   ├── secrets/                # Encryption and secret handling
│   ├── services/               # Application services
│   ├── store/                  # SQLite stores, adapters, migrations and BlobStore
│   ├── taskattempts/           # Canonical Task execution-attempt state
│   ├── taskgraph/              # Canonical Task DAG, leases and readiness
│   └── workers/                # Worker registry, sessions, commands and heartbeat
RemoteCodex/                     # Worker agent (Go) + native video engine (C++/FFmpeg)
shared/                          # Shared Go contracts, identity, validation and media types
deploy/                          # Install scripts, systemd, runtime templates and Ansible
├── install-server.sh
├── velox-server.service
├── velox-server.env.example
├── ansible.cfg
├── requirements.yml
├── group_vars/
├── inventory/
├── playbooks/
├── runtime/
├── scripts/
└── templates/
docs/                            # Architecture, API, deployment and canonical completion plan
.github/workflows/               # CI and image release pipelines
scripts/                         # Canonical CI and repository checks
VERSION.txt                      # Single source of product version truth
```

## Placeholder contract

Every IP, hostname, worker ID and credential in versioned files is a `CHANGE_ME_*` placeholder. Production secrets live only in:

- `deploy/group_vars/vault.yml` encrypted with Ansible Vault and never committed;
- `deploy/inventory/production.ini`, which is excluded from Git;
- `/etc/velox-server.env` on the master;
- `/etc/velox-worker/worker.env` on each worker.

A `CHANGE_ME_*` token must be replaced before deployment and must never be copied verbatim into production.

The production worker allowlist is validated by `ValidateProductionWorkers` in `DataServer/internal/config/workers_validator.go`. The fleet may scale up or down; the runtime enforces the allowlist shape, uniqueness and absence of wildcards.

The agent operating contract — where canonical values live, what an agent (LLM, scripted, or CI-driven) must never print, and which workflow is allowed to publish an image — is the single source of truth in [`docs/architecture/AGENT-CONTRACT.md`](docs/architecture/AGENT-CONTRACT.md). The seven rules in that document bind every action on `main` and are backed by `scripts/ci/check-secrets.sh`, `deploy/validate-master-env.sh`, and `scripts/operator/with-production-env.sh`.

## Operator onboarding

> One-time local setup for any operator running jobs, canaries, or smoke checks against the production master from a workstation. Once set up, every local command runs through [`scripts/operator/with-production-env.sh`](scripts/operator/with-production-env.sh) — the wrapper is the only sanctioned path that exports the canonical env into a child process.

### One-time setup

```bash
# 1. Create the local config directory (already .gitignored).
mkdir -p .velox

# 2. Copy the tracked template. NEVER handwrite this from scratch —
#    the canonical variable names and order live in
#    `.velox/production.env.example`, not in human memory.
cp .velox/production.env.example .velox/production.env

# 3. Restrict permissions — the wrapper refuses anything looser.
chmod 600 .velox/production.env

# 4. Fill in real values from your operator's secret notes.
$EDITOR .velox/production.env
```

Required values (the wrapper validates every one on every run):

- `VELOX_MASTER_HOST`, `VELOX_MASTER_URL`
- `VELOX_ADMIN_TOKEN`
- `GHCR_SERVER_REPOSITORY`, `GHCR_WORKER_REPOSITORY`

### Daily workflow

Every local operator command must be wrapped, so the canonical env is exported into the child process:

```bash
scripts/operator/with-production-env.sh <command>
```

Examples:

- Submit a job: `scripts/operator/with-production-env.sh bash ops/jobs/submit_jackie_chan_doc_voiceover_clips.sh`
- Run the canary: `scripts/operator/with-production-env.sh bash deploy/runtime/submit-canary.sh`
- Probe readiness directly: `scripts/operator/with-production-env.sh curl -sS -H "Authorization: Bearer ${VELOX_ADMIN_TOKEN}" "${VELOX_MASTER_URL}/health/ready"`

Skip the wrapper at your own risk: the master token and admin endpoints are not in the shell environment by default, and pasting them into a command line pollutes shell history and bypasses agent rule §2 ([`docs/architecture/AGENT-CONTRACT.md`](docs/architecture/AGENT-CONTRACT.md)).

### What the wrapper enforces

- refuses world/group-readable `.velox/production.env` (must be `chmod 600`);
- refuses to start if `VELOX_MASTER_URL`, `VELOX_ADMIN_TOKEN`, or `GHCR_SERVER_REPOSITORY` are missing;
- exports the env into the child process via `set -a; source … ; set +a`;
- never echoes secret values — only reports presence or absence.

Override the env-file location with `VELOX_PRODUCTION_ENV=/path/to/file` if you need to source a non-default file (CI smoke, second operator, etc.).

## Canonical architecture

The canonical ownership map is [`docs/architecture/OWNERSHIP.md`](docs/architecture/OWNERSHIP.md). Every important state must have one owner, one writer and one mutation path.

## Canonical path to 100%

The active completion roadmap is intentionally limited to five documents:

1. [`00-TARGET-AND-DEFINITION-OF-DONE.md`](docs/100-percent-plan/00-TARGET-AND-DEFINITION-OF-DONE.md) — target system, invariants and final gates.
2. [`01-RUNTIME-CONSISTENCY-AND-RECOVERY.md`](docs/100-percent-plan/01-RUNTIME-CONSISTENCY-AND-RECOVERY.md) — state correctness, artifacts and failure recovery.
3. [`02-CI-TESTING-AND-RELEASE.md`](docs/100-percent-plan/02-CI-TESTING-AND-RELEASE.md) — required tests, E2E, images and immutable releases.
4. [`03-PRODUCTION-OPERATIONS-AND-SECURITY.md`](docs/100-percent-plan/03-PRODUCTION-OPERATIONS-AND-SECURITY.md) — doctor, mTLS, readiness, observability and worker certification.
5. [`04-DISTRIBUTED-RENDERING-PERFORMANCE-AND-SCALE.md`](docs/100-percent-plan/04-DISTRIBUTED-RENDERING-PERFORMANCE-AND-SCALE.md) — DAG execution, caching, scheduling, sharding and performance.

Historical snapshot audits and temporary PR-by-PR roadmaps are not active implementation contracts. The five documents above must be reconciled with `main` whenever a checklist item is completed.
