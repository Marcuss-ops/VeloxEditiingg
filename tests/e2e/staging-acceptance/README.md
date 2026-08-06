# Isolated staging acceptance environment

This directory provides a local/staging-only Compose topology and runner for
acceptance checks. It is intentionally separate from production deployment
files and uses a private Compose project, private loopback ports, and a
caller-selected work directory.

## Topology

```text
master (digest-pinned) ─┬─ worker-a (digest-pinned)
                       └─ worker-b (digest-pinned)
        │
        ├── isolated SQLite + staging/final artifact storage
        └── runtime-generated 5-second video fixture
```

The master and workers share only the Compose network. Artifact storage and
SQLite are bind-mounted below `VELOX_STAGING_WORKDIR`; no repository path is
used for mutable state. The fixture is generated with `ffmpeg` at runtime and
is mounted read-only into the containers.

## Image and credential policy

`run.sh` rejects every image that is not exactly an image name followed by
`@sha256:<64 hexadecimal characters>`. Tags such as `latest`, semver tags, and
short digests are rejected. The repository contains no usable token, worker
secret, certificate, or real image digest.

Create the runtime env file outside the repository:

```bash
cp tests/e2e/staging-acceptance/staging.env.example /tmp/velox-staging.env
$EDITOR /tmp/velox-staging.env
bash tests/e2e/staging-acceptance/run.sh --env-file /tmp/velox-staging.env
```

The example contains placeholders only. Keep the completed file in `/tmp`, a
secret manager, or another ignored location. The runner rejects placeholders,
empty credentials, and credentials containing CR/LF. The parser accepts only
simple `KEY=VALUE` lines (optionally quoted); `export KEY=value` and shell
syntax are intentionally rejected and never executed.

## Acceptance bootstrap

The runner:

1. validates Docker Compose, `curl`, `jq`, `ffmpeg`, and `sha256sum`;
2. creates isolated master/worker directories and JSON configs;
3. generates and `ffprobe`s a local video fixture, recording its SHA-256;
4. validates the Compose model without starting anything;
5. starts the master plus both workers with `docker compose up -d`;
6. waits for master and worker readiness;
7. verifies both configured WorkerIDs through the admin API;
8. exercises the asynchronous admin operation contract with a drain request;
9. polls `GET /api/v1/admin/operations/:operation_id` using `wait_operation`;
10. always tears down only its own Compose project.

`admin_api METHOD PATH [curl arguments...]` adds the configurable staging
base URL, admin bearer, and JSON content type. `wait_operation OPERATION_ID
[TIMEOUT_SECONDS]` treats
`SUCCEEDED` as success and `FAILED`, `CANCELLED`, `ROLLED_BACK`, and `ROLLBACK`
as terminal failure; it fails on timeout.

The bootstrap runner does not submit a real render job because a valid job
payload and external delivery destination are staging-specific. Workload
scenarios can source the same env file and reuse the isolated ports, fixture,
and storage paths after the bootstrap has been adapted for that scenario.

## Useful controls

```bash
VELOX_STAGING_KEEP_WORKDIR=1 bash tests/e2e/staging-acceptance/run.sh --env-file /tmp/velox-staging.env
VELOX_STAGING_KEEP_WORKDIR=0 bash tests/e2e/staging-acceptance/run.sh --env-file /tmp/velox-staging.env
VELOX_STAGING_OPERATION_TIMEOUT=600 bash tests/e2e/staging-acceptance/run.sh --env-file /tmp/velox-staging.env
```

Defaults are deliberately local (`127.0.0.1:18080`, `127.0.0.1:19000`, and
`/tmp/velox-staging-acceptance`). Override them in the external env file when
running multiple isolated staging projects concurrently. The Compose command is
fixed to the canonical `docker compose` executable; the runner does not execute
commands supplied by the env file.
