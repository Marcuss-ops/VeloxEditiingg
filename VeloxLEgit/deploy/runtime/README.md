# deploy/runtime/ — worker container runtime

This directory declares the standard worker container runtime for the single co-located worker pair
(master + velox-worker-1) in `deploy/inventory/production.ini`. It pairs with `deploy/group_vars/`
(which the master-side playbooks consume) and with `.github/workflows/
worker-image.yml` (which builds and publishes the worker image).

## Files

| File | Role |
|---|---|
| `compose.yml` | Referenceable Compose v2 service definition. Read-only root fs, `cap_drop: ALL`, `no-new-privileges`, isolated per-worker container name. |
| `worker.env.example` | Template for the per-host env file the worker reads. Copy to `/etc/velox-worker/worker.env` on the host and fill in. |
| `prepare-host.sh` | Idempotent setup: creates dirs, sets ownership to uid 10001 (matching the image's `velox` user), pulls the pinned image, brings the container up. |

## First-time setup on a fresh worker host

```bash
# 1. Install docker + compose plugin (see your distro's package repo).

# 2. Copy + fill in the worker env.
sudo install -d /etc/velox-worker
sudo cp deploy/runtime/worker.env.example /etc/velox-worker/worker.env
$EDITOR /etc/velox-worker/worker.env
# Set VELOX_WORKER_ID (must match the inventory), VELOX_GRPC_MASTER_URL
# (public IP or DNS of the master), VELOX_WORKER_IMAGE
# (= ghcr.io/<owner>/velox-worker@sha256:<digest>).

# 3. Drop TLS cert and credential files (read-only):
sudo install -d /etc/velox-worker/certs /etc/velox-worker/secrets
sudo install -m 0600 worker.crt worker.key ca.crt /etc/velox-worker/certs/
sudo install -m 0600 worker_credential    /etc/velox-worker/secrets/

# 4. Drop binaries into /etc/velox-worker, then run prepare-host.sh.
sudo deploy/runtime/prepare-host.sh
```

## Image pinning — digest-only

`VELOX_WORKER_IMAGE` must be set to a **pinned digest**, never to a moving
mutable tag (no `:latest`, no `:worker-v1.1.2`). Pull the digest from the
`worker-image-digest` GitHub Actions artifact attached to the relevant
release run and store it as:

```
VELOX_WORKER_IMAGE=ghcr.io/marcuss-ops/velox-worker@sha256:<full-digest>
```

The image carries BuildKit-generated SBOM + provenance attestations and is
cosign-signed (keyless OIDC).

## Rollout order (rolling deploy)

1. Build & publish a new image by pushing tag `worker-vX.Y.Z` (or via
   `workflow_dispatch`).
2. Read the `worker-image-digest` artifact to extract the immutable digest.
3. Update `/etc/velox-worker/worker.env` on the worker host (canary) and run:
   ```bash
   sudo deploy/runtime/prepare-host.sh
   ```
4. Verify health: `docker compose -p velox-worker-<id> -f /opt/velox-worker/compose.yml ps`.
5. Probe the worker over gRPC from the master (`jobs/summary`) to confirm it
   accepted and processed at least one job.
6. Repeat on subsequent worker hosts as you scale out. Do NOT proceed until
   the previous host's health + at-least-one-job success is confirmed.

## Rollback

`prepare-host.sh` recreates the container with the pinned digest in
`VELOX_WORKER_IMAGE`. To roll back, edit `/etc/velox-worker/worker.env`,
replace the digest with the previous version's value, then re-run
`prepare-host.sh`. The persistent directories under `/var/lib/velox-worker/`
are not touched, so jobs in flight complete naturally before the container
is recycled by `stop_grace_period: 60s`.

## Security posture

* `read_only: true`, `tmpfs: /tmp:4g` — runtime filesystems are immutable
  except for the explicit tmpfs scratch.
* `cap_drop: ALL` — no Linux capabilities. The worker only binds high ports.
  If a future deployment must bind low ports, add
  `cap_add: [NET_BIND_SERVICE]` to a per-host override.
* `security_opt: no-new-privileges:true`.
* `/etc/velox-worker` is `root:root` mode 0750 (no traversal for `other`).
* Both `/etc/velox-worker/certs` and `/etc/velox-worker/secrets` are
  `root:root` mode 0750 and mounted **read-only** into the container at
  `/run/velox/{certs,secrets}`.
* The image runs as uid 10001 (non-root `velox` inside the Dockerfile).
  `/var/lib/velox-worker/` is chowned to 10001:10001 by `prepare-host.sh`.

## Observability

```bash
# Logs (last 100 lines, followed).
PROJECT=velox-worker-<id>
docker compose -p "$PROJECT" -f /opt/velox-worker/compose.yml logs --tail=100 -f

# Healthcheck state.
docker inspect "velox-worker-<id>" --format='{{json .State.Health}}' | jq .

# Resource use.
docker stats "velox-worker-<id>" --no-stream
```

Logs are rotated by the json-file driver (`max-size: 10m`, `max-file: 5`).
If you need to ship them to a central log store, swap the driver by adding
a per-host override compose file that sets `logging.driver: <your driver>`.
