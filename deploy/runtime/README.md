# deploy/runtime/ — worker container runtime

This directory declares the standard worker runtime: one `velox-worker.service`, one Compose
project (`velox-worker`), one container (`velox-worker`), and one persistent state tree. It pairs with `deploy/group_vars/`
(which the master-side playbooks consume) and with `.github/workflows/
worker-image.yml` (which builds and publishes the worker image).

## Files

| File | Role |
|---|---|
| `compose.yml` | The sole Compose service definition; fixed project/container names, hardened mounts, and readiness healthcheck. |
| `velox-worker.service` | The sole systemd owner; systemd starts/stops the fixed Compose project and never runs a second worker process. |
| `velox-worker-mtls-renew.service` / `.timer` | Daily proactive PKI renewal; the oneshot invokes the canonical CSR resolver and restarts the active worker only after a new bundle is selected. |
| `migrate-legacy-worker.sh` | One-time, idempotent migration of per-host units, containers, env files, and persistent state. |
| `compose.chronon.yml` | Configuration reference for Chronon settings consumed by the canonical Compose service; startup remains owned by `velox-worker.service`. |
| `worker.env.example` | Template for `/etc/velox-worker/worker.env`, the only runtime env path. |
| `openbao-fetch-worker-secrets.sh` | OpenBao resolver: reads only `worker_credential` from KV, generates the mTLS private key locally, submits a CSR to the per-worker PKI role, and atomically materializes the signed certificate/CA. Configured production hosts fail closed instead of using hand-copied mTLS files. |
| `prepare-host.sh` | Idempotent migration + setup + digest pull + systemd convergence. Runs the OpenBao resolver when `VELOX_OPENBAO_ADDR` is set. |

## First-time setup on a fresh worker host

```bash
# 1. Install docker + compose plugin (see your distro's package repo).

# 2. Copy + fill in the worker env.
sudo install -d /etc/velox-worker
sudo cp deploy/runtime/worker.env.example /etc/velox-worker/worker.env
$EDITOR /etc/velox-worker/worker.env
# Set VELOX_WORKER_ID (must match the inventory), VELOX_GRPC_MASTER_URL
# (public IP or DNS of the master), VELOX_STATE_DIR, VELOX_WORKER_IMAGE
# (= ghcr.io/<owner>/velox-worker@sha256:<digest>).

# 3. Install only bootstrap inputs; never upload worker.key to OpenBao:
sudo install -d /etc/velox-worker/certs /etc/velox-worker/secrets/approle
sudo install -m 0600 role-id /etc/velox-worker/secrets/approle/role-id
sudo install -m 0600 secret-id /etc/velox-worker/secrets/approle/secret-id
sudo install -m 0644 openbao-ca.crt /etc/velox-worker/certs/openbao-ca.crt
# The resolver selects /etc/velox-worker/certs/current atomically; the
# container mounts that selected bundle read-only.

# 3b. OpenBao PKI enrollment (AppRole per-worker): the resolver generates
# worker.key locally, sends only a CSR to pki/sign/worker-<worker-id>, validates
# the returned chain, and atomically materializes worker.crt/worker.key/ca.crt.
# Set VELOX_OPENBAO_ADDR in /etc/velox-worker/worker.env:
# VELOX_OPENBAO_ADDR=https://127.0.0.1:8200   (loopback/tunnel verso il master)
# VELOX_OPENBAO_CA_FILE=/etc/velox-worker/certs/openbao-ca.crt
# Il certificato pubblico server.crt deve essere distribuito insieme al
# materiale AppRole; senza CA il resolver fallisce chiuso (mai -k).
# prepare-host.sh invokes the resolver and enables the daily renewal timer;
# configured production hosts do not fall back to manually copied cert/key files.
# The resolver writes
# .openbao-pki-issued only after successful CSR enrollment; --check rejects
# bundles that lack this provenance marker.

# 4. On a legacy host, migrate once before the first canonical start:
sudo deploy/runtime/migrate-legacy-worker.sh

# 5. Run the canonical systemd convergence (it repeats migration safely).
sudo deploy/runtime/prepare-host.sh
```

## OpenBao worker tunnel (loopback-only)

When OpenBao remains bound to the Master's loopback (`127.0.0.1:8200`), the
operator must first extend the canonical `remote-velox-tunnel` with its local
forward (`127.0.0.1:18200 -> master:127.0.0.1:8200`). The worker-side helper
then creates one reverse SSH forward per worker:

```bash
# On the operator workstation; the master tunnel must already be active.
HOME=/home/pierone scripts/operator/remote-velox-tunnel.sh status
HOME=/home/pierone scripts/operator/remote-worker-openbao-tunnel.sh start \
  host_57_129_132_133 57.129.132.133 pierone
```

Repeat the helper for the other workers. It binds only worker loopback port
`8200`, requires strict SSH host-key checking, and refuses password auth. For
persistence across operator logout/reboot, install the repository template as
a user service and one mode-`0600` env file per worker:

```bash
mkdir -p /home/pierone/.config/systemd/user /home/pierone/.config/velox/worker-openbao
install -m 0644 deploy/velox-worker-openbao-tunnel@.service \
  /home/pierone/.config/systemd/user/velox-worker-openbao-tunnel@.service
sudo install -m 0755 scripts/operator/worker-openbao-tunnel-service.sh \
  /usr/local/bin/velox-worker-openbao-tunnel-service
sudo install -m 0755 scripts/operator/probe-worker-openbao.sh \
  /usr/local/bin/probe-worker-openbao.sh
sudo install -m 0755 scripts/operator/check-local-velox-tunnel.sh \
  /usr/local/bin/check-local-velox-tunnel
sudo install -m 0644 deploy/velox-remote-tunnel.service \
  /home/pierone/.config/systemd/user/velox-remote-tunnel.service
systemctl --user daemon-reload
systemctl --user restart velox-remote-tunnel.service
install -m 0600 deploy/velox-worker-openbao-tunnel.env.example \
  /home/pierone/.config/velox/worker-openbao/host_57_129_132_133.env
# Edit only topology/path values in the copied env file; never add passwords.
systemctl --user daemon-reload
systemctl --user enable --now velox-worker-openbao-tunnel@host_57_129_132_133.service
loginctl enable-linger pierone
```

The service template uses the same strict host-key and TLS health-check
contract. The helper command remains useful for a one-shot controlled rollout.
The worker's `/etc/velox-worker/worker.env` must use:

```text
VELOX_OPENBAO_ADDR=https://127.0.0.1:8200
VELOX_OPENBAO_CA_FILE=/etc/velox-worker/certs/openbao-ca.crt
```

After the AppRole material and CA are installed, run the canonical host
convergence and verify without printing secret values:

```bash
sudo /opt/velox-worker/openbao-fetch-worker-secrets.sh --check
sudo /opt/velox-worker/prepare-host.sh
```

The tunnel is a transport prerequisite, not a secret source; install the
repository's `scripts/operator/probe-worker-openbao.sh` at the path referenced
by the user service before enabling that service. AppRole files
remain `0600` and the fetch script authenticates with the worker's own
least-privilege identity.

## Chronon worker

To run the worker with Chronon3D, publish and pin the image built with
`RemoteCodex/native/worker-agent-go/Dockerfile.chronon`, then update the canonical `/etc/velox-worker/worker.env`:

```bash
VELOX_WORKER_IMAGE=ghcr.io/<owner>/velox-chronon-worker@sha256:<digest>
VELOX_RENDER_BACKEND=chronon
CHRONON3D_CLI=/opt/chronon3d/bin/chronon3d_cli
```

Converge through the sole runtime owner:

```bash
sudo systemctl restart velox-worker.service
```

The canonical Compose definition reads these values. Without
`VELOX_RENDER_BACKEND=chronon`, Velox keeps the existing native engine backend.

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
4. Verify health: `systemctl is-active velox-worker.service` and `docker compose -p velox-worker -f /opt/velox-worker/compose.yml ps`.
5. Probe the worker over gRPC from the master (`jobs/summary`) to confirm it
   accepted and processed at least one job.
6. Repeat on subsequent worker hosts as you scale out. Do NOT proceed until
   the previous host's health + at-least-one-job success is confirmed.

## Rollback

`prepare-host.sh` recreates the canonical container with the pinned digest in
`VELOX_WORKER_IMAGE`. To roll back, edit `/etc/velox-worker/worker.env`,
replace the digest with the previous version's value, then re-run
`prepare-host.sh`. The persistent directories under `VELOX_STATE_DIR`
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
  `VELOX_STATE_DIR` is explicit and mounted at the same path in the container;
  existing state/work owner, mode, ACLs, and contents are preserved, while
  missing directories receive uid 10001:10001 and mode 0750.

## Observability

```bash
# Logs (last 100 lines, followed).
PROJECT=velox-worker
docker compose -p "$PROJECT" -f /opt/velox-worker/compose.yml logs --tail=100 -f velox-worker

# Healthcheck state.
docker inspect velox-worker --format='{{json .State.Health}}' | jq .

# Resource use.
docker stats velox-worker --no-stream
```

Logs are rotated by the json-file driver (`max-size: 10m`, `max-file: 5`).
If you need to ship them to a central log store, swap the driver by adding
a per-host override compose file that sets `logging.driver: <your driver>`.
