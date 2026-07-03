# Agent contract

> Canonical rules for any agent — LLM-driven, scripted, CI-driven, or
> human-driven — acting on the Velox repository on `main`. This
> document is the single source of truth for the operating contract.
> The top-level [`README.md`](../../README.md) (`Placeholder contract`
> section) points back to it for the most condensed version.

## Scope

These rules bind every actor that:

- reads from or writes to this repository on `main`;
- runs CI/CD against the GitHub repository `marcuss-ops/VeloxLEgit`;
- invokes `.github/workflows/master-image.yml`,
  `.github/workflows/worker-image.yml`, or
  `.github/workflows/deploy.yml`;
- executes a Velox master or worker against the production host(s);
- sources `.velox/production.env` locally, or consumes the GitHub
  Environment `production` (vars + secrets) in CI.

The rules are not aspirational: every clause is backed by either the
runtime validators (`deploy/validate-master-env.sh`,
`scripts/operator/with-production-env.sh`) or the CI check suite
(`scripts/ci/check-secrets.sh` and friends).

## The seven rules

### 1. Do not request credentials that already live in the GitHub `production` environment

Canonical production values are bound to the GitHub Environment
`production` as `vars.*` (non-secret) and `secrets.*` (secret).
An agent MUST NOT ask the operator to paste one of these values back
into chat, into a log, or into a PR comment. If a flow is missing a
value, the agent MUST source it from the canonical home:

| Value | Canonical home |
| --- | --- |
| `VELOX_MASTER_HOST`, `VELOX_MASTER_URL` | GitHub Environment `production` → `vars.*` |
| `VELOX_ADMIN_TOKEN` | GitHub Environment `production` → `secrets.VELOX_ADMIN_TOKEN` (scoped per-step, not per-job) |
| `ANSIBLE_VAULT_PASSWORD` | GitHub Environment `production` → `secrets.ANSIBLE_VAULT_PASSWORD` |
| `VELOX_DEPLOY_SSH_KEY` | GitHub Environment `production` → `secrets.VELOX_DEPLOY_SSH_KEY` (Ansible-only consumer) |

### 2. Local operator operations read `.velox/production.env`

For every operation that runs outside CI — job submission
(`ops/jobs/...`), smoke checks, canaries — the canonical entry
point is:

```bash
scripts/operator/with-production-env.sh <command>
```

The wrapper:

- sources `.velox/production.env` (or `$VELOX_PRODUCTION_ENV` if
  overridden), and refuses world/group-readable files (must be
  `chmod 600`);
- validates the mandatory variables: `VELOX_MASTER_URL`,
  `VELOX_ADMIN_TOKEN`, `GHCR_SERVER_REPOSITORY`;
- exports them into the child process (`set -a; source … ; set +a`).

The wrapper never echoes secret values: it reports only presence or
absence, via the `:?` parameter expansion.

### 3. Redact: never print `VELOX_ADMIN_TOKEN`, PATs, vault passwords, or SSH keys

The following MUST NEVER be logged, echoed, returned in an agent
message, or pasted into a chat / PR / commit:

- `VELOX_ADMIN_TOKEN`
- GHCR / Docker personal access tokens (classic or fine-grained)
- `ANSIBLE_VAULT_PASSWORD`
- SSH private keys (`-----BEGIN ... PRIVATE KEY-----`)
- the contents of `~/.ssh/id_*`
- the plain-text values of `vault_velox_*` variables

Error messages that need a hint MUST reference the canonical
location — for example
`missing VELOX_ADMIN_TOKEN — see AGENT-CONTRACT.md §1, §2` — and
never the value itself.

### 4. No local `docker push`. Ever.

Every `velox-server` and `velox-worker` release is published by a CI
workflow that uses `secrets.GITHUB_TOKEN` and signs the digest with
cosign (keyless OIDC). Local operators MUST NOT:

- `docker login ghcr.io` interactively;
- `docker push ghcr.io/marcuss-ops/velox-{server,worker}:…`;
- `cosign sign …` against a digest from a local build;
- mutate `.velox/production.env` with a `GHCR_TOKEN` PAT — none is
  required; CI uses `secrets.GITHUB_TOKEN` via `docker/login-action`.

CI workflows are the only publishers:

- `.github/workflows/master-image.yml` → publishes `velox-server`;
- `.github/workflows/worker-image.yml` → publishes `velox-worker`.

### 5. `velox-server` is published only by `master-image.yml`

A change to the build / signing / push path of the master image MUST
land as a modification to `.github/workflows/master-image.yml` on
`main`. No local build path (Makefile target, hand-rolled
`docker build`, side script) reproduces the canonical signed result.
The workflow's digest step (`master-image-digest` artifact) is the
only authoritative emission point.

### 6. Install by SHA-256 digest only; mutable tags are forbidden

The single install path is `.github/workflows/deploy.yml`, which:

1. resolves a versioned tag (e.g. `v1.2.3`) to its immutable digest
   via `docker buildx imagetools inspect`;
2. verifies the digest with cosign (keyless OIDC);
3. hands the digest to Ansible (`velox_server_image`, etc.).

Production inventory, ansible vars, runbooks, smoke tests, and CI
scripts MUST reference `@sha256:` digests. Mutable tags
(`:latest`, `:v1.2.3`, `:main`, branch refs) MUST NOT appear in any
versioned file. Two enforcers back this rule:

- `deploy/validate-master-env.sh` (`is_pinned_image_ref` regex on
  `VELOX_SERVER_IMAGE`);
- `scripts/ci/check-secrets.sh` regex on the wider set of regulated
  refs.

### 7. Ask the operator only when the canonical value is absent

If a flow needs a value that has no canonical home, the agent MUST
declare the canonical home that should hold it (referencing this
document) and stop. It MUST NOT fabricate or guess:

- an IP, a hostname, a worker ID;
- a deploy token;
- a vault password;
- a SSH key.

If the canonical home is empty / placeholder / missing, the agent
emits an explicit error pointing to the canonical path:

```text
VELOX_ADMIN_TOKEN not found.
  Canonical home: GitHub Environment "production" → secret VELOX_ADMIN_TOKEN
                  or local .velox/production.env (see AGENT-CONTRACT.md §1, §2)
```

## Verification

Each rule has at least one enforcer:

| Rule | Enforcer |
| --- | --- |
| 1 | `.github/workflows/deploy.yml` consumes `vars.VELOX_MASTER_URL`, `secrets.VELOX_ADMIN_TOKEN`, `secrets.ANSIBLE_VAULT_PASSWORD` from environment `production` |
| 2 | `scripts/operator/with-production-env.sh` (chmod 600 + required vars validation) |
| 3 | `scripts/ci/check-secrets.sh` + the wrapper's `:?` validation + `set -a` block |
| 4 | `scripts/ci/check-secrets.sh` regex on mutable GHCR refs |
| 5 | The workflow file IS the publisher; no local sibling |
| 6 | `deploy/validate-master-env.sh` (`is_pinned_image_ref`) + `scripts/ci/check-secrets.sh` |
| 7 | No CI enforcer — relies on convention. The canonical-home hint is the contract |

## Update procedure

1. Edit this file on `main`, no branch.
2. If a rule's enforcement is changing: update the matching CI
   script in `scripts/ci/check-*.sh` and/or the matching runtime
   validator (`deploy/validate-master-env.sh`,
   `scripts/operator/with-production-env.sh`).
3. If a rule summary in `README.md` (`Placeholder contract` section)
   becomes stale, replace the cross-reference (do not duplicate the
   rule prose there — this file stays authoritative).
4. Single commit per change on `main`, push frequently.

## Cross-references

- [`README.md` Placeholder contract](../../README.md#placeholder-contract)
- [`docs/architecture/OWNERSHIP.md`](OWNERSHIP.md) — single-writer rule,
  one owner per capability
- [`docs/SECURITY_RUNBOOK.md`](../SECURITY_RUNBOOK.md) — incident
  handling and emergency contact paths
- [`scripts/operator/with-production-env.sh`](../../scripts/operator/with-production-env.sh) — local ops wrapper
- [`deploy/validate-master-env.sh`](../../deploy/validate-master-env.sh) — runtime env validator
- [`scripts/ci/check-secrets.sh`](../../scripts/ci/check-secrets.sh) — pre-merge secrets + mutable-tag guard
- `.github/workflows/deploy.yml` (`environment: production`, scoped
  smoke-test step) — canonical install path
- `.github/workflows/master-image.yml` — single `velox-server`
  publisher
