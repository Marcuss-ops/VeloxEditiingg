# DEV-LOOP — fast local loop for master changes

Deploying through the full CI release (race matrix + image build + sign)
takes 15-30+ minutes from push to a runnable master. That path is for
**production promotion only**. Day-to-day iteration uses the tiers
below, fastest first. Measured 2026-10-02 on the master host (8 vCPU).

| Tier | What | Measured | When |
|---|---|---|---|
| 0. Unit | `go test` on touched packages (`shared/contract`, `DataServer/internal/...`) | seconds | every change, before commit |
| 1. Dev master | `scripts/ops/dev-master.sh run` → real binary on `:18001`, scratch DB | build ~5s, boot ~15s | intake/validation/projection behavior (dry-run, warnings, envelopes) |
| 2. Local image | `scripts/ops/dev-master.sh build-image` then `push-image` | first build slow (cold cache), then cached | container-parity checks before release |
| 3. Release image | CI `Master image release` (push to main) | ~17 min push → signed GHCR image | production deploy only |

## Tier 1: dev master (the one that kills the "hours" perception)

```bash
scripts/ops/dev-master.sh run
# -> http://127.0.0.1:18001, gRPC :19001, scratch DB, dev admin token printed at boot
```

Then validate a creator payload without creating anything:

```bash
scripts/ops/dev-master.sh dry-run /tmp/my-payload.json /api/v1/creator/jobs http://127.0.0.1:18001
# or against production (read-only): ... /api/v1/creator/jobs http://127.0.0.1:8000
# (with VELOX_ADMIN_TOKEN set to the production token)
```

Safety properties (do NOT change them casually):

- Fresh scratch DB under `$VELOX_DEV_DIR` (default `/tmp/velox-dev-master`);
  production data is never copied or mounted.
- Ports `:18001`/`:19001` stay clear of production (`:8000`/`:9000`)
  **and** of the `:18000`/`:18200` SSH tunnels to the OVH master.
- Dev capability posture: `VELOX_ENVIRONMENT=development` +
  `VELOX_SMOKE_MODE=development` (explicit dev doubles; production
  `Validate()` rejects this combination — see AGENTS.md §6).
- `VELOX_ALLOWED_WORKERS=dev-worker-01` satisfies the non-empty
  allowlist boot gate without touching fleet state.

## Tier 2: local image

```bash
scripts/ops/dev-master.sh build-image [tag]   # default velox-server:local
scripts/ops/dev-master.sh push-image <tag>    # ghcr.io/<owner>/velox-server:<tag> (+ digest print)
```

`push-image` needs GHCR write access (docker is already authed on the
master host). Prefer the CI release image for production; local tags
are for pre-release container checks.

## What NOT to wait for

The long tail after push (`main-baseline`, `golden-e2e`, `e2e-workload*`,
30+ min) is **not** on the deploy path: `Master image release` builds
from main independently (race shards + build + sign, ~17 min measured).
Two CI failures are pre-existing on main and unrelated to intake work
(`make verify` ratchet-sql in `drive_stream_relay.go`, `dependency-scan`);
per AGENTS.md §4 they are tracked findings, not deploy blockers —
confirm via `git diff <base>..HEAD -- <area>` that a red check touches
none of your files before promoting.
