# Runtime Certification — 2026-08-04 (Fase 2)

> Evidence produced by `scripts/ops/runtime-cert.sh --fleet` on
> **2026-08-04 ~11:39 UTC**. All facts below were collected live via SSH
> (worker hosts) + the master REST admin API (token read on the master
> host, never printed). No secrets are contained in this document.

## Summary matrix

| Worker | Host | systemd | registered | session_active | restarts@5min | bootstrap gate | master image_digest | env==disk bundle | Verdict |
| --- | --- | --- | --- | --- | --- | --- | --- | --- | --- |
| `host_57_129_132_133` | 57.129.132.133 | ✅ active | ✅ CONNECTED | ✅ true | ✅ 0 | ✅ READY | `3631d271…` | ❌ env stale | **FAIL** (coherence) |
| `host_57_131_20_173` | 57.131.20.173 | ✅ active | ✅ CONNECTED | ✅ true | ✅ 0 | ✅ READY | `3631d271…` | ✅ | **PASS** |
| `velox-worker-13197` | 149.56.131.97 | ❌ activating (auto-restart) | ❌ DISCONNECTED | ❌ false | ❌ loop | ❌ `engine_self_render` FAIL | `3631d271…` (last) | ❌ | **FAIL** (down) |
| `velox-worker-523925eb` | 51.222.204.158 | ✅ active | ✅ CONNECTED | ✅ true | ✅ 0 | ✅ READY | `3631d271…` | ✅ | **PASS** |

## Verdict

**2/4 PASS, 2/4 FAIL.** Three workers are CONNECTED + session_active with
a READY bootstrap; one worker (`velox-worker-13197`) is down in a restart
loop. A fourth (`host_57_129_132_133`) is healthy but carries a stale env
bundle hash (coherence failure only).

The Fase-2 goal — 4/4 workers with identical digest, identical bundle,
`registered=true`, `session_active=true`, 5-min restart stability and
bootstrap PASS — is **NOT yet met**.

## Root cause — velox-worker-13197 restart loop

First real error (NOT "Main process exited, status=1"):

```
bootstrap gate failed (RW-PROD-003): bootstrap: engine_self_render/
engine_selftest_baseline_mismatch
  engine self-render SHA mismatch:
    actual=9247ff97…   (what the engine really renders)
    expected=f180058d… (stale baseline file on this host)
  baseline=/var/lib/velox/workers/worker_13197/work/tests/fixtures/
           engine_selftest_baseline.sha256
```

- The engine binary on the 3 healthy hosts produces `9247ff97…` and their
  baseline files contain `9247ff97…` — consistent.
- `velox-worker-13197` has a **stale baseline** (`f180058d…`) from an
  older bundle while its engine renders `9247ff97…` → gate fails → systemd
  `auto-restart` loop → worker never registers.

**Fix is NOT "disable the bootstrap gate"** (explicitly forbidden by the
verdict). The correct fix is to **refresh the baseline file** on 13197 to
the canonical `9247ff97…` (matching the 3 healthy workers) and restart the
unit. This belongs in the canonical Ansible path (Fase 3) so a re-deploy
reproduces it.

## Image digest uniformity — NOT uniform at docker level

All 4 workers advertise `image_digest=3631d2716f20…` to the master (the
pinned ghcr digest), but the **actually-running container image IDs
differ per host** despite the same tag `velox-worker:v1.2.20-capsfix`:

| Worker | tag | local image ID (running) |
| --- | --- | --- |
| host_57_129 | `v1.2.20-capsfix` | `sha256:656f4ed2f869…` |
| host_57_131 | `v1.2.20-capsfix` | `sha256:e356b1472111…` |
| velox-worker-523925eb | `v1.2.20-capsfix` | `sha256:703c25768192…` |
| velox-worker-13197 | `v1.2.20-9a9c386` (env) | (no container — down) |

Same tag name, different image content per host → the "all workers
updated" claim from the transcript is **not proven at the running-image
level**. Fase 2 requires the same pinned `sha256:` digest on every host.

## Bundle hash — 3 distinct values in the fleet

| Worker | env `VELOX_BUNDLE_HASH` | disk (`/opt/velox/current`) | container |
| --- | --- | --- | --- |
| host_57_129 | `v1.2.20-bgmfix20cb…` (STALE) | `7ba60510c233…` | `7ba60510c233…` |
| host_57_131 | `v1.2.20-bgmfix20cb…` | `v1.2.20-bgmfix20cb…` | `v1.2.20-bgmfix20cb…` |
| velox-worker-13197 | (old env) | `v1.2.20-bgmfix20cb…` | — |
| velox-worker-523925eb | `v1.2.20-bgmfix20cb…` | `v1.2.20-bgmfix20cb…` | `v1.2.20-bgmfix20cb…` |

- The env value `v1.2.20-bgmfix20cb…` is a *version-prefixed* string, not
  a bare 64-hex bundle hash — the same corruption class flagged in the
  verdict (newline / version-prefix inside `BUNDLE_HASH.txt`).
- Only `host_57_129` runs the clean `7ba60510…` bundle, but its env still
  declares the old value → `bundle_hash_coherent` FAIL.

## Binary / engine version drift

| Worker | container VERSION.txt | engine binary sha | selftest baseline |
| --- | --- | --- | --- |
| host_57_129 | `v1.2.20` | `94888ad7…` | `9247ff97…` ✅ |
| host_57_131 | **`v1.1.0`** (stale VERSION.txt) | `94888ad7…` | `9247ff97…` ✅ |
| velox-worker-13197 | — | `5aad673a…` (host) | `f180058d…` ❌ |
| velox-worker-523925eb | `v1.2.20` | `94888ad7…` | `9247ff97…` ✅ |

`host_57_131` runs a container whose `VERSION.txt` says `v1.1.0` while the
master records `software_version=v1.2.20` — another packaging drift signal.

## Per-worker evidence

### host_57_129_132_133
- unit `velox-worker-worker_57_129.service`, active since 11:25:54Z, NRestarts=0
- container `74e4c51dddcc`, image `velox-worker:v1.2.20-capsfix`
- master: CONNECTED, session_active=true, image_digest `3631d271…`, sw `v1.2.20`
- bootstrap READY; baseline `9247ff97…` ✅; bin `v1.2.20`
- FAIL only: env bundle hash stale (`v1.2.20-bgmfix…` vs disk `7ba60510…`)

### host_57_131_20_173
- unit `velox-worker-worker_57_131.service`, active since 11:25:58Z, NRestarts=0
- container `d83b826d7604`, image `velox-worker:v1.2.20-capsfix`
- master: CONNECTED, session_active=true, image_digest `3631d271…`, sw `v1.2.20`
- bootstrap READY; baseline `9247ff97…` ✅; container VERSION.txt `v1.1.0` (drift)

### velox-worker-13197 — FAIL, down
- unit `velox-worker-worker_13197.service`, state `activating (auto-restart)`
- master: DISCONNECTED, session_active=false, last heartbeat 10:38:34Z
- bootstrap FAIL: `engine_selftest_baseline_mismatch` (actual `9247ff97…`,
  expected `f180058d…` — stale baseline file)
- env references old image `velox-worker:v1.2.20-9a9c386` (other hosts: capsfix)

### velox-worker-523925eb
- unit `velox-worker-worker_523925.service`, active since 11:26:06Z, NRestarts=0
- container `ffc3c0caac03`, image `velox-worker:v1.2.20-capsfix`
- master: CONNECTED, session_active=true, image_digest `3631d271…`, sw `v1.2.20`
- bootstrap READY; baseline `9247ff97…` ✅; bin `v1.2.20`

## Next step (per verdict ordering)

Fase 2 is not complete until 4/4 certify. The immediate blockers:

1. `velox-worker-13197` — refresh `engine_selftest_baseline.sha256` to
   `9247ff97…` and restart; re-certify (target: CONNECTED + READY).
2. `host_57_129_132_133` — reconcile env `VELOX_BUNDLE_HASH` with the disk
   value `7ba60510…` (or re-run canonical deploy).
3. Fleet-wide — unify the running image onto one pinned digest and the
   bundle hash onto one canonical value (Ansible, Fase 3).

Then re-run `scripts/ops/runtime-cert.sh --fleet` and expect 4× PASS.
