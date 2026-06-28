# Cap. 8 — Image Upgrade + Rollback (Phase 7 / 100% Velox)

Operator runbook: rolling the worker image forward (digest A → digest B)
and rolling it back (B → A) without losing credentials, without manual DB
writes, and without pulling an operator-unpinned image.

## What it proves

| Invariant  | What it asserts                                                          | How                                            |
| ---------- | ------------------------------------------------------------------------ | ---------------------------------------------- |
| **NR-10**  | Post-upgrade image digest ≠ pre-upgrade digest (we actually swapped)     | `docker inspect` snapshot before+after        |
| **NR-13**  | Post-rollback image digest IS the previously-baselined registered digest | `baselines/_index.json` consulted before swap  |
| **NR-15**  | Same `task_id` is NOT finalized SUCCEEDED twice across the swap boundary | `task_attempts` table grouped by `task_id`     |

It also asserts the indirect invariant that runs alongside every swap:

- **NR-14** (server-side) — server-side `workers.drain` column flipped to
  `1` before the binary swap. This is asserted via the `/admin/worker/
  drain` route + post-snapshot DB read.

## The 5-step swap (upgrade)

```
       UPGRADE STATE           ACTION                  PERSISTS
   ┌─────────────────┐    ┌────────────────────┐    ┌──────────────────────┐
   │ A is running    │ →  │ 1. drain A         │ →  │ workers.drain=1      │
   │ tasks=N         │    │ 2. wait 0 active   │    │ task_attempts UNCHGD │
   │ digest=digest_A │    │ 3. cosign-verify B │    │                      │
   │                 │    │ 4. docker pull B   │    │                      │
   │                 │    │ 5. docker run B    │ →  │ workers.drain=0*     │
   └─────────────────┘    └────────────────────┘    │ tasks=N (new job)    │
   ┌─────────────────┐                              │ digest=digest_B     │
   │ B is running    │ ←                            └──────────────────────┘
   └─────────────────┘
```

*Note: drain flips back to `0` once B re-registers and is fully schedulable.

## The 3-step swap (rollback)

```
       ROLLBACK STATE          ACTION                  PERSISTS
   ┌─────────────────┐    ┌───────────────────────┐   ┌──────────────────────┐
   │ B is running    │ →  │ 1. cosign-verify A    │ →  │ Read baselines SOT  │
   │ (defective)     │    │   from baselines.json │    │ reject unknown      │
   │ digest=digest_B │    │ 2. swap B→A (atomic)  │    │                      │
   │                 │    │ 3. restart container  │ →  │ digest=digest_A     │
   └─────────────────┘    └───────────────────────┘   │ workers.drain untouched│
   ┌─────────────────┐                               └──────────────────────┘
   │ A is running    │ ←
   │ (pinned, signed)│
   └─────────────────┘
```

## Why a fixed baseline index for rollback?

- Pinning a digest once and pulling it again later is NOT bit-for-bit
  identical (build cache eviction, registry GC, retagged releases). An
  operator who "rolls back" must guarantee the same bytes as the digest
  the system just left.
- The `baselines/_index.json` row carries:
  - `digest` (64-hex)
  - `registry_image` (full pin)
  - `signing_envelope_sha256` (cosign envelope hash)
  - `tags` (e.g. `worker-v1.2.3`)
- The script refuses a rollback to a digest not present in the index —
  any "new" pull from a tag that has been mutated since the original
  upgrade is rejected at the cosign step (exit 2).

## How to run

### UPGRADE

```bash
sudo scripts/cert/cap-8-upgrade-rollback.sh \
  --direction upgrade \
  --target-digest "ghcr.io/.../velox-worker@sha256:<64hex-B>" \
  --image-ref "$DIGEST_A"   # (current pin; passed in for clarity) \
  --evidence-root "$HOME/evidence"
```

### ROLLBACK

```bash
sudo scripts/cert/cap-8-upgrade-rollback.sh \
  --direction rollback \
  --target-digest "sha256:<12hex-A-short>" \
  --evidence-root "$HOME/evidence"
```

The 12-hex prefix is resolved against `$EVIDENCE_ROOT/baselines/_index.json`
locally — no network call required for the backout.

### Simulator

```bash
make cap-8-upgrade-rollback   # both scenarios (upgrade + rollback)
make cap-8-upgrade-rollback-cap8-upgrade
make cap-8-upgrade-rollback-cap8-rollback
```

The simulator uses `$DATA_DIR/.active-digest` as a stand-in for
`docker inspect {{index .RepoDigests 0}}`. This proves:

- Same mounting strategy + certs-dir + work_dir means a swap is exactly
  equivalent to a docker stop+rm+run on the operator host.
- Same evidence-merging locking so the `baselines/_index.json` is
  consulted at swap time (see `cosign-verify mock` in `simulator.sh`).

## Evidence produced

For each direction:

- `verdict.json` — schema `velox.cert-8-upgrade-rollback.v1`
- `cosign.json` — cosign envelope (VPS path) / mocked envelope hash
- `docker-pull.log`, `docker-run.log` — VPS path only
- `drain.json`, `drain-http.txt` — HTTP ACK for the drain route (NR-14)
- `snapshot.json` — pre+post image digest + config sha256

The simulator also emits a roll-up `verdict.json` with both
`scenarios.upgrade` and `scenarios.rollback` subtrees so the operator
gets a single file per `make cap-8-upgrade-rollback` invocation.

## Verdict schema (canonical)

```jsonc
{
  "schema":             "velox.cert-8-upgrade-rollback.v1",
  "worker_id":          "host1-cap8-20260515",
  "cert_date":          "2026-05-15",
  "direction":          "upgrade",        // or "rollback"
  "final_status":       "PASS",
  "failed_invariants":  [],
  "invariants": {
    "NR-10-image-digest-changed":     true,   // upgrade only
    "NR-13-image-digest-baselined":   true,   // rollback only
    "NR-11-cert-and-config-preserved":true,
    "NR-14-drain-acked":              true,
    "NR-15-no-double-finalization":   true
  },
  "evidence": {
    "pre_image":            "ghcr.io/.../velox-worker@sha256:aaaa...",
    "post_image":           "ghcr.io/.../velox-worker@sha256:bbbb...",
    "target":               "ghcr.io/.../velox-worker@sha256:bbbb...",
    "pre_config_sha256":    "5b2f...",
    "post_config_sha256":   "5b2f..."
  },
  "generated_at": "2026-05-15T22:31:08Z"
}
```

## Failure modes & recovery

| Symp­tom                              | Diagnose                                              | Recover                                                 |
| ------------------------------------- | ----------------------------------------------------- | ------------------------------------------------------- |
| `NR-10 FAIL` (digest unchanged)       | `cosign_verify` mocked but actual swap didn't happen  | Re-run with verbose `docker pull` log                  |
| `NR-13 FAIL` (active digest not A)    | `_index.json` was wiped, or digest A removed          | Re-pin via `pin-worker-digest.sh`                      |
| `NR-14 FAIL` (drain not ack'd)        | Master's `/admin/worker/drain` route not reachable    | Restart velox-server; verify the route is registered   |
| `NR-15 FAIL` (double SUCCEEDED)       | Reaper/dedup logic regressed; check `MarkCommandSeen` mapping | Rollback + audit `task_attempts.attempt_number`      |

## What this runbook explicitly does NOT do

- It does not pull a fresh, operator-unpinned image. The swap target is
  either the operator's previously-baselined B (upgrade) or the
  previously-baselined A (rollback). A typo'd `--target-digest` that is
  not in `baselines/_index.json` is rejected at step 1.
- It does not modify the SQLite DB. Drain is asserted via `/admin/worker
  /drain` (server-side `UpdateWorker({drain:true})`); reaper/dedup
  semantics are independently owned by NR-15.
- It does not require reissuing creds. Volume mounts carry the cert dir
  intact, so worker identity persists.

## Cross-references

- `scripts/cert/pin-worker-digest.sh` — the operator pinner; the
  baselines `_index.json` it produces IS the SOT consulted by this script.
- `scripts/cert/real-bootstrap.sh` — the *immutable* certifier of a
  pinned digest (referenced by NR-9).
- `DataServer/internal/workers/registry_update.go` — `SetWorkerDrain`
  server-side handler, exposed via `/admin/worker/drain` (NR-14).
- `RemoteCodex/native/worker-agent-go/internal/worker/worker_persistence.go`
  — `worker_state.json` JSON-on-disk; preserved by the volume mount.
