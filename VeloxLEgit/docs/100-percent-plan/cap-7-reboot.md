# Cap. 7 — Reboot Recovery (Phase 6 / 100% Velox)

Operator runbook: an in-flight Job must survive a host `reboot` and recover
without operator intervention, without manual DB writes, and without
losing worker identity.

## When to run this

- After any change to the systemd unit, docker compose, or volume layouts.
- After any change to `TaskLeaseReaper`, the lease-expiry oracle, or the
  re-registration loop.
- After any change to the worker that affects `worker_state.json` or the
  `worker_config.json` schema.
- As part of quarterly disaster-recovery drill.

## What it proves

| Invariant  | What it asserts                                                  | Pre-check required                |
| ---------- | ---------------------------------------------------------------- | --------------------------------- |
| **NR-8**   | `worker_config.json` sha256 unchanged across the reboot          | Operator hasn't edited config     |
| **NR-9**   | Pinned image digest unchanged                                    | Worker is running pinned digest  |
| **NR-11**  | `worker.crt`, `worker.key`, `ca.crt` sha256 unchanged            | Certs are volume-mounted          |
| **NR-12**  | Orphan tasks enter `LEASE_EXPIRED` and a fresh attempt is re-minted by `TaskLeaseReaper` | Long-running job is in flight |

## How to run

### On a real VPS ($HOSTNAME is the worker host)

```bash
sudo scripts/cert/cap-7-reboot-recovery.sh \
  --worker-id "$(hostname)-cap7-$(date -u +%Y%m%d)" \
  --image-ref "ghcr.io/.../velox-worker@sha256:..." \
  --evidence-root "$HOME/evidence"
```

The script will:

1. Take a pre-reboot snapshot under
   `$EVIDENCE_ROOT/$DATE/$WORKER_ID/pre/`:
   - `image-digest.txt`  (docker image @sha256:… pin)
   - `worker-config.sha256`
   - `certs.sha256`
   - `db-inventory.txt` (workers + RUNNING/LEASED tasks snapshot)

2. **Pause** — print "NEXT STEP: sudo reboot now" and wait for the
   operator to issue the reboot manually. The pre-snapshot is durable
   under `pre/`, so a Ctrl-C abort loses nothing.

3. After the host reboots, **re-run the same script** with the same
   `--worker-id` (so the verdict merges on the same path). The script
   detects the post-reboot state and proceeds to take a post-snapshot,
   diff against pre, and emit `$EVIDENCE_ROOT/$DATE/$WORKER_ID/verdict.json`.

4. The orchestrator (`tests/e2e/cap-7-reboot-recovery/simulator.sh`) is
   the same shell logic minus the `reboot` step — kill+restart the local
   stub master/worker instead of waiting for the host to come back.
   This is what CI runs on every commit; the operator script is the
   gate before promoting a digest.

### Simulator (CI — no real host needed)

```bash
make cap-7-reboot-recovery          # alias for the simulator
# EVIDENCE_ROOT override accepted
make cap-7-reboot-recovery EVIDENCE_ROOT=/tmp/run-cap7
```

The simulator asserts the same four invariants against a stub master +
worker pair, without pulling containers. It is intentionally hermetic — a
green CI run proves the orchestrator mechanics; the live operator runbook
proves the same mechanics on the deployment host.

## Evidence produced

- `pre/image-digest.txt` — pinned digest before reboot
- `pre/worker-config.sha256` — config fingerprint before reboot
- `pre/certs.sha256` — TLS fingerprints before reboot
- `pre/db-inventory.txt` — RUNNING/LEASED tasks snapshot
- `post/image-digest.txt` — pinned digest after reboot
- `post/worker-config.sha256` — config fingerprint after reboot
- `post/certs.sha256` — TLS fingerprints after reboot
- `post/db-inventory.txt` — RUNNING/LEASED tasks snapshot
- `verdict.json` — schema `velox.cert-7-reboot-recovery.v1`

## Verdict schema (canonical)

```jsonc
{
  "schema":               "velox.cert-7-reboot-recovery.v1",
  "worker_id":            "host1-cap7-20260515",
  "cert_date":            "2026-05-15",
  "image_ref":            "ghcr.io/.../velox-worker@sha256:...",
  "evidence_dir":         "$HOME/evidence/2026-05-15/host1-cap7-20260515",
  "final_status":         "PASS",
  "failed_invariants": [],
  "invariants": {
    "NR-8-config-sha256-preserved":  true,
    "NR-9-image-digest-preserved":   true,
    "NR-11-cert-sha256-preserved":   true,
    "NR-12-orphan-task-recovery":    true
  },
  "evidence": {
    "pre_image_digest":   "ghcr.io/.../velox-worker@sha256:aaaa...",
    "post_image_digest":  "ghcr.io/.../velox-worker@sha256:aaaa...",
    "pre_config_sha256":  "5b2f...",
    "post_config_sha256": "5b2f...",
    "pre_certs_sha256":   "abc...  worker.crt\\n def...  worker.key\\n ...",
    "post_certs_sha256":  "abc...  worker.crt\\n def...  worker.key\\n ...",
    "nr12_notes":         "..."
  },
  "generated_at": "2026-05-15T22:17:04Z"
}
```

## Failure modes & recovery

| Symp­tom                              | Diagnose                                              | Recover                                                 |
| ------------------------------------- | ----------------------------------------------------- | ------------------------------------------------------- |
| `NR-8 FAIL` (config drift)            | Operator accidentally edited `worker_config.json`     | Re-pin from git; rerun the script                      |
| `NR-9 FAIL` (digest drift)            | Worker auto-updated outside the operator pinner       | Re-pin via `pin-worker-digest.sh`; rerun                |
| `NR-11 FAIL` (cert drift)             | Cert mount fell off, or rotated outside the script    | Re-mount cert volume; rerun                            |
| `NR-12 FAIL` (no orphan recovery)     | `TaskLeaseReaper` is unhealthy                       | Check `velox.log | grep LEASE_EXPIRED`; rollback    |

## What this runbook explicitly does NOT do

- It does not pull a worker image. The pin is asserted (`NR-9`), not
  re-fetched. Image updates happen via the cap. 8 workflow.
- It does not modify the SQLite DB. Operationally, the orphan-task
  recovery is driven entirely by `TaskLeaseReaper` from SQLite timeouts.
- It does not require root for the simulator (it mocks sleep+kill+restart
  with shell stubs). The VPS path requires `sudo`.
- It does not exit non-zero on transient SQLite-wal-checkpoint hiccups
  (the content of the DB is what matters).
