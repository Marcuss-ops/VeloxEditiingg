# Cap. 6 — Recovery Matrix (15 fault-injection scenarios + NR-1..NR-7 invariants)

Cap. 6 of the 100% Velox certification plan. Orchestrates 15 fault-injection
scenarios and verifies that 7 canonical post-recovery invariants hold for
each one. Companion to `tests/e2e/recovery-matrix/`.

## Quickstart

```bash
# Run all 15 scenarios (writes evidence to $EVIDENCE_ROOT/<date>/fleet-recovery/)
make recovery-matrix

# Run an individual scenario: 1..15
bash tests/e2e/recovery-matrix/run.sh --scenario 7

# Syntax sweep only (no DB writes, no docker)
make recovery-matrix-dry
```

Optional env:

| Var                         | Default                                | Effect                                          |
| --------------------------- | -------------------------------------- | ----------------------------------------------- |
| `EVIDENCE_ROOT`             | `/tmp/velox-recovery-matrix`           | where `verdict.json` and per-scenario logs land |
| `VELOX_RECOVERY_ALLOW_ROOT` | unset                                  | opt in to running as root (ENOSPC sim fragile)  |
| `VELOX_SQLITE_BIN`          | `sqlite3`                              | override sqlite3 binary path                    |

## The 7 invariants

| ID     | Surface (canonical query)                                                                                             | Fail-closed rule                                |
| ------ | ---------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------- |
| NR-1   | `task_attempts` grouped by `task_id HAVING COUNT(active)>1`                                                            | exactly one attempt per task in active state    |
| NR-2   | `task_attempts.status='SUCCEEDED' AND tasks.worker_id != attempts.worker_id OR lease_id drift`                          | old lease cannot finalize                       |
| NR-3   | `jobs.status IN (PENDING,LEASED,RUNNING,AWAITING_ARTIFACT) AND updated_at < now-window`                                | no Job blocked forever                          |
| NR-4   | `artifact_uploads.status NOT IN (terminal) AND created_at < now-window AND no READY artifact link`                     | no partial files in final storage               |
| NR-5   | `artifacts.status='READY' AND (sha256 IS NULL OR size_bytes <= 0)`                                                     | no READY without valid bytes                    |
| NR-6   | `jobs.status='SUCCEEDED' AND NOT EXISTS (artifacts.status='READY')`                                                   | no SUCCEEDED without READY                      |
| NR-7   | `tasks JOIN task_attempts WHERE attempts.status='TIMED_OUT' AND tasks.status IN (READY..RUNNING) AND tasks.attempt_id=attempts.id` | new attempt_id+lease_id after reap              |

The `_check()` helper in `invariants.sh` accepts a `neg_expected` flag.
Negative scenarios (scenarios 11-15) pass `neg_expected=1`: the system
must REJECT the bad input for the invariant to be considered PASS.

## The 15 scenarios

| #   | Scenario                              | Fault                                      | Invariants exercised |
| --- | ------------------------------------- | ------------------------------------------ | -------------------- |
| 01  | kill worker stage 1 (mid-offer)       | SIGKILL worker after `ClaimNext` commit    | NR-1, NR-7           |
| 02  | kill worker stage 2 (mid-execution)   | SIGKILL worker mid-RUNNING                 | NR-1, NR-3, NR-7     |
| 03  | kill worker stage 3 (mid-upload)      | SIGKILL worker between `CREATED`→`FINALIZING` | NR-1, NR-3, NR-4, NR-7 |
| 04  | kill worker stage 4 (post-result)     | SIGKILL worker after Send(TaskResult)      | NR-1, NR-5, NR-6     |
| 05  | restart master stage 1 (post-claim)   | SIGKILL master before `safeSend(TaskOffer)` | NR-1, NR-3, NR-7     |
| 06  | restart master stage 2 (mid-FinalizeVerified) | SIGKILL master mid-tx                | NR-4, NR-5, NR-6     |
| 07  | network partition (worker SIGSTOP)    | `kill -STOP` worker, wait TTL, `kill -CONT`  | NR-1, NR-2, NR-7     |
| 08  | certificato revocato                  | `UPDATE worker_flags SET revoked=1`        | NR-1, NR-7           |
| 09  | disco pieno                           | `chmod 555` staging dir                    | NR-3, NR-4           |
| 10  | asset rimosso                         | `rm -f` input scene                        | NR-1, NR-3           |
| 11  | TaskResult duplicato                  | duplicate ingest (negative)                | NR-1, NR-6           |
| 12  | TaskResult vecchio                    | stale (worker, lease, attempt) tuple       | NR-2                 |
| 13  | TaskRejected vecchio (stale lease)    | stale lease_id in rejection                | NR-1                 |
| 14  | hash upload errato                    | wrong sha256 in completion                 | NR-5, NR-6           |
| 15  | size upload errata                    | wrong size in completion                   | NR-5, NR-6           |

## Verdict

`$EVIDENCE_ROOT/<date>/fleet-recovery/verdict.json` is the canonical verdict
document for cap. 6:

```json
{
  "schema": "velox.cert-6-recovery-matrix.v1",
  "final_verdict": "CERTIFIED" | "REJECTED",
  "matrix_summary": { "PASS": 12, "FAIL": 0, "DEGRADED": 0, "total": 12 },
  "scenarios_emitted": ["01","02","03","04","05","06","07","08","09","10",
                       "11","12","13","14","15"],
  "evidence_root": "/tmp/velox-recovery-matrix/2026-06-28/fleet-recovery",
  "generated_at": "2026-06-28T12:34:56Z"
}
```

Schema: `velox.cert-6-recovery-matrix.v1`. Final verdict is **CERTIFIED**
iff `FAIL == 0`. DEGRADED is informational only and surfaces when a
scenario required a privilege the orchestrator couldn't acquire (e.g.,
iptables DROP).

## Design notes

- **Why bash + sqlite3 (no Go probe needed):** the canonical surface
  for all 15 recovery decisions lives in the SQL CAS chain
  (`TransitionTaskToTerminalAtomic`, `ExpireTaskLeaseAtomic`,
  `FinalizeVerified`, `handleTaskRejected`'s `lease_id` check). A
  custom gRPC probe client would only add value for scenarios 11-15;
  for those, the bash stubs mutate DB rows to simulate a buggy worker's
  retry and assert the rejection path keeps the invariants intact.

- **Why not iptables for scenario 7:** requires root + `NET_ADMIN` cap.
  Equivalent partition semantics via `SIGSTOP`/`SIGCONT` (no root, no
  docker). Lease TTL is the "wait" timer.

- **Why `chmod 555` for scenario 9:** equivalent ENOSPC surface that
  doesn't require pre-allocating GB of zeros. The script must own the
  staging dir — refuses to run as root by default.

- **Negative-pass semantics:** scenarios 11-15 use `neg_expected=1` in
  the invariant helper. The `worker_messages` table captures the CANONICAL
  decision contract (an operator-side auditable log of what would have been
  surfaced in the live system).
