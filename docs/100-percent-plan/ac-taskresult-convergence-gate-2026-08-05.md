# AC/TaskResult convergence gate — 2026-08-05

## Verdict

**BLOCKED / FAIL — not converged.**

The canonical gate was executed in read-only mode against the active master
DB and the candidate job. It could not run the worker-side zero-state/ACK
checks because the configured runtime spool database was absent. Independent
read-only queries also found global convergence violations. No repair or
reconciliation was performed.

No production job, task, attempt, artifact, delivery, lease, spool row, or DB
record was modified by this gate.

## Evidence timestamp and inputs

```text
Checked: 2026-08-05 (UTC)
Master DB: /opt/velox/current/.velox/data/velox.db
Candidate job: job_ecc3077dbe5d900a
Worker log: /tmp/tyson/worker.log
Expected worker spool DB: /var/lib/velox-worker/work/executor_spool/worker_output_spool.sqlite3
```

The canonical invocation against the expected runtime spool path returned:

```text
RC=2
FATAL: DB, worker spool DB, and worker log must be regular files.
```

The expected spool path and `/var/lib/velox-worker/state/executor_spool/worker_output_spool.sqlite3`
were both missing. Three `worker_output_spool.sqlite3` files under
`/tmp/velox-worker/{test,tests,two-key-test}` were found, but they are test
fixtures and were not used as live evidence.

## Candidate job chain

Read-only DB evidence for `job_ecc3077dbe5d900a`:

| Entity | ID | Status / evidence |
|---|---|---|
| Job | `job_ecc3077dbe5d900a` | `SUCCEEDED` |
| Task | `7461df49-c9ee-4583-8738-7055441ccedb` | `SUCCEEDED` |
| Winning attempt | `a3dd976a-b0b4-48bb-9dc0-0dd866f779a0` | `SUCCEEDED` |
| Attempt commit | `76cb7798bc795eedb6453e1ff20835f6` | `COMMITTED` |
| Final artifact | `2e3ae00e8cd75b27bbe41063d7f93b59` | `READY`, size `34727`, SHA-256 present |
| Final upload | `ba6c279b86796cc2637badf06a21f1b9` | `COMPLETED` |
| Delivery | `jbd_comp_2e3ae00e8cd75b27bbe41063d7f93b59_comedy_test` | `SUCCEEDED` |
| Drive remote ID | `1-VnXSYR7DEaXInNi3rkF2N5-FjXPxb7R` | present and non-empty |

The candidate task also has a READY engine-progress sidecar artifact and two
DECLARED output declarations linked to the same task, attempt, and commit.
The candidate-specific DB chain is therefore internally converged for the
final video and delivery fields that were independently queried.

## Worker ACK evidence

The candidate task/attempt pair is:

```text
task=7461df49-c9ee-4583-8738-7055441ccedb
attempt=a3dd976a-b0b4-48bb-9dc0-0dd866f779a0
```

Exact searches of `/tmp/tyson/worker.log` found no matching:

```text
[TASK_RESULT_OUTBOX] TaskResultAck received task=... attempt=...
TASK_COMMIT_ACK_RECEIVED ... task_id=... attempt_id=...
```

The log contains the task offer, lease grant, and execution start, but no
candidate-specific worker-consumed ACK markers. Because the spool DB is also
missing, worker-side TaskResult outbox emptiness and spool age cannot be
certified.

## Global zero-state evidence

Independent read-only queries against the master DB returned:

| Invariant | Count | Result |
|---|---:|---|
| Running jobs | 0 | PASS |
| Running tasks | 0 | PASS |
| Non-terminal task attempts | 19 | **FAIL** |
| Expired active leases | 0 | PASS |
| Non-terminal deliveries | 0 | PASS |
| Stale artifact uploads | 4 | **FAIL** |

The 19 non-terminal attempts include one `PENDING` attempt and 18 `RUNNING`
attempts. The four stale uploads are:

| Upload ID | Job ID | Status | Expires at |
|---|---|---|---|
| `7f5b309a57d0322b7293fed62a7cf69d` | `scriptclip_10b26964-a8d6-497c-b515-2c03e3cc01b3` | `RECEIVED` | `2026-07-04T10:54:15Z` |
| `e0790063a75897274c510118e2f51dc4` | `mike-tyson-cache-e2e11-en-20260804T122300Z` | `RECEIVED` | `2026-08-05T12:22:10Z` |
| `21d30857a9051f8ae57d8a6238216608` | `mike-tyson-cache-e2e11-de-20260804T122300Z` | `RECEIVED` | `2026-08-05T12:22:22Z` |
| `b1d7fbdcc8b891308445fea2933d29d3` | `mike-tyson-cache-e2e11-id-20260804T122300Z` | `RECEIVED` | `2026-08-05T12:22:56Z` |

The four rows are global violations of the gate's stale-upload query even
though their `completed_at` values are populated; they require the approved
reconciler/cleanup path rather than manual SQLite edits.

## Gate self-test

The official isolated self-test passed:

```text
bash scripts/ci/test-ac-taskresult-convergence.sh
[test-ac-taskresult-convergence] OK
```

It rejected all required negative cases, including running tasks/attempts,
missing winning attempt, non-terminal delivery, expired lease, missing Drive
ID, pending TaskResult outbox, stale spool, and missing commit ACK.

## Final assessment

| Area | Verdict |
|---|---|
| Candidate job/task/attempt | PASS from independent read-only DB queries; full gate did not complete |
| Attempt commit | PASS from independent read-only DB query (`COMMITTED`) |
| Final artifact | PASS from independent read-only DB query (`READY`, non-empty SHA/size) |
| Delivery/Drive ID | PASS from independent read-only DB query (`SUCCEEDED`, remote ID present) |
| Worker TaskResult ACK | NOT CERTIFIED — marker absent for candidate |
| Worker commit ACK | NOT CERTIFIED — marker absent for candidate |
| Running jobs/tasks | PASS (zero) |
| Expired leases | PASS (zero) |
| Non-terminal attempts | **FAIL (19)** |
| Stale uploads | **FAIL (4)** |
| Worker spool/outbox | **BLOCKED — runtime spool DB missing** |
| Overall convergence | **BLOCKED / FAIL** |

Required follow-up is to use the approved stale-execution/upload reconciler,
restore or correctly configure the worker spool evidence path, and rerun the
canonical gate. No manual SQL mutation is part of this report.
