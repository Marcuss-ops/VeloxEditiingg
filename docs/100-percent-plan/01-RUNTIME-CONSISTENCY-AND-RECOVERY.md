# Track 1 — Runtime Consistency and Recovery

Status: canonical TODO  
Priority: P0 before production

## Outcome

The runtime must remain correct across normal execution, retries, process crashes, database errors and network failures. No Job, Task, TaskAttempt, lease, upload or artifact may require manual database repair.

## 1. Restore the verification baseline

- [ ] Run the repository from a fresh checkout with a clean working tree.
- [ ] Run `make verify-fast` and archive the complete output.
- [ ] Run `make verify` with Docker and the native toolchain available.
- [ ] Fix every formatting drift before feature work continues.
- [ ] Confirm `gofmt` does not modify tracked files.
- [ ] Confirm all Go modules pass `go vet` and `go test -race`.
- [ ] Confirm architecture, migration, single-writer, registry, secret and DB-access checks pass.
- [ ] Add a regression check proving the baseline command fails on an intentionally dirty or malformed fixture.

## 2. Preserve canonical ownership

- [ ] Verify all Job business-state writes pass through `internal/jobs` services and repositories.
- [ ] Verify Task state, attempt number, revision and lease expiry are owned only by `internal/taskgraph`.
- [ ] Verify execution reports and phase metrics are owned only by `internal/taskattempts`.
- [ ] Verify Job `SUCCEEDED` has exactly one writer in verified artifact finalization.
- [ ] Verify handlers contain no direct SQL mutation.
- [ ] Verify background runners contain no side-channel state mutation.
- [ ] Verify JSON files and in-memory maps are not authoritative.
- [ ] Add or tighten full-tree invariant tests for every canonical writer.

## 3. Make lease expiry and Job reconciliation durable

Current risk to close: a Task lease can be reaped successfully while the later Job roll-up fails.

- [ ] Remove ignored errors from the post-reap Job update path.
- [ ] Decide one durable mechanism: transactional outbox, reconciliation table or idempotent reconciliation runner.
- [ ] Persist enough identity to replay the Job roll-up after process restart.
- [ ] Ensure a committed Task reap is never treated as fully reconciled until the Job result is durable.
- [ ] Make reconciliation idempotent across repeated ticks.
- [ ] Expose a metric for unreconciled terminal Tasks.
- [ ] Expose an alert when reconciliation age exceeds the SLO.
- [ ] Test DB failure after the Task/Attempt transaction commits but before Job update.
- [ ] Test master crash in the same failure window.
- [ ] Test repeated reconciler execution produces one final Job transition.
- [ ] Prove no Job remains indefinitely RUNNING or RETRY_WAIT after its Tasks are terminal.

## 4. Enforce attempt and lease fencing

- [ ] Validate every Task result against `task_id`, `attempt_id`, `worker_id` and `lease_id`.
- [ ] Reject results from expired leases.
- [ ] Reject results from superseded attempts.
- [ ] Reject a result whose executor ID or version differs from the Task contract.
- [ ] Make TaskAccepted idempotent.
- [ ] Make lease renewal idempotent and revision-fenced.
- [ ] Make TaskResult ingestion idempotent.
- [ ] Ensure only one attempt can become the winner.
- [ ] Store the rejection reason as a typed code.
- [ ] Count stale, duplicate and mismatched reports in metrics.

Required tests:

- [ ] duplicate TaskAccepted;
- [ ] duplicate TaskResult;
- [ ] late TaskResult after retry begins;
- [ ] result from wrong worker;
- [ ] result with wrong lease;
- [ ] result with stale revision;
- [ ] two workers racing to complete the same Task;
- [ ] master restart between acceptance and result.

## 5. Guarantee artifact integrity and finalization

- [ ] Compute SHA-256 and size on the worker before upload.
- [ ] Verify SHA-256 and size on the master or BlobStore.
- [ ] Validate MIME, codec, dimensions and duration for final video artifacts.
- [ ] Keep upload status transitions CAS-protected.
- [ ] Ensure a partial or corrupt upload cannot become READY.
- [ ] Ensure Job completion timestamp is not earlier than artifact verification.
- [ ] Ensure one logical final output has at most one winning READY artifact.
- [ ] Make duplicate finalization return the existing result without a second transition.
- [ ] Reject finalization from a losing attempt.
- [ ] Reconcile orphan promoted blobs after transaction rollback.
- [ ] Reconcile STAGING or FINALIZING uploads abandoned by a crash.
- [ ] Quarantine READY metadata whose blob is missing.
- [ ] Record and alert on every reconciliation action.

Required tests:

- [ ] valid upload and finalization;
- [ ] wrong hash;
- [ ] wrong size;
- [ ] interrupted upload;
- [ ] duplicate finalize request;
- [ ] process crash after blob promotion;
- [ ] process crash before transaction commit;
- [ ] storage-key collision;
- [ ] stale attempt finalization;
- [ ] missing final blob.

## 6. Recover from master restart

- [ ] Reconstruct schedulable Task state only from repositories.
- [ ] Restore pending outbox and delivery work.
- [ ] Invalidate or expire old worker sessions deterministically.
- [ ] Require workers to establish a new authenticated session.
- [ ] Keep active Task identity and lease state consistent during reconnect.
- [ ] Return readiness to false during bootstrap and recovery.
- [ ] Return readiness to true only after mandatory dependencies pass.
- [ ] Define and measure master recovery SLO.

Required tests:

- [ ] restart while fleet is idle;
- [ ] restart with READY Tasks;
- [ ] restart with LEASED Task before acceptance;
- [ ] restart with RUNNING Task;
- [ ] restart during upload;
- [ ] restart during finalization;
- [ ] repeated restart soak.

## 7. Recover from worker crash

- [ ] Detect missing renewal and expire the lease.
- [ ] mark the previous attempt with a terminal or stale outcome according to the contract.
- [ ] create exactly one replacement attempt when retry budget permits.
- [ ] assign the Task to another eligible worker.
- [ ] reject the old worker result after it returns.
- [ ] remove or quarantine temporary outputs from the dead attempt.
- [ ] maintain exactly one final READY artifact.
- [ ] define and measure worker-crash recovery SLO.

Required tests:

- [ ] crash before TaskAccepted;
- [ ] crash after lease grant;
- [ ] crash during native render;
- [ ] crash during upload;
- [ ] crash during finalization;
- [ ] crash after successful completion but before acknowledgement.

## 8. Recover from network partition

- [ ] Simulate worker-to-master loss.
- [ ] Simulate master-to-worker loss.
- [ ] Simulate latency, jitter and packet loss.
- [ ] Expire sessions and leases without split-brain finalization.
- [ ] Reconnect through a new authenticated session.
- [ ] Suppress duplicate accepts, renewals and results.
- [ ] Prove two attempts cannot both finalize.
- [ ] Record typed disconnect and recovery reasons.

Required cases:

- [ ] 60-second partition;
- [ ] 120-second partition;
- [ ] partition shorter than lease TTL;
- [ ] partition longer than lease TTL;
- [ ] reconnect while replacement attempt is running;
- [ ] repeated flapping connection.

## 9. Drain, cancellation and shutdown

- [ ] Set worker readiness false before drain begins.
- [ ] Reject new Task offers while draining.
- [ ] Allow active work to finish within a configurable grace period.
- [ ] Propagate context cancellation to the native C++ process.
- [ ] Send TERM and escalate to KILL after the configured timeout.
- [ ] Wait for child-process cleanup before worker exit.
- [ ] Remove incomplete temporary files.
- [ ] Never mark a cancelled or partial output READY.
- [ ] Configure container and systemd stop timeouts consistently.
- [ ] Emit structured shutdown phase logs.

Required tests:

- [ ] SIGTERM while idle;
- [ ] SIGTERM during render;
- [ ] SIGTERM during upload;
- [ ] timeout escalation;
- [ ] drain with multiple active Tasks;
- [ ] master shutdown during finalization.

## Definition of Done

- [ ] Lost Jobs = 0 in the recovery suite.
- [ ] Duplicate READY final artifacts = 0.
- [ ] Tasks without a terminal or retryable state after SLO = 0.
- [ ] Winning attempts are unambiguous.
- [ ] All failure windows above have automated tests.
- [ ] Recovery behavior is observable through typed logs, metrics and database assertions.
- [ ] The complete recovery suite passes repeatedly from a clean environment.
