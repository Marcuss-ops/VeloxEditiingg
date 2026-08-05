# Cache ProtectionBarrier verification — 2026-08-05

## Verdict

**PASS — focused startup-barrier scenarios verified.**

The verification exercised the existing worker-side `ProtectionBarrier`,
protected-assets poller, readiness state, and cleanup loop. No runtime service,
worker configuration, cache entry, database row, or production asset was
modified. This commit records evidence only; it does not change the barrier
implementation.

## Verification context

```text
Checked at: 2026-08-05T12:41:10Z
Branch: main
HEAD/origin-main at preflight: dd7ad900af19b153b19702418d662085712f5ba9
Worker module: RemoteCodex/native/worker-agent-go
```

The working tree contained unrelated pre-existing modifications and
untracked files. None were included in this report commit.

## Commands executed

Combined focused run:

```bash
cd RemoteCodex/native/worker-agent-go
go test -count=1 ./internal/worker ./internal/workercache ./internal/telemetry \
  -run 'ProtectionBarrier|ProtectedAssets|CacheProtection|Readiness|CleanupLoop'
```

Result:

```text
ok  velox-worker-agent/internal/worker       0.405s
ok  velox-worker-agent/internal/workercache  0.137s
ok  velox-worker-agent/internal/telemetry    [no tests to run]
```

The telemetry package produced no selected tests for that combined filter; it
was not counted as additional evidence.

A verbose scenario run was then executed:

```bash
cd RemoteCodex/native/worker-agent-go
go test -count=1 -v ./internal/worker \
  -run 'TestProtectedAssetsBarrier|TestProtectedAssetsPoller|TestProtectedAssetsErrorsAfterStartupBlockCleanupUntilRecovery'
go test -count=1 -v ./internal/workercache \
  -run 'TestCleanupLoop_RunWaitsForProtectionBarrier'
```

All selected tests passed.

## Scenario evidence

| Scenario | Test | Result |
|---|---|---|
| Initial 401 does not open barrier; retry with first valid snapshot opens it | `TestProtectedAssetsBarrier_WaitsUntilFirstValidSnapshot` | PASS |
| Initial 401 leaves readiness false; valid snapshot sets `cache_protection_ready=true` | `TestProtectedAssetsBarrier_UpdatesReadinessOnlyAfterValidSnapshot` | PASS |
| Initial stale snapshot does not open barrier or become current | `TestProtectedAssetsPoller_InitialStaleSnapshotDoesNotOpenBarrier` | PASS |
| 503 preserves last-good snapshot and readiness | `TestProtectedAssetsBarrier_503KeepsLastGoodSnapshot` | PASS |
| 503 preserves readiness and does not make snapshot age newer | `TestProtectedAssetsBarrier_503PreservesReadinessAndLastSnapshotAge` | PASS |
| Stale later response does not replace last-good snapshot | `TestProtectedAssetsPoller_StaleResponseKeepsLastGoodSnapshot` | PASS |
| Poll failures after startup block cleanup until a valid recovery snapshot | `TestProtectedAssetsErrorsAfterStartupBlockCleanupUntilRecovery` | PASS |
| Registration loss re-arms the barrier/readiness; new session reopens it | `TestProtectedAssetsBarrier_RearmsAfterRegistrationLoss` | PASS |
| Cleanup remains blocked between sessions and resumes after recovery | `TestProtectedAssetsBarrier_CleanupBlocksBetweenSessions` | PASS |
| Cleanup loop waits for the barrier before its first destructive pass | `TestCleanupLoop_RunWaitsForProtectionBarrier` | PASS |

## Verified invariants

- no protected-assets snapshot before the first valid response can open the
  barrier;
- HTTP 401 and 503 failures do not create readiness or discard the last valid
  snapshot;
- a valid first snapshot opens `ProtectionBarrier` and publishes
  `cache_protection_ready=true`;
- an initial or later stale snapshot is rejected;
- readiness exposes the protected snapshot age and does not reset it to a
  fresher value after a 503;
- registration loss closes/re-arms the barrier and clears protection readiness;
- cleanup cannot run destructively while the current session has no valid
  protected snapshot;
- a fresh authenticated snapshot reopens cleanup for the new session;
- cleanup loop startup requires a non-nil barrier and waits for it before its
  first tick.

## Scope and limitations

This is a focused unit/integration test verification, not a live master/worker
reconfiguration or production readiness probe. No live 401/503 requests were
sent to the running deployment by this report. The tests use deterministic
`httptest` servers and in-memory cache state, which is intentional to avoid
mutating the live environment.
