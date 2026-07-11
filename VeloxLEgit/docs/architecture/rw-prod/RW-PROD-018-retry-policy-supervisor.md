# RW-PROD-018 — Supervisor single-retry policy: operator runbook

**Scope:** on-call procedure for backend runners (delivery, outbox,
forwarding, metrics-supervisor) that have exhausted the
`supervisor.FailureTracker` consecutive-error threshold and escalated
to the `BackgroundSupervisor`.

**Decision authority:** Verdetto P1 #10 (Blocco 4, "kill
log-and-continue"). Every per-tick error now flows through
`supervisor.ClassifyError` and `supervisor.FailureTracker.Record`;
after `DefaultRetryPolicy.ConsecutiveErrorThreshold` (default 5)
consecutive `ErrInfrastructure` hits within the `ResetWindow`
(default 30s), the runner returns the wrapped sentinel to the
BackgroundSupervisor, which then takes the configured
restart-or-die action for that runner.

**Audience:** L2/L3 on-call operator investigating stuck queues, /ready
degradation, or master-side retry-storm alerts.

---

## 1. When this runbook applies

You are here because:

- `/ready` is reporting one or more runners in `Failed` or `Restarting`
  state with a `last_error` containing `consecutive=N`.
- `velox_master_outbox_pending` (metrics/supervisor.go) is flatlining
  or steadily climbing.
- The master log shows a stream of `consecutive=N (since=…
  last=…) original=…` lines and a corresponding restart signal.
- `velox_placement_rejections_total{reason=…}` (separate runbook
  scope, RW-PROD-015 territory) is high — that path uses the same
  tracker but is owned by the placement pipeline, not these three.

The four background runners this runbook covers:

| Runner            | Time-tick path                                                      |
|-------------------|---------------------------------------------------------------------|
| DeliveryRunner    | `DataServer/internal/deliveries/runner.go::Run`                     |
| Outbox Dispatcher | `DataServer/internal/outbox/dispatcher.go::Run`                     |
| CreatorForwardingRunner | `DataServer/internal/forwarding/runner.go::Run`               |
| Metrics Supervisor| `DataServer/internal/metrics/supervisor.go::Run`                    |

Each of them owns a one-per-instance `supervisor.FailureTrackerWithClock`
created against `DefaultRetryPolicy()` — so when one trips, the others
are unaffected and continue with their own counter.

---

## 2. Symptom recognition

### 2.1 The exhaustion line

`supervisor/policy.go::FailureTracker.Record` (around line 240) emits
this exact format when the threshold trips:

```
fmt.Errorf("%w: consecutive=%d (since=%s last=%s) original=%v",
    ErrInfrastructure, t.consecutive,
    t.firstErrAt.Format(time.RFC3339Nano),
    t.lastErrAt.Format(time.RFC3339Nano), err)
```

Which renders as:

```
supervisor: infrastructure error: consecutive=5
  (since=2026-07-02T18:30:01.123456789Z last=2026-07-02T18:30:18.789012345Z)
  original=sql: connection is busy
```

That wrapped sentinel propagates from `runner.Run()` to the
`BackgroundSupervisor`:

- delivery — `fmt.Errorf("delivery runner: %w", escalated)`
  (`deliveries/runner.go::Run`)
- outbox — `fmt.Errorf("outbox dispatcher: %w", escalated)`
  (`outbox/dispatcher.go::Run`)
- forwarding — `fmt.Errorf("forwarding runner: %w", escalated)`
  (`forwarding/runner.go::Run`)

### 2.2 The supervisor restart signal

The `BackgroundSupervisor` next log line will be one of:

- `ClassRestartable` (network/DB blip) — the runner is restarted on a
  short backoff and picks up at the next claim batch.
- `ClassCritical` (panic recovered into `ErrPanicked` wrapped via
  `errors.Join` and escalated) — the master is hard-restarted by
  systemd / the platform supervisor.

### 2.3 Metrics signals

`metrics/supervisor.go::refreshMasterHealth` writes
`velox_master_outbox_pending`, `velox_master_memory_rss_bytes`,
`velox_master_goroutines` per tick. When the dispatcher runner is the
one that escalated, `velox_master_outbox_pending` will be stale
(numbers from before the escalation, not new ones) until
`BackgroundSupervisor` re-establishes the dispatcher and resumes the
`outbox.Store::PendingCount` callback.

### 2.4 `/ready` endpoint

The handler returns `state: "Failed"` and `last_error: "delivery
runner: supervisor: infrastructure error: consecutive=5 ..."` (or
the matching outbox/forwarding/metrics prefix). An operator should
treat any `state != "Running"` as a paging-worthy signal.

---

## 3. Log forensics

All snippets assume the master logs at `/var/log/velox/master.log`;
adapt path as needed.

### 3.1 Find the streak that tripped

```bash
# Any consecutive= count >= the policy threshold (default 5).
grep -E 'consecutive=[0-9]+' /var/log/velox/master.log \
  | tail -n 50
```

The most recent line is the one that escalated; the line(s) before it
show the inflight streak that built up.

### 3.2 Identify the wrapped original

```bash
# Look at the "original=" payload to classify the failure.
grep -E 'consecutive=[0-9]+' /var/log/velox/master.log \
  | tail -n 1 \
  | grep -oE 'original=[^ ]+'
```

Useful `original=` substrings and their meaning:

- `database is closed` → DB handle closed (vet restarts, app reboot,
  or another handle holder killed it). Infrastructure.
- `sql: connection is busy` → SQLite contention from a long write
  tx. Infrastructure; usually self-clears.
- `context deadline exceeded` (without row-mutation context) →
  upstream-deadline propagation. Infrastructure.
- `runner panicked` → handler-level panic recovered.
  `ErrPanicked` was joined during classify → counts as
  infrastructure. Codebug, not transient.
- `transition conflict` / `lease … conflict` →
  `ErrLeaseLost`. **Does NOT count** toward the consecutive
  threshold; the row stays untouched and the next tick re-claims.

### 3.3 Surface the failing runner's /ready state

```bash
curl -fsS http://localhost:8080/ready | jq '
  .runners[]
  | select(.state != "Running")
  | {name: .name, state: .state, last_error: .last_error}
'
```

### 3.4 Detect outbox-side panic loops

```bash
# Outbox panics are logged by dispatchEvent's defer-recover block.
grep 'PANIC in handler' /var/log/velox/master.log | tail -n 20
```

If this surface is non-empty **and** §3.1 shows `original=runner
panicked`, treat it as a codebug and page engineering — the master
restart will recur until the handler is fixed.

---

## 4. Decision tree

```
/ready == Failed
  ├─ last_error contains "database is closed" / "sql: connection is busy"
  │     → infrastructure (DB). Wait for ClassRestartable recovery.
  │       If retries recur 3+ times in 5m → see §5.1 (delivery)
  │       or §5.3 (whatever runner fired).
  ├─ last_error contains "runner panicked"
  │     → codebug. Restart the master once via systemd, then
  │       page engineering with the panic stack trace from §3.4.
  │       DO NOT mark any rows FAILED; the panic is in a handler,
  │       not on data.
  ├─ last_error contains "consecutive=" but original="context deadline exceeded"
  │     → upstream-down (network partition, parent ctx cancelled).
  │       Restart the master, then investigate the upstream
  │       source.
  └─ /ready == Running, but velox_master_outbox_pending not draining
        → lease-loop starvation (rows in PROCESSING with stale locks)
          OR max-attempts reached. Jump to §5 (manual unblock).
```

---

## 5. Manual unblock ("casella bloccata")

The "casella" is shape-different per runner:

- **delivery** — a `delivery_attempts` row in `RUNNING` with a lease
  held by a runner that died mid-upload.
- **outbox** — an `outbox_events` row in `PROCESSING` with
  `locked_until > now()` (lock owns the row).
- **forwarding** — a `creator_forwardings` row in
  `FORWARDING`/`READY_TO_FORWARD`/`PENDING`/`RETRY_WAIT` with
  `lease_id` owned by a dead runner.

In every case the **safe** unblock is to push the lease / lock
expiry into the past so the next tick re-claims; **never** `DELETE`
the row (FKs + audit trail).

### 5.1 Delivery

Delivery leases are stored on `delivery_attempts.lease_expires_at`.

Force the runner to reclaim on its next tick (preferred — preserves
the attempted upload state):

```sql
UPDATE delivery_attempts
   SET lease_expires_at = '1970-01-01T00:00:00Z'
 WHERE status = 'RUNNING'
   AND delivery_id = '<DELIVERY_ID>';
```

Force-terminate (only after you've confirmed the remote destination
isn't mid-upload and you don't want a retry):

```sql
UPDATE delivery_attempts
   SET status = 'FAILED',
       error_code = 'MANUAL_OVERRIDE',
       error_message = 'operator unblock (RW-PROD-018 §5.1)'
 WHERE status = 'RUNNING'
   AND delivery_id = '<DELIVERY_ID>';
```

The runner's `ClaimDeliveries` will skip rows with `lease_expires_at
> now()` and re-claim rows whose lease has expired — so setting it
to the epoch is the standard reclaim nudge.

### 5.2 Outbox

Outbox rows are locked via `outbox_events.locked_until` and
`outbox_events.locked_by`.

Inspect before unlocking (lock owner is the dispatcher's per-instance
id from `dispatcher.id`):

```sql
SELECT event_id, event_type, attempt_count, locked_by,
       locked_until, last_error
  FROM outbox_events
 WHERE status = 'PROCESSING'
   AND event_id = '<EVENT_ID>';
```

Force re-claim — set the lock expiry into the past and clear the
owner so the dispatcher picks it up fresh:

```sql
UPDATE outbox_events
   SET locked_until = '1970-01-01T00:00:00Z',
       locked_by = ''
 WHERE status = 'PROCESSING'
   AND event_id = '<EVENT_ID>';
```

> **Caveat (read §3.4 first):** if `last_error` shows a handler
> panic, do NOT clear the lock until the panic is fixed — clearing
> the lock just creates a recurring panic loop.

If the row has already hit `cfg.MaxAttempts` (default 5), the
dispatcher will refuse to retry and mark `FAILED` on the next
`MarkFailed` path — so in that case force-terminate instead:

```sql
UPDATE outbox_events
   SET status = 'FAILED',
       last_error = 'max attempts reached (operator unblock RW-PROD-018 §5.2)'
 WHERE status = 'PROCESSING'
   AND attempt_count >= 5
   AND event_id = '<EVENT_ID>';
```

### 5.3 Forwarding

Forwarding locks live on `creator_forwardings.lease_expires_at`.

Force re-claim:

```sql
UPDATE creator_forwardings
   SET lease_expires_at = '1970-01-01T00:00:00'
 WHERE forwarding_id = '<FORWARDING_ID>'
   AND status IN ('PENDING', 'RETRY_WAIT', 'POLLING');
```

Force-terminate when the remote creator is confirmed unreachable:

```sql
UPDATE creator_forwardings
   SET status = 'FAILED',
       error_message = 'operator unblock (RW-PROD-018 §5.3)"
 WHERE forwarding_id = '<FORWARDING_ID>';
```

The runner's `ClaimCreatorForwardings` re-claims rows whose
`lease_expires_at < now()`. If the row is already past
`MaxAttempts` (default 12), the runner will mark `FAILED` anyway on
the next retry path — prefer a re-claim nudge first.

---

## 6. Post-recovery verification

After the unblock or the restart:

1. The next tick of the corresponding runner should call
   `tracker.Reset()` on a successful tick — you should see the
   log line associated with a normal cycle (claim → dispatch →
   persist) and NO new `consecutive=N` matches.
2. `/ready` should report all four runners in `Running`:

   ```bash
   curl -s localhost:8080/ready \
     | jq -r '.runners[] | "\(.name): \(.state)"'
   ```

3. `velox_master_outbox_pending` should start decreasing
   (assuming the outbox was the trigger); if it's still growing,
   jump back to §5.2.

4. Spot-check the row you unblocked went to the expected next
   state (also via SQL; e.g.
   `SELECT status FROM delivery_attempts WHERE delivery_id = '…'`).

If symptoms recur twice in 15 minutes, escalate to engineering — the
tracker is doing its job warning you about something the lease layer
can't recover on its own.

---

## 7. Operator anti-patterns (DO NOT)

1. **Mass-mark `FAILED`.** Do not `UPDATE … SET status='FAILED'`
   across an entire table to silence an alert. You destroy retry
   budgets and the audit trail. Only mutate individual,
   investigated rows.

2. **Clear locks without checking the panic log.** §3.4
   (`PANIC in handler`) is the first thing to run before any
   outbox-lock reset. If the handler panicked on a poisoned
   payload, clearing the lock creates an immediate re-panic —
   and the new panic increments the consecutive counter toward
   `ClassCritical` master restart.

3. **`DELETE` from runner tables.** The FK constraints will
   usually block it; if they don't, the audit trail is gone. Use
   a `FAILED` transition instead.

4. **Restart in lieu of reading.** `systemctl restart
   velox-master` does NOT clear active leases — durable locks
   (`lease_expires_at` / `locked_until`) survive restart and bind
   the new process to the same rows. Always read the streak
   first (§3.1, §3.2) before considering a restart.

5. **Reset the FailureTracker manually as the "fix".** The
   tracker reflects reality. Resetting it (`tracker.Reset()` via
   an admin hook, if one is wired) without first fixing the
   underlying cause just hides the next escalation.

---

## Appendix A — Source-of-truth references

- `DataServer/internal/supervisor/policy.go` — sentinels
  (`ErrInfrastructure`, `ErrElementScoped`, `ErrLeaseLost`,
  `ErrPanicked`), `DefaultRetryPolicy` (5/30s), `ClassifyError`,
  `FailureTracker.Record` wrap shape.
- `DataServer/internal/supervisor/clock.go` — `Clock` seam for
  testability; production uses `RealClock`, tests use
  `MockClock`.
- `DataServer/internal/deliveries/runner.go` — `Run`, `tick`,
  `processLease`; per-delivery retry budget via
  `lease.MaxAttempts` (job-level override).
- `DataServer/internal/outbox/dispatcher.go` — `Run`, `Poll`,
  `dispatchEvent` (handler panic recovery → mark `FAILED`).
- `DataServer/internal/forwarding/runner.go` — `Run`, `tick`,
  `processLease`, `handleRetry`, `handleEnqueueRetry`,
  `renewLeaseLoop` (lease-loss cancellation).
- `DataServer/internal/metrics/supervisor.go` — `velox_master_*
  gauges refreshed per tick from the supervisor's
  `refreshMasterHealth`.

## Appendix B — Glossary

- **Casella** — a runner's claim on a single row (lease / lock /
  PROCESSING slot). One casella = one row in
  PROCESSING/RUNNING/PENDING/RETRY_WAIT for one runner instance.
- **Streak** — the FailureTracker's `consecutive` count. Resets
  on a successful tick (any `tracker.Record(nil)` path OR a tick
  whose classified error is element/lease-lost).
- **ResetWindow** — default 30s. After 30s of silence since the
  first inflight error, the streak restarts at 1 (single blip
  doesn't poison the counter long-term).
