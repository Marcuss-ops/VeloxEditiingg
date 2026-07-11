# Cap. 10 — 24h–72h Soak with Chaos Engineering (Phase 9)

## Scope

A continuous-job soak with chaos injection (random worker restart,
short network interruption, controlled master restart, worker rotation,
mixed small/large Jobs) that exercises every moving part of the
production stack — image-pin → cosign verify → mTLS handshake → WAL
replay → TaskLeaseReaper → upload finalization — under the same kind of
fault a real deployment sees.

The deliverable is dual-track:

- **Operator runbook** (`scripts/cert/cap-10-soak.sh`) — runs the **real**
  24h–72h soak on a VPS worker + master pair with cosign-pinned images,
  real mTLS fingerprint allowlist, real SIGKILL / iptables / SIGTERM chaos.
- **CI simulator** (`tests/e2e/cap-10-soak/simulator.sh`) — hermetic
  shell-driven simulator that compresses the 24h–72h timeline into
  288–864 ticks (1 tick = 5 simulated minutes, matching `velox-worker-watchdog.timer`
  cadence + `TaskLeaseReaper` 30 s bin) and asserts all 10 acceptance
  invariants against the FSM state.

Both tracks emit a verdict.json conforming to **`velox.cert-10-soak.v1`**.

## Time compression math

| Soak length | Simulator ticks | Real wall-clock for CI |
|-------------|------------------|------------------------|
| 24h         | 288 (1 tick/5min) | ~3 min     |
| 48h         | 576              | ~6 min     |
| 72h         | 864              | ~9 min     |

A 5-minute tick granularity is the smallest interval that captures
both the watchdog (every 5 min) and the reaper (30 s default, rendered
at 1 bin per tick) without collapsing race conditions. Smaller
granularity would inflate CI runner cost; larger would mask reconnect
windows shorter than the tick.

## Chaos schedule (deterministic)

| Tick (= 5 sim-min) | Sim hour | Event                       | Operator action |
|--------------------|----------|-----------------------------|----------------|
| 60                 | T+5      | worker_1 SIGKILL            | `kill -9 $WORKER_PID` |
| 84                 | T+7      | network block 30 s          | `iptables -I INPUT -p tcp --dport 50051 -j DROP` |
| 120                | T+10     | master SIGTERM              | `systemctl kill -s SIGTERM velox-server.service` |
| 156                | T+13     | worker_2 SIGKILL            | `kill -9 $WORKER_PID` |
| 204                | T+17     | worker rotation (cert)      | `bash scripts/cert/cap-7-reboot-recovery.sh --rotate-only` |
| 228                | T+19     | network block 60 s          | iptables |
| 264                | T+22     | master SIGTERM              | systemctl |
| 62 / 158 / 206     | +2 ticks | reconnect slots             | master-side `LogCertAccepted` |
| 4 randomized slots | every ~1h | extra chaos from PRNG 7-10  | deterministic via `RM_CHAOS_SEED=42` |

The chaos schedule is **seeded deterministic** (`RM_CHAOS_SEED=42`); a CI
rerun with the same seed produces identical injection order, so the 10
invariants are reproducible across hosts.

## 13 acceptance thresholds (NR-26 … NR-38)

Each invariant is independently reported. The simulator prints them as
boolean keys in `verdict.json → invariants`; a failed key lists in
`failed_invariants`. The 13 thresholds split across two dimensions:

- **NR-26 … NR-35** — stability / chaos-soak (10 invariants). The original
  cap-10 deliverable covering fault injection endurance, artifact
  integrity, lease reaper, RSS boundedness, staging GC, and outcome
  coherence.
- **NR-36 … NR-38** — scale / RPC profile (3 invariants). Phase-9
  extension to cover the operator's "scheduler fairness under sustained
  queue pressure" + "RPC reconnect / rotation bounded" acceptance gates
  not previously exercised by the chaos simulator alone.

### NR-26: 0 Jobs lost

Every Job reaches a terminal state (`SUCCEEDED`, `FAILED`, `CANCELED`)
by the end of the soak. A Job whose `terminal_at` is null at soak end
qualifies as "lost" and the invariant fails.

Verified against the canonical SQLite `jobs.status` query:
`SELECT count(*) FROM jobs WHERE status NOT IN ('SUCCEEDED','FAILED','CANCELED')`.

### NR-27: 0 duplicate active Tasks

No two `task_attempts` rows are `RUNNING` or `LEASED` for the same
`task_id` simultaneously. Multiple active attempts slip when a
re-issuance race opens a window before the watchdog marks the prior
attempt `CRASHED`. Verified via `GROUP BY task_id, attempt_number
HAVING count > 1` query against `task_attempts`.

### NR-28: 0 duplicate artifacts

Every `artifacts.sha256` is unique. A sha256 collision under the
artifact store fingerprint means two distinct Jobs produced byte-
identical output — a strong indicator of an off-by-one in artifact
keying. Verified via `GROUP BY sha256 HAVING count > 1`.

### NR-29: 0 corrupt artifacts

Every finalized artifact's `computed_crc` matches its `expected_crc`.
Finalization writes both columns; a mismatch flags either a
truncated upload or a swap of `expected_crc` post-write.

### NR-30: 0 unauthorized connections

`SELECT count(*) FROM connection_attempts WHERE allowed = 0` is
bounded by `AUTH_REJECT_THRESHOLD` (default 20). Rejected connections
come from:
- Network block chaos (dropped handshake attempts)
- Cert rotation window (old cert fingerprint rejected after renewal)
- Out-of-allowlist fingerprint requests from non-canonical paths

The threshold is a ceiling not a floor: a production soak rejecting
> 20 connections in 24h indicates an active scenario triggering
rejections, not steady state.

Canonical surface: `RemoteCodex/native/worker-agent-go/pkg/logger/logger.go`
`LogCertRejected` writes one row per rejection; the operator runbook
streams this audit log into `events/connection_attempts.jsonl`.

### NR-31: 0 stuck workers after reconnect

Every worker whose `workers.death_tick` was set must have a
`reconnect_tick` within `WATCHDOG_GRACE_TICKS × 5` simulated minutes.
The 5-minute grace matches the `velox-worker-watchdog.timer` cadence
(`DataServer/data/ansible/playbooks/tasks/systemd_setup.yml` lines
431–448). A worker whose death_tick has no reconnect_tick within
grace is "stuck after reconnect" and the invariant fails.

### NR-32: 0 Jobs RUNNING beyond TTL + reaper

Every task whose `status='RUNNING'` at soak end must have
`lease_expires_at < total_ticks - REAPER_GRACE_TICKS` (i.e. be in
its final tick window, not a stuck-RUNNING leak). TaskLeaseReaper
(`DataServer/internal/taskgraph/reaper.go`) sweeps every 30 s in
production; the simulator collapses to 1 tick per 5 min and the
reaper equivalent transitions `RUNNING` past TTL to
`LEASE_EXPIRED` within 1 tick.

### NR-33: 0 linear RAM growth

RSS samples taken once per simulated tick are bounded by linear-
regression slope `|bytes_per_tick| ≤ rss_baseline / 6000` (~50 KB/tick,
matching cap. 9 large profile accepted slope). Linear growth beyond
this band indicates either a leak in the simulator DB layer or
production-time memory growth in the protobuf buffer pool.

### NR-34: 0 uncontrolled staging-cache growth

Active staging files (rows in `staging_files` with `evicted_at_tick IS
NULL`) are bounded by:

```
STAGING_TOLERANCE_BYTES = MAX_ACTIVE_JOBS × AVG_JOB_STAGING_BYTES × 2
```

Defaults: `MAX_ACTIVE_JOBS=15`, `AVG_JOB_STAGING_BYTES=50_000_000`
(50 MB). Tolerance = 1.5 GB. The 2× buffer accommodates in-flight
files during burst peaks; `staging_files.evicted_at_tick` updates
after each tick's GC for any finalized Job.

### NR-35: 100% coherent outcomes

Every Job whose terminal state is `SUCCEEDED` or `FAILED` must have
`status == expected_terminal`. A Job pre-seeded with
`expected_terminal=SUCCEEDED` that ends `FAILED` is counted as
incoherent.

**Seed rates (`simulator.sh`):**
- 70% small / 30% large.
- Within each class, default `expected_terminal=SUCCEEDED`; ~15% of large
  are seeded with `expected_terminal=FAILED` (after the modulo-47 chaos
  gate, ≈1/47 of the large-FAILED seeds produce a tick-injected FAILED
  transition; the rest terminalize at the cleanup pass).
- Net effect: ≈4.5% of all 576 seeded Jobs have `expected_terminal=FAILED`,
  ≈95.5% have `expected_terminal=SUCCEEDED`. NR-35 asserts every Job
  lands in the same terminal state it was seeded for.

**Implementation note:** The per-tick SUCCEEDED UPDATE in `simulator.sh`
MUST filter on `expected_terminal='SUCCEEDED'` (added in Phase-9
closure fix). Without this filter, large-FAILED jobs are transitioned
to SUCCEEDED before the FAILED UPDATE can take them, leaving
`n35_incoherent_outcomes = N`. The fix enforces two-arm mutual
exclusion between the SUCCEEDED and FAILED transitions.

### NR-36: RPC reconnect / rotation latency p50/p95/p99 bounded

For every chaos event resulting in `RECONNECTED` or `ROTATED`, take
`latency = resolved_at_tick - injected_at_tick` (in sim-ticks). Compute
`p50`, `p95`, `p99` percentiles of the latency vector.

**Threshold:** `p99 ≤ 10 ticks` (50 simulated minutes for 1-tick/5-min
compression). Trivially PASS when fewer than 3 chaos events landed
(degenerate sample — no claim is made). Canonical RPC surface:
`RemoteCodex/native/worker-agent-go/internal/transport/grpc_stream.go`
exposes `ConnectLatencyMs` histograms that mirror this latency vector.

### NR-37: Jain-index fairness across workers bounded

Jain's index `J = (sum(x))^2 / (n * sum(x^2))` over the vector
`x_i = SUCCEEDED count of worker i`. Range `[1/n, 1]`; `1.0` is perfect
fairness, `0.85` is healthy.

**Threshold:** `J ≥ 0.85` when ≥2 workers have ≥1 SUCCEEDED Job.
Returns `1.0` (perfect) trivially for n<2 (single-worker configurations
have nothing to compare against). Maps to Track 4 §10 "Scheduler
fairness under sustained queue pressure."

### NR-38: Cross-worker load-balance ratio bounded

`ratio = max(succeeded) / min(succeeded for active workers)` over the
same SUCCEEDED-per-worker vector.

**Threshold:** `ratio ≤ 2.5` when ≥2 workers have ≥1 SUCCEEDED Job.
Returns `1.0` trivially for n<2. The 2.5× ceiling ensures no single
straggler or hot worker dominates the dispatcher; mirrors the
`max_active_jobs_seen=15` durability budget while permitting one
worker to absorb up to 2.5× the load of the lightest.

## Verdict schema (`velox.cert-10-soak.v1`)

```json
{
  "schema":                "velox.cert-10-soak.v1",
  "worker_id":             "host-cap10-simulator",
  "cert_date":             "2026-10-14",
  "soak_duration_hours":    72,
  "final_status":          "PASS",
  "failed_invariants":      [],
  "invariants": {
    "NR-26-0-jobs-lost":                       true,
    "NR-27-0-duplicate-active-tasks":           true,
    "NR-28-0-duplicate-artifacts":              true,
    "NR-29-0-corrupt-artifacts":                true,
    "NR-30-0-unauthorized-connections":         true,
    "NR-31-0-stuck-workers":                    true,
    "NR-32-0-jobs-running-beyond-reaper":       true,
    "NR-33-0-linear-ram-growth":                true,
    "NR-34-0-uncontrolled-staging-growth":     true,
    "NR-35-100-percent-coherent-outcomes":      true,
    "NR-36-rpc-reconnect-latency-p99-bounded":  true,
    "NR-37-jain-fairness-index-bounded":        true,
    "NR-38-cross-worker-load-balance-bounded":  true
  },
  "evidence": {
    "chaos_events_injected":              11,
    "chaos_events_recovered":               3,
    "n26_jobs_lost":                        0,
    "n27_duplicate_active_tasks":           0,
    "n28_duplicate_artifact_hashes":        0,
    "n29_corrupt_artifacts":                0,
    "n30_unauthorized_connections":         7,
    "n31_stuck_workers":                    0,
    "n32_jobs_running_beyond_reaper":       0,
    "n33_rss_slope_bytes_per_tick":    -1234,
    "n33_rss_slope_max_bytes_per_tick": 53333,
    "n33_rss_baseline_bytes":       335544320,
    "n34_active_staging_bytes":          230400,
    "n34_staging_tolerance_bytes":  1500000000,
    "n35_incoherent_outcomes":              0,
    "n36_rpc_reconnect_events":             5,
    "n36_rpc_p50_ticks":                    4,
    "n36_rpc_p95_ticks":                    7,
    "n36_rpc_p99_ticks":                    8,
    "n36_rpc_p99_max_ticks":               10,
    "n37_active_workers":                   5,
    "n37_per_worker_succeeded":     {"worker-1": 92, "worker-2": 95, "worker-3": 91, "worker-4": 90, "worker-5": 89},
    "n37_jain_index":                    0.9994,
    "n37_jain_index_min":                0.85,
    "n38_load_balance_ratio":          1.0674,
    "n38_load_balance_busiest":           95,
    "n38_load_balance_least_busy":        89,
    "n38_load_balance_ratio_max":       2.5
  },
  "thresholds": {
    "auth_reject_threshold":            20,
    "rss_slope_max_bytes_per_tick":  53333,
    "staging_tolerance_bytes":    1500000000,
    "watchdog_grace_ticks":               2,
    "reaper_grace_ticks":                 2,
    "lease_ttl_ticks":                    6,
    "rpc_latency_p99_max_ticks":         10,
    "jain_index_min":                  0.85,
    "load_balance_ratio_max":           2.5
  },
  "evidence_root":            "/var/lib/velox/cap10-evidence/20261014-020000",
  "generated_at":             "2026-10-14T02:00:00Z"
}
```

## Per-cell evidence layout

```
$SCEN_DIR/cap10-evidence/<run>/
├── pre/
│   ├── digest.txt                    # cosign-pinned worker image digest
│   ├── cosign.json                   # raw cosign verify stdout
│   ├── cosign-envelope.sha256        # envelope sha256 (audit trail)
│   ├── fingerprint_allowlist.txt     # worker cert → sha256 fingerprint map
│   ├── rss_start_kb.txt              # master RSS before soak
│   └── staging_start_bytes.txt
├── events/
│   ├── chaos.jsonl                   # one row per chaos event
│   ├── connection_attempts.jsonl     # one row per mTLS handshake outcome
│   ├── jobs.jsonl                    # one row per job-submit attempt
│   └── job-N.json / job-N.err        # per-job submit response
├── post/
│   ├── rss_end_kb.txt                # master RSS after soak
│   ├── staging_end_bytes.txt
│   └── verdict.json                  # velox.cert-10-soak.v1
├── tick_log.csv                      # tick, rss_bytes, fd, threads, staging
├── evidence.jsonl                    # run-time invariant snapshots
├── cap10.sqlite                      # FSM DB (full schema)
└── staging/                          # staging cache proxy directory
```

## Makefile targets

- `make cap-10-soak-dry`     — bash -n sweep + python preflight only.
- `make cap-10-soak`         — default 24h (288-tick) CI simulator run.
- `make cap-10-soak-48`      — 48h (576-tick) CI simulator run.
- `make cap-10-soak-72`      — 72h (864-tick) CI simulator run.
- `make cap-10-soak-operator`— REAL 24h VPS operator runbook (requires
  cosign + master cert + systemd). NOT safe to run from CI on a hosted
  runner — operator mode only.

The operator runbook is intentionally *not* invoked by `make` defaults:
production pools need a clean coordinator and explicit confirmation
that the cosign-pinned image is the desired candidate. Operators run
`make cap-10-soak-operator` only when the issue / PR has been signed
off for soak.
