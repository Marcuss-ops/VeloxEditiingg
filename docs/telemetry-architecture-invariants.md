# Telemetry architecture invariants

This document is the architectural contract for telemetry changes. The
executable gate is `scripts/ci/check-telemetry-architecture.sh`; it is called
by the canonical `scripts/ci/verify.sh` entry point.

## Dependency direction

```text
canonical catalog/schema
        ↓
raw fact producer / EventRecorder / AttemptTelemetrySession
        ↓
immutable AttemptSnapshot
        ↓
projections (receipt, Prometheus, heartbeat, TaskResult, SQL)
        ↓
sinks and dashboards
```

There is no return edge from a projection or sink to a producer. A renderer
must not know whether its facts will be exported to Prometheus, written into a
receipt, persisted in SQL, or shown in Grafana.

## Invariants

### 1. One event catalog

- `shared/telemetry/catalog.json` is the only language-neutral event catalog.
- Go loads and validates it through `catalog_source.go`.
- C++ consumes the generated `catalog_generated.hpp`; it must not maintain a
  hand-written descriptor array or a second origin/scope/event list.
- Worker and Master registries are projections/lookups, never new taxonomy
  sources.
- A new event is added by changing the JSON source and regenerating bindings.

The gate rejects additional catalog source files and legacy parallel registry
names. The existing Go/C++ bindings and their parity tests remain the runtime
proof that both languages consume the same source.

### 2. One owner for derived formulas

Raw producers emit observations only: bytes, frames, durations, counters,
resource samples, and statuses. Ratios and benchmark KPIs are derived in the
canonical performance/Master projection boundary, not in a renderer,
transport adapter, dashboard, or SQL writer.

The current repository still has compatibility projections in the receipt,
Master collectors, and historical rollups. Those paths are explicitly
allowlisted in the gate and documented in `docs/telemetry-authority-map.md`.
The allowlist is a ratchet: a new production file containing canonical KPI
formula names fails CI until it is deliberately routed through an approved
projection owner.

In particular, `accounted_ratio` may use only catalog events with
`kind=duration`, `timing_mode=exclusive`, and `cardinality=per_attempt`.
Child/parent spans and parallel per-segment/per-track timings must never be
summed into the wall-clock denominator.

### 3. No concurrent authoritative timers

- `EventRecorder` is the append-only worker attempt journal.
- Native `PhaseRecorder` is the C++ transport producer for engine facts.
- `PhaseTimer` is a legacy compatibility accumulator with exactly one
  production definition; it is not safe for concurrent mutation and must not
  acquire new production callers.
- `AttemptTelemetrySession` owns host/cgroup/process/I/O resource sampling.
- Pipeline and native boundary clocks are compatibility inputs until they are
  projected from the journal; they are not permission to add another phase
  timer.

The gate rejects new `PhaseTimer` definitions/callers and new direct writes to
`PhaseMS`/`DetailedPhases` outside the parser/assembler boundary. Every new
timed fact must identify its catalog timing mode and owner before it is added.

### 4. No direct sink writes from render producers

Render producers may record raw facts or return transport metrics. They may
not reference:

- `PrometheusMetrics`, `GetPrometheusMetrics`, `promauto`, or registration
  APIs;
- `PerformanceReceiptV1`, `NewPerformanceReceiptV1`, or `DerivedMetrics`;
- benchmark/SQL/dashboard sinks.

`PerformanceReceiptAssembler` is the single receipt construction boundary.
Prometheus and Master metric registries are projections. Existing worker
lifecycle operational calls are a compatibility surface outside renderer
code; their eventual migration is tracked separately rather than hidden by
this gate.

## Gate and migration policy

Run locally with:

```bash
bash scripts/ci/check-telemetry-architecture.sh
```

The check is intentionally structural and fail-closed. Allowlist entries are
not endorsements of duplicated semantics; they are named migration debt. A
new entry requires a code-review decision about the fact owner and projection
boundary. Tests should prove behavior; this gate prevents the architecture
from regressing while the remaining legacy projections converge.
