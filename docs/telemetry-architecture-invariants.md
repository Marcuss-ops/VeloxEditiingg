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

- `shared/telemetry/schema/catalog.json` is the only language-neutral event catalog.
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
- Pipeline and native boundary clocks are raw observations. The authoritative
  attempt timing projection is the event/span data in `AttemptSnapshot`;
  compatibility clocks may be carried as input facts, but they must not become
  a second receipt, Prometheus, or derived-metric timer.

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

`PerformanceReceiptAssembler` is the single receipt construction boundary:
`pkg/performance/receipt_builder.go` is fed by the `RunMetrics` and
`AttemptSnapshot` adapters and invokes the single `DerivedMetricsCalculator`.
Prometheus, TaskResult, heartbeat, benchmark, and Master metric registries are
projections. Asset cache verification, invalid-entry eviction, completed
download facts, and invalid-event counts are recorded once and projected by
their sinks; the gate rejects their legacy producer-side writes. Worker-
lifetime operational calls (lease lifecycle, download-manager gauges,
prefetch counters, and result transport timing) remain an explicit
compatibility surface outside attempt snapshots and are not silently mixed
into attempt facts.

### 5. Compatibility is one-way

- Legacy `map[string]interface{}` metrics are produced only through the
  executor `legacyMetricsProjection` adapter and are never authoritative.
- Typed raw facts flow into `AttemptSnapshot`; projections may expose a legacy
  map, but the normal path never reconstructs typed facts from that map.
- Every registered sink receives an isolated snapshot view and cannot mutate
  the canonical snapshot or influence rendering.
- Invalid event observations are counted by `EventRecorder` and projected by
  Prometheus; the recorder does not write to a sink directly.
- `CollectorRegistry` and `SinkRegistry` are the only composition points for
  attempt collectors and projections. New paths must follow
  `schema -> owner -> recorder -> snapshot -> projection -> sink`.

## Gate and migration policy

Run locally with:

```bash
bash scripts/ci/check-telemetry-architecture.sh
```

The check is intentionally structural and fail-closed. Compatibility adapters
are named boundaries, not additional authorities. A new exception requires a
code-review decision about the fact owner and projection boundary. Tests prove
behavior; this gate prevents the architecture from regressing as legacy
writers are removed.
