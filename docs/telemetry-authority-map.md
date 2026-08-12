# Velox Telemetry Authority Map

> **Status:** repository map, not a new runtime contract
> **Mapped:** 2026-08-12
> **Scope:** worker-agent Go, native video-engine C++, Master ingestion, SQL read models, Prometheus, and performance receipts

This document records where telemetry facts are currently produced, recorded,
projected, persisted, and exported. It distinguishes an authoritative fact
from a projection or compatibility representation. A path appearing in the
**projection/sink** columns is not an additional source of truth unless the
row explicitly says so.

## 1. Current verdict

Velox already has a strong event-taxonomy SSOT, but not yet one universal
telemetry SSOT for every metric:

- `shared/telemetry/catalog.json` is the canonical **language-neutral event
  taxonomy** for the shared Go/C++ wire contract. Go loads it through
  `catalog_source.go`; the C++ header is generated from the same source.
- `RemoteCodex/native/worker-agent-go/internal/telemetry/phase_recorder.go`
  is the worker's append-only per-attempt event journal.
- The native engine has a parallel recorder implementation in
  `RemoteCodex/native/video-engine-cpp/include/velox/telemetry/phase_recorder.hpp`
  and `src/telemetry/phase_recorder.cpp`. It emits the same sidecar event
  shape, but currently repeats origin/scope constants in C++.
- `AttemptTelemetrySession` is the authoritative worker collector for host,
  cgroup, process-tree, and I/O resource observations. Its
  `ApplyToMap` method is a compatibility adapter into the legacy report map.
- `PerformanceReceiptAssembler` is the only current receipt constructor, but
  it consumes `pipeline.RunMetrics` and sidecar data rather than an
  `AttemptSnapshot` built directly from the EventRecorder.
- The Master has a separate canonical **Prometheus metric-name catalog** in
  `DataServer/internal/metrics/catalog.go`. This is a catalog of exported
  metric families, not the shared event taxonomy.
- The authoritative Master terminal boundary is
  `IngestTaskResultAtomic`; it persists the typed report, event timeline,
  summaries, and read-model rows in one transaction.

Therefore the present shape is best described as:

```text
shared event taxonomy
        |
        +--> worker EventRecorder --------------------+
        |                                               |
        +--> C++ PhaseRecorder -> sidecar -> Go map ----+--> TaskResult
        |                                               |
        +--> AttemptTelemetrySession -> typed/map ------+
                                                        |
                                                        v
                                             Master atomic ingest
                                                        |
                     +------------------+---------------+------------------+
                     v                  v                                  v
             append-only SQL      typed SQL read models              raw report
                     |                  |                                  |
                     +---------> Prometheus projection              audit/replay
                     |
                     +---------> performance queries / dashboards

worker-local PrometheusMetrics and receipt assembly currently also consume
some of the same execution lifecycle facts outside this single journal.
```

## 2. End-to-end execution path

| Boundary | Current owner | Evidence in code | Role |
| --- | --- | --- | --- |
| Task execution lifecycle | `TaskRunner` / worker execution boundary | `RemoteCodex/native/worker-agent-go/internal/worker/task_execution.go` | Starts/stops the attempt resource session, runs the executor, uploads outputs, drains late recorder events, and submits the result. |
| Canonical worker event journal | `EventRecorder` | `RemoteCodex/native/worker-agent-go/internal/telemetry/phase_recorder.go` | Thread-safe append-only `RecordedPhase` sequence for one attempt; validates/canonicalizes event taxonomy before recording. |
| Native engine event journal | `PhaseRecorder` | `RemoteCodex/native/video-engine-cpp/include/velox/telemetry/phase_recorder.hpp`, `src/telemetry/phase_recorder.cpp` | Records engine-side events and serializes the `phases[]` sidecar. It is a transport-side producer, not a Master sink. |
| Sidecar-to-worker bridge | Native binary resolver | `RemoteCodex/native/worker-agent-go/pkg/video/services/native/binary_resolver.go` | Maps sidecar phases and segments into `pipeline.RenderMetrics`. |
| Worker report assembly | `TaskRunner` report finalization | `RemoteCodex/native/worker-agent-go/internal/taskrunner/runner_report.go` | Merges executor detailed phases with worker lifecycle events and preserves ordering. |
| Resource collection | `AttemptTelemetrySession` | `RemoteCodex/native/worker-agent-go/internal/telemetry/attempt_session.go` | Samples host/cgroup/process/I/O resources over the attempt; produces typed `AttemptTelemetry`. |
| Wire report construction | `submitTaskResult` | `RemoteCodex/native/worker-agent-go/internal/worker/task_result_builder.go` | The single builder for `pb.TaskResult`, including typed metrics, phase timings, segment timings, artifacts, report hash, and schema versions. |
| Master wire boundary | `handleTaskResult` | `DataServer/internal/grpcserver/handler_result.go` | Validates identity, translates protobuf payloads, and delegates to the ingestion service. |
| Master terminal transaction | `IngestTaskResultAtomic` | `DataServer/internal/ingest/service.go`, `DataServer/internal/store/sqlite_task_atomic_ingest.go` | Single legal terminal persistence boundary for task/attempt state plus metrics, cache, cost, artifacts, phases, events, and raw report. |
| Prometheus refresh | Metrics supervisor and heartbeat handlers | `DataServer/internal/metrics/supervisor_tick.go`, `DataServer/internal/grpcserver/handler_workers.go` | Projects persisted attempt rows, detailed phase rows, segments, parallelism, and heartbeat resources to Master metric families. |

## 3. Event taxonomy and recorder map

### 3.1 Taxonomy SSOT

| Surface | Current status | Notes |
| --- | --- | --- |
| `shared/telemetry/catalog.json` | **Authoritative for shared event descriptors** | The language-neutral source owns component, action, origin, scope, phase, event type, unit, kind, timing mode, aggregation, cardinality, and owner. |
| `RemoteCodex/native/worker-agent-go/internal/telemetry/phase_registry.go` | Derived worker view | `LookupPhaseSpec`, `RegisteredPhaseSpecs`, and `CanonicalizeEventSpec` read from `velox-shared/telemetry`; it does not declare a second component/action list. |
| `shared/telemetry/catalog_source.go` | Go loader/validator | Embeds and validates `catalog.json`, then projects it into the existing Go catalog API. |
| `RemoteCodex/native/video-engine-cpp/include/velox/telemetry/catalog_generated.hpp` | Generated C++ binding | Produced by `telemetry/cmd/cataloggen`; C++ consumers do not maintain a second event or vocabulary list. |
| Master event validation | Consumer of shared catalog | `DataServer/internal/store/execution_event_persistence.go` calls `sharedtelemetry.Catalog.Validate` before inserting detailed events. |
| SQL event constraints | Storage guard | `DataServer/internal/store/migrations/sqlite/110_task_execution_events.sql` and subsequent migrations constrain event identity, append-only behavior, and replay/replacement semantics. |

### 3.2 Worker recorder chain

```text
producer code
    -> EventSpec
    -> EventRecorder.Start/Begin/Emit/Record
    -> RecordedPhase[]
    -> TaskRunner.attachDetailedPhases / AppendDetailedPhases
    -> TaskExecutionReport.DetailedPhases[]
    -> submitTaskResult
    -> pb.TaskResult.phase_timings
```

Relevant files:

- `internal/telemetry/phase_recorder.go`: journal, event handles,
  per-origin indexes, snapshot/drain, and catalog validation.
- `internal/telemetry/attempt_event_machine.go`: lifecycle-event helper that
  writes through the same recorder.
- `internal/telemetry/canonical_attempt_events.go`: read-only lifecycle view
  for heartbeat/compact progress; deterministic IDs are derived from the
  recorder snapshot.
- `internal/taskrunner/runner_report.go`: appends recorder events to the
  report and drains post-run upload/commit events without replacing the
  earlier sequence.
- `internal/worker/task_result_builder.go`: converts detailed phases to the
  protobuf wire representation and stamps worker execution identity; the
  Master overwrites identity fields from Master-owned rows at ingest.

### 3.3 Native engine chain

```text
C++ engine operation
    -> PhaseRecorder / ScopedPhase
    -> progress sidecar phases[]
    -> binary_resolver.mapEngineSidecar
    -> pipeline.RenderMetrics.DetailedPhases
    -> executor/taskrunner report
    -> TaskResult
```

The C++ recorder is thread-safe and monotonic-clock based. It also carries
segment, track, byte, frame, CPU, queue-wait, offset, and metadata fields.
Its sidecar is a transport artifact: the native engine remains the
authoritative producer of the observed engine facts; worker mapping and Master
persistence establish the durable accepted projection through the atomic
ingest path.

## 4. Fact-owner matrix

The following table identifies the current producer that should be treated as
authoritative for each fact. A later SQL, receipt, heartbeat, or Prometheus
value is a projection unless noted otherwise.

| Fact | Authoritative producer/current owner | Main path | Current projections or adapters |
| --- | --- | --- | --- |
| Event component/action/origin/scope/phase/type | Shared catalog plus the event producer validated against it | `shared/telemetry/catalog.go` -> `EventRecorder` | Worker phase registry, C++ sidecar fields, SQL validation |
| Attempt event ordering and event index | Worker `EventRecorder` for worker events; Master sequence for Master events | `phase_recorder.go`, `master_execution_event.go` | TaskResult, heartbeat lifecycle view, `task_execution_events` |
| Engine packet/frame/mux observations | Native media engine | C++ `PhaseRecorder` and engine sidecar | Go `DetailedPhaseTiming`, TaskResult, SQL event rows, receipt, Prometheus histogram |
| Timeline/workload shape | Compiled render plan / task payload | `shared/contract` render-plan types and Master enqueue path | `WorkloadProfile`, attempt metrics input-context columns, dashboards |
| Pipeline phase clocks | Worker pipeline/executor | `pkg/video/pipeline/runner.go` and executor reports | `PerformanceReceipt`, typed execution metrics, Master phase metrics |
| Engine process spawn and child process counts | Worker native process client/process sampler | `pkg/video/services/native/engine_process.go`, `process_counter.go`, `render_client.go` | `pipeline.RenderMetrics`, `PerformanceReceipt.Process`, typed TaskResult metrics |
| Asset download bytes and cache operations | Worker asset/cache resolver path | worker asset/cache telemetry packages | Attempt typed metrics, cache stats, worker Prometheus, Master cache projections |
| CPU, RSS, disk, network, cgroup resource observations | `AttemptTelemetrySession` and its sampler | `internal/telemetry/attempt_session.go` | Typed execution metrics, legacy dotted map, `task_attempt_metrics`, heartbeat resource snapshots where applicable |
| Task/attempt terminal status | TaskRunner reports the outcome; Master owns accepted terminal state | worker `executeTask` -> `IngestTaskResultAtomic` | TaskResult, `task_attempts`, job roll-up, compute outcome metrics |
| Artifact bytes and final SHA | Master verified artifact/finalization path | artifact upload/finalization store code | Attempt render identity, output artifact rows, receipt/output fields, dashboards |
| Raw worker report | Worker `submitTaskResult` serialization | `task_result_builder.go` | `task_attempt_reports.raw_report_json`, replay/audit tooling |
| Performance receipt | `PerformanceReceiptAssembler` only | `pkg/performance/assembler.go` | `PerformanceReceiptV1` JSON / benchmark artifacts |
| Derived ratios and benchmark KPIs | Current sources are distributed | Receipt fields, `taskattempts` methods, Master collector, SQL rollups | `PerformanceReceiptV1.Derived`, Prometheus gauges, `render_performance_daily`, Grafana |
| Master worker-resource gauges | Master `WorkerResourceSink` / `Collector.RecordWorker` | `handler_workers_metrics.go` -> `collector_workers.go` | Master Prometheus registry, worker registry extras |
| Master attempt/engine metrics | Master metrics supervisor | `collector_attempts.go`, `collector_engine.go`, `supervisor_tick.go` | Master Prometheus registry and daily SQL rollups |

## 5. Projection and sink inventory

### 5.1 Worker-side projections and sinks

| Projection/sink | Input | Output | Authority status |
| --- | --- | --- | --- |
| `AttemptTelemetrySession.ApplyToMap` | Typed `AttemptTelemetry` | `report.Metrics map[string]interface{}` with dotted legacy keys | Compatibility adapter; must not become a producer API. |
| `TypedMetricsFromMap` fallback | Legacy report map | Typed execution metrics | Compatibility reverse bridge used for legacy/failure paths; this is the main bidirectional-contract risk. |
| `TaskExecutionReport.DetailedPhases` | Executor detailed phases + `EventRecorder` snapshot | Ordered report slice | Projection of event journal plus native transport data. |
| `CanonicalAttemptEvents` | Recorder snapshot | Compact heartbeat lifecycle events | Read-only live view; not a second event journal. |
| `PerformanceReceiptAssembler` | `pipeline.RunMetrics`, sidecar mappings, process counters, caller context | `PerformanceReceiptV1` | Final typed artifact; not intended as a mutable metric store. |
| `telemetry.PrometheusMetrics` | Worker lifecycle/cache/upload/ack calls | Worker-local `/metrics` text | Directly updated by many worker lifecycle helpers; not currently derived solely from the EventRecorder. |
| TaskResult outbox | Serialized `pb.TaskResult` | Durable retry payload | Transport durability, not telemetry authority. |

### 5.2 Master-side projections and sinks

| Projection/sink | Input | Output | Authority status |
| --- | --- | --- | --- |
| Atomic typed persistence | `IngestCommand` from one TaskResult | `task_attempt_metrics`, cache/cost rows, segments, parallelism, raw report | Durable read models; projections of one accepted report. |
| Event persistence | `PhaseTimings` / legacy fallback | `task_execution_events` append-only rows + `task_phase_timings` summaries | Event rows preserve detail; phase summaries are an aggregate projection. |
| Master `Collector` | Attempt read models, heartbeat resources, supervisor state | Master Prometheus `Family` registry | Export projection. Metric families are registered in `DataServer/internal/metrics/catalog.go` and collector family files. |
| Operational telemetry | Delivery/DB/cache method calls | `OperationalTelemetry` families | Direct instrumentation sink for Master operational behavior; separate from worker attempt facts. |
| Daily performance rollup | `task_attempts`, `task_attempt_metrics`, `task_phase_timings` | `render_performance_daily` | SQL analytical projection; recalculates distributions/baselines. |
| Raw report/audit path | Worker proto JSON | `task_attempt_reports` and audit/replay readers | Immutable transport/audit representation; not a competing typed source. |
| Dashboard SQL/Grafana | Prometheus and SQL projections | `prometheus/observability-dashboards.sql`, `dashboards/*.json` | Read-only consumer. |

### 5.3 Catalogs that must not be conflated

There are currently two legitimate registries with different namespaces:

1. **Event catalog:** `shared/telemetry/catalog.go` — component/action event
   descriptors and wire taxonomy.
2. **Master metric catalog:** `DataServer/internal/metrics/catalog.go` and
   `catalog_*.go` — exported metric family names, units, components, and
   metric kind.

The second catalog is not a duplicate of the first by itself. The risk is
that a producer can still encode metric semantics outside either registry,
for example in worker `PrometheusMetrics` method names, SQL column names,
receipt fields, or dashboard formulas.

## 6. Identified duplication and drift boundaries

These are the concrete places found in the inspected execution, ingestion,
metrics, receipt, and native-engine paths where the same fact or semantic can
currently be represented or calculated more than once. The inventory is
scoped to those paths; it is not a claim that unrelated diagnostic counters
outside this execution path have been exhaustively enumerated.

### D1 — Generated C++ binding must stay synchronized

- Language-neutral authority: `shared/telemetry/catalog.json`.
- C++ binding: `catalog_generated.hpp`, produced by
  `shared/telemetry/cmd/cataloggen`.
- Guard: `scripts/ci/check-telemetry-catalog.sh` runs the Go tests and generator
  in `-check` mode, so a changed JSON source with a stale C++ header fails
  closed. The PhaseRecorder aliases its origin/scope vocabulary to the
  generated header rather than declaring literals locally.

### D2 — Typed resource metrics and legacy map are both active contracts

- Source: `AttemptTelemetrySession` returns typed fields.
- Adapter: `ApplyToMap` writes dotted keys.
- Reverse compatibility: `TypedMetricsFromMap` can reconstruct typed fields.
- Consumer: `task_execution.go` writes both `report.Metrics` and
  `report.TypedMetrics` before `TaskResult` serialization.
- Impact: a future producer can update the map directly and bypass typed
  ownership; a disagreement between the two representations has no single
  conflict policy.

### D3 — Event journal and phase/summary views coexist by design, but need a
clear authority boundary

- Detailed source: `EventRecorder` and native sidecar phases.
- Report view: `TaskExecutionReport.DetailedPhases`.
- SQL detail: `task_execution_events`.
- SQL aggregate: `task_phase_timings`.
- Heartbeat view: `CanonicalAttemptEvents`.
- Impact: these are valid projections of the same event stream, but code that
  writes a phase summary or heartbeat lifecycle event independently would
  create a second truth. The current map documents the intended direction.

### D4 — Receipt input is not yet the recorder snapshot

`PerformanceReceiptAssembler.Assemble` consumes `pipeline.RunMetrics`, which
contains `PhaseMS`, `DetailedPhases`, segments, process counters, and resource
fields. It does not consume a single immutable `AttemptSnapshot` from
`EventRecorder` plus `AttemptTelemetrySession`. The receipt is therefore a
canonical final artifact constructor, but not yet a pure projection of one
journal.

There is also an implementation/documentation gap in the current receipt
shape: `PerformanceReceiptV1` declares `DerivedMetrics`, while
`Assembler.Assemble` currently populates timing, process, I/O, media, phases,
and segments but does not calculate or assign `receipt.Derived`. The field is
therefore a reserved/zero-valued projection until a canonical derivation step
is wired.

### D5 — Receipt assembler contains compatibility inference

`assembleWorkload` fills `ClipCount` from `run.TimelineItems` when the caller
leaves it zero. `assemblePhases` falls back from detailed sidecar phases to
`PhaseMS`. These fallbacks preserve legacy workers, but they are inference or
reconstruction paths and should not be mistaken for independently observed
facts.

### D6 — Worker and Master expose overlapping Prometheus family names

The worker-local `internal/telemetry/prometheus.go` and the Master
`internal/metrics` registry both expose families such as cache download,
cache request, render, worker, and TaskResult operational metrics. They are
served by different processes and registries, so the duplicate name is not
necessarily a runtime collision; however, the semantic owner and aggregation
rule must remain explicit when dashboards combine them.

Examples visible in the current code include:

- `velox_cache_requests_total` and cache hit/miss counters;
- `velox_cache_downloads_total` and `velox_cache_download_bytes_total`;
- render/upload/TaskResult submit and acknowledgement timings;
- worker resource and cache gauges.

### D7 — Master engine timing has detailed and aggregate paths

`Supervisor.tickOnce` prefers `GetPhaseTimingsDetailed` and calls
`RecordEnginePhase`; only when no detailed rows exist does it call
`RecordEngineAggregate` from the flat `task_attempt_metrics` columns. This is
the correct compatibility precedence, but both paths remain live. The same
attempt also feeds `PerformanceReceipt.Phases` and SQL rollups, so consumers
must not sum all projections as if they were independent observations.

### D8 — Raw report, typed rows, event rows, and summaries intentionally repeat

The same accepted TaskResult is stored as:

- raw JSON for audit/replay;
- typed attempt metrics/cache/cost rows for queryability;
- append-only detailed events for the execution timeline;
- compact phase summaries for aggregation;
- segment and parallelism rows for specialized analytics.

This is intentional denormalized read-model storage, not five authoritative
producers. The atomic ingest transaction and report hash are the consistency
boundary.

### D9 — Derived KPI formulas are distributed

The codebase currently has at least these derived surfaces:

- `PerformanceReceiptV1.Derived` fields and assembler comments;
- `taskattempts.AttemptMetrics` ratio helpers;
- Master `Collector.RecordAttempt` normalized gauges;
- Master `render_performance_daily` rollup calculations;
- Grafana/SQL dashboard expressions.

They consume related raw rows but are not all visibly routed through one
`DerivedMetricsCalculator`. In particular, the receipt's `Derived` section is
currently not populated by `Assembler.Assemble`, as noted in D4. This is the
clearest remaining semantic-SSOT gap for benchmark KPIs.

### D10 — Master attempt counters require supervisor-level deduplication

`Collector.RecordAttempt` documents that its counter writes are not
idempotent for repeated input. `Supervisor.tickOnce` currently supplies the
necessary attempt-ID seen set and only records a newly-terminal attempt once,
while removing the ID when a read fails so it can be retried. This is a valid
current guard, but it means idempotency is owned by the polling coordinator,
not by the projection method or the persisted metric row itself.

## 7. Dependency direction currently intended by the code

```text
canonical event catalog
        |
        +--> worker phase registry
        +--> worker EventRecorder validation
        +--> Master event validation
        +--> SQL constraints / quarantine

worker/native producers
        |
        +--> EventRecorder or RenderMetrics transport
        +--> AttemptTelemetrySession for resource observations
        +--> TaskExecutionReport
        +--> TaskResult builder
        +--> durable TaskResult outbox

TaskResult
        |
        +--> Master identity gate
        +--> atomic ingestion transaction
              |
              +--> SQL detail + typed read models + raw audit
              +--> Prometheus projection
              +--> daily performance projection
              +--> API/dashboard consumers

PerformanceReceiptAssembler
        |
        +--> PerformanceReceiptV1 benchmark artifact

Prometheus / receipt / dashboards
        |
        +--> read-only consumers; no return edge to recorder or TaskRunner
```

The intended no-return-edge rule is mostly present at the Master boundary:
`IngestTaskResultAtomic` owns terminal persistence, and dashboards read
projections. The worker still has separate direct lifecycle calls into
`PrometheusMetrics`, and receipt assembly is fed from `RunMetrics`, so the
worker-side graph is not yet a single recorder-only pipeline.

## 8. Evidence and verification notes

The map was built from the following implementation evidence:

- shared taxonomy, Go loader, and worker-derived view:
  `shared/telemetry/catalog.json`, `shared/telemetry/catalog_source.go`,
  `shared/telemetry/catalog.go`, and
  `RemoteCodex/native/worker-agent-go/internal/telemetry/phase_registry.go`;
- worker journal and resource session:
  `internal/telemetry/phase_recorder.go`,
  `internal/telemetry/attempt_session.go`,
  `internal/taskrunner/runner_report.go`;
- native transport:
  `RemoteCodex/native/video-engine-cpp/include/velox/telemetry/phase_recorder.hpp`,
  `src/telemetry/phase_recorder.cpp`, and
  `pkg/video/services/native/binary_resolver.go`;
- wire and Master boundary:
  `internal/worker/task_result_builder.go`,
  `DataServer/internal/grpcserver/handler_result.go`,
  `DataServer/internal/ingest/service.go`,
  `DataServer/internal/store/sqlite_task_atomic_ingest.go`;
- Master metric projection:
  `DataServer/internal/metrics/catalog.go`,
  `collector_attempts.go`, `collector_engine.go`,
  `collector_workers.go`, `supervisor_tick.go`;
- human metric contract and SQL analytical projection:
  `docs/metrics-catalog.md`,
  `DataServer/internal/metrics/render_performance_rollup.go`.

This document intentionally does not treat the pre-existing working-tree
changes as part of the map's implementation. It adds only the repository
map itself.
