# Architecture audit measurements — 2026-11-27

## Refactor metrics

Reproduced with the repository analyzer, using the audit commit as baseline
and the post-fix documentation commit as head:

```bash
python3 scripts/metrics/refactor_metrics.py \
  --repo-root . --base-ref 4d42269c --head-ref 415be011 \
  --out-dir /tmp/velox-refactor-audit-metrics
```

Analyzer schema 1, tracked source scope and definitions are unchanged from
[`refactor-metrics.md`](refactor-metrics.md).

| Metric | Base | Head | Delta |
| --- | ---: | ---: | ---: |
| Files | 2,808 | 2,808 | 0 |
| Physical LOC | 489,377 | 489,420 | +43 |
| Code LOC | 370,045 | 370,086 | +41 |
| Decision-complexity proxy | 71,058 | 71,064 | +6 |
| Maximum per-file proxy | 195 | 195 | 0 |
| Duplicate normalized blocks | 13,158 | 13,156 | -2 |
| Duplicate lines | 130,380 | 130,368 | -12 |
| Duplicate ratio | 0.352336 | 0.352264 | -0.000072 |

The small complexity increase comes from the additional delivery-state and
publication-evidence guards; it is not evidence of a broad simplification.
The duplicate reduction is small and should not be overstated. Targeted
changes did remove a repeated publication predicate and redundant runtime
type assertions; global repository metrics cannot isolate their cognitive
benefit.

Largest targeted changes reported by the analyzer:

| File | LOC delta | Complexity-proxy delta | Reason |
| --- | ---: | ---: | --- |
| `runtime_finalize.go` | -5 | -8 | one typed planner dependency replaces repeated assertions |
| `runner_phases.go` | -4 | -3 | shared publication evidence predicate |
| `completion_repository_finalize.go` | +20 | +7 | pending-delivery gate, retry policy, and exact conflict target |
| `runner_process.go` | +9 | +3 | fail-closed preflight for stored `PUBLISHED` state |

## Historical change coupling

The reproducible window is 2026-05-31 through 2026-11-28, inclusive. The
analysis reads `git log --name-only`, counts distinct commits touching each
production file or pair, excludes test/generated files from pair ranking, and
excludes commit subjects containing `format`, `gofmt`, `generated`, `vendor
sync`, `rename files`, or `mechanical`. It is a screening metric; broad feature
commits can inflate cross-domain co-change.

Top production-file churn in the window:

| Commits | File |
| ---: | --- |
| 35 | `DataServer/internal/grpcserver/handler_placement.go` |
| 33 | `RemoteCodex/native/worker-agent-go/internal/worker/task_dispatch.go` |
| 30 | `RemoteCodex/native/worker-agent-go/internal/worker/task_execution.go` |
| 30 | `RemoteCodex/native/worker-agent-go/internal/worker/worker_types.go` |
| 29 | `DataServer/cmd/server/bootstrap_modules.go` |
| 29 | `DataServer/cmd/server/bootstrap_wiring.go` |
| 25 | `DataServer/internal/handlers/server/pipeline/intake_types.go` |
| 25 | `DataServer/internal/handlers/server/pipeline/worker_payload_projection.go` |
| 24 | `DataServer/internal/grpcserver/handler_futureasset.go` |
| 24 | `RemoteCodex/native/worker-agent-go/internal/worker/worker_init.go` |

Most frequent same-commit cross-domain pairs (distinct commits, not file-pair
occurrences):

| Commits | Domains |
| ---: | --- |
| 58 | `server-bootstrap` + `store` |
| 50 | `grpcserver` + `store` |
| 49 | `metrics-telemetry` + `worker` |
| 43 | `grpcserver` + `worker` |
| 37 | `store` + `worker` |
| 34 | `fleet` + `server-bootstrap` |
| 32 | `pipeline` + `store` |
| 29 | `metrics-telemetry` + `store` |

High-frequency production file pairs include:

| Commits | Files | Interpretation after sampling |
| ---: | --- | --- |
| 19 | pipeline `intake_types.go` + `worker_payload_projection.go` | same producer payload boundary |
| 14 | pipeline `projection/input.go` + `worker_payload_projection.go` | projection contract |
| 13 | worker `worker_init.go` + `worker_types.go` | shared worker lifecycle state |
| 12 | API `admin_workers_dto.go` + `admin_workers_handler.go` | one endpoint DTO and handler |
| 11 | deliveries `runner.go` + `runner_process.go` | one runner split by lifecycle responsibility |
| 11 | worker `task_dispatch.go` + `worker_types.go` | dispatch reads shared worker state |
| 10 | worker `task_execution.go` + `worker_types.go` | execution reads shared worker state |

Sampled commits confirm semantic coupling rather than unrelated churn:

- `dabb11af40` (2026-11-26), “retire legacy video preparation and slice
  executors”, changed Master routing, persistence retry, worker dispatch and
  executor registration together as one cutover.
- `b5839c90a0` (2026-11-25), “expose final job queue breakdown”, changed the
  event protocol, server ingest, queue projection, SQLite telemetry and worker
  progress reporting together.
- `4e900225` (2026-11-26), release metadata and error wrapping, changed
  production files in the same functional fix; it was not excluded as
  mechanical because the errors changed API/runtime classification.

The co-change signal supports keeping transaction adapters together at the
SQLite boundary and keeping worker session orchestration around the shared
`Worker` state. It does not justify a package split. The next targeted
inspection priority is `handler_placement.go` and the pipeline payload
projection boundary, not moving files based only on counts.

## Commands for the custom history summary

The historical summary uses only Python's standard library:

```bash
git log --since=2026-05-31 --until=2026-11-28 \
  --date=short --format='COMMIT%x09%H%x09%cs%x09%s' --name-only
```

Then count distinct commit IDs per file and per unordered production-file
pair after applying the stated filter. Domain labels are assigned by the
repository prefixes documented in the cohesion review. The analyzer and
history summary are independent: the former measures a two-ref source delta;
the latter measures churn/co-change within the date window.
