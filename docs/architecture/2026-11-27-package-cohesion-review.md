# Package cohesion review — store and worker

**Date:** 2026-11-27  
**Refs:** `4d42269c` audit baseline through `415be011` plus the follow-up
commits recorded in `CHANGELOG.md`  
**Decision:** retain both package boundaries; no package split justified by
the current ownership or change-coupling evidence.

## Reproducible source counts

The count is tracked Go production files directly inside each package
directory. It excludes `_test.go`, generated `.pb.go` and `_generated.go`
files, and does not include nested packages.

```bash
python3 - <<'PY'
import subprocess
paths = subprocess.check_output(['git', 'ls-files', '-z']).decode().split('\0')
for label, prefix in [
    ('store', 'DataServer/internal/store/'),
    ('worker', 'RemoteCodex/native/worker-agent-go/internal/worker/'),
]:
    files = [p for p in paths if p.startswith(prefix)
             and '/' not in p[len(prefix):] and p.endswith('.go')
             and not p.endswith('_test.go')
             and not p.endswith(('.pb.go', '_generated.go'))]
    print(label, len(files))
PY
```

| Package | Production Go files | Audit count reproduced |
| --- | ---: | --- |
| `DataServer/internal/store` | 156 | yes |
| `worker-agent-go/internal/worker` | 95 | yes |

## `internal/store`

`internal/store` is the SQLite composition and adapter package. Its direct
dependencies include domain contracts (`jobs`, `taskgraph`, `taskattempts`,
`deliverystore`, `publicationstate`, `assets`), persistence leaves
(`completionstore`, `forwardingstore`, `migrations`), and shared SQLite
infrastructure (`platform/database`, `storecore`, `repository`). This fan-in
is expected at the concrete backend boundary; domain packages do not import
the store package back.

The package responsibilities are grouped by state ownership, not by file
count:

| Responsibility | Domain/API owner | SQLite boundary |
| --- | --- | --- |
| Job transitions and creation | `jobs`, `jobs/enqueue` | `jobs_repository_*`, `sqlite_task_atomic*` |
| Task and attempt lifecycle | `taskgraph`, `taskattempts`, `completion` | `sqlite_task_*`, `completionstore` |
| Artifact/upload persistence | `artifacts`, `repository` | `artifactsstore`; remaining store files are compatibility/read adapters |
| Delivery and publication | `deliveries`, `deliverystore`, `publicationstate` | `deliverystore` plus the transaction-bound parent-job roll-up in `store_deliveries.go` |
| Reconciliation and audit | `stalereconcile`, `reconcile`, `audittrail` | store transaction adapters and audit repository |
| Schema and shared SQLite | migration registry and `platform/database` | `migrations`, `sqlite.go`, transaction/telemetry helpers |

The existing extraction is real: delivery CRUD is in `deliverystore`,
completion persistence in `completionstore`, artifact finalization in
`artifactsstore`, and other bounded contracts have leaf packages. The
remaining `store` surface still owns transactions that span jobs, tasks,
attempts, artifacts, delivery rows, and outbox records. Moving those SQL
statements without moving the transaction owner would split atomic operations
and add import edges rather than reduce shared knowledge.

The compatibility inventory in `OWNERSHIP.md` remains active work, not dead
code proof. Several aliases have production or test callers and have an
explicit 2026-12-31 sunset; this review does not remove them early. New
production callers should use the leaf owner listed in that inventory.

## `internal/worker`

The worker package owns the session-level lifecycle and dispatch coordination.
The `Worker` state and its methods are spread across files because lifecycle
operations share cancellation, lease identity, active-task registration,
transport state, and result-reporting fences.

| Concern | Representative files | State/resource ownership |
| --- | --- | --- |
| Startup, registration, and session lifecycle | `worker_init.go`, `worker_registration.go`, `worker_lifecycle.go`, `worker_types.go` | one session context, connection state, registration identity |
| Task admission and execution | `worker_claim*.go`, `task_dispatch.go`, `task_execution.go`, `task_lifecycle.go` | active task registry, per-task context, cancellation, lease fences |
| Result and artifact protocol | `task_result_reporter*.go`, `active_task_*.go`, `artifact_upload_resume.go`, `worker_artifacts.go` | output spool state, resumable upload, completion acknowledgements |
| Asset and cache orchestration | `asset_*.go`, `compiled_plan_*.go`, `future_asset_hydration.go`, `prefetch_*.go` | worker-local binding and transfer coordination; specialized implementations live in downloader/cache packages |
| Lease and heartbeat recovery | `heartbeat_*.go`, `lease_*.go`, `active_lease_registry.go` | lease renewal, liveness, and stale-task recovery |
| Resource/network admission | `cpu_tokens.go`, `network_admission.go`, `resource_admission.go`, `prejob_disk_*.go` | worker-wide permits and sampled admission state |
| Telemetry and progress | `asset_metrics*.go`, `telemetry_*.go`, `report_observability.go` | snapshots and event projection; metric schema lives in telemetry packages |

The import direction is one-way from `worker` into executor, taskrunner,
transport, downloader, spool, cache, and telemetry packages. Those leaves own
specialized execution or storage; `worker` owns the ordering between control
messages and task lifecycle. Co-change pairs such as `worker_init.go` with
`worker_types.go` (13 commits), `task_dispatch.go` with `worker_types.go`
(11), and `task_execution.go` with `worker_types.go` (10) support that shared
state boundary. Moving methods to packages that still need the `Worker`
mutable state would introduce interfaces or accessors without isolating an
independent lifecycle.

## Findings and follow-up

- Keep both package boundaries. The source count alone is not a defect.
- Continue compatibility-facade removals only when callers are migrated and
  the sunset/ADR rules allow removal.
- Use the lifecycle ownership map for the small number of cross-domain
  transaction methods; do not create a second SQLite repository interface.
- No worker package split is proposed. Revisit only if a measured change
  coupling or a resource lifecycle can be isolated without sharing `Worker`
  state.

## Verification

The counts above use `git ls-files` and the explicit direct-directory filter.
The Go package boundaries are confirmed by `go list` for
`./internal/store` and `./internal/worker`. Historical co-change evidence and
the filtering window are in
[`2026-11-27-architecture-audit-metrics.md`](../metrics/2026-11-27-architecture-audit-metrics.md).
