# Orchestrator Wrap Audit — 2026-07-19

## Background

Following commit `0d2158d` ("fix(worker): drop orchestrator error-wrap
in resolveTaskAssets") — which dropped the only post-split error-wrap
inside `(*Worker).resolveTaskAssets` to keep outer error semantics
identical to the pre-split contract — this audit re-applies the same
wrap-drop rule across the **remaining 7 orchestrators** to confirm the
project ships wrap-clean.

**Audit rule**: drop a wrap iff the orchestrator adds an error-wrap
prefix that didn't exist pre-split (i.e. it names a newly extracted
submodule solely to identify which submodule failed). Pre-existing
wraps that provide context the inner helper can't supply (operation
names, stderr/stdout buffers, inline-DB-step labels) survive.

## Scope

| # | Orchestrator (entry-point)                                       | File                                                                                            | Inner call sites                                                  |
|---|------------------------------------------------------------------|-------------------------------------------------------------------------------------------------|-------------------------------------------------------------------|
| 1 | `Worker.heartbeatLoop`                                           | `RemoteCodex/native/worker-agent-go/internal/worker/heartbeat_loop.go:28`                       | `sendHeartbeat`                                                   |
| 2 | `Worker.leaseRenewLoop`                                          | `RemoteCodex/native/worker-agent-go/internal/worker/lease_renewal.go:22`                        | `transport.Send`, `SnapshotActiveTaskLeases`                      |
| 3 | `RenderClient.Render` → `RenderWithMetrics` (the orchestrator carrying the wrap; `Render` is a thin pass-through alias) | `RemoteCodex/native/worker-agent-go/pkg/video/services/native/render_client.go`             | `preparePlanTemp`, `runEngineProcess`, `verifyOutputExists`, …    |

> **Note on entry-point disambiguation** for row 3: `Render` (public,
> ~L70) is a 3-line wrapper that calls `RenderWithMetrics` (orchestrator,
> ~L76) and returns the inner err unchanged. It carries NO wrap.
> `RenderWithMetrics` carries the single `engine failed: %w
> (stderr=… stdout=…)` wrap at the subprocess-failure branch. The file
> ALSO contains a wrap in the constructor `NewRenderClient` (the
> `locate native engine: %w` wrap), but `NewRenderClient` is a
> constructor, not one of the 7 orchestrators in this audit scope —
> counted separately below in § *File-level vs orchestrator-boundary*.
| 4 | `Worker.executeTask`                                             | `RemoteCodex/native/worker-agent-go/internal/worker/task_execution.go:71`                       | `runJobTask`, `uploadTaskOutputs`, `recordTaskStart`, …           |
| 5 | `AssetService.ResolveAndRegister`                                | `DataServer/internal/assets/registration.go:25`                                                  | `registry.ResolveByInference`; `blobStore.{StagingPath,Promote…}`; `repo.{Insert,InsertSource}`; `identity.NewHex128` |
| 6 | `WorkersHandler.ListWorkers` / `GetWorker` (HTTP entrypoints)    | `DataServer/internal/handlers/server/api/workers_handler.go:35,52`                               | `registry.List` / `registry.GetWorker`                            |
| 7 | `SQLiteStore.PersistWorkerHeartbeat`                             | `DataServer/internal/store/store_worker_heartbeat.go:34`                                         | `upsertWorkerExec`; `reconcileWorkerRuntime`; `maybeInsertWorkerMetric`; `prune{WorkerMetricSamples,WorkerEvents}`; `detectAndPersistPartitionTransition`; `bulkEmitTaskRuntimeDisappearedOnPartition`; `appendWorkerStateChangedEvent` |

## Method

For each orchestrator, enumerate every `fmt.Errorf("…: %w", innerErr)`
/ `errors.Wrap(innerErr, "…")` at the function's outer boundary, then
ask:

- Does the wrap name the **operation** (an intrinsic phase label the
  inner helper can't supply on its own) — or does it name a
  **submodule** (syntactic restatement of the call site, i.e. only
  useful because the inner helper was just extracted)?
- If pre-split, would the same wrap have existed?

Concrete discriminators:

- `RenderWithMetrics → "engine failed: %w (stderr=%s stdout=%s)"` — the
  stderr/stdout buffers are accumulated locally in the orchestrator;
  pre-split, the inline subprocess code already produced the same
  wrap. → **pre-existing**.
- `executeTask → "upload task outputs: %w"` — wraps the SEMANTIC phase,
  referenced in the function's docstring as the canonical failed
  branch; pre-split, the inline upload code had the same wrap. →
  **pre-existing**.
- `ResolveAndRegister` — wraps up-calls to interface methods that
  didn't move during the split (`blobStore.*`, `repo.*`,
  `identity.*`). The 9 wraps by name, each a step label rather than a
  submodule name:
  1. `resolve %s: %w` (wraps `registry.ResolveByInference`)
  2. `staging path: %w` (wraps `blobStore.StagingPath`)
  3. `create staging file: %w` (wraps `os.Create`)
  4. `stage bytes: %w` (wraps `io.Copy`)
  5. `sync staging: %w` (wraps `stagingFile.Sync`)
  6. `promote to final: %w` (wraps `blobStore.PromoteToFinal`)
  7. `insert asset: %w` (wraps `repo.Insert`)
  8. `insert source: %w` (wraps `repo.InsertSource`)
  9. `generate source ID: %w` (wraps `identity.NewHex128`)
  → **pre-existing** (submodule name missing from each prefix).
- `PersistWorkerHeartbeat → {"persist worker snapshot", "touch worker
  session"}` — the `touch worker session` wrap covers an INLINE
  `tx.ExecContext`, proving that step-name wraps over inline DB ops
  existed pre-split. The `persist worker snapshot` wrap co-located with
  the inline `touch worker session` is the same step-name pattern. →
  **pre-existing**.

No test asserts on the wrap-prefix text in any of the 7 orchestrators
(`grep `strings.Contains\|err.Error()` *\|assert\|require` in *_test.go`
for each function returns 0 matches). Dropping the wraps would not
break tests either — the audit is purely defensive against nuances
that future refactors could re-introduce.

## Results

| # | Orchestrator                | Wraps found | Verdict | Action     |
|---|-----------------------------|------------:|---------|------------|
| 1 | `heartbeatLoop`             | 0           | PASS    | no edit    |
| 2 | `leaseRenewLoop`            | 0           | PASS    | no edit    |
| 3 | `Render(RenderWithMetrics)` | 1           | PASS    | no edit    |
| 4 | `executeTask`               | 1           | PASS    | no edit    |
| 5 | `ResolveAndRegister`        | 9           | PASS    | no edit    |
| 6 | `WorkersHandler.{List,Get}` | 0           | PASS    | no edit    |
| 7 | `PersistWorkerHeartbeat`    | 2           | PASS    | no edit    |

**Total wraps audited at the orchestrator boundary: 13 wraps across 7
orchestrators — all 13 are pre-existing.**

#### File-level vs orchestrator-boundary

The reproducer `grep -nE 'fmt\.Errorf.*%w|errors\.Wrap' "$f"` returns
14 matches when run against the 7 file paths in the Scope table. The
delta is one wrap in `RemoteCodex/native/worker-agent-go/pkg/video/services/native/render_client.go` — `NewRenderClient`'s `locate native engine: %w` (constructor, not in the orchestrator scope). The 13 rows in the Results table count wraps at the **orchestrator boundary only**; the file-level count of 14 includes that constructor wrap. The audit scope is the orchestrator boundary by design, since the wrap-drop rule applies to orchestrators (functions that compose inner helpers), not to constructors.

## Verdict

The project is already wrap-clean. The only post-split wrap that
needed dropping was the `resolveTaskAssets` wrap removed by `0d2158d`.
No additional code edits are required to land the same audit rule on
the remaining 7 orchestrators.

## Reproducer

Static analysis — no script needed. Re-verify with:

```bash
for f in \
  RemoteCodex/native/worker-agent-go/internal/worker/heartbeat_loop.go \
  RemoteCodex/native/worker-agent-go/internal/worker/lease_renewal.go \
  RemoteCodex/native/worker-agent-go/pkg/video/services/native/render_client.go \
  RemoteCodex/native/worker-agent-go/internal/worker/task_execution.go \
  DataServer/internal/assets/registration.go \
  DataServer/internal/handlers/server/api/workers_handler.go \
  DataServer/internal/store/store_worker_heartbeat.go ; do
  echo "=== $f ==="
  grep -nE 'fmt\.Errorf.*%w|errors\.Wrap' "$f"
done
```

Each grep match corresponds to a wrap at the orchestrator boundary, plus
the one constructor wrap in `render_client.go`. The Reasoning column
above justifies each orchestrator-boundary row's PASS verdict; the
`NewRenderClient` wrap is exempt by scope (constructor, not
orchestrator).

> **Pin**: line numbers in the Scope table reflect HEAD `d620fe3`
> (the commit that wired `scripts/ci/run-split-regression.sh` into CI).
> Future commits that move functions between files will shift them; if
> this doc is re-audited, re-locate each `^func …` declaration with
> ripgrep and update the table. The auditor's verdict column is
> robust to line drift — it depends only on which call sites the
> orchestrator's wrap covers, not on the literal line number.
