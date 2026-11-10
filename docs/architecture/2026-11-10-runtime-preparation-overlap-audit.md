# Runtime preparation overlap audit — 2026-11-10

## Scope

This note records the production timing for `job_4c6a039545880409` ("Garlasco — overlay separati + finale remoto") and traces the two waits around worker preparation. It distinguishes observations from code-level hypotheses. The job ran entirely on remote worker `host_57_129_132_133`; no local render was involved.

## Observed job timeline

All timestamps are UTC on 2026-11-10.

| Time | Event | Evidence |
|---|---|---|
| 15:52:59 | PREPARE accepted; assembly task created and made READY | Master log and `jobs` / `tasks` rows |
| 15:53:00 | FINALIZE request accepted | `[API_V1_JOBS_ACCEPTED phase=FINALIZE]` |
| 15:53:04 | Runtime finalizer published 213 `video.prepare.window.v1` tasks | `[PIPELINE] runtime FINALIZE published preparation DAG` |
| 15:53:06 | First window task offer sent | `[PLACEMENT] TaskOffer sent` |
| 15:53:13 | First window task accepted and started | `[GRPC_TASK_ACCEPTED]`; `task_attempts` |
| 15:54:31.494 | Last of 213 window attempts completed | `task_attempts` maximum `completed_at` |
| 15:54:31 | Runtime finalizer logged the assembly task as materialized | `[PIPELINE] runtime FINALIZE materialized` |
| 15:54:31–15:55:04 | Placement repeatedly blocked the assembly task with `runtime_assets_pending` | Master `[PLACEMENT] preparation gate BLOCKED runtime_assets_pending` logs |
| 15:55:04 | Assembly TaskOffer queued and sent | Master `[PLACEMENT] TaskOffer queued/sent` |
| 15:55:05.071 | Remote assembly started | `task_attempts` |
| 15:55:16.057 | Remote assembly completed | `task_attempts` and `jobs` |

The resulting artifact is 307,221,657 bytes. The assembly diagnostics show audio present and 22 overlays. The render plan duration is 1,184.97 seconds (about 19 minutes 45 seconds).

## Where the 137 seconds went

| Interval | Wall time | Interpretation |
|---|---:|---|
| Job arrival → first worker start | 14 s | 1 s until FINALIZE was accepted; 4 s until the preparation DAG was published; first offer followed 2 s later; another 7 s elapsed between that offer and worker acceptance/start. The available records do not split the 4 s finalizer work into planning, cache lookups, and DAG persistence. |
| Window preparation | 78 s | 213 window tasks overlapped on the remote worker. 175 used packet-copy; 38 encoded 2,452 frames. FFmpeg encode time summed across those attempts was 43.18 s, so it is worker-time, not additional wall time. |
| Final preparation → assembly offer | 33.6 s | This was not assembly or encoding. The Master logged the task as materialized at 15:54:31 but continued to reject it under the `runtime_assets_pending` gate until 15:55:04. |
| Assembly | 11.0 s | The native packet mux event itself was about 0.78 s; the rest of the task includes input checks, output handling, verification, and result reporting. All 261 final segments were packet-copied. |

Total from accepted PREPARE to completed job: 137.1 s. The intervals above sum to that wall time, subject to timestamp rounding in task and log records.

## Finding 1: stock preparation starts too late in the lifecycle

The current flow intentionally separates PREPARE from FINALIZE:

1. `job_prepare_finalize.go` persists the runtime-finalize request and marks the parent task `runtime_assets_pending=true` when Drive stock needs worker-side preparation.
2. `RuntimeFinalizationProcessor.processOne` then plans all stock windows, checks the prepared-window cache, and only after that publishes the preparation DAG.
3. The preparation gate keeps the assembly task unclaimable until FINALIZE clears `runtime_assets_pending`.

Relevant code: `DataServer/internal/handlers/server/pipeline/job_prepare_finalize.go`, `DataServer/internal/handlers/server/pipeline/runtime_finalize.go`, `DataServer/internal/assets/stock_drive_task_plan.go`, and `DataServer/internal/grpcserver/preparation_gate.go`.

The Master already has a PREPARE-time future-asset hook (`PrefetchSubmittedJob` in `DataServer/internal/grpcserver/handler_futureasset.go`). That can reserve/download known source files while rendering is still blocked. It does **not** create `video.prepare.window.v1` tasks. CPU-side window preparation only starts after FINALIZE gives `RuntimeFinalizationProcessor` the finalized plan and runtime bindings.

For this job, PREPARE and FINALIZE were only one second apart, so the run does not demonstrate a long wait for late voiceover or overlay delivery. It does establish that the architecture has a boundary where early stock preparation could overlap later asset production.

### Proposed lifecycle change

Separate three states that are currently coupled on the parent assembly task:

- **Source known and immutable:** source identity/locator and size are known; download and source probe may start.
- **Window preparation known:** source window and canonical output profile are fixed; its certified fragment may be produced and cached.
- **Assembly ready:** every final input, including late audio and overlays, is present and the final plan is immutable.

At PREPARE, publish source/window work for the stock and clip inputs already present in the compiled plan. Keep the assembly task gated. When FINALIZE supplies voiceover and overlays, attach the already-certified fragments if their source SHA, source interval, output frame count, and preparation profile still match. `VideoPrepareWindowCacheIdentity` already excludes timeline placement, so moving an unchanged prepared window to a different final timeline position does not change its content cache identity.

The initial implementation should only precompute a window when all fields that determine its output are stable. A changed source, trim interval, frame count, or output-profile revision must miss the cache and create new work. Do not let early-prepared tasks make the final assembly executable.

Current limitation: `PlanDriveStockWindowTasks` is the only planner in this flow and runs from FINALIZE bindings. The separate clip path needs to be checked and explicitly included; stock-pool members are intentionally excluded from eager prefetch because downloading an entire pool would create a blocking gate.

## Finding 2: the 33.6-second gap is a readiness-gate delay

The final preparation attempt ended at 15:54:31.494, and the runtime finalizer logged `runtime FINALIZE materialized` at 15:54:31. Yet placement logs continued to report `preparation gate BLOCKED runtime_assets_pending` for the parent assembly task until the Master queued its TaskOffer at 15:55:04. The final stored task spec now has `runtime_assets_pending=false` and `runtime_finalize_status=SUCCEEDED`.

This makes the 33.6 s a Master-side state propagation/readiness problem, not a slow FFmpeg stage. The exact stale state is not proven by the persisted evidence: `ListFutureReservations` joins the current `task_specs.payload_json`, while the gate checks `runtime_assets_pending` on the reservation payload. The contradictory production observations mean the gate's reservation snapshot, task revision, and FINALIZE commit need finer event-level tracing before assigning a definitive code defect.

### Required instrumentation and fix direction

Record these values together for each parent task:

- FINALIZE payload commit time and resulting task-spec revision;
- reservation revision and the `runtime_assets_pending` value read by the placement gate;
- each gate decision with a reason and the reservation/task revision pair;
- readiness transition, TaskOffer queued/sent, TaskAccepted, and lease granted.

Then make the successful FINALIZE commit explicitly invalidate/reload the future-reservation snapshot and wake placement for the owning worker. The placement gate must evaluate the latest committed task-spec payload; a stale reservation snapshot must not impose a timer-length wait after the finalizer has cleared the marker. Add a regression test that starts with a `runtime_assets_pending=true` reservation, commits FINALIZE (`false`), and proves the next placement pass can offer the assembly without waiting for a periodic refresh.

## Finding 3: first task startup also has avoidable latency

The 213 preparation tasks were published at 15:53:04. The first TaskOffer was sent at 15:53:06, but the worker did not accept it until 15:53:13. The current records do not show whether that seven-second interval is worker polling/admission, offer buffering, or a scheduler capacity condition. The 4 seconds before DAG publication also include sequential prepared-window cache lookups in `RuntimeFinalizationProcessor.reusablePreparations`; the code issues one lookup per window, but no per-lookup timings were recorded, so its contribution is not yet measured.

Add timestamps around finalizer planning, cache lookup batches, DAG persistence, offer, acceptance, and lease grant. If the cache lookups dominate, add a batch lookup by preparation identity rather than parallelizing SQLite calls without measurement.

## Priority order

1. Fix and regression-test the post-FINALIZE `runtime_assets_pending` gate delay. This has the clearest measured opportunity: up to 33 seconds on this run.
2. Start certified stock-window work at PREPARE for immutable windows already described by the plan; keep assembly gated until late audio/overlay assets arrive.
3. Trace the 7-second offer-to-accept interval and the 4-second finalizer planning interval; optimize only the measured substage.
4. Extend early preparation to clips only after tracing their current source/preparation path; do not conflate clip audio, stock-pool members, and fixed video windows.

This note is based on the production SQLite records and Master journal for this job plus static code inspection. It does not claim the proposed overlap or gate fix has been implemented or benchmarked.
