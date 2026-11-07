# Stock-window FINALIZE recovery checklist

## Diagnosis and implementation

- [x] Confirm why the Master job stayed `PENDING`: the synchronous FINALIZE stock materializer outlived the caller's 45-second HTTP timeout, leaving `runtime_assets_pending=true` and no schedulable task.
- [x] Persist stock FINALIZE requests in the task spec and acknowledge them with HTTP 202 before materialization.
- [x] Add a durable, indexed Master runner that resumes interrupted work after restart.
- [x] Keep a task unclaimable until stock windows and the final render plan are committed; notify the worker only after that commit.
- [x] Surface queued/running/failed FINALIZE state through the owner-scoped job polling endpoint.
- [x] Cover successful background commit and notification ordering with a unit test.
- [x] Update the sparse legacy migration fixture for migration 180.
- [x] Pass `go vet ./...` and `go build ./...` in `DataServer/`.
- [x] Pass targeted `pipeline`, `store`, and `cmd/server` tests.
- [x] Pass the migration 180 legacy-upgrade regression test.
- [x] Pass `go test -count=1 ./...` after the migration fixture update.
- [x] Recompute the renderer executor from the materialized plan and runtime assets; atomically update task, task spec, and spec hash before waking placement.
- [x] Add regression coverage for slice-to-copy executor transition and atomic task/spec executor persistence.
- [x] Re-pass targeted pipeline/store tests, `go vet ./...`, and `go build ./...` after the routing fix.

## Rollout and end-to-end acceptance

- [x] Build the Master image from verified commit `ab0b1939` (`local-ab0b1939`).
- [x] Deploy the new Master image; `/api/health` reports healthy with embedded commit `ab0b1939d61563ed60fc535c10fc790d4869a57b`.
- [x] Confirm worker `host_57_129_132_133` reconnected to the updated Master.
- [x] Build and deploy the Master image containing the post-FINALIZE executor-routing fix (`local-0d8271a0`, image `sha256:416acafe331a5d0567799b59e8561e421396ad66c3002452cf848c4c437c647c`); `/api/health` reports commit `0d8271a0491a5a7c8d2fdf8e1750292669dd7ae8`.
- [x] Confirm the durable FINALIZE processor starts and worker `host_57_129_132_133` authenticates/reconnects after rollout.
- [ ] Confirm r59f (or a replay/new equivalent) is dispatched as `video.assemble.copy.v1` after stock preparation.
- [ ] Re-submit FINALIZE for the prepared video job (or submit a fresh job if its request cannot be replayed).
- [ ] Verify FINALIZE returns promptly and reports `stock_preparation_queued` on the next Producer request.
- [ ] Verify background materialization completes, the task is leased by worker `host_57_129_132_133`, and the job reaches a terminal success state.
- [ ] Verify the resulting video artifact and record its job ID and image digest.
