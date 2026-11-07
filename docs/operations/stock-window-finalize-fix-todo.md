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

## Rollout and end-to-end acceptance

- [ ] Build the Master image from the verified commit and inspect its embedded commit/version.
- [ ] Deploy the new Master image and confirm service health/readiness.
- [ ] Re-submit FINALIZE for the prepared video job (or submit a fresh job if its request cannot be replayed).
- [ ] Verify FINALIZE returns promptly and reports `stock_preparation_queued`.
- [ ] Verify background materialization completes, the task is leased by worker `host_57_129_132_133`, and the job reaches a terminal success state.
- [ ] Verify the resulting video artifact and record its job ID and image digest.
