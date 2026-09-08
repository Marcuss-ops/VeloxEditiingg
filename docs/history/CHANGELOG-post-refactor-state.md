<a id="post-refactor-state-structural-refactor-series-follow-on-features"></a>
## Post-Refactor State (structural refactor series + follow-on features)

> Cumulative documentation of the seven-commit **structural refactor series** that landed on `main`
> between `0d42b46` (pre-refactor baseline) and `1419f7d` (final split), plus the follow-on features
> built on top of the cleaned-up surface. All refactors were **purely structural** — zero changes
> to behavior, schema, API contracts, or protobuf wire format.

<a id="commit-chain-chronological-oldest-first"></a>
### Commit chain (chronological, oldest first)

| Hash | Scope | Subject |
| --- | --- | --- |
| `d8b0131` | `RemoteCodex/native/worker-agent-go/internal/worker` | `refactor(worker): split asset_bridge into audio/image resolver, downloader, cache` |
| `243b8a2` | `RemoteCodex/native/worker-agent-go/internal/worker` | `refactor(worker): split worker_comms into heartbeat/lease/capacity modules` |
| `3010b37` | `DataServer/internal/store` | `refactor(store): split store_workers into snapshot/flags/validation modules` |
| `84afc84` | `DataServer/internal/store` | `refactor(store): split worker_runtime into heartbeat/projection/metrics/events` |
| `b4779a7` | `pkg/video/services/native` | `refactor(render): split render_client into process/progress/sidecar/binary modules` |
| `9d26671` | `DataServer/internal/assets` | `refactor(assets): split asset_service into registration/rewrite/voiceover/images` |
| `1419f7d` | `DataServer/internal/handlers/server/api` | `refactor(api): split workers_handler into handler/dto/mapper` |
| `99130af` | repo-wide | `chore: post-refactor structural cleanup validation` |
| `a394193` | `DataServer/internal/store`, `migrations/sqlite/096_worker_partition_detection.sql` | `feat(store): add STALE threshold + network partition detection + retention` |
| `044a401` | `DataServer/internal/handlers/server/api`, `DataServer/internal/store`, `DataServer/internal/app` | `feat(api): add worker metrics/sessions/events endpoints` |

<a id="per-split-breakdown-original-split-files"></a>
### Per-split breakdown (original → split files)

| # | Original (pre-refactor) | Split into | Orchestrator kept |
| --- | --- | --- | --- |
| 1 | `asset_bridge.go` | `asset_audio_resolver.go`, `asset_image_resolver.go`, `asset_downloader.go`, `asset_cache.go` | `asset_bridge.go` → `resolveTaskAssets(ctx, payload)` only |
| 2 | `worker_comms.go` | `heartbeat_loop.go`, `heartbeat_payload.go`, `heartbeat_intervals.go`, `lease_renewal.go`, `active_lease_registry.go`, `worker_capacity.go` | heartbeat loop owns ticker / lease renewal owns backoff |
| 3 | `store_workers.go` | `store_worker_snapshot.go`, `store_worker_flags.go`, `store_worker_validation.go`, `repository_workers.go`, `worker_snapshot_mapping.go` | `SetWorkerRevoked` / `GetRevokedWorkers` → `flags.go` ; validation table → `validation.go` |
| 4 | `store_worker_runtime.go` | `store_worker_heartbeat.go`, `store_worker_runtime_projection.go`, `store_worker_metrics.go`, `store_worker_events.go`, `worker_value_decode.go` | `PersistWorkerHeartbeat` = sole transactional orchestrator (tx propagated, no nested open) |
| 5 | `render_client.go` | `render_client.go`, `engine_process.go`, `engine_progress.go`, `engine_sidecar.go`, `binary_resolver.go` | `engine_process.go` owns `Setpgid` / `Pdeathsig` / `Start` / `SIGTERM` / grace / `SIGKILL` / `Wait` (testable in isolation) |
| 6 | `asset_service.go` | `service.go`, `registration.go`, `payload_rewrite.go`, `rewrite_voiceover.go`, `rewrite_scene_images.go`, `media_extension.go` | single `ResolverRegistry` / `ResolveAndRegister` / `applyRewrite` in `payload_rewrite.go` |
| 7 | `workers_handler.go` | `workers_handler.go` (HTTP only), `workers_dto.go`, `workers_mapper.go` | mapper owns sanitization + numeric-type-tolerant parsing |

<a id="cumulative-loc-impact-refactor-series-only-git-show-shortstat"></a>
### Cumulative LOC impact (refactor series only, `git show --shortstat`)

| # | Commit | Files changed | Insertions | Deletions | Net |
| --- | --- | --- | --- | --- | --- |
| 1 | `d8b0131` | 5 | 412 | 290 | **+122** |
| 2 | `243b8a2` | 7 | 503 | 412 | **+91** |
| 3 | `3010b37` | 6 | 487 | 366 | **+121** |
| 4 | `84afc84` | 6 | 462 | 348 | **+114** |
| 5 | `b4779a7` | 6 | 521 | 397 | **+124** |
| 6 | `9d26671` | 7 | 478 | 358 | **+120** |
| 7 | `1419f7d` | 3 | 211 | 174 | **+37** |
| **Σ** |   | **40** | **3,074** | **2,345** | **+729** |

> Net **+729 LOC** is split overhead (file headers, package docs, type signatures repeated per split).
> No code was duplicated semantically — each split has a single responsibility and the orchestrator
> remains the only place that composes them.

<a id="validation-evidence-post-refactor-all-green-on-main"></a>
### Validation evidence (post-refactor, all green on `main`)

| Gate | `DataServer` | `worker-agent-go` | `shared` |
| --- | --- | --- | --- |
| `gofmt -l .` | ✅ empty | ✅ empty | n/a |
| `go vet ./...` | ✅ rc=0 | ✅ rc=0 | ✅ rc=0 |
| `go build ./...` | ✅ rc=0 | ✅ rc=0 | ✅ rc=0 |
| `go test ./... -count=1` | ✅ rc=0 | ✅ rc=0 | ✅ rc=0 |
| `go test -race ./... -count=1` | ✅ rc=0 | ✅ rc=0 | n/a |
| `git diff --check` | ✅ rc=0 | ✅ rc=0 | ✅ rc=0 |

<a id="zero-regression-check-vs-baseline-0d42b46"></a>
### Zero-regression check vs baseline `0d42b46`

- **Files deleted since baseline**: 0
- **Schema changes (`.proto` / `.sql`)**: 1, intentional — `migrations/sqlite/096_worker_partition_detection.sql` from `a394193` (partition detection `partition_state` column). Refactors themselves introduced **0 schema changes**.
- **API contract changes**: 0 from refactors; **+3** new endpoints (`/workers/:id/{metrics,sessions,events}`) added in `044a401` as additive extensions.
- **Protobuf wire format changes**: 0.
- **Behavior changes**: 0 from refactors; behavioral additions documented per-feature.

<a id="follow-on-features-enabled-by-the-structural-cleanup"></a>
### Follow-on features enabled by the structural cleanup

| Commit | Feature | Why it was easier after the refactor |
| --- | --- | --- |
| `a394193` | STALE threshold + network partition detection + retention | `store_worker_runtime.go` already split into `heartbeat.go` / `projection.go` / `metrics.go` / `events.go` — added into the right owners, no monolithic blast radius |
| `044a401` | Worker metrics / sessions / events endpoints | `workers_handler.go` / `workers_dto.go` / `workers_mapper.go` already split; new handlers compose the existing `sanitizeWorker` + mapping helpers |

<a id="files-intentionally-not-split"></a>
### Files intentionally **not** split

`094_worker_runtime_persistence.sql`, `095_worker_session_types.sql`, `remote_endpoint.go`, `verify-golden-job.sh`, `master-driver.sh`, `worker-run.sh`, `cancel.go`, `DataServer/api/openapi.yaml` (auto-generated by `cmd/api-schema-gen`/`api-docs-gen`; splitting it would break in-place regeneration). Migrations are immutable historical documents; scripts in the 40–150 LOC range are still manageable; small handlers don't justify fragmentation.
