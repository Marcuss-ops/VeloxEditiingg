## [Unreleased] - 2026-09-05

### Audit-driven correctness sweep (OAuth, SQLite FK, blobstore temp matcher, news fetcher, command dispatch)

- **Drive OAuth** — token refresh is now serialized under the service mutex
  with a double-checked expiry window: concurrent callers inside the
  5-minute refresh window wait for the winner and reuse its token instead
  of issuing N parallel refresh-token grants (last-writer-wins).
- **SQLite foreign_keys** — FK enforcement moved to the DSN parameter
  `_foreign_keys=true` (platform/database `sqliteDSNParams`), so EVERY
  pooled connection enforces referential integrity on connect; runtime
  `db.Exec` PRAGMAs only affect the connection that ran them. Migration 103
  now documents the DSN as the enforcement point, and the stale
  `sqliteTunePragmas` FK/synchronous entries were dropped.
- **Blobstore reconciler** — `walkFinalDir` now skips leftover
  `PromoteToCanonical` temp files with a suffix-precise matcher
  (`isBlobstoreTempName`: `<base>.tmp.<decimal>` CreateTemp pattern)
  instead of a bare `.tmp` substring, so legitimate artifacts whose names
  merely contain `.tmp` stay in the DB-diff orphan-sweep set.
- **gRPC worker metrics** — `lastSeenByWorker` growth is bounded: a lazy
  sweep (stale-entry eviction + hard cap, oldest-first fallback) runs when
  a NEW worker first contacts the master, never on session teardown or
  heartbeat rate.
- **News fetcher** — cache map is mutex-guarded and bounded (cap + expiry
  eviction), outbound requests use a shared bounded client and limited body
  reads, per-source failures are joined into one diagnostic error, and
  `parsePublishedAt` never silently zeroes a timestamp (RFC3339/1123/date
  ladder, error on garbage).
- **Alerts** — the Telegram notifier refuses to send when the configured
  webhook URL carries no `chat_id` parameter (`ErrTelegramChatIDMissing`)
  instead of failing with a misleading provider-side 400.
- **Enqueue** — unique-conflict classification no longer falls back to
  untyped `strings.Contains(err.Error(), "jobs.job_id")`; only a typed
  mattn `sqlite3.Error` with the jobs.job_id constraint is treated as an
  idempotent-conflict success.
- **Command dispatch (poll → wake)** — a persisted `worker_commands` row
  now immediately wakes connected sessions via `Handler.NotifyCommand`
  (wired through `CommandManager.SetWakeHook`); the in-stream 1s ticker
  remains purely as a loss-recovery backstop.
- **Render-performance rollup** — migration 172 adds an expression index on
  the day filter
  (`substr(COALESCE(NULLIF(completed_at,''), updated_at),1,10)`) so the
  daily rollup seeks instead of scanning; test pins the query plan uses it
  (both `=`/`<`) and source-pins the live rollup expression.

## [v1.4.2] - 2026-08-28

### Preparation gate test correction

- Preserve explicit legacy empty reservation state in test fixtures while
  retaining the strict `PREPARED` claim fence in production.

## [v1.4.1] - 2026-08-28

### Strict preparation claim fence

- Keep asset-bearing READY tasks out of `ClaimTaskForWorkerAtomic` until the
  worker has a matching reservation-scoped prepared certificate.
- Preserve the normal claim path for tasks without required assets.

## [v1.3.9] - 2026-08-28

### Strict preparation gate

- Add reservation-scoped prepared-asset lineage and an opt-in claim gate so
  strict workers do not create an Attempt before required assets are ready.
- Carry the task revision fence through worker prefetch lifecycle events.

## [v1.3.8] - 2026-08-28

### Release metadata

- Refresh canonical worker build metadata so the image version and checkout
  remain aligned during certification.

## [v1.3.7] - 2026-08-28

### Release verification

- Accept indented Buildx digest output when verifying the published worker
  image against the immutable digest emitted by the build.

## [v1.3.6] - 2026-08-28

### Prefetch certification and benchmark context

- Certify asset preparation, cache binding, prefetch origin, and durable
  worker telemetry for execution inspection.
- Pass worker context and benchmark identity through the render runner so
  scorecards measure the actual worker/cache/concurrency configuration.

## [v1.3.5] - 2026-08-27

### Observability timeline labels

- Use the canonical event name, action, or component fallback when rendering
  durable execution events in operator inspection.

## [v1.3.4] - 2026-08-27

### Observability timeline

- Expose the durable task execution timeline in `job inspect` and the job
  events endpoint alongside the legacy lifecycle journal.
- Preserve compatibility with stores that predate the execution-event table.

## [v1.3.3] - 2026-08-27

### Benchmark scorecard validation

- Persist worker benchmark results and scorecard validation data.
- Add the `benchmark-collect` fleetctl command for Matt Damon workload measurements.
- Keep the worker image release pinned to a new immutable digest for this checkout.

## [v1.3.2] - 2026-08-27

### Worker admission and progressive part tuning

- Publishing and commit-wait no longer consume render admission capacity;
  active publication remains bounded by the existing `PublisherPool`.
- Progressive upload part concurrency is configurable through
  `VELOX_PROGRESSIVE_PART_CONCURRENCY`, defaulting to 4 for compatibility and
  allowing controlled 4-vs-8 benchmarking.
- Progressive COMPLETE, artifact locking, resume and the fMP4 disabled gate
  are unchanged.

## [v1.3.0] - 2026-08-24

### Worker image v1.3.0 — warm assembly prefetch + delivery consolidation

Canonical worker image release built from the v1.2.43 codebase. Published
and deployed to all 4 fleet workers (`host_57_129_132_133`,
`host_57_131_20_173`, `velox-worker-13197`, `velox-worker-523925eb`) via
canary rollout. Image digest: `sha256:ca617b2ef22344cd64ebc428501217973f8cfc0b656108d7cea810f1e9aaa11a`.

**Features:**

- `92f874f1` — Complete warm assembly prefetch path: workers now pre-fetch
  and stage assembly inputs before job assignment, reducing cold-start
  latency for scenes with cached source assets.
- `4d1ba8e0` — Consolidate delivery runner and session handling: unified
  session lifecycle management reduces code duplication across delivery
  paths and simplifies error propagation.

**Fixes:**

- `79876991` — Preserve applied migration checksum: the schema migration
  verifier now correctly handles the applied-migration checksum across
  restarts, preventing spurious migration re-runs.
- `6e672fef` — Handle multiline master image tags: the fleet controller
  correctly parses Docker image tags that span multiple lines in the
  deployment configuration.

**Ops:**

- Canary rollout script and test harness pinned to v1.3.0 image digest.
- CI build pipeline (`worker-image.yml`) triggered and certified all steps
  (build, sign, verify, baseline, real-bootstrap) in 8m44s.

## [Unreleased] - 2026-08-27

### STEP D — asset_preparation drill-down on the wire (wall vs work)

The waterfall STEP A–C chain (canonical attempt milestones, durable reports,
`WaterfallBuilder` with `coverage_pct`/`unaccounted_ms`, execution-level
waterfall block for `fleetctl job inspect`) is closed by the drill-down INSIDE
the dominant bucket. Wire→ingest→read model, one atomic tranche:

- `proto/velox/control/worker_control.proto` — new `AssetPreparationBreakdown`
  message + `TaskResult.asset_preparation = 24`; descriptor regenerated via
  `scripts/gen-proto.sh` (`shared/controltransport/pb`).
- Worker — the resolver-sink accumulator already measured the per-attempt
  drill-down (cache lookup / remote wait / download wall-vs-work / hash verify /
  metadata probe / local materialize); it now rides the typed report field and
  `buildTaskResult` maps it 1:1 onto the pb message. An idle resolver attaches
  NO breakdown: absence stays honest, never zero-filled.
- Master — `raw_report_json` decode accepts protojson camelCase string-int64
  AND snake_case number spellings; the breakdown rides `AttemptWaterfall` and
  therefore the §20 execution `waterfall` block verbatim. Sub-phase sums may
  overlap (parallel downloads) and are deliberately never re-combined into a
  coverage number master-side.
- Pins — worker: `TestAssetPreparationSummary_AggregatesPerAttemptDrillDown`
  extends to the typed field; idle-tracker absence locked in
  `TestAttachAssetOperationsPreservesAbsentCacheFacts`. Master:
  `TestDecodeAttemptWaterfall_CarriesAssetPreparationDrillDown` locks the exact
  protojson wire shape end-to-end from raw report to read model.

Verification given the Windows host: cross-compiled linux/amd64 build+vet of
the touched worker packages and the regenerated pb, full `velox-shared`
telemetry vet/test, DataServer observability package green (`go vet` +
`go test -count=1`).

## [Unreleased] - 2026-08-16

### Content-addressed asset cache + pressure LRU + volatile artifact staging

A 7-commit tranche on `main` (dependency-ordered, no branches) that replaces
the worker's TTL asset cache with a content-addressed blob store, converts
cache eviction to a pressure-based LRU, and introduces volatile tmpfs staging
for the final render artifact. Full-module verification in `worker-agent-go`
(`go vet ./...`, `go build ./...`, `go test -count=1 ./...`) green.

**Content-addressed blob store (schema v5) — 2 commits:**

- `c7ac05bc` — `workercache` splits the cache into `cached_blobs`
  (`content_hash` → `local_path` / `size_bytes` / `verified_at` /
  `download_complete`) and `cached_assets` (`asset_key` → `content_hash`). A
  verified blob lives at `<cache>/<sha[:2]>/<full-sha>`, so distinct asset IDs
  with identical bytes share one physical file. The forward-only migration
  preserves lease/reservation state; hashless rows become per-asset `legacy:`
  blobs.
- `54d720a4` — `CacheResolver` routes through the blob store: a known SHA
  probes `cached_blobs` directly (stat + size, no re-hash), and the downloader
  single-flights on `content_hash`, so concurrent jobs sharing bytes download
  once.

**Pressure-based LRU eviction (80%/72% hysteresis) — 2 commits:**

- `3a18858c` — `CleanupLoop` becomes the cache pressure controller:
  `EvictUnderPressure` evicts nothing below the HIGH watermark and evicts LRU
  blobs down to the LOW watermark. Eviction is blob-scoped and gated by
  lease/reservation/snapshot protection, retaining the `asset_key` → SHA
  mapping. Adds `VELOX_CACHE_HIGH/LOW_WATERMARK_PERCENT`,
  `VELOX_CACHE_EVICTION_BATCH_SIZE` / `_INTERVAL_SECS`, a statfs usage probe,
  and the pressure eviction metrics.
- `3a295cea` — `EvictUnderPressure` is pure; the `CleanupLoop` OnTick is the
  single Prometheus boundary (removes the eviction double-count and adds the
  evicted-bytes + disk-usage gauges).

**Volatile artifact staging (ARTIFACT_STAGING) — 3 commits:**

- `3a18858c` — `storage.Resolver` gains the `ARTIFACT_STAGING` class with a
  tmpfs RAM reservation ledger (`MaxPercent` ceiling + `ReserveBytes`
  headroom) and durable-NVMe fallback; the spool records `storage_tier`
  (`TMPFS_VOLATILE` / `NVME_DURABLE`). Upload failure and graceful shutdown
  spill volatile artifacts to NVMe; the tmpfs file + reservation are freed
  only after the terminal `TaskResultAck`.
- `8234b54e` — `render_batch@1` and `encode@1` resolve their final output
  through `Place(ArtifactStaging, ...)` instead of hardcoding `outputRoot`;
  the uploader interface is unchanged (`ArtifactRef.URI` remains the
  `LocalPath` the publisher opens).
- `c724bf66` — promoted content-addressed blobs are chmod `0444` (immutable
  from the normal worker); re-download (atomic rename) and eviction (unlink)
  remain directory operations.

**Acceptance tests — 1 commit:**

- `1f45d495` — e2e staging acceptance: tmpfs render → upload → commit →
  unlink + reservation release only after the terminal `TaskResultAck`, and
  insufficient-RAM → `no_space` fallback to NVMe without failing the job.
  Cache-level acceptance (3 asset IDs → one file, zero eviction under 80%, LRU
  down to 72%, lease/reservation/snapshot-protected blobs) shipped in
  `3a18858c`.

### Removal of the pre-v5 flat filesystem fallback + index-driven cache-evict

Follow-up on the content-addressed blob store: the downloader no longer
reads or writes the pre-v5 flat `<assetID>_<sha12>.ext` layout, and the
`velox-cache-evict` admin tool now resolves asset paths exclusively through
the SQLite index.

- `internal/worker/asset_cache.go` — `cachedAssetPathTimedWithContext` drops
  the flat migration fallback: a hit requires the full integrity contract
  (SHA + size) and probes only the content-addressed blob glob
  `<cacheDir>/<sha[:2]>/<sha>.*`. `verifyAndPromoteVeloxAsset` always promotes
  to `assetBlobPath` (the flat `finalPath` branch is gone). The dead `assetID`
  parameter was removed from both; `cacheKeyPrefix` was renamed
  `assetPartialKey` and now serves only the resumable `.part` namespace.
- `internal/cacheevict` — `discover`/`matchesAssetFile`/`allowedCacheExtension`
  /`shaPrefixPattern` removed; `Run` resolves asset → blob path only via
  `Index.Find → LocalPath` and deletes through the fenced `EvictIfUnleased`
  (correct blob-level dedup: the physical file is removed only when the blob
  becomes orphaned). `cmd/velox-cache-evict` now requires `--index` in both
  dry-run and execute modes (read-only open for dry-run).
- Tests — `TestDownloadVeloxAsset_IgnoresLegacyFlatFile` pins the miss +
  re-download contract: a flat file carrying the correct bytes is never
  resolved, and the asset is promoted to the content-addressed blob path.

Full-module verification in `worker-agent-go` (`go build ./...`,
`go vet ./...`, `go test -count=1 ./...`) green.

## [Unreleased] - 2026-08-15

### Refactor tranche — telemetry SSOT, foundation layering, observability, and complexity reduction

A 13-commit atomic tranche on `main` (commit-at-a-time, dependency-ordered,
no branches) covering four themes. Full-module verification
(`bash scripts/ci/pre-removal-verify.sh` in `DataServer` → vet 0, build 0,
test 0, 197s) plus `worker-agent-go` and `shared` build/vet all green.

**Telemetry single-source-of-truth vocabulary (3 commits):**

- `f0961cb3` — `shared/telemetry` now declares the canonical phase and
  attempt-event vocabulary as the SSOT root, so the Go and C++ bindings
  and every consumer resolve one language-neutral source instead of a
  parallel `canonicalEventKeys` / `canonicalOriginScope` / `canonicalPhaseSpecs`.
- `4172c0f2` — `worker-agent-go` aliases the shared vocabulary instead of
  maintaining its own copy.
- `53f6de19` — `internal/store` consumes the shared attempt-event vocabulary
  (event-name switch) instead of its own constants.

**Foundation layering + dead-code / YAGNI cleanup (5 commits):**

- `9be8e4e2` — `internal/storecore` drops the `database/sql` import from its
  error leaf (`RowsAffectedReader`), so the error contract no longer depends
  on a concrete driver; SQL ratchet moved to the store layer.
- `75487d2d` — `internal/fleet` reuses the canonical `store.SmokeStatus*`
  enum; the forked smoke-status enum is removed (single source of truth).
- `4d6494c5` — dead exported helpers removed from `internal/store` (7
  functions + `disconnectedAt`).
- `a93d21e6` — the single-implementation `Dialect` abstraction removed from
  `internal/store` (YAGNI; SQLite is the only backend after the Postgres
  removal).
- `ef42692f` — `internal/netsecurity` unifies non-public IP classification
  into one canonical helper (removes duplicated classification logic).

**Observability + asset transfer (3 commits):**

- `a0ff8bd0` — `internal/remoteengine` gains structured logging and
  Prometheus telemetry.
- `6c16b15c` — master asset bridge serves HTTP Range (206) requests for
  staged Drive/external sources (`http.ServeContent`).
- `d9155aff` — `worker-agent-go` downloads assets via parallel chunked
  Range requests (client side of the Range-serving feature).

**Complexity reduction + architecture guard (2 commits):**

- `430b3f13` — `internal/store` `computeParallelism` and
  `internal/observability` `SummarizeTask` refactored into pure, testable
  helpers (`collectValidSegments` / `sweepSegments`, `rollupPhaseTimings` /
  `mergeWallBounds` / `earlierTime` / `laterTime`) with new table-driven
  tests; `SummarizeTask` drops 48 → 35 branches.
- `1b4ae5a4` — `scripts/ci/check-architecture.sh` adds guard rule #13:
  upward imports and network I/O (`net/http`) in foundation packages are
  statically forbidden, pinning the layering boundaries in CI.

## [Unreleased] - 2026-07-29

### Added — parallel chunked asset download + Drive asset Range serving

Per-asset parallel chunked download on the worker, plus HTTP Range (206)
serving on the master asset bridge for staged Drive/external sources.

- Worker: `masterAssetTransferer` now splits an asset at/above the chunked
  threshold into N parallel `Range: bytes=start-end` requests, writes each
  chunk at its offset into a pre-allocated `.part` (`fallocate(2)` on Linux,
  sparse truncate elsewhere) via `WriteAt`, and reports aggregate progress
  through a shared atomic counter. The single-stream resumable path remains
  the automatic fallback when the upstream ignores Range. The integrity gate
  (size + SHA-256 before atomic promotion) is unchanged.
- Config: `asset_chunked_download_enabled` /
  `asset_chunked_download_threshold_bytes` (default 64 MiB) /
  `asset_chunked_download_concurrency` (default 4) in `worker_config.json`,
  bound from `VELOX_ASSET_CHUNKED_DOWNLOAD` /
  `VELOX_ASSET_CHUNKED_DOWNLOAD_THRESHOLD_BYTES` /
  `VELOX_ASSET_CHUNKED_DOWNLOAD_CONCURRENCY`. Fail-closed: enabling the
  feature with a zero/negative threshold or concurrency is rejected by
  `Validate()`.
- Master: `GET /api/v1/agent/assets/:id` now serves Range requests for
  staged Drive/external sources via `http.ServeContent` (previously a plain
  `io.Copy`), matching the local-final-blob path.

### Added — capability state machine rule (DISABLED / READY / MISCONFIGURED)

Codified in AGENTS.md §6 after the 2026-08-10 fail-open audit: every
capability is `DISABLED`, `READY` or `MISCONFIGURED` — never "enabled but
with a hidden nil/noop/stub". Noop/test doubles are test-only;
constructors fail closed on missing dependencies; every
`AddReadinessCapability` exposure is paired with a fail-closed
`AddReadinessCheck` gate so MISCONFIGURED flips /ready red.

- `scripts/ci/check-capability-contract.sh` — static CI gate (forbidden
  fail-open symbols in production code + readiness pairing pin).
- `.github/workflows/capability-contract.yml` — wires the gate + the
  runtime pairing test.
- `HealthModule.CapabilityNames` / `CheckNames` accessors + runtime pin
  `TestReadiness_CapabilityExposuresHaveFailClosedGates` in
  `cmd/server/bootstrap_readiness_test.go`.

### Removed — Postgres backend (runtime + test-only adapters)

Postgres is not an active user story for the master. `VELOX_DB_DRIVER=postgres`
was already rejected by `Config.Validate()` before any database I/O; this
change removes the entire dormant backend from the production tree:

- `internal/platform/database`: `DriverPostgres`, `openPostgres`, the `URL`
  connection field and the `pgx` import are gone — SQLite is the only driver.
- `internal/store`: deleted `postgres_artifact_repository.go`,
  `postgres_job_delivery_counter.go`, `postgres_jobs_dialect.go` and
  `postgres_jobs_repository.go`; the `Dialect` contract is now SQLite-only.
- `internal/store/migrations/postgres/` (24 files) and
  `PostgresMigrationsFS` removed; the migration runner serves the SQLite chain
  only.
- Env-gated Postgres contract tests, `testdb_helpers.go`, the postgres
  factories and `DataServer/run-tests-postgres.sh` deleted.
- `github.com/jackc/pgx/v5` dependency dropped.

A future cutover must rebuild the Postgres chain from scratch (see
`docs/` for the pre-removal audit). `tests/e2e/master-worker-lifecycle`
keeps `VELOX_DB_DRIVER=postgres` as its master fail-fast probe — the
config boundary still rejects it.

### Tracked finding — pre-existing import cycle in `internal/metrics` (uncommitted WIP)

Full-module verification (`go vet ./...` + `go build ./...` +
`go test -count=1 ./...` in `DataServer`, as mandated by the
post-split verification rule) surfaced an **import cycle** that makes
`go test ./internal/metrics/` fail with `[setup failed]`:

```
internal/metrics (test render_performance_test.go)
  → internal/store (store_assets.go)
    → internal/assets (service.go, UNCOMMITTED)
      → internal/metrics (velmetrics import, UNCOMMITTED)
```

**This is NOT a regression from the file-splitting refactor.** The
cycle exists only because of **uncommitted work-in-progress from
another session** in `DataServer/internal/assets/` (the `velmetrics
"velox-server/internal/metrics"` import + `SecurityMetricFamilies` in
`service.go`). At `HEAD` (committed tree) `assets/service.go` does not
import `metrics`, so no cycle exists. None of the split commits touch
`internal/assets` (last commit there: `9d26671d`, pre-existing).

Per AGENTS.md §4 this is a **finding, not a blocker**: it must be
tracked as a followup for the owning session to resolve when the
`assets` WIP is completed. The post-split verification rule remains
active for future splits; the working tree was left untouched.

**RESOLVED** — the owning session committed `70c7dc9f feat(security):
harden all remote input acquisition`, and the committed
`assets/service.go` does **not** import `metrics` (verified at HEAD).
Full-module verification now passes: `go test -count=1 ./...` in
`DataServer` is green, `internal/metrics` passes (0.685s), and the
workspace (`DataServer` + `worker-agent-go` + `shared`) builds clean.
The remaining untracked `assets` files (`video_segments.go`,
`video_trimmer.go` + tests) are additive work-in-progress and no
longer introduce the cycle.

Separate event (not part of this finding): the first full-module run
also surfaced a transient `TestGenerateWithImages_UsesCreatorStageWhenConfigured`
failure in `internal/handlers/server/script`, which passed 3/3 in
isolation and in the full-package run — a load-parallelism flake, not a
regression, and not reproducible in isolation.

### File-splitting refactor — large files broken down by domain

A 28-commit pass broke the repo's largest files (>600 lines) into
per-domain files. Each split is a **verbatim extraction**: no symbol,
error string, SQL statement, or test assertion changed — only file
placement. Facades, interfaces, public signatures, and call sites are
untouched; every package still compiles and passes its full test suite
identically.

**Why:** single files above ~600 lines are hard to navigate, review,
and reason about; splitting by domain (entity / responsibility /
message type / phase) makes each file self-describing and keeps
diffs reviewable. The rule applied to every split: work directly on
`main` (no branches), verify with `gofmt` + `go build ./...` +
`go vet ./...` + `go test`, and commit + push immediately after each
extraction.

| Base file (before) | Lines | Split by | Resulting files | Commits |
|---|---|---|---|---|
| `internal/metrics/collector.go` | — | metric family | `collector_attempts.go`, `collector_cache.go`, `collector_costs.go`, `collector_engine.go`, `collector_ffmpeg.go`, `collector_health.go`, `collector_render.go`, `collector_sinks.go`, `collector_video.go`, `collector_workers.go` | `b10f7a30` |
| `internal/grpcserver/handler_jobs.go` | 686 | message type | `handler_accept.go`, `handler_reject.go`, `handler_renewal.go`, `handler_result.go` | `5e59cc19`, `f6b161a7`, `f3d9d74b`, `5679b646` |
| `internal/store/sqlite_task_atomic_persistence_helpers.go` | 652 | entity | `sqlite_task_atomic_persistence_attempt.go`, `sqlite_task_atomic_persistence_task.go`, `sqlite_task_atomic_persistence_lease.go` | `60ae3fee`, `a531bd99` |
| `internal/services/drive/service.go` | 614 | sub-domain | `folders.go`, `groups.go`, `tokens.go`, `master.go` | `52082c0e`, `6c2b71b3`, `556a9e1c`, `b056be63` |
| `internal/store/sqlite_task_atomic_claim_test.go` | 971 | test scenario | `sqlite_task_atomic_claim_failures_test.go`, `sqlite_task_atomic_claim_concurrency_test.go` | `93880800`, `57ce329f` |
| `internal/artifacts/reconciler.go` | 628 | responsibility | `retry.go`, `cleanup.go` | `c63299ff`, `8c20e174` |
| `internal/handlers/server/pipeline/pipeline_run_actions.go` | 634 | run phase | `pipeline_run_submit.go`, `pipeline_run_progress.go`, `pipeline_run_completion.go` | `3352dd71`, `7a50b30e`, `fd2eba57` |
| `internal/store/sqlite_task_lease.go` | 661 | lease operation | `sqlite_task_lease_claim.go`, `sqlite_task_lease_renew.go`, `sqlite_task_lease_expire.go` | `b7391111`, `3f2a0c40`, `38d0a4ae` |
| `worker-agent-go/cmd/velox-worker-agent/main.go` | 702 | new package | `internal/bootstrap/config.go` + `internal/bootstrap/dispatch.go` (main.go trimmed to a thin composition root) | `e88a9f6d` |
| `internal/ingest/service.go` | 714 | concern | `identity.go`, `timing.go`, `job_transitions.go` | `be15fa27` |
| `internal/store/store_worker_control.go` | 765 | DB table | `store_worker_commands.go`, `store_worker_sessions.go`, `store_worker_credentials.go` | `91cdc63a` |
| `internal/publishing/resolver.go` | 628 | responsibility | `resolver_normalize.go`, `resolver_selection.go` | `a6ec3741` |
| `internal/remoteengine/client_test.go` | 946 | client behavior | `client_auth_test.go`, `client_retry_test.go`, `client_contract_test.go`, `client_http_errors_test.go`, `client_endpoints_test.go` | `13601bef` |

**Validation on every split** (before push):

- `gofmt` clean on all extracted files; `go build ./...` + `go vet ./...` pass on the affected module.
- `go test -count=1` passes on the affected package(s) with identical results (e.g. `internal/remoteengine` 38s green post-split, `internal/store` 105s green post-split).
- A code review pass confirmed verbatim extraction, complete per-file imports, and no orphaned/duplicated symbols.

### Technical-debt cleanup audit — proven unused metric adapter removed

The final cleanup audit removed only `metrics.Collector.ScanAttempt`, an
exported compatibility adapter with no production or test references. The
supervisor already uses `ScanAttemptWithLabels`, so the removal does not
change the active metric-ingestion path.

The audit explicitly retained compatibility aliases, migration metrics,
validation endpoints, and the validation-store wiring because each still has
runtime registration, callers, tests, or an operator-facing contract. No
endpoint was removed based on roadmap status alone, and no mutable global
state was changed in this tranche.

Validation for this removal is gated by `scripts/ci/pre-removal-verify.sh`
(the full `DataServer` vet/build/test gate) before any push to `main`.

### PR-15.17 — Runbook §0.1/§0.2/§0.3 emission

Promotes the Velox → Social API migration runbook to a complete
cross-repo operator map. The 5-commit chain covers: round-1 initial
§0.1/§0.2/§0.3 emission (env-var bootstrap, 4 channel-readiness
prerequisites, sender-side `destination_id` selection); round-2
canonical-path / canonical-function-name alignment in §0.2;
round-2 §0.2.2 triage table aligned with the canonical
target_resolver.go taxonomy (BLOCKED_AUTH / TARGET_NOT_AVAILABLE +
the underlying `platform_accounts.status` enum); round-3 §0.3.4
catalog-verdict list aligned with the canonical taxonomy;
round-4 pinning of the `platform_accounts.status` enum to
`InstaeditLogin/internal/models/user.go:49-72` with the canonical
8-value declaration (`active`, `reauth_required`, `revoked`,
`disconnected`, `expired`, `error`, `pending_authorization`,
`suspended`).

Operator-visible outcome: a sender or on-call reading the runbook
now sees one canonical mapping per condition (the §0.2 chart) and
one canonical triage row per verdict (the §0.2.2 cheat-sheet); the
non-canonical codes `binding_disabled` / `account_inactive` that
drifted through earlier drafts have been REMOVED from the runbook
in favor of the canonical taxonomy.

CHANGELOG anchor: PR-15.17. Commits in this anchor (already on
`main`):

  - `422e5c1`  `docs(runbook): add §0.1/§0.2/§0.3 (env bootstrap, channel prerequisites, sender-side destination_id selection)`
  - `cdec3c7`  `docs(runbook): replace speculative function names + paths with verified canonicals in §0.2`
  - `fb1f663`  `docs(runbook): align §0.2.2 triage with canonical taxonomy (round-2)`
  - `736e1ee`  `docs(runbook): align 0.3.4 catalog-verdict list with canonical taxonomy (round-3)`
  - `74973df`  `docs(runbook): pin platform_accounts.status enum to user.go:49-72 + correct 8 canonical values (round-4)`

### Fleet Operator: 4/4 workers — 16/16 health checks passing

Complete fleet audit, onboarding, and hardening session. All 4 remote
workers are reachable via SSH key auth, connected to the Master, and
passing the full 4-level health probe (A=host, B=container, C=registry,
D=smoke).

**Fleet Health Matrix (final):**

| Level | Worker | 57.129 | 57.131 | 523925 | 13197 |
|---|---|---|---|---|---|
| A | Host (SSH, CPU, disk, Docker, NTP) | ✅ | ✅ | ✅ | ✅ |
| B | Container (running, /ready, digest, restart) | ✅ | ✅ | ✅ | ✅ |
| C | Master (status, session, executor, heartbeat) | ✅ | ✅ | ✅ | ✅ |
| D | Smoke (lease→ffmpeg→artifact→delivery) | ✅ | ✅ | ✅ | ✅ |

**Onboard `host_57_129_132_133`** (57.129.132.133, pierone, vps-21accdce):
- Port 22 was open — previous "connection refused" was a transient
  network issue or the key wasn't configured yet.
- SSH key auth configured (`id_ed25519_velox`), sudo works, `pierone`
  already in `docker` group.
- Cleaned 11.25 GB: 55 old Docker images + 7 stopped containers +
  `/tmp` residue. Disk 60% → 42%.
- Created `/var/lib/velox-worker/smoke` (was missing on host, needed
  for SSH-level smoke commands).
- Fixed `health_port: 8138 → 8081` in `worker_config.json`.

**Smoke Level-D now working on all 4 workers**:
- `asset://` pickup URLs (StubAssetResolver) treated as dev-mode
  fallback — generate ffmpeg lavfi clip instead of curl (which can't
  resolve the synthetic scheme). Applied to both `SSHWorkerExec` and
  `LocalShellWorker`.
- Asset resolver wired in production mode (was nil, causing
  `smoke_runner_not_wired`). Drive falls back to `LocalFileDriveUploader`
  when Google Drive isn't configured.
- SSH client map covers all 4 workers (was 3; added worker-129).
- Key deployed at `/etc/velox/ssh/id_ed25519_velox` on the Master.

**Container name aligned on `velox-worker-13197`**:
- `chronon.conf` had `--name velox-worker-13197` → renamed to
  `velox-worker-velox-worker-13197` matching the convention used by
  the other 3 workers.

**`health_ready` fixed on workers 129 and 13197**:
- NOT a port binding issue (both already use `--network host`).
- Root cause: `health_port` in `worker_config.json` was 8138 (129)
  and 8132 (13197). Fixed → 8081. The Level B probe curls 8081.

**`image_digest_match` enabled on all 4 workers**:
- Populated `deployment_records` table with SUCCEEDED records
  carrying each worker's current image digest.
- 3 workers on `sha256:a1774003...`, worker-13197 on `sha256:63fd3a...`.

**Ansible inventory + vault**:
- `inventory.ini`: SSH users corrected (pierone/ubuntu/debian — no more
  `velox-deploy`), `container_name` per-worker var, all 4 workers ✅.
- `group_vars/vault.yml`: encrypted with `ansible-vault`, contains
  `vault_velox_admin_token` + `vault_velox_sudo_password`.
  Password file at `~/.vault-velox-pass` (0600, NOT committed).
- `fleet-restart.yml`: dual-mode auto-detection (compose vs raw docker)
  with per-worker `container_name` support.

**Health probe code fixes** (Go backend):
- `hasExecutorAdvertisement`: added `"executors"` key check — workers
  send proto-structured list under this key, not legacy
  `supported_executors`. Was causing false negative on all workers.
- SSH client wired into health handler (was nil → Level A+B were
  audit-only, returning "ssh client not wired").

**Docker cleanup — 33 GB reclaimed across 4 workers**:
- 112 old Docker images removed (chronon alpha 1-5, v1.0-v1.2.x,
  golang, qdrant, ubuntu, busybox, hello-world, the worker console image).
- 7 stopped containers pruned.
- Old `/tmp` directories cleaned on all workers.
- `worker-13197`: 82% → 77% (was the critical one).

**Commit chain on `main`** (all atomic, oldest → newest):
- `09f5c9c` feat(ansible): add sudo password to vault, fix SSH users
- `ae29413` fix(health): add "executors" key to hasExecutorAdvertisement
- `b826934` feat(health): wire shared SSH client into health handler
- `4306390` feat(fleet-restart): auto-detect compose vs raw docker
- `a47d098` feat(fleet-restart): container_name per-worker in inventory
- `14d9cd2` fix(inventory): worker-129 now reachable via SSH
- `98bcb5e` feat(smoke): add worker-129 SSH target + Asset/Drive fallbacks
- `c4c8fcf` fix(smoke): treat asset:// pickup URLs as dev-mode fallback
- `ef9657f` docs(changelog): fleet-operator 4/4 workers onboarded + Level-D smoke

## [v1.3.0-creator-push] - 2026-07-25

### New intake path: `POST /api/v1/creator/jobs`

The Master now accepts **creator-initiated job pushes** directly from the
Creator app. The new HTTP endpoint:

```http
POST /api/v1/creator/jobs
Authorization: Bearer <VELOX_ADMIN_TOKEN>
Content-Type: application/json
```

returns `202 Accepted` after transforming the typed payload
(`RemotePipelineResult`) and routing it through the **canonical Resolver**
— the same single write path used by the legacy Creator runner. The
Resolver is the **only writer** for `creator_forwardings + jobs + tasks`;
the new handler does not write to the database directly. The standing
architectural invariant "no parallel writers" is preserved.

**Wire contract (202 envelope):**

```json
{
  "ok": true,
  "accepted_from": "creator_push",
  "source_provider": "creator_pc_1",
  "source_job_id": "creator-job-001",
  "target_executor_id": "scene.composite.v1",
  "job_id": "job_...",
  "status": "PENDING",
  "dispatch_status": "queued_for_workers"
}
```

The `accepted_from=creator_push` overlay lets operators distinguish the
new path from the legacy Creator flow in logs/metrics; the
`dispatch_status` overlay (documented in `[Unreleased]` below) is
preserved verbatim and surfaces the upstream Resolver emission when one
exists (e.g. `"dispatching"` / `"dispatched"`).

### Files added or modified

- `DataServer/internal/handlers/server/pipeline/creator_push.go` — endpoint, typed DTO normalization, identity derivation
- `DataServer/internal/handlers/server/pipeline/creator_intake.go` — typed intake sink + counter `accepted_from={creator_push,legacy}`
- `DataServer/internal/handlers/server/pipeline/creator_push_e2e_test.go` — real-`VELOX_ADMIN_TOKEN` E2E, idempotency replay, DB row assertions
- `DataServer/internal/handlers/server/pipeline/forwarding.go` — common adapter shared by creator_push + legacy remote-engine
- `DataServer/internal/metrics/catalog_pipeline.go` — adds `creator_intake_accepted_total{accepted_from}`
- `DataServer/cmd/server/router.go` — composition root wires `WithIntakeSink(velmetrics.NewCreatorIntakeSink())` on the pipeline handler
- `DataServer/api/openapi.yaml` (NEW, 698 lines) — canonical OpenAPI 3.1.0 spec for the Master API surface (`CreatorPushRequest`, `CreatorPushPayload`, `RemotePipelineResult`, `CreatorPushAcceptedResponse`, `ErrorEnvelope`, `ErrorCode`)
- `scripts/api/validate_openapi.py` (NEW) — PyYAML≥6.0 standalone validator (bidir `ErrorCode` equality, 401/422/500→`ErrorEnvelope` enforcement, exit 0 only on all invariants)
- `scripts/creator_push_smoke.sh` (NEW) — operator smoke test for the new endpoint
- `docs/CREATOR-PUSH.md` — full contract + operator runbook
- `docs/ARCHITECTURE.md` — Resolver-as-unique-writer callout
- `CHANGELOG.md` — this entry

### Architectural invariant: Resolver-as-unique-writer

The new handler **never** writes to the database directly. It always
calls `creatorflow.Resolver.Resolve` so the same atomic
`forwarding + Job + Task` triple is produced whether the job originated
from the legacy Creator runner, the remote-engine fan-out, or the new
creator_push path. Future intake surfaces MUST go through the same
Resolver; any parallel writer path is a regression.

### Tag

`v1.3.0-creator-push` is annotated on commit
`c2f3b6661564665eee7372dc3f82e0e8c5b2c6d1` (the canonical creator-push
docs commit), **not** on HEAD. Subsequent commits
(`c5ebae8`, `f26695b`, `a069579`, `d4970f2`, `6d8e8f1`) build on top of
`c2f3b66` and are NOT pinned by this tag — the tag marks the **first
canonical commit** at which the creator-push intake was documented as
a coherent feature surface. Future operators wanting to inspect the
feature boundary should `git checkout v1.3.0-creator-push` and read
`docs/CREATOR-PUSH.md` from that tree; HEAD always carries the latest
fixes layered on top.

### Verified on `main` (commit `6d8e8f1`)

- `python3 scripts/api/validate_openapi.py DataServer/api/openapi.yaml` → `--- TOTAL PASS: 1 openapi file(s) meet all invariants ---` (exit 0)
- `cd DataServer && go build ./...` → exit 0 (full module, post-`6d8e8f1` legacy editor wire cleanup)
- `cd DataServer && go vet ./...` → exit 0 (no diagnostic-level findings beyond the unrelated `bootstrap_composition.go` unused-imports warning from pre-session refactor WIP)
- `go test -run IntakeSinkOrNoop ./internal/handlers/server/pipeline/...` → 3/3 PASS
- `git push origin v1.3.0-creator-push` → exit 0

### Migration notes

Operators currently running `velox-server` on `v1.2.21-yt-removed` can
adopt `v1.3.0-creator-push` (or any later HEAD) without config changes:

- The new endpoint is **additive** — `POST /api/v1/creator/jobs` is a
  new path that does not affect any existing route.
- The `accepted_from` enum is currently `{creator_push}`; the legacy
  runner continues to emit `accepted_from=legacy`.
- `VELOX_ADMIN_TOKEN` is the same env var that protects admin routes
  today — no new secrets required.
- Strict-mode JSON consumers should add `dispatch_status` to their
  accepted-key allowlist (see `[Unreleased]` entry below).

## [Unreleased] - 2026-07-25

### Removal: `/api/remote/pipeline` fully retired

Initially soft-deprecated in commits 51a307d→5d484c4 (6 commits with telemetry + docs); the user pivoted and full removal landed in commits d433e97→c322182, tagged as `v1.4.0-legacy-removed` (the post-removal stable checkpoint). Git log preserves the full audit trail.

### Creator-push response: `dispatch_status` overlay

The `POST /api/v1/creator/jobs` handler now stamps a top-level
`dispatch_status` field (currently the literal `"queued_for_workers"`)
on every accepted 202 envelope. The overlay is **guarded**: the
handler only stamps the field when the upstream Resolver response
does not already carry one, so a future Resolver emission
(e.g. `"dispatching"` / `"dispatched"`) is preserved instead of
silently clobbered back to `"queued_for_workers"`.

Wire contract change — callers that consume the 202 envelope MUST be
prepared to read the new top-level `dispatch_status` key. Operators
that grep observability logs for `accepted_from=creator_push` are
unaffected; the new key is orthogonal to that filter.

Also lands alongside a tightening of `creator_push_e2e_test.go`:

- **Real-`VELOX_ADMIN_TOKEN` E2E coverage** — `TestCreatorPushJobsE2E_RealAdminAuthWired`
  replaces the `adminAuthFake` stub for the auth chain with the
  production `api.AdminAuthMiddleware(cfg)` and asserts: 401 on no
  `Authorization`, 401 on wrong bearer, 202 on the right bearer.
  `req.RemoteAddr` is pinned to RFC 5737 TEST-NET-2
  (`198.51.100.1:1234`) so the middleware's `IsLocalRequestIP`
  loopback bypass cannot accidentally satisfy the suite; `gin.SetTrustedProxies(nil)`
  blocks `X-Forwarded-For` spoofing on the test path; `t.Setenv("VELOX_ADMIN_TOKEN", "")`
  pins any leftover env.
- **Idempotency replay envelope** — the second POST now asserts
  `created: false` (fast-path marker) AND `dispatch_status: queued_for_workers`
  (carried across replays identically). Guards against future
  regressions that strip overlay fields on the idempotent path.
- **Schema-correct DB counts** —
  - `jobs.id` → `jobs.job_id` (2 sites: idempotency replay + 422 zero-rows).
  - `tasks.id` non esiste; usa `tasks.job_id`. `task_specs.job_id` non esiste;
    usa JOIN via `tasks.task_id`: `WHERE task_id IN (SELECT task_id FROM tasks WHERE job_id = ?)`.
  - Counts su `tasks` e `task_specs` ora esatti (`== 1` invece di `<= 1`).
  - Path 422 ora asserisce 0 rows anche su `tasks` (atomic CAS non lascia
    residui parziali).
  - Path 400 asserisce 0 rows in `creator_forwardings` per la chiave
    `source_provider` (handler rejected in `normalizeCreatorPushRequest`
    prima di raggiungere il Resolver).

ADITIVE: callers that ignore unknown JSON fields are unaffected. Strict-mode
consumers (typed unmarshalling into a fixed-shape Go struct, observability
dashboards pinning the response schema) MUST update because the response
payload now carries `dispatch_status` in addition to the previous shape.

Refs: `DataServer/internal/handlers/server/pipeline/creator_push.go`,
`DataServer/internal/handlers/server/pipeline/creator_push_e2e_test.go`,
`docs/CREATOR-PUSH.md`.

**Verified on `main`** (commit `3165528` + the follow-up trailing
polish commit applied on top):

- `cd VeloxEditiingg/DataServer && go vet ./internal/handlers/server/pipeline/... ./internal/creatorflow/...`: PASS.
- `cd VeloxEditiingg/DataServer && go build ./internal/handlers/server/pipeline/... ./internal/creatorflow/...`: PASS.
- `cd VeloxEditiingg/DataServer && go test -count=1 -v -run 'TestCreatorPushJobsE2E' ./internal/handlers/server/pipeline/...`: PASS for all four subtests:
  - `TestCreatorPushJobsE2E_VoiceoverStockClipScene` (happy path + idempotency replay, with `created:false` + `dispatch_status` carry-through assertions)
  - `TestCreatorPushJobsE2E_IncompletePayloadReturns422` (zero-rows on `jobs` + `creator_forwardings` + `tasks`)
  - `TestCreatorPushJobsE2E_MissingSourceJobIDReturns400` (zero-rows on `creator_forwardings` for the supplied source_provider)
  - `TestCreatorPushJobsE2E_RealAdminAuthWired` (401 missing, 401 wrong bearer, 202 right bearer) with env-pinned `VELOX_ADMIN_TOKEN` + `TOKEN_FILE` to defend against shell/CI env-leak.
- `cd VeloxEditiingg/DataServer && go test ./internal/handlers/server/pipeline/... ./internal/creatorflow/... -count=1`: PASS (entire pipeline + creatorflow suites green).
- `git log --oneline -8` on `main`:
  ```
  <polish commit>  fix(pipeline)+test+changelog: address 3 residual polish items
  e047407         fix(pipeline)+test+pipeline+changelog: guard dispatch_status, pin admin token env, document contract
  97b64ed         test(pipeline): add real-VELOX_ADMIN_TOKEN E2E suite + dispatch_status replay assert
  a36fdc9         test(pipeline): fix task_specs JOIN, drop dead SQL/logic, assert created=false on replay
  efbeabc         test(pipeline): align creator_push E2E assertions with canonical schema
  bfc82ed         test(pipeline): cover creator-push E2E scenario (voiceover+stock+clip+scene)
  582a4bc         fix(pipeline): emit dispatch_status=queued_for_workers on creator_push response
  ```

### Architecture: creator_push intake + single-writer invariant

`docs/architecture/current-architecture.md` (PARTE I) now documents the
new `POST /api/v1/creator/jobs` intake path alongside the existing
`CreatorForwardingRunner` (sections 6 and 12). Both paths converge on
the same `creatorflow.Resolver` and the same `AtomicForwardAndEnqueue`,
preserving the single-writer invariant (`runtime-invariants.md §4.2`).

- §6 "Ingresso e compilazione Job" — intake enumeration of three
  canonical paths (master HTTP handler, async runner, synchronous
  creator_push) + mermaid diagram showing the convergence on the
  Enqueuer.
- §12 "Creatorflow e forwarding" — new subsection "Due percorsi di
  intake, un solo writer" with a mermaid diagram of the dual-intake
  architecture and an explicit single-writer invariant
  reaffirmation.
- Bidirectional cross-reference with `docs/CREATOR-PUSH.md` (this
  release also adds a back-link from the contract doc to the
  architecture doc).

Refs: `docs/architecture/current-architecture.md`, `docs/CREATOR-PUSH.md`,
`docs/architecture/runtime-invariants.md` (§4.2).

**Verified on `main`** (commit `4868256`):

- `git log --oneline -1`: `4868256 docs(architecture+creator-push+changelog): document creator_push intake + single-writer invariant`.
- `wc -l docs/architecture/current-architecture.md`: 478 lines (was ~185 before this update; +293 lines for the intake enumeration, two new mermaid blocks, invariant paragraphs, and cross-references).
- `head -5 docs/CREATOR-PUSH.md`: shows the new back-link blockquote to `current-architecture.md §12`.
- Cross-reference targets exist: `ls docs/CREATOR-PUSH.md docs/architecture/runtime-invariants.md` → both present.

### API spec: `POST /api/v1/creator/jobs` OpenAPI yaml

The Master HTTP API now has a canonical, machine-readable contract
at `DataServer/api/openapi.yaml` (OpenAPI 3.1.0). This rev documents
the new `POST /api/v1/creator/jobs` intake path: the request envelope,
the `202 Accepted` response envelope, the Bearer `VELOX_ADMIN_TOKEN`
security scheme, and the 401 / 422 / 500 error envelopes.

Highlights of the spec (matching the Go handler
`DataServer/internal/handlers/server/pipeline/creator_push.go` and
the typed DTO `DataServer/internal/remoteengine/dto.go::RemotePipelineResult`):

- **Security scheme `bearerAdminToken`** — HTTP `bearer` opaque token
  matching `cfg.Auth.AdminToken` (sourced from the `VELOX_ADMIN_TOKEN`
  env var on the Master process; see `DataServer/internal/config/config_misc.go::loadAuth`).
  Tokens MUST NOT be echoed in client logs; rotation via
  `scripts/rotate_token.sh` + restart.
- **`CreatorPushRequest`** envelope — `source_provider` (required),
  `source_job_id` (optional, falls back to `payload.job_id`),
  `target_executor_id` (optional, defaults to `scene.composite.v1`),
  and `payload` (typed `RemotePipelineResult`). The same
  `source_provider + source_job_id + target_executor_id` triple is
  documented as idempotent: replays converge to the same Velox job.
- **`CreatorPushAcceptedResponse`** envelope — `ok=true`,
  `accepted_from="creator_push"`, the three identity fields echoed,
  `job_id` (canonical Velox-side handle from `Resolver.Resolve`),
  `status="PENDING"`, `dispatch_status="queued_for_workers"`. The
  `accepted_from` marker is the canonical way for callers and for
  the Prometheus metric `pipeline_creator_intake_accepted_total{path=…}`
  to split the sync push from the async `creator_forwarder` poller.
- **`RemotePipelineResult` DTO** — matches the Go struct fields
  (`status`, `job_id`, `video_name`, `script_text`,
  `voiceover_paths[]`, `scenes[]`, `delivery_plan[]`, plus the
  internal `script` / `metadata` / `assets` blocks surfaced by
  `ParseRemotePipelineResult`). Asset URIs MUST follow the
  `^(velox-asset://|https?://).+` pattern; the spec calls this out
  as a 422-boundary constraint.
- **Error envelopes** — `ErrorEnvelope` lists `ok=false`, an
  `error` machine code (`missing_authorization`, `invalid_bearer`,
  `invalid_payload`, `resolver_failure`), a `message`, and an
  optional `details[]` array for 422 with `path / issue` per offending
  field. **No Job is created** for 422 — the handler fails closed
  before delegating to `Resolver`.
- **Other endpoints under `/api/*`** are intentionally out of scope
  of this revision (placeholder server block, no paths included).
  Future revisions will fold in the master pipeline routes. The
  cross-references at the top of the yaml (CREATOR-PUSH.md,
  current-architecture.md §6 + §12, runtime-invariants.md §4.2,
  creator_push.go, dto.go) keep the spec in lockstep with the
  narrative contract.

**Wire-key parity preserved.** The yaml matches:

- `creator_push_e2e_test.go` — happy-path expectations on the 202
  envelope (`accepted_from`, identity fields, `job_id`, `status=PENDING`,
  `dispatch_status=queued_for_workers`) and 401 / 422 boundaries.
- `scripts/creator_push_smoke.sh` — the `Authorization: Bearer ${VELOX_ADMIN_TOKEN}`
  curl invocation reflects the bearerAdminToken security scheme; the
  payload is the canonical voiceover+stock+clip+scene example.

**Refs:** `DataServer/api/openapi.yaml` (new, 527 lines), `docs/CREATOR-PUSH.md`
(updated with a back-link to the yaml).

**Verified on `main`** (commit `1884f4d` + Commit Task-1 `c5ebae8` + this commit on top; actual capture at commit-time, not future-asserted):

- `python3 scripts/api/validate_openapi.py DataServer/api/openapi.yaml`: exit `0`. ACTUAL stdout captured to `/tmp/velox_openapi_push/validator_final.txt`:
  ```
  --- validating DataServer/api/openapi.yaml ---
  PASS
  --- TOTAL PASS: 1 openapi file(s) meet all invariants ---
  ```
- `python3 -m py_compile scripts/api/validate_openapi.py`: PASS.
- `python3 -c "import ast; ast.parse(open('scripts/api/validate_openapi.py').read())"`: PASS.
- `cd DataServer && go vet ./internal/handlers/server/pipeline/... ./internal/metrics/...`: exit `0`.
- `cd DataServer && go test -count=1 -run IntakeSinkOrNoop ./internal/handlers/server/pipeline/...`: exit `0` (3 subtests).
- `cd DataServer && go test -count=1 -short -run TestCreatorPushJobsE2E ./internal/handlers/server/pipeline/...`: exit `0`.
- `git show --name-only --no-patch HEAD~1 | grep -c "":`: 8 Task-1 files (creator_intake.go + creator_intake_sink_test.go + creator_push.go + catalog_pipeline.go + handlers.go + router.go + 2 router_instaedit tests).
- `git show --name-only --no-patch HEAD | grep -c "":`: 4 Task-2 files (openapi.yaml + validate_openapi.py + CHANGELOG.md + CREATOR-PUSH.md).
- `wc -l DataServer/api/openapi.yaml scripts/api/validate_openapi.py docs/CREATOR-PUSH.md`: 698 + 273 + 105 = 1076 lines (post-finalize, NOT the stale 527 line count previously cited).
- `head -9 docs/CREATOR-PUSH.md`: shows the bidirectional blockquote referencing `DataServer/api/openapi.yaml`.
- Cross-reference targets exist: `ls DataServer/api/openapi.yaml scripts/api/validate_openapi.py DataServer/internal/remoteengine/dto.go DataServer/internal/handlers/server/pipeline/creator_push.go DataServer/internal/handlers/server/pipeline/creator_push_e2e_test.go docs/CREATOR-PUSH.md` → all present.

NOTE: The forward-looking `python3 -c "import yaml; ..."` one-liner and the stale `wc -l 527` from the prior draft were removed. Every claim in this footer is backed by an ACTUAL command run during commit-time verification (captured outputs in `/tmp/velox_openapi_push/*`).
## [Unreleased] - 2026-07-28

### `POST /api/v1/jobs`: canonical per-scene asset intake

The external job intake now accepts one canonical scene asset shape: nested
`scene.clip`, `scene.voiceover`, and `scene.subtitles` objects. The removed
flat/top-level aliases (`voiceover_paths`, `subtitle_tracks`, `scene.clip_link`,
and `scene.image_link`) are rejected by strict JSON decoding with
`invalid_json`; they are no longer projected into the worker payload. Calendar
jobs emit per-scene clip objects and use top-level `audio_tracks` for global
voiceover paths. Creator Push compatibility remains separate and unchanged.


### `POST /api/v1/jobs`: optional `manifest_ref` field on the wire

The Master `/api/v1/jobs` contract now accepts an OPTIONAL `manifest_ref`
on the request body. A client that already uploaded clip / voiceover /
subtitle assets to a reachable store (Drive, GCS, S3, …) and packaged
the immutable scene list into a `velox.render-manifest.v1` JSON can pass
a pointer to that JSON instead of inlining the scene list. The Master
fetches the JSON, verifies `manifest_ref.sha256` against the raw
downloaded bytes, validates the manifest's internal
`integrity.manifest_sha256`, and substitutes the manifest-derived payload
into the worker input before enqueue.

Wire-level shape:

```json
{
  "idempotency_key": "pg_20260728_4f82d731a91c",
  "manifest_ref": {
    "schema_version": "velox.render-manifest.v1",
    "url": "https://drive.google.com/file/d/MANIFEST_FILE_ID/view",
    "sha256": "0123456789abcdef…"
  },
  "delivery_plan": [ … ]
}
```

Byte-level invariants enforced by `ValidateSubmitJobRequest` (handler-side,
NOT relying on a third-party validator — `velox-asset://` is not a
standard URI format and `regex=…` on the apiwire tag is duplicated
intentionally so the wire schema and the runtime validator agree):

- `manifest_ref` is `*SubmitManifestRef` — a nil pointer is the canonical
  "field omitted entirely" path and MUST pass validation silently so
  every existing client (legacy body shape) sees no wire-shape drift.
  A non-nil pointer with empty body is rejected with three aggregated
  422 violations (one per nested field).
- `schema_version` is a closed enum (`oneof="velox.render-manifest.v1"`
  on the apiwire tag, mirrored as `manifestRefSchemaVersions` in the
  handler). A future v2 bump MUST update both surfaces.
- `url` MUST match `^(https?://|velox-asset://).+` AND be ≤ 2048 bytes
  after `TrimSpace`. The byte cap (`max=2048` tag + `MaxManifestRefURLBytes`
  constant) is pinned by a drift-guard test that asserts the apiwire
  tag still says `max=2048` (the project-wide convention for byte-cap
  constants in `validate:"..."` tags; see also `MaxVideoNameBytes=300`).
- `sha256` MUST match `^[0-9a-f]{64}$` (lowercase hex, exactly 64 chars).
  The strict lowercase check is intentional: the resolver will compare
  byte-for-byte against the recomputed SHA of the fetched JSON, so a
  mixed-case drift is a wire-shape mismatch, not a runtime convention.

OpenAPI contract:

- New schema `SubmitManifestRef` added to
  `DataServer/api/openapi.yaml.components.schemas` via
  `go run ./cmd/api-schema-gen -apply`.
- `SubmitJobRequest.manifest_ref` carries `$ref: '#/components/schemas/SubmitManifestRef'`.
- `python3 scripts/api/validate_openapi.py DataServer/api/openapi.yaml`: PASS (exit 0).

### Files added or modified

- `DataServer/internal/apiwire/apiwire.go` — `SubmitManifestRef` struct
  + `ManifestRef *SubmitManifestRef` field on `SubmitJobRequest` with
  the validate tags listed above.
- `DataServer/internal/handlers/server/pipeline/job_submit.go` —
  handler-side mirror struct (no validate tags; runtime validator
  enforces the same rules), regex helpers `manifestRefURLRegexp` +
  `manifestRefSHA256Regexp`, helper `containsString`, and the
  validator block in `ValidateSubmitJobRequest` that runs ONLY when
  `req.ManifestRef != nil` and aggregates all three nested-field
  violations into a single 422.
- `DataServer/cmd/api-schema-gen/main.go` — `SubmitManifestRef`
  added to the codegen registry.
- `DataServer/api/openapi.yaml` — regenerated via `cmd/api-schema-gen -apply`.
- `DataServer/internal/apiwire/apiwire_test.go` —
  `TestSubmitManifestRef_Roundtrip`, `TestSubmitJobRequest_ManifestRef_Roundtrip`
  (nil-omits-field / non-nil-carries-fields), and
  `TestSubmitManifestRef_MaxLengthMatchesHandlerConstant` (drift-guard
  between apiwire tag's `max=2048` and the handler constant).
- `DataServer/internal/handlers/server/pipeline/job_submit_test.go` —
  12 boundary tests: nil-accepts, good-shape-accepts,
  bad-schema_version-rejects, bad-scheme-rejects (file://, javascript:,
  data:, ftp:, ssh:, not-a-url), all-allowed-schemes-accept (http,
  https, velox-asset://), bad-sha256-rejects (too short, too long,
  uppercase, mixed case, non-hex, empty, 0x prefix), empty-url-rejects,
  empty-object-aggregates-three-violations, empty-schema_version-rejects,
  url-whitespace-trimmed, url-max_length-boundary (exactly
  MaxManifestRefURLBytes bytes pass, +1 byte rejected).

### Verified on `main`

- `cd DataServer && go vet ./...`: PASS (exit 0).
- `cd DataServer && go build ./...`: PASS (exit 0).
- `cd DataServer && go test -count=1 -run 'TestSubmitManifestRef|TestSubmitJobRequest_ManifestRef|TestSubmitJobValidateManifestRef' ./internal/apiwire/... ./internal/handlers/server/pipeline/...`: PASS (all 15 tests).
- `python3 scripts/api/validate_openapi.py DataServer/api/openapi.yaml`: `--- TOTAL PASS: 1 file(s) ---` (exit 0).

### Out of scope (separate commits)

- Worker-side `worker_payload_sha256` receipt for cryptographic
  proof that the remote computer received the manifest payload.

### Worker allowlist: HTTP 403 deny rule + minimum remote-worker configuration

`POST /api/v1/workers/register` now rejects workers whose
`worker_id` is not in the master-side `VELOX_ALLOWED_WORKERS`
allowlist with **HTTP 403 `worker_not_allowed`** — the canonical
operator-visible rejection path, surfaced BEFORE the gRPC stream
handshake and BEFORE any credential storage so an unlisted worker
cannot accidentally leave a row in `worker_credentials`.

The implementation mirrors the existing gRPC stream-side allowlist
rule in `DataServer/internal/grpcserver/authorizer.go::IsAllowed`
(and in `DataServer/internal/grpcserver/handler_stream.go::Stream`)
byte-for-byte — including the `*` wildcard semantics — so both
paths cannot drift at the byte level (defence in depth). They
differ only in the status-code surface:

- gRPC stream path: `codes.PermissionDenied` ("worker %q is not in VELOX_ALLOWED_WORKERS").
- HTTP register path: **HTTP 403** with `{ok:false, error:"worker_not_allowed", message:"worker_id is not in VELOX_ALLOWED_WORKERS on this master"}`.

A future refactor could move both behind a shared
`internal/auth/workerauthz` package; until then the duplication is
intentional and tested.

Byte-level invariants on the helper
(`handler.go::IsWorkerAllowed`):

- `worker_id` empty            → deny (always).
- allowlist CSV empty OR `*` + production → deny (bootstrap should have fail-fast blocked this).
- allowlist CSV empty OR `*` + dev (`Runtime.GRPCAllowInsecureDev=true`) → allow with a one-time warn.
- allowlist CSV non-empty AND non-`*` → `worker_id` MUST exact-match after `TrimSpace`.

Order-of-operations invariant: the HTTP gate runs AFTER JSON parse +
`worker_id` non-empty (so 400 still wins on malformed bodies) and
BEFORE credential validation + registry insert (so we do NOT store
credentials for, or register, an unlisted worker). Pinned by
`TestRegisterV2_AllowlistGate_BeforeCredentialStorage`.

#### `docs/worker_deployment.md` — "Minimum Remote Worker Configuration"

New section documenting the five env vars a remote worker MUST
have to register + execute jobs (the canonical operator contract):

1. `VELOX_WORKER_ID` — worker id; MUST appear in master's `VELOX_ALLOWED_WORKERS`.
2. `VELOX_GRPC_MASTER_URL` — master gRPC control-plane endpoint (host:port).
3. `VELOX_WORKER_SECRET` — credential secret; combined with `worker_id` to derive `credential_hash` (validated against `worker_credentials` table on the master).
4. `VELOX_GRPC_TLS_CERT_FILE` + `VELOX_GRPC_TLS_KEY_FILE` + `VELOX_GRPC_TLS_CA_FILE` — three PEM files (mandatory except in dev). RW-PROD-001 A1/A2 invariants: 14-day min residual validity; key perms 0600 in production; partial TLS rejected.
5. `VELOX_RENDER_BACKEND` + `VELOX_VIDEO_ENGINE_CPP_BIN` + `VELOX_MAX_ACTIVE_JOBS` — render backend selection + C++ engine path + max concurrent jobs per worker.

Plus a failure-mode table mapping each misconfiguration to its
operator-visible master response (HTTP 403 / 401 / 4xx / gRPC
`FailedPrecondition` etc.), so a new operator reading the doc
top-to-bottom sees the canonical signature for every known
breakage class without needing to dig through Go source.

#### Files added or modified

- `DataServer/internal/handlers/remote/workers/lifecycle/handler.go` — `IsWorkerAllowed` method on `*Handler` (imports `log` + `strings`).
- `DataServer/internal/handlers/remote/workers/lifecycle/registration.go` — 403 gate inserted in `RegisterV2Handler`.
- `DataServer/internal/handlers/remote/workers/lifecycle/worker_registration_test.go` (NEW, 9 tests) — happy 200, deny 403, whitespace-trimmed match, prod-empty deny, dev-empty allow, no-credential still gated, no-credential-row leak invariant, `*`-wildcard prod deny, `*`-wildcard dev allow.
- `docs/worker_deployment.md` — "Minimum Remote Worker Configuration" section + failure-mode table.

#### Verified on `main` (pre-push)

- `cd DataServer && go vet ./...`: PASS (exit 0).
- `cd DataServer && go build ./...`: PASS (exit 0).
- `cd DataServer && go test -count=1 -run 'TestRegisterV2|TestAllowlistAuthorizer|TestValidateWorkerAllowlist' ./internal/handlers/remote/workers/lifecycle/... ./internal/grpcserver/... ./internal/config/...`: PASS (all suites green; the existing `grpcserver` allowlist + config validator tests still pass after the HTTP-side change).

#### Out of scope (separate commits / future refactor)

- A `internal/auth/workerauthz` package consolidating the HTTP
  + gRPC allowlist lookups behind one interface (the byte-for-byte
  duplication today is intentional until the consolidation lands).
- `scripts/ci/check-worker-allowlist-coverage.sh` — a CI guard
  that fails any PR / push that removes `worker_id` references from
  the allowlist CSV (catches operator-level drift).

### `velox.render-manifest.v1` canonical spec + CI canonicality guard

The `velox.render-manifest.v1` wire contract is now a first-class
specification with its own canonical reference doc and a CI guard
that pins the contract to a fixture file (so a future contributor
cannot drift the wire shape silently).

**`docs/manifest-spec.md` (NEW, 12 sections)** — the canonical
human-readable reference for the contract. Sections:

1. Top-level envelope (`schema_version`, `manifest_id`, `created_at`).
2. `source` object (`provider`, `pipelinegen_job_id`, `generation_schema`).
3. `video` object (`name`, `language`, `width`, `height`, `fps`, `output_format`).
4. `script` object (`text`, `google_doc_url`, `language`).
5. `scenes[]` array — per-scene mandatory fields (`scene_id`, `index`,
   `kind`, `text`, `duration_ms`, `clip`, `voiceover`, `subtitles`) and
   optional fields (`scene_id`/`index` are required; `clip`/`voiceover`/
   `subtitles` nested objects are required when the upstream pipeline
   has those assets for the scene).
6. `clip` object (`asset_id`, `drive_file_id`, `url`, `sha256`,
   `start_ms`, `end_ms`, `duration_ms`).
7. `voiceover` object (`asset_id`, `drive_file_id`, `url`, `sha256`,
   `duration_ms`, `language`).
8. `subtitles` object (`asset_id`, `format`, `url`, `sha256`, `language`).
9. `delivery_plan[]` entries (typed envelope, mirrors the existing
   `SubmitDeliveryPlanEntry` schema).
10. `integrity` object (`algorithm`, `manifest_sha256`, `scene_count`,
    `total_duration_ms`). `manifest_sha256` is the SHA-256 of the
    canonical-form JSON (sorted keys, `, ` and `: ` separators) of
    the manifest body **minus the `integrity` field itself**, so
    the verification is reproducible from the on-disk JSON alone.
11. Reject envelope — 422 / 400 / 409 response shapes that the
    handler returns when the manifest fails shape rules, the
    SHA-256 doesn't match, or the `schema_version` is not in the
    closed enum.
12. Acceptance test matrix — enumerates the canonical
    good-fixture / bad-fixture cases a CI guard MUST pin.

The spec doc is the single human-readable source of truth for the
contract. The Go wire validator in
`DataServer/internal/handlers/server/pipeline/job_submit.go::ValidateSubmitJobRequest`
and the OpenAPI schema in `DataServer/api/openapi.yaml::SubmitManifestRef`
are the corresponding machine-readable enforcement surfaces.

**`scripts/ci/check-manifest-schema-canicality.sh` (NEW)** — the CI
guard. Three sections in sequence:

- **Spec coverage** — asserts the spec doc lists every mandatory
  top-level block (`schema_version`, `manifest_id`, `created_at`,
  `source`, `video`, `script`, `scenes`, `delivery_plan`, `integrity`)
  and the per-object sections (`source`, `video`, `script`, `scene`,
  `integrity`). Case-insensitive match against section headings so
  a future markdown linting pass cannot accidentally drop a
  heading and silently break the contract reference.
- **Good-fixture integrity** — parses `manifest.v1.fixture.json`,
  asserts `schema_version == "velox.render-manifest.v1"`, every
  required top-level field is present, every per-scene required
  field is present (n_scenes > 0), and the `integrity.manifest_sha256`
  matches the recomputed canonical-form SHA-256 (so a future fixture
  edit that forgets to recompute the hash is caught at CI time).
- **Bad-fixture mismatch** — parses `manifest.v1.bad-fixture.json`,
  asserts its `integrity.manifest_sha256` does NOT match the
  recomputed SHA-256. This pins that the bad-fixture is genuinely
  bad (i.e., someone hasn't accidentally edited it back into a
  good-fixture without updating the SHA-256 to match).

**Fixtures (NEW)**:

- `scripts/ci/fixtures/manifest.v1.fixture.json` — minimal-valid
  manifest: 1 scene, full clip + voiceover + subtitles objects,
  `delivery_plan` with `drive`, `integrity.manifest_sha256` set
  to the canonical-form SHA-256 of the body minus `integrity`.
- `scripts/ci/fixtures/manifest.v1.bad-fixture.json` — same shape
  as the good fixture but with a deliberately wrong SHA-256 (all
  zeros) so the mismatch-pin assertion has something to assert
  against. The fixture is the canonical example of
  "manifest_ref was supplied but the hash doesn't match" — the
  same shape an operator would see from a corrupted upload.

#### Files added or modified

- `docs/manifest-spec.md` (NEW, 12 sections, 19 561 bytes).
- `scripts/ci/check-manifest-schema-canicality.sh` (NEW, executable
  Python-free shell + `jq`, no third-party deps).
- `scripts/ci/fixtures/manifest.v1.fixture.json` (NEW).
- `scripts/ci/fixtures/manifest.v1.bad-fixture.json` (NEW).
- `CHANGELOG.md` — this entry.

#### Verified on `main` (pre-push)

- `bash scripts/ci/check-manifest-schema-canicality.sh`: exit `0`,
  full PASS (spec coverage + good-fixture integrity + bad-fixture
  mismatch all green).
- `python3 -c "import json, hashlib; ..."`: stated SHA-256
  `e5090c2eec68a0edab87d649d4ca55b8782ab473bbb0aaaa7c5b071400e50c03`
  matches the canonical-form SHA-256 byte-for-byte (paranoia check
  that the fixture is not silently tampered with).
- `ls -la scripts/ci/check-manifest-schema-canicality.sh
  scripts/ci/fixtures/manifest.v1.fixture.json
  scripts/ci/fixtures/manifest.v1.bad-fixture.json
  docs/manifest-spec.md`: all four files present, validator
  executable.

### Legacy-body-shape warning on POST /api/v1/jobs

`POST /api/v1/jobs` now emits a non-blocking warning when a client
submits the pre-`manifest_ref` compatibility body shape WITHOUT
a `manifest_ref`. The submission still passes through the canonical
resolver path; the warning is the operator-visible signal that
PipelineGen migration to `manifest_ref` is overdue.

**Detection criteria** (any of):
- `voiceover_paths` (top-level array, non-empty).
- any `scenes[i].clip_link` non-empty after trim.
- `subtitle_tracks` (top-level array, non-empty; now rejected as a retired alias).

A scene carrying the new nested `clip{}` / `voiceover{}` /
`subtitles{}` objects is NOT a legacy-shape signal (the per-scene
enrichment is the migration target). A body that ALSO supplies
`manifest_ref` is also NOT a legacy-shape signal — the client has
migrated and the resolver will use the manifest side instead.

**Structured warning surfaces**:

- **Metric** — `pipeline.legacy_body_shape_total{client_kind="pipelinegen_pre_manifest_ref"}`.
  New catalog entry (`DataServer/internal/metrics/catalog_pipeline.go`),
  bounded `client_kind` label enum (today only
  `pipelinegen_pre_manifest_ref`; future values are additive). The
  counter is the dashboard signal — operators compute the
  migration rate over time by `rate(pipeline_legacy_body_shape_total[1d])`,
  with the goal of trending to zero as PipelineGen migrates.
- **Log** — `pipelineLog("LEGACY_BODY_WARNING client_kind=… idempotency_hash=… voiceover_paths=N scenes_with_clip_link=N subtitle_tracks=N manifest_ref=absent")`
  via `DataServer/internal/handlers/server/pipeline/job_submit.go::NormalizeExternalJobSubmission`.
  Carries the per-scene distribution count so operators can see
  the compat-shape breakdown in the structured log without
  grepping every scene.
- **No gate** — the warning emission is INTENTIONALLY NON-BLOCKING.
  Existing PipelineGen clients (and any other compat-shape
  producer) keep working until they migrate; only the operator-
  visible signal fires.

**API surface change** — `NormalizeExternalJobSubmission` is now
a method on `*Handlers` (`DataServer/internal/handlers/server/pipeline/job_submit.go`)
so it can call `h.legacyBodySinkOrNoop()` from the emit site. The
call site in the `SubmitJob` handler updates accordingly
(`h.NormalizeExternalJobSubmission(req)`). All existing test sites
in `job_submit_test.go` + `normalize_test.go` (9 call sites
total) update to `(&Handlers{}).NormalizeExternalJobSubmission(req)`
— mechanical, one-character change per call site. No wire-contract
drift: the public `SubmitJob` HTTP surface is unchanged.

**Composition root** — `DataServer/cmd/server/router.go` wires
`velmetrics.NewCreatorBodyWarningSink()` into the pipeline handler chain
via `.WithLegacyBodySink(...)`. Mirrors the existing
`WithIntakeSink(...)` wiring pattern (`creator_intake.go`).

#### Files added or modified

- `DataServer/internal/metrics/catalog_pipeline.go` — new
  `pipeline.legacy_body_shape_total` MetricDefinition.
- `DataServer/internal/metrics/legacy_body_shape.go` (NEW) —
  CounterFamily + `LegacyBodySink` interface + `LegacyBodySinkImpl`
production type + `NewCreatorBodyWarningSink()` constructor. Mirrors
  `creator_intake.go` byte-for-byte.
- `DataServer/internal/handlers/server/pipeline/legacy_body_shape_sink.go` (NEW) —
  `LegacyBodySinkClientKindPreManifestRef` constant + handler-side
  `LegacyBodySink` interface + `noopLegacyBodySink{}`.
- `DataServer/internal/handlers/server/pipeline/handlers.go` —
  `legacyBodySink` field on `Handlers` struct + `WithLegacyBodySink()`
  mutator.
- `DataServer/internal/handlers/server/pipeline/job_submit.go` —
  `NormalizeExternalJobSubmission` converted to a method on
  `*Handlers`; legacy-shape detection + emission at the top of
  the method; pure helpers `isLegacyCompatShape(req)` +
  `countScenesWithClipLink(scenes)` + accessor
  `legacyBodySinkOrNoop()`. `SubmitJob` handler call site
  updated.
- `DataServer/internal/handlers/server/pipeline/job_submit_test.go` —
  4 call-site updates (mechanical).
- `DataServer/internal/handlers/server/pipeline/normalize_test.go` —
  5 call-site updates (mechanical).
- `DataServer/internal/handlers/server/pipeline/legacy_body_warning_test.go` (NEW) —
  11 sub-tests covering the full matrix: sink wired/nil/explicit-nil,
  isLegacyCompatShape positive + negative branches + whitespace trim +
  nested-Clip negative + combination, countScenesWithClipLink
  boundaries, integration (legacy-emits-warning, manifest_ref-
  suppresses, no-legacy-no-warning, no-sink-still-works,
  clip_link-alone, subtitle_tracks-alone), constant value lock.
- `DataServer/cmd/server/router.go` — wires `velmetrics.NewCreatorBodyWarningSink()`.
- `CHANGELOG.md` — this entry.

#### Verified on `main` (pre-push)

- `cd DataServer && go vet ./...`: PASS (exit 0).
- `cd DataServer && go build ./...`: PASS (exit 0).
- `cd DataServer && go test -count=1 -run 'TestWithLegacyBodySink|TestIsLegacyCompatShape|TestCountScenesWithClipLink|TestNormalizeExternalJobSubmission_LegacyBodyEmitsWarning|TestNormalizeExternalJobSubmission_ManifestRefSuppressedWarning|TestNormalizeExternalJobSubmission_NoLegacyFieldsNoWarning|TestNormalizeExternalJobSubmission_NoSinkStillWorks|TestNormalizeExternalJobSubmission_ClipLinkAloneTriggers|TestLegacyBodySinkClientKindPreManifestRef_Value|TestNormalizeExternalJobSubmission_SubtitleTracksAloneTriggers|TestNormalizeExternalJobSubmission_ProducesCanonicalPayload|TestNormalizeExternalJobSubmission_MatchesCreatorPushShape|TestNormalizeExternalJobSubmission_OmittedRetryBudgetDefaultsToThree|TestNormalizeExternalJobSubmission_ExplicitRetryBudgetZeroPreserved|TestNormalizeExternalJobSubmission_PerSceneVoiceoverNotPositionCoupled|TestNormalizeExternalJobSubmission_PerSceneClipAndSubtitlesRoundtrip|TestIntakeSinkOrNoop|TestCatalog_NoDuplicateNames|TestValidateMetricName' ./internal/handlers/server/pipeline/... ./internal/metrics/...`: PASS (all suites green; the existing `intakeSink` + `TestCatalog_*` invariants hold after the catalog addition).

## Historical changelog archive

Entries dated **2026-07-27 and earlier** are preserved in the dedicated
[historical changelog archive](docs/history/CHANGELOG-2026-07-27-and-earlier.md).
The archive keeps the original headings, commit references, verification notes,
and chronological order.

## Historical anchors

The following compatibility anchors preserve direct links that used to
point into the parent document:

<a id="unreleased-2026-07-27"></a>
- [[Unreleased] - 2026-07-27](docs/history/CHANGELOG-2026-07-27-and-earlier.md#unreleased-2026-07-27)

<a id="validator-extensibility-data-driven-per-route-invariants"></a>
- [Validator extensibility — data-driven per-route invariants](docs/history/CHANGELOG-2026-07-27-and-earlier.md#validator-extensibility-data-driven-per-route-invariants)

<a id="payload-hash-idempotency-409-on-idempotencykeyreused"></a>
- [Payload-hash idempotency: 409 on `idempotency_key_reused`](docs/history/CHANGELOG-2026-07-27-and-earlier.md#payload-hash-idempotency-409-on-idempotencykeyreused)

<a id="v1221-2026-07-11"></a>
- [v1.2.21 (2026-07-11)](docs/history/CHANGELOG-2026-07-27-and-earlier.md#v1221-2026-07-11)

<a id="behavior-changes"></a>
- [Behavior changes](docs/history/CHANGELOG-2026-07-27-and-earlier.md#behavior-changes)

<a id="unreleased-2026-07-17"></a>
- [[Unreleased] - 2026-07-17](docs/history/CHANGELOG-2026-07-27-and-earlier.md#unreleased-2026-07-17)

<a id="youtubesocial-cleanup-finale"></a>
- [YouTube→Social: cleanup finale](docs/history/CHANGELOG-2026-07-27-and-earlier.md#youtubesocial-cleanup-finale)

<a id="submodule-relationship"></a>
- [Submodule relationship](docs/history/CHANGELOG-2026-07-27-and-earlier.md#submodule-relationship)

<a id="pr-157-size-benchmark-regression-net-artefacts"></a>
- [PR-15.7 — Size-benchmark regression-net artefacts](docs/history/CHANGELOG-2026-07-27-and-earlier.md#pr-157-size-benchmark-regression-net-artefacts)

<a id="pr-158-youtube-social-api-separation-final"></a>
- [PR-15.8 — YouTube → Social API separation (final)](docs/history/CHANGELOG-2026-07-27-and-earlier.md#pr-158-youtube-social-api-separation-final)

<a id="pr-159-youtube-social-api-migration-closure-conclusive-record"></a>
- [PR-15.9 — YouTube → Social API migration closure (conclusive record)](docs/history/CHANGELOG-2026-07-27-and-earlier.md#pr-159-youtube-social-api-migration-closure-conclusive-record)

<a id="removed"></a>
- [Removed](docs/history/CHANGELOG-2026-07-27-and-earlier.md#removed)

<a id="added"></a>
- [Added](docs/history/CHANGELOG-2026-07-27-and-earlier.md#added)

<a id="changed"></a>
- [Changed](docs/history/CHANGELOG-2026-07-27-and-earlier.md#changed)

<a id="commit-chain-10-commits-chronological"></a>
- [Commit chain (10 commits, chronological)](docs/history/CHANGELOG-2026-07-27-and-earlier.md#commit-chain-10-commits-chronological)

<a id="verification"></a>
- [Verification](docs/history/CHANGELOG-2026-07-27-and-earlier.md#verification)

<a id="refs"></a>
- [Refs](docs/history/CHANGELOG-2026-07-27-and-earlier.md#refs)

<a id="pr-1510-socialgateway-legacy-alias-honor-cycle-retired"></a>
- [PR-15.10 — `SOCIAL_GATEWAY_*` legacy alias honor-cycle retired](docs/history/CHANGELOG-2026-07-27-and-earlier.md#pr-1510-socialgateway-legacy-alias-honor-cycle-retired)

<a id="pr-1516-no-youtube-regression-ci-guard-workflow"></a>
- [PR-15.16 — no-youtube-regression CI guard workflow](docs/history/CHANGELOG-2026-07-27-and-earlier.md#pr-1516-no-youtube-regression-ci-guard-workflow)

<a id="pr-1514-residuo-4-closure-externaldestinationid-canonical-rename"></a>
- [PR-15.14 — Residuo 4 closure: ExternalDestinationID canonical rename](docs/history/CHANGELOG-2026-07-27-and-earlier.md#pr-1514-residuo-4-closure-externaldestinationid-canonical-rename)

<a id="pr-1513-residuo-3-closure-opaque-mode-wire-contract"></a>
- [PR-15.13 — Residuo 3 closure: opaque-mode wire contract](docs/history/CHANGELOG-2026-07-27-and-earlier.md#pr-1513-residuo-3-closure-opaque-mode-wire-contract)

<a id="pr-1512-residuo-2-closure-opaque-mode-destination-model"></a>
- [PR-15.12 — Residuo 2 closure: opaque-mode Destination model](docs/history/CHANGELOG-2026-07-27-and-earlier.md#pr-1512-residuo-2-closure-opaque-mode-destination-model)

<a id="pr-1511-operator-facing-youtube-residue-audit-script"></a>
- [PR-15.11 — Operator-facing YouTube-residue audit script](docs/history/CHANGELOG-2026-07-27-and-earlier.md#pr-1511-operator-facing-youtube-residue-audit-script)

<a id="post-refactor-state-structural-refactor-series-follow-on-features"></a>
- [Post-Refactor State (structural refactor series + follow-on features)](docs/history/CHANGELOG-2026-07-27-and-earlier.md#post-refactor-state-structural-refactor-series-follow-on-features)

<a id="commit-chain-chronological-oldest-first"></a>
- [Commit chain (chronological, oldest first)](docs/history/CHANGELOG-2026-07-27-and-earlier.md#commit-chain-chronological-oldest-first)

<a id="per-split-breakdown-original-split-files"></a>
- [Per-split breakdown (original → split files)](docs/history/CHANGELOG-2026-07-27-and-earlier.md#per-split-breakdown-original-split-files)

<a id="cumulative-loc-impact-refactor-series-only-git-show-shortstat"></a>
- [Cumulative LOC impact (refactor series only, `git show --shortstat`)](docs/history/CHANGELOG-2026-07-27-and-earlier.md#cumulative-loc-impact-refactor-series-only-git-show-shortstat)

<a id="validation-evidence-post-refactor-all-green-on-main"></a>
- [Validation evidence (post-refactor, all green on `main`)](docs/history/CHANGELOG-2026-07-27-and-earlier.md#validation-evidence-post-refactor-all-green-on-main)

<a id="zero-regression-check-vs-baseline-0d42b46"></a>
- [Zero-regression check vs baseline `0d42b46`](docs/history/CHANGELOG-2026-07-27-and-earlier.md#zero-regression-check-vs-baseline-0d42b46)

<a id="follow-on-features-enabled-by-the-structural-cleanup"></a>
- [Follow-on features enabled by the structural cleanup](docs/history/CHANGELOG-2026-07-27-and-earlier.md#follow-on-features-enabled-by-the-structural-cleanup)

<a id="files-intentionally-not-split"></a>
- [Files intentionally **not** split](docs/history/CHANGELOG-2026-07-27-and-earlier.md#files-intentionally-not-split)
## [v1.4.0] - 2026-08-28

### Asset origin certification

- Record resolution timestamps and enforce temporal proof for prefetch-origin
  classification.
- Add worker tests covering identity, timing, and byte-level attribution.
