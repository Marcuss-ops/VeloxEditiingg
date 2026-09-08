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

### Removal — 11 orphaned internal packages (~2,600 LOC)

Full removal per ADR 0008 (C2 fails: unreachable from outside the repo;
C1 fails: zero importers verified via `go list -deps` over every `cmd/*`
entrypoint + module-wide grep, incl. scripts/CI). Decision record in
`docs/audit/orphaned-packages-mount-or-delete.md`.

Deleted: `internal/handlers/server/calendar`, `internal/integrations/news`,
`internal/translation`, `internal/handlers/remote/install`,
`internal/handlers/remote/workers/management`, `internal/handlers/server/smoke`,
`internal/preparedassets`, `internal/store/contracts`, `internal/slo`,
`internal/metricscenter`, `internal/handlers/server/health` — 34 files,
none of them reachable from any binary. `go.mod` has no orphaned require
entries (the packages used only already-shared deps). Gate evidence:
full-module vet/build/test green on the deletion tree.

### 6-area audit remediation (dead code, contracts, concurrency, hot path, duplication, error handling)

Findings from the 2026-09-06 targeted audit (report in session history;
disposition brief for the remaining orphaned packages in
`docs/audit/orphaned-packages-mount-or-delete.md`). All changes verified by
the full-module gate (`scripts/ci/pre-removal-verify.sh`: vet/build/test
all green).

- **Dead code removed** — unused `TokenManager.RevokeToken` (zero callers;
  the revoke path remains `RevokeWorkerTokens`), the dead `outputArtRepo`
  DI edge on `ingest.NewTaskReportIngestionService` (required-but-never-read
  dependency; constructor and all bootstrap/test callers updated), the
  dead `sshPass` parameter on the Ansible `hostINI` helper (accepted and
  discarded secret-derived arg), stale `_ =` suppressions in the smoke
  executor and deliveries plan resolver, and the hand-rolled `abs()` in
  the video trimmer (replaced with `math.Abs`).
- **Validation contract at ingress** — `POST /api/v1/agent/validation` now
  rejects a non-empty malformed `timestamp` with 400 instead of letting
  the store silently substitute the server clock (which masked worker
  clock bugs as fresh `validated_at`); the empty-timestamp fallback is
  documented at the store boundary and pinned by a regression test.
- **Concurrency** — `translation.TranslateScenes` acquires its bounded
  semaphore BEFORE spawning each goroutine (a huge scenes array no longer
  materializes len(input) parked goroutines for a limit of 4);
  `drive/folders` getLinks re-checks TTL inside the write lock so a
  concurrent refresher cannot trigger redundant DB reloads.
- **Metrics observability** — the cache-stats derivation fallback WARN in
  the gRPC metrics handler is time-throttled (`logging.WarnThrottled`,
  5-min interval) instead of one-shot `sync.Once`, so a long-running
  process re-raises the signal periodically instead of logging it exactly
  once per process lifetime; the stale `RecordAttempt` idempotence comment
  now documents the actual supervisor dedup-on-attempt-id contract.
- **Timestamp parsing SSOT** — new dependency-free leaf
  `internal/persistedtime` is the single parser for the three persisted
  timestamp layouts (RFC3339Nano / RFC3339 / bare SQLite datetime);
  duplicated ladders in `smokerunstore`, `store`, `artifactsstore`, and
  the inline `deliverystore` ladder now route through it (4 copies → 1).
- **Max-retry SSOT parity pin** —
  `TestExtractPlanMaxRetry_MatchesValidatePlanPayloadWriter` binds the
  INSERT-path writer (`extractPlanMaxRetry`) and the post-create
  precondition writer (`validatePlanPayload`) to identical semantics, so
  the two `jobs.max_retries` writers cannot drift.
- **Error handling** — swallowed errors surfaced on the credential-vault
  audit writes, drive-service startup load, Level-D smoke `markFailed`,
  drive folder saveToDisk, and smoke-ssh cleanup paths: best-effort
  cleanup failures now log at warn/error with the failing operation
  instead of vanishing.
- **Repo hygiene** — resolved the 9 files left in unresolved stash-pop
  conflict state (`UU`) since the pre-`f0c734b0` era by restoring current
  `HEAD` (the stash side predates the JobStatus alias consolidation and
  the publication-evidence boundary; stashes remain in `stash@{0..4}`).

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


## Historical changelog archive

Entries dated **2026-07-27 and earlier** are preserved in the dedicated
[historical changelog archive](docs/history/CHANGELOG-2026-07-27-and-earlier.md).
The archive keeps the original headings, commit references, verification notes,
and chronological order.

More recent archived entries (kept with their original headings and
verification notes):

- [2026-08-16 to 2026-07-29](docs/history/CHANGELOG-2026-08-16-to-2026-07-29.md)
- [2026-07-28](docs/history/CHANGELOG-2026-07-28.md)
- [2026-07-25 and earlier](docs/history/CHANGELOG-2026-07-25.md)

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
- [PR-15.16 — no-youtube-regression CI guard workflow](docs/history/CHANGELOG-2026-07-27-and-earlier-part2.md#pr-1516-no-youtube-regression-ci-guard-workflow)

<a id="pr-1514-residuo-4-closure-externaldestinationid-canonical-rename"></a>
- [PR-15.14 — Residuo 4 closure: ExternalDestinationID canonical rename](docs/history/CHANGELOG-2026-07-27-and-earlier-part2.md#pr-1514-residuo-4-closure-externaldestinationid-canonical-rename)

<a id="pr-1513-residuo-3-closure-opaque-mode-wire-contract"></a>
- [PR-15.13 — Residuo 3 closure: opaque-mode wire contract](docs/history/CHANGELOG-2026-07-27-and-earlier-part2.md#pr-1513-residuo-3-closure-opaque-mode-wire-contract)

<a id="pr-1512-residuo-2-closure-opaque-mode-destination-model"></a>
- [PR-15.12 — Residuo 2 closure: opaque-mode Destination model](docs/history/CHANGELOG-2026-07-27-and-earlier-part2.md#pr-1512-residuo-2-closure-opaque-mode-destination-model)

<a id="pr-1511-operator-facing-youtube-residue-audit-script"></a>
- [PR-15.11 — Operator-facing YouTube-residue audit script](docs/history/CHANGELOG-2026-07-27-and-earlier-part2.md#pr-1511-operator-facing-youtube-residue-audit-script)

<a id="post-refactor-state-structural-refactor-series-follow-on-features"></a>
- [Post-Refactor State (structural refactor series + follow-on features)](docs/history/CHANGELOG-post-refactor-state.md#post-refactor-state-structural-refactor-series-follow-on-features)

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
