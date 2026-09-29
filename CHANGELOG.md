## [Unreleased] - 2026-09-28

### Performance follow-up — preparation and prefetch

- Reserve the selected warm worker synchronously at submission, before the
  wider plan refresh, so a claim tick cannot race ahead and assign the task to
  a different worker.
- Start a stock-asset plan for PENDING PREPARE tasks. FINALIZE refreshes the
  same reservation with the new task revision and runtime assets, allowing
  known stock downloads to overlap generated media.
- Persist worker-verified Drive asset identities in the Master catalog and
  reuse fresh SHA-256/size metadata for later plans. Catalog entries expire
  from reuse after 24 hours because Drive locators may be overwritten.
- Coalesce matching asset work items across jobs in the worker queue while
  retaining one resolver lease and PREPARED certificate per job. Reuse verified
  media metadata for content-identical assets; the wire plan still carries
  each job's asset manifest for lineage and claim verification.
- Fast assembly already fails closed when any required asset is missing or
  unverifiable at execution; the worker records that condition in
  `velox_assembly_assets_missing_at_execution`.
- Build verification: `go build ./...` in `DataServer/` and
  `RemoteCodex/native/worker-agent-go/`. Tests were not run.

### Fixed — stock clips stay packet-copy only

- Recover unsafe stock cut points with packet-copy handling; never send the
  rest of the timeline through a video encoder because one stock boundary is
  not copy-safe. Scope tail-gap recovery to stock plans and keep standard mux
  inputs lazily opened.
- When a stock interval cannot be copied safely, skip/replace that interval
  while preserving timeline duration and audio sync. Do not transcode it.
- Commits: `0de1ccb8`, `b492ca28`, `51e3fecc`, `15d9cf16`.

### Verified — full Milton job rerun on worker 51

- Re-submitted the original complete task payload, retaining its 177 base
  clips, 15 overlays and 101 runtime assets, including final audio and BGM
  references. The run completed as `job_07310e506f58e8c1` and was delivered
  to the requested Drive folder.
- The render produced 207 packet-copy segments: 100% packet copy, zero video
  transcode/encode passes, and zero decoded/composited frames. The output has
  H.264 video and stereo AAC audio. Render task time was 20.3 seconds; Drive
  delivery took about 59 seconds.

### Performance finding — asset preparation delays job start

- The run waited 3 minutes 35 seconds between job creation and task start.
  The first 209-entry prefetch plan went to `velox-worker-13197`; the task was
  later assigned to `velox-worker-523925eb`, which received a second plan.
- The assigned worker recorded 17 cache misses and about 86 MB of transfers.
  The earlier worker later failed to warm the BGM asset because its cache file
  was missing. This identifies worker reassignment and cache preparation as
  the current startup delay; packet-copy rendering itself took about 20
  seconds.
- Follow-up: bind prefetch to the selected worker, start stock-asset prefetch
  earlier while the 77 generates overlays/audio, and deduplicate the prefetch
  plan against unique required assets.

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
