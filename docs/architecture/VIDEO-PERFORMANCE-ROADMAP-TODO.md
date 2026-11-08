# Video performance roadmap — action list

Status updated: 2026-11-08

This checklist tracks the P0–P3 work needed to keep video bytes off the
control plane, reuse certified work across retries, and publish one clean
worker image after implementation and verification are complete.

## Current completion snapshot

- Complete: assembly-only retry, cache-first lookup, deterministic persisted
  stock selection, the worker window-preparation executor, prepared-fragment
  link/clone staging, and warm-cache attestation.
- Complete: FINALIZE-to-TaskGraph publication, worker-to-Master fragment
  exchange, Master-side cross-worker fragment reuse, and worker-side source
  identity discovery. The control plane no longer materializes Drive stock
  bytes when worker preparation is wired.
- Complete: canonical H.264 encoder identity. Master and worker share the
  pinned `libx264` preset, reference count and BT.709 VUI metadata; the
  encoder-argument digest is part of the preparation revision, and worker
  certification compares the actual `avcC` hash with a per-encoder-build
  canonical reference instead of accepting any 64-hex value.
- Complete: worker dispatch probes the worker-local prepared-fragment cache
  before resolving or downloading the source. A warm fragment executes
  without source bindings and reports `frames_encoded = 0`; the certified
  output frame count remains available in the certificate.
- In progress: production certification of progressive fMP4 upload. The
  fail-closed schema-v2 gate now requires 100 independently certified jobs,
  measured overlap in every certified job, positive uploaded bytes before
  render completion, a positive p95 overlap and a valid RFC3339 timestamp.
  Running it still needs production jobs and a retained evidence artifact.
- Implemented (opt-in): media-aware sparse MP4 range fetching with a
  full-download fallback.
- Not implemented (blocked by an explicit architecture decision): single-pass
  native visual-layer compositing; layered plans are routed to the Chronon GPU
  backend.
- Full-module verification is green (`scripts/ci/pre-removal-verify.sh`:
  vet, build and `go test ./...`). Cold/warm/retry production measurements and
  the final worker-image build/certification/publication remain open.

## Completed locally

- [x] Remove duplicated payload and pipeline-run HTTP handling. The local
  cleanup centralizes repeated run lookup/error mapping and shares runtime
  audio payload parsing across Master and worker.
- [x] Make stock-pool selection deterministic and persist the selection with
  the task. FINALIZE now stores a `runtime_stock_pool_snapshot` tied to job,
  timeline and folder; the worker validates and consumes that exact ordered
  snapshot. Legacy tasks without a snapshot use the shared deterministic
  selector.
- [x] Pin stock-pool stability with contract tests: candidate input ordering
  cannot change the selection, a persisted snapshot survives new Drive
  candidates, and a changed identity is rejected.
- [x] Cache-first source preparation. FINALIZE looks up windows by certified
  source SHA before materializing source bytes, reuses READY fragments, and
  downloads only sources with an unknown identity or a cache miss. Unknown
  source identities are no longer downloaded by the Master: they are planned
  as worker discovery tasks.
- [x] Pin a full cache hit with a media integration test that removes the
  source file before retry; preparation still succeeds from the certified
  cached fragment without source materialization.

## I/O and cache efficiency (audit §7B)

- [x] Prepared fragments no longer perform a second staging copy. The staging
  helper tries a filesystem clone (FICLONE), then a hard link, then a streaming
  copy; it hashes the staged bytes once and rejects size drift. Tests cover the
  clone/link path, the copy fallback and the byte limit
  (`prepared_stage.go`, `prepared_stage_test.go`).
- [x] Warm worker-cache hits no longer reread every blob for SHA-256. A cache
  attestation combines `dev/ino/size/mtime/ctime` with a 256 KiB head+tail
  sampled fingerprint (needed on filesystems with coarse timestamps) and is
  invalidated on mutation; only an invalid attestation triggers the full hash.
  Tests cover hit, mutation and oversized-file cases
  (`asset_cache_attestation.go`, `asset_cache_attestation_test.go`).
- [x] Dead-code cleanup after the audit: `staticcheck -checks U1000 ./...`
  reports zero findings on `shared`, `DataServer` and
  `RemoteCodex/native/worker-agent-go`. Removed the unused worker HTTP
  trace/metrics subsystem, the uncalled `CleanupAssetPartials` wrapper,
  `assetPath` mutators, `ClipLease.AssetKeys/Bindings`, `RecordNetworkBytes`,
  `Worker.acquireResumePublisher/canAcceptPhase`, `WaitForShutdown`, the
  duplicate fMP4 gate test, the obsolete `validateSubmitScenes` path that
  `ValidateSubmitJobRequest` supersedes (it rejects `scenes` as legacy),
  orphaned enqueue/remote-engine DTO/normalizer code, and the stray 17 MB
  Codex session transcript. The GPU sampler test now compares against
  `detectGPU()`, so it is valid on GPU and GPU-less hosts.

## P0 — recovery and cache behavior

- [x] **Assembly-only retry from the immutable compiled plan.** The current
  pipeline-run retry endpoint re-submits the original Producer request and can
  repeat acquisition and preparation. The endpoint now atomically reactivates
  only an eligible failed assembly task, its parent job and pipeline-run
  projection. The saved task spec/payload is preserved byte-for-byte, and
  successful dependencies must already be complete. A later-created
  preparation task in the same job cannot be mistaken for assembly. Pending
  preparation is rejected without changing state.
- [x] **Persist deterministic stock-pool selection.** Snapshot selected file
  IDs and available immutable source identities into the durable task payload
  before dispatch. Every retry consumes that exact snapshot. If upstream
  identity is unavailable, the verified worker download establishes it.

## P1 — distributed preparation and certification

- [x] Add worker executor `video.prepare.window.v1`. Its shared contract binds
  a source reference and SHA, source/output frame intervals, timeline placement
  and output-profile revision. The worker resolves only that source, encodes
  with shared canonical FFmpeg arguments, probes stream/frame/keyframe
  properties, hashes the output and returns its certificate in segment metadata.
  The shared request now accepts either a CFR source-frame interval or a
  source-time interval; the latter uses accurate FFmpeg keyframe seek and
  certifies exact canonical output frames. Both modes pass local FFmpeg tests.
- [x] Make TaskGraph fan-out tasks executable: atomic expansion now writes
  each task row, dependency edges and its hashed `task_specs` payload in the
  same SQLite transaction. Integration coverage checks the persisted payload
  as well as readiness transitions.
- [x] Keep runtime-finalization parents unclaimable while their immutable
  media bindings are unresolved. Both placement candidate queries and all
  SQLite lease/claim paths exclude `runtime_assets_pending=true`; focused
  regression tests cover both shared dispatch query variants, repository
  candidate listing, and exact-worker claims. This is the dispatch safety
  gate required before preparation DAG fan-out can land.
- [x] Change FINALIZE into durable TaskGraph publication for independent
  preparation tasks. `publishWorkerPreparations` atomically extends the graph
  with one `video.prepare.window.v1` task per content identity, marks the
  parent `WAITING_PREPARATION`/`runtime_assets_pending`, and
  `completeWorkerPreparations` rewrites the assembly plan only after every
  certificate is verified. Final packet-copy assembly stays one task when all
  fragments are ready; FINALIZE processes independent candidates with a
  bounded parallel pool and separate download/probe/encode semaphores.
- [x] Add a no-download stock-window task planner. It emits validated,
  content-identity-deduplicated `video.prepare.window.v1` requests from the
  immutable plan. It is called from FINALIZE. A positive declared source size
  is required; an unknown SHA emits a deterministic discovery request instead
  of failing or downloading on the Master.
- [x] Worker-side source identity discovery. A discovery request carries the
  source asset/Drive identity, declared size and window but no SHA. The worker
  resolves only that source through the verified download path, binds the
  discovered SHA into its certificate, and publishes the resolved request so
  the Master registers the fragment under the content identity. The contract
  refuses a discovery request that also declares a SHA, and the FINALIZE
  coordinator validates the discovered SHA against the source size.
- [x] FINALIZE queue execution is bounded parallel across independent
  candidates, with an SQLite atomic `QUEUED→RUNNING` claim and refreshed spec
  hash so multiple Master instances cannot prepare the same candidate. Shared
  download/probe and window encode semaphores keep concurrent candidates
  within separate global resource budgets. Per-window worker TaskGraph
  fan-out now lands through `publishWorkerPreparations`; when worker planning
  is wired, the Master fallback is disabled and a residual stock plan fails
  closed instead of downloading on the control plane.
- [x] Reuse fragment certification evidence keyed by blob SHA-256, encoder
  build revision and profile. Worker `video.prepare.window.v1` stores immutable
  content-addressed blobs and output evidence in persistent cache, rebinds the
  certificate to a new timeline, and warm reuse avoids encode, probe and SHA
  rereads after stat/attestation checks. Master-side fragments carry their
  certificate in the source metadata; `LookupPreparedWindowAsset` lets
  FINALIZE reuse a READY fragment from another job/worker before publishing a
  preparation task, and the reused bindings are persisted in
  `runtime_prepare_window_reuse` until the assembly plan is rewritten.
- [x] **Close the H.264 encoder-identity gap.** `CanonicalVideoEncodeArgs` now
  pins `-preset veryfast`, `-refs 1` and BT.709 `colorspace/color_primaries/`
  `color_trc/color_range` in addition to profile, level, GOP and B-frames. The
  argument projection is hashed by `CanonicalVideoEncoderArgsDigest` and folded
  into `VideoWindowPreparationRevision`, so changing any identity-bearing
  encoder flag invalidates cached fragments. Worker certification builds a
  one-frame reference with the same canonical contract, probes its `avcC`
  SHA-256 and rejects a fragment whose extradata differs (`WINDOW_EXTRADATA_`
  `IDENTITY_MISMATCH`); warm attestations apply the same check. A real-FFmpeg
  contract test encodes one source through the Master normalization projection
  and through `CanonicalVideoWindowArgs` and requires byte-identical `avcC`.
- [x] **Make the worker cache-first before source resolution.**
  `TaskRunner.ProbeLocalPreparation` asks the registered executor whether the
  fragment identity, encoder build and canonical `avcC` already match a local
  attestation. Dispatch skips both the legacy asset resolver and
  `resolveCompiledRenderPlanAssets` on a hit; the executor serves the artifact
  without source bindings. A probe error is treated as a miss, so an
  unavailable probe never breaks a job that could still be prepared normally.
  Focused tests cover hit-with-unresolvable-source and miss-still-resolves.
- [x] **Separate encoder work from output frames.** Cache-hit segments now
  report `frames_encoded = 0` instead of attributing `OutputFrameCount` to a
  window that did not run FFmpeg. The exact output frame count remains in the
  certificate (`output_frame_count`), and fresh encodes still report their
  real encoded frames.
- [x] Centralize the stable cache-key serialization for `video.prepare.window.v1`
  in the shared contract. Timeline revision/hash/placement are excluded from
  the content-preparation identity, while source bytes/window and output
  profile revision remain bound. The worker executor now uses this shared
  helper; contract tests pin both reuse across placements and invalidation
  when the source window changes.
- [x] Worker chunk-manifest emission reuses the resolver's verified
  `runtimeassets.Binding` identity instead of hashing each prepared fragment
  again. Chunk installation uses a hard link for immutable cache blobs on the
  same filesystem, with atomic copy fallback across filesystems. Master-side
  cache keys now include a revision hash over preparation algorithm, profile
  and quality policy, and cache hits require matching persisted source/window,
  profile/version, frame-count and keyframe/GOP evidence. The new worker
  executor emits source/output SHA, encoder build, profile and timeline-bound
  certification. The worker-local cache reuses that evidence across jobs;
  successful prepared-window results persist through TaskGraph task attempts,
  and FINALIZE reuses matching certified fragments from the Master cache. The
  remaining check is production multi-worker measurement, tracked below.
- [~] Build the internal prepared-window exchange outside final artifact
  publication. The Master now has worker-authenticated GET/PUT endpoints keyed
  by the shared content identity; PUT validates the request/certificate,
  recomputes the output hash, independently probes the canonical stream and
  frame count, and registers a content-addressed asset with its certificate.
  The worker checks the Master cache before uploading and removes registered
  preparation outputs from the final-video publication protocol. Focused
  Master and worker tests pass. Successful task-attempt segment evidence is
  now readable from SQLite, including persisted `registered_asset_id`; a
  new query selects only the latest successful attempt for a prepared-window
  task and excludes other executors. Remaining: wire this evidence into the
  FINALIZE coordinator, rewrite the assembly plan only after every preparation
  dependency succeeds, and verify the complete DAG on multiple workers. Do not
  model intermediate windows as `final_video` artifacts.

## P2 — transfer and publication overlap

- [x] Add media-aware HTTP Range fetching for supported MP4 sources
  (`internal/mediarange`). It scans top-level boxes, parses the video track's
  `moov/trak/stbl` (`stts`, `ctts`, `stsc`, `stsz`, `stco`/`co64`, `stss`),
  resolves windows by composition time, applies a single-entry edit-list
  offset, includes decoder pre-roll from the preceding sync sample and decodes
  through the next sync sample. Only container metadata and those sample
  ranges are written to an atomically renamed sparse file; ranges are merged
  within 128 KiB. The opt-in `VELOX_SPARSE_SOURCE_FETCH` flag wires it into
  `video.prepare.window.v1`; unsupported containers, fragmented MP4, servers
  without Range support and any parse error fall back to the canonical full
  download. Tests cover synthetic hole/requested-sample/fallback cases and a
  real FFmpeg B-frame fixture whose sparse decode is compared to the full
  source with `framemd5`.
- [~] Progressive upload plumbing exists: the worker negotiates early upload
  only for a fragmented-container profile, streams immutable prefixes, and
  has resume/abort handling. The fMP4 profile is deliberately disabled until
  its 100-job benchmark gate passes; do not flip the flag before that evidence
  exists. The gate is now pinned fail-closed by
  `shared/contract/canonical_video_profile_fmp4_gate_test.go`: missing, empty,
  malformed, wrong-schema, under-sampled, uncertified or undated evidence all
  return false, and even valid 100/100 evidence does not enable the profile
  without the separate operator opt-in. Remaining: run and record the gate
  against the final profile and verify final MP4 publication semantics on
  production-like workers.

## P3 — overlay work

- [ ] Composite visual layers in one pass when pixels must change. Remove
  avoidable intermediate MP4s and preserve the certified final video profile.
  **Blocked by an explicit architecture decision, not by missing plumbing.**
  `render_plan_parser_v1.cpp` and `v2.cpp` reject any plan carrying `layers`
  with “layers are not supported by the native video renderer: compositing is
  owned by the Chronon GPU backend”; v1 also states “Do NOT wire a native
  compositor here: route layered plans to Chronon”. On the Go side,
  `pkg/video/pipelines/clips/compiler.go` rejects composite overlays with
  “composite overlays require Chronon prepared fragments … native Velox layers
  are unsupported”. Implementing P3 in this repository would contradict those
  fail-closed guards; it requires the separate Chronon backend and is therefore
  not implemented here.

## Completion gate

- [x] Add/adjust focused tests for retry reuse, cache hits/misses, snapshot
  stability, executor validation, TaskGraph recovery, certification evidence,
  HTTP Range fallback, progressive upload, overlay equivalence, canonical
  `avcC` equality and cache-first dispatch. Sparse fetching has synthetic
  range/hole/fallback tests plus a real FFmpeg B-frame `framemd5` equivalence
  test; the fMP4 gate has fail-closed tests; staging, warm-cache attestation,
  extradata mismatch/reference and cache-first hit/miss paths have focused
  tests. Overlay equivalence remains blocked with P3.
- [x] Focused tests for assembly retry reuse/ineligibility, stock-pool
  snapshot and warm-cache paths pass
  (`go test ./contract`, `go test ./internal/mediarange`, focused
  `./internal/assets` and `./internal/worker`); worker resolver/executor
  packages pass (`go test ./internal/worker ./internal/taskrunner/executors`).
  The new worker window executor is exercised against real local FFmpeg/
  ffprobe output, including a cross-job warm-cache hit with timeline rebinding
  and no source bindings. The Master/worker `avcC` contract test runs against
  real FFmpeg output. SQLite FINALIZE claims pass 10 concurrent repetitions
  with exactly one successful claimant, and FINALIZE handler tests pass. The
  full DataServer assets suite is green; the exact-window filter forces CFR, so
  a 75-frame window reports 3.125 s instead of the muxer's last-PTS duration
  of 3.083333 s.
- [x] Source-tree verification is green (`go vet ./...`, `go build ./...`,
  `go test -count=1 ./...` on the affected modules; changelog records the
  full-module gate).
- [~] Production-only gates, not source work in this repository: build and
  certify the immutable worker image/digest, run cold/warm/retry production
  measurements (task counts, bytes by host, disk reads, encoded frames,
  cache-hit ratio, `created→ready`, `ready→first byte`, `first
  byte→published` p50/p95), verify prepared-window reuse across multiple live
  workers, and run the 100-job fMP4 gate. Evidence must use schema v2 and
  show certified artifacts plus real progressive-upload overlap. These
  require the deployed fleet and cannot be closed from the source tree.
- [~] Mark this roadmap and the associated work objective complete only after
  the production gates above are certified.
