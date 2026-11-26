# Video performance roadmap — action list

Status updated: 2026-11-26

## 2026-11-26 — first-use source certification and same-job assembly

FINALIZE now plans one `video.source.certify.v1` task for each missing
immutable video identity, independent of window count and timeline position.
The task requires a complete, SHA-verified source, returns structured source
evidence without a media output, and commits that evidence atomically under the
task attempt/lease/revision fence. FINALIZE then enriches and rehashes the
current plan from the newly persisted certificate before dispatching assembly.
Transient source download, probe, and block-hash failures use the existing
bounded retry mechanism; permanent identity, profile, and compatibility
failures remain terminal.

Production FINALIZE refuses to schedule new window-fragment tasks once the
source-certification planner is wired. It may reuse a previously registered
fragment when every needed cache entry is already present, but a missing or
incompatible source window fails explicitly. Persisted legacy preparation DAGs
remain supported until their retry/recovery references can be retired safely.
The M2M capabilities response advertises the source-certification executor
only for connected workers that actually register it.

The complete-file cache verification pass now computes the full SHA-256 and,
for video files up to 1 GiB, the optional fixed 1 MiB block digests together.
The block index is stored as atomic metadata beside the immutable CAS blob and
is passed into the certification executor, avoiding the prior additional full
file read on cold downloads. Old cache entries without this metadata fall back
to a complete verified read. Large sources omit range evidence and remain
eligible for complete downloads.

Verified locally: DataServer pipeline/assets/completion/gRPC affected suites;
worker source resolver, certification, runtime binding, and cache suites.
Still not verified: production rollout, live cold/warm timings, and PERFLIVE
results. Those require an authorized remote measurement window.

## 2026-11-26 — authenticated sparse inputs for certified direct assembly

Full-source certificates now optionally carry SHA-256 evidence for fixed
1 MiB source blocks. Workers produce the block list while rereading a complete
source whose whole-file SHA and exact frame facts have already been verified;
the evidence is stored in the dedicated `video_source_certificates` registry,
independent of media asset/blob rows, and may be upgraded from a prior
certificate without block hashes. Conflicting source or block evidence is
rejected. Sources above 1 GiB omit this optional evidence and continue through
complete download.

When a cold approved HTTPS video source has a complete block certificate and every
referenced segment is directly packet-copy eligible, the assembly resolver
fetches only MP4 metadata and required sample ranges. Every fetched 1 MiB block
is authenticated before its bytes are parsed or written. Authenticated blocks
are written directly into the private sparse staging file destined for the
muxer, eliminating the separate block-cache file and its second copy pass.
Sparse files do not enter the complete-asset cache or leases and are removed at
dispatch completion. A warm complete source is preferred; range failure or
unsupported containers fall back to the regular whole-file resolver, which
verifies the complete SHA. The muxer sees only authenticated block bytes;
materialization is block-granular, so bytes outside the exact sample spans but
inside a needed 1 MiB block can also be present. Production first-use
certification now happens before direct assembly as described below.

Regression coverage includes multi-window sparse extraction, altered block
rejection, malformed proof rejection before network access, sparse-binding
validation, proof upgrade/conflict handling, and nanosecond ordering of the
existing SQLite source registry. Production cold/warm render measurements and
deployment verification remain operator follow-ups.

Probe caches now accept and persist facts only from complete bindings. Sparse
preparation probes bypass both in-memory and disk caches, and the index schema
revision invalidates older entries whose byte completeness was not recorded.
The native packet mux also inspects the H.264 access unit at each requested
boundary and requires an IDR NAL, not only the container's generic key flag.

Known immutable SHA/size inputs and newly discovered identities now use the
same source-level certification task. The task result contains no MP4 and is
committed independently of asset/blob rows. FINALIZE enriches the current plan
from the newly registered certificate and retains the exact source offsets.
Direct packet assembly still performs native stream and IDR checks. If a
requested window cannot be proven compatible, FINALIZE fails instead of
materializing a new fragment. Legacy persisted preparation tasks remain for
recovery; their removal depends on auditing task/retry state and applying the
full-module pre-removal verification gate.

## 2026-11-26 — remaining source-contract work

Stock slots now continue from the consumed source frame between slots and
restart only when the pool cycles. Exact canonical source frame counts take
precedence over rounded duration metadata when SHA and size match. Legacy slice
assembly shares copy assembly's IO classification and on-demand input gate.
Binding integrity is checked once per declared asset; failed resolution batches
cancel sibling work.

Source-contract state and remaining work:

- Implemented: worker preparation now emits an optional full-source certificate
  from verified complete files with exact container frame count and zero
  presentation origin. The Master commits it to the independent source registry
  inside the fenced completion transaction after independently verifying the
  prepared fragment. FINALIZE resolves these proofs in a batch before stock
  selection/dependency planning; later renders can mux source windows directly.
  Missing counts, non-zero origins, sparse files and invalid proofs retain the
  existing window path.
- Implemented: sparse assembly inputs require fixed-size block SHA evidence
  bound to a complete source certificate. Only authenticated materialized bytes
  enter the mux; sparse files never claim full-file SHA verification and never
  enter the whole-asset cache. Complete downloads remain the fallback.
- Implemented: deterministic stock selection prefers already certified compatible
  candidates. After a partial slot consumes a mid-GOP boundary, expansion selects
  the next certified keyframe, restarting only when the pool cycles. Exact source
  frames take precedence over rounded duration metadata. Unselected assets are
  pruned before dispatch. Explicit editorial cuts are not silently moved, and the
  mux retains its packet-level checks.
- Retain independent verification for newly registered fragments. Reused READY
  fragments already skip repeated probes; removing the initial verification
  requires an explicit trust/evidence contract, not another cache toggle.

## 2026-11-25 — remove the unused Master trimmer and duplicate probe index

The last production code for Master-side source-window rewriting and trimming
had no callers: FINALIZE schedules worker preparation, and the assembly path
consumes registered prepared assets. Removed that dead materializer, its trim
planning and normalization helpers, and the Master persistent keyframe index
(which was not reusable because the Master probe had no immutable source SHA
or size). The Master keeps only the independent ffprobe validation needed when
registering worker-produced windows. Worker source certification and its
content-addressed cache remain the active preparation authority.

One-worker topology is the deployment target. Multi-worker test and rollout
requirements are out of scope. Local verification is the completion gate for
this source change; production latency and cold/warm render measurements are
left to the operator after deployment, not a blocker for implementation.

## 2026-11-25 — direct certified source windows and single new-job route

In the current working tree, the stock-window planner now skips
`video.prepare.window.v1` when the source already carries canonical immutable
evidence. The copy assembler accepts such source windows with non-zero offsets;
the native mux remains responsible for checking that the requested offset is
an exact keyframe. New compiled video plans now route to
`video.assemble.copy.v1`; the slice contract and executor remain registered to
serve tasks already persisted with the legacy executor ID. The legacy executor
is now an adapter to the copy implementation, so validation, native muxing,
output handling and metrics have one implementation. This removes slice work
from new-job scheduling without invalidating queued work.

The shared preparation cache key now uses a versioned normalized source-window
identity. Frame windows and timestamp windows share a key when they denote the
same canonical-profile frame range; non-frame-aligned timestamps retain their
exact microsecond coordinates to avoid unsafe cache collisions. This makes old
key-format entries miss and be rebuilt on demand; their stored blobs are not
deleted by this change.

New preparation requests now carry `video-window-preparer.v2`, whose revision
binds the canonical profile, avcC identity and source-acceptance constraints,
not encoder arguments. Workers accept syntactically valid v1 revisions only
for persisted tasks; the v2 cache key and the revision value keep those results
separate from new evidence. CRF-only changes no longer invalidate packet-copy
preparation evidence.

Master registration also checks the READY asset for the same content-window
identity before probing or staging an upload. It reuses that asset only when
its persisted certificate validates against its original window and reproduces
the current identity, output SHA/size, profile, and canonical avcC. A real-media
registration test pins the cache hit to zero additional FFprobe calls.

Targeted planner, contract, DataServer pipeline, task-contract and worker
executor tests pass, including a direct-source execution test that verifies
the worker sends the non-zero source offset to the native mux. The canonical
x264 argument projection now pins BT.709 primaries, transfer, and matrix
coefficients in SPS VUI parameters as well as container metadata. That fixes
the local FFmpeg 8 mismatch: producer, worker, and whole-source normalization
real-FFmpeg tests now emit the pinned `7201ce…` avcC. The full DataServer gate
(`go vet ./...`, `go build ./...`, `go test -count=1 ./...`) passes after
updating a stale enqueue assertion that still expected new Drive stock plans
to use the legacy slice executor. This working-tree change has not been
deployed.

The C++ `media_packet_pipeline_tests` and
`render_copy_only_zero_intermediates_tests` also pass against the local LibAV
build, covering exact non-zero keyframe offsets and packet-copy assembly
without video intermediates.

## 2026-11-25 — zero-reencode assembly correction

The local implementation now has one video-preparation behavior: packet-copy
an already certified compatible window or fail. Existing video is not
normalized or encoded during `video.prepare.window.v1`, worker-owned stock
preparation, or native mixed assembly. The Master snapshots stock selection
and expands pool slots into explicit source windows using metadata only. The
worker downloads, certifies and prepares those windows. The worker rejects
unresolved `stock_pool` or `stock_pool_slot` entries; assembly does not select
stock or invoke nested preparation. The ordinary `/jobs` submit path persists
this work as queued FINALIZE instead of materializing windows synchronously on
the Master. The old Master download/probe/trim/cache implementation has been
removed. Serving a missing prepared fragment is now byte retrieval only; it no
longer downloads the source and reconstructs a fragment inline. Registered
worker fragments are recorded as packet-copy outputs without legacy
re-encode/normalization metadata.

Incompatible sources fail before publication. The canonical encoder arguments
now emit the pinned avcC locally; other source encoders still need the same
immutable stream evidence before direct packet-copy assembly can accept them.

Legacy cache migration is fail-closed: current cache keys include the v2 key
schema and preparation revision, so old entries miss rather than being treated
as current evidence. Persisted v1 tasks remain executable for compatibility,
but their evidence is revision-separated; a shared-contract regression pins
that separation. Legacy blobs are retained and are not deleted by this change.
The direct source mux path is ready for operator-led production measurement
after deployment. Local DataServer, worker, shared-contract, and C++ gates are
the completion criteria for this implementation; production timing collection
is operational follow-up and this source change does not deploy to another
computer.

This checklist tracks the P0–P3 work needed to keep video bytes off the
control plane, reuse certified work across retries, and publish one clean
worker image after implementation and verification are complete.

## Current objective

Close the source-tree gaps behind the recurring r67f stall: make executor
required assets, prefetch eligibility and completion behavior explicit in one
authority, expose a read-only job doctor for operator diagnosis, and pin both
with regression coverage. Production-only gates — fleet rollout, 100-job
benchmark evidence and digest promotion — remain open below.

## Current completion snapshot

- Complete: executor task contracts in the canonical `shared/taskcontract`
  package now declare required assets, prefetch eligibility and completion
  kind; the DataServer placement and preparation gate consume that declaration
  instead of inferring inputs from the whole job payload. Window preparation declares
  exactly one source and skips the job-wide prefetch gate; unregistered
  executors stay conservative.
- Complete: read-only `velox-admin doctor` reports an evidence-backed verdict
  and stable cause code for one job, including pending preparation, expired
  reservations, fence conflicts and capacity/worker starvation. It never
  writes to SQLite and says `INSUFFICIENT_EVIDENCE` when a projection is
  missing.
- Complete: assembly-only retry, cache-first lookup, deterministic persisted
  stock selection, the worker window-preparation executor, prepared-fragment
  link/clone staging, and warm-cache attestation.
- Complete: FINALIZE-to-TaskGraph publication, worker-to-Master fragment
  exchange, Master-side cross-worker fragment reuse, and worker-side source
  identity discovery. The control plane no longer materializes Drive stock
  bytes when worker preparation is wired.
- Complete: canonical H.264 encoder identity. Master and worker share the
  pinned `libx264` preset, reference count and BT.709 SPS VUI metadata. Worker
  certification compares actual `avcC` bytes with the canonical identity and
  a per-build reference. Packet-copy preparation revision v2 binds the
  canonical profile, `avcC`, and source acceptance rules; encoder quality
  arguments do not invalidate packet-copy evidence.
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

## Executor contracts and job diagnostics (r67f)

- [x] Add executor contracts to the canonical `shared/taskcontract` package.
  Contracts key on executor ID/version and consume a `TaskInput` view, so the
  persisted `TaskSpec`/spec-hash contract is unchanged. The declaration point
  covers `Validate`, `RequiredAssets`, `RequiresPrefetch` and `CompletionKind`.
  Register `video.prepare.window.v1` as one-source/no-prefetch with
  `prepared_segment` completion, `video.assemble.copy.v1` as
  compiled-plan/runtime-asset with on-demand fetch after FINALIZE, and
  `video.assemble.slice.v1` as a legacy copy adapter with the same
  on-demand input path; both have `artifact_commit` completion. Unknown executors keep
  the legacy payload scan and require prefetch.
- [x] Resolve placement `RequiredAssetKeys` from the executor contract.
  Window preparation no longer inherits every sibling asset in the job; the
  r67f 246-asset shape is pinned by a regression test.
- [x] Make the preparation gate ask the contract instead of special-casing the
  window executor ID. Only a registered contract that explicitly returns
  `RequiresPrefetch == false` bypasses the asset reservation/evidence wait,
  but never the `runtime_assets_pending` PREPARE/FINALIZE fence. Packet-copy
  assembly opts into this because its worker fetches content-addressed inputs
  on demand; the legacy slice adapter shares this behavior; unregistered executors remain strict.
- [x] Add `velox-admin doctor --db --job [--json] [--output] [--stall-after]`.
  It opens SQLite read-only via `invariants.OpenReadOnly`, reports
  `ADVANCING`/`STALLED`/`BLOCKED`/`COMPLETE`/`UNKNOWN`, and uses closed cause
  codes for preparation, reservations, workers, capacity, fencing, missing
  commits and no-progress. Missing evidence yields
  `INSUFFICIENT_EVIDENCE` rather than a fabricated cause.
- [x] Cover the contracts and doctor with focused tests: single-source window
  declaration, assembly assets, conservative unknown executor, malformed
  payloads, duplicate registration, pending preparation, expired reservation,
  fence conflict, complete job and insufficient evidence.

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
- [x] Reuse fragment certification evidence keyed by source-window identity,
  canonical profile and output SHA-256. Worker `video.prepare.window.v1` stores
  immutable content-addressed blobs and output evidence in persistent cache,
  rebinds the certificate to a new timeline, and warm reuse avoids encode,
  probe and SHA rereads after stat/attestation checks. The certificate keeps
  the originating FFmpeg build as provenance; a warm hit validates the
  recorded evidence and pinned avcC without launching the current FFmpeg.
  Master-side fragments carry their
  certificate in the source metadata; `LookupPreparedWindowAsset` lets
  FINALIZE reuse a READY fragment from another job/worker before publishing a
  preparation task, and the reused bindings are persisted in
  `runtime_prepare_window_reuse` until the assembly plan is rewritten.
- [x] **Close the H.264 encoder-identity gap.** `CanonicalVideoEncodeArgs` now
  pins `-preset veryfast`, `-refs 1` and BT.709 `colorspace/color_primaries/`
  `color_trc/color_range` and the corresponding x264 SPS VUI values in addition
  to profile, level, GOP and B-frames. `CanonicalVideoEncoderArgsDigest`
  identifies producer settings, while `VideoWindowPreparationRevision` v2
  intentionally binds packet-copy evidence to the profile, canonical `avcC`,
  and source acceptance rules rather than encoder or quality arguments. Worker
  certification builds a one-frame reference with the same canonical
  contract, probes its `avcC` SHA-256 and rejects a fragment whose extradata
  differs (`WINDOW_EXTRADATA_IDENTITY_MISMATCH`); warm attestations apply the
  same check. Real-FFmpeg tests require Master, worker and whole-source
  normalization to produce byte-identical `avcC`.
- [x] **Make the worker cache-first before source resolution.**
  `TaskRunner.ProbeLocalPreparation` asks the registered executor whether the
  fragment identity, immutable file metadata, canonical profile and `avcC`
  already match a local attestation. A warm hit does not require the current
  FFmpeg binary or version. Dispatch skips both the legacy asset resolver and
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
- [x] Build the internal prepared-window exchange outside final artifact
  publication. The Master now has worker-authenticated GET/PUT endpoints keyed
  by the shared content identity; PUT validates the request/certificate,
  recomputes the output hash, independently probes the canonical stream and
  frame count, and registers a content-addressed asset with its certificate.
  The worker checks the Master cache before uploading and removes registered
  preparation outputs from the final-video publication protocol. Focused
  Master and worker tests pass. Successful task-attempt segment evidence is
  now readable from SQLite, including persisted `registered_asset_id`; a
  new query selects only the latest successful attempt for a prepared-window
  task and excludes other executors. FINALIZE consumes that evidence, requires
  a registered asset for each successful preparation, and rewrites the
  immutable assembly plan only after every dependency and cache binding is
  validated. A regression test covers recovery after the worker result has
  been persisted. Prepared-window cache lookup also treats any persisted avcC
  other than the exact canonical hash as a miss, preventing incompatible
  legacy evidence from reaching assembly. Remaining: collect production
  multi-worker measurements. Do not model intermediate windows as
  `final_video` artifacts.

## P2 — transfer and publication overlap

- [x] Add media-aware HTTP Range fetching for supported MP4 sources
  (`internal/mediarange`). It scans top-level boxes, parses the video track's
  `moov/trak/stbl` (`stts`, `ctts`, `stsc`, `stsz`, `stco`/`co64`, `stss`),
  resolves windows by composition time, applies a single-entry edit-list
  offset, includes decoder pre-roll from the preceding sync sample and decodes
  through the next sync sample. Only container metadata and those sample
  ranges are written to an atomically renamed sparse file; ranges are merged
  within 128 KiB. The former unauthenticated opt-in path through
  `video.prepare.window.v1` has been removed; partial fetch is permitted only
  by the certified direct-assembly resolver with per-block SHA-256 evidence.
  Unsupported containers, fragmented MP4, servers without Range support and
  any parse error fall back to the canonical full download. Tests cover
  synthetic hole/requested-sample/fallback cases and a
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
- [x] Executor contracts and the read-only job doctor are covered by focused
  tests and the full-module gate. The SUCCEEDED-writer audit explicitly
  classifies the completion DAG fixture as test-only: it seeds
  `task_attempts`, never `jobs.status`.
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
