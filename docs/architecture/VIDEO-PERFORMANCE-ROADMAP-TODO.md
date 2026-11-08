# Video performance roadmap — action list

Status updated: 2026-11-08

This checklist tracks the P0–P3 work needed to keep video bytes off the
control plane, reuse certified work across retries, and publish one clean
worker image after implementation and verification are complete.

## Current completion snapshot

- Complete: assembly-only retry, cache-first lookup, deterministic persisted
  stock selection, and the worker window-preparation executor.
- In progress: FINALIZE-to-DAG integration, worker-to-Master fragment
  exchange/cross-worker assembly integration, and production certification of
  progressive fMP4 upload.
- Not implemented: media-aware sparse MP4 range fetching and single-pass
  visual-layer compositing.
- Final gate not run: full repository verification, cold/warm/retry production
  measurements, and one final worker-image build/certification/publication.

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
- [~] Cache-first source preparation. FINALIZE looks up windows by certified
  source SHA before materializing source bytes, reuses READY fragments, and
  downloads only sources with an unknown identity or a cache miss. Unknown
  sources still follow the verified download/probe path.
- [x] Pin a full cache hit with a media integration test that removes the
  source file before retry; preparation still succeeds from the certified
  cached fragment without source materialization.

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
- [ ] Change FINALIZE into durable TaskGraph publication for independent
  preparation tasks. Keep final packet-copy assembly as one task when all
  fragments are ready; bound download, encode, I/O and plan-commit capacity
  separately.
- [~] Add a no-download stock-window task planner. It emits validated,
  content-identity-deduplicated `video.prepare.window.v1` requests from the
  immutable plan and rejects windows whose source SHA/size are unknown. It is
  not yet called from FINALIZE; unknown source identity still needs a worker
  identity-discovery path before all Master materialization can be removed.
- [~] FINALIZE queue execution is now bounded parallel across independent
  candidates, with an SQLite atomic `QUEUED→RUNNING` claim and refreshed spec
  hash so multiple Master instances cannot prepare the same candidate. This
  removes head-of-line blocking between jobs. Shared download/probe and window
  encode semaphores keep concurrent candidates within separate global resource
  budgets. The required per-window worker TaskGraph fan-out remains open;
  current FINALIZE still materializes missing windows through the Master.
- [~] Reuse fragment certification evidence keyed by blob SHA-256, encoder
  build revision and profile. Worker `video.prepare.window.v1` stores immutable
  content-addressed blobs and output evidence in persistent cache, rebinds the
  certificate to a new timeline, and warm reuse avoids encode, probe and SHA
  rereads after stat/attestation checks. Master-side evidence persistence and
  cross-worker artifact reuse through TaskGraph remain.
- [x] Centralize the stable cache-key serialization for `video.prepare.window.v1`
  in the shared contract. Timeline revision/hash/placement are excluded from
  the content-preparation identity, while source bytes/window and output
  profile revision remain bound. The worker executor now uses this shared
  helper; contract tests pin both reuse across placements and invalidation
  when the source window changes.
- [~] Worker chunk-manifest emission now reuses the resolver's verified
  `runtimeassets.Binding` identity instead of hashing each prepared fragment
  again. Chunk installation uses a hard link for immutable cache blobs on the
  same filesystem, with atomic copy fallback across filesystems. Master-side
  cache keys now include a revision hash over preparation algorithm, profile
  and quality policy, and cache hits require matching persisted source/window,
  profile/version, frame-count and keyframe/GOP evidence. The new worker
  executor emits source/output SHA, encoder build, profile and timeline-bound
  certification. The worker-local cache reuses that evidence across jobs;
  persistence and reuse through the distributed task DAG and Master fragment
  cache remain.
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

- [ ] Add media-aware HTTP Range fetching for supported MP4 sources. Read
  container indexes and required sample ranges, including decoder pre-roll;
  use a full-download fallback when the source format/server cannot safely
  serve the requested ranges.
- [~] Progressive upload plumbing exists: the worker negotiates early upload
  only for a fragmented-container profile, streams immutable prefixes, and
  has resume/abort handling. The fMP4 profile is deliberately disabled until
  its 100-job benchmark gate passes; do not flip the flag before that evidence
  exists. Remaining: run and record the gate against the final profile and
  verify final MP4 publication semantics on production-like workers.

## P3 — overlay work

- [ ] Composite visual layers in one pass when pixels must change. Remove
  avoidable intermediate MP4s and preserve the certified final video profile.
  Current clips compilation refuses composite overlays without the Chronon
  preparation path, and the native renderer does not support layers; implement
  a certified single-pass compositor before routing these plans through it.

## Completion gate

- [~] Add/adjust focused tests for retry reuse, cache hits/misses, snapshot
  stability, executor validation, TaskGraph recovery, certification evidence,
  HTTP Range fallback, progressive upload and overlay equivalence.
- [~] Focused tests for assembly retry reuse/ineligibility, stock-pool
  snapshot and warm-cache paths pass
  (`go test ./contract`, `go test ./internal/assets`); worker resolver/executor
  packages pass (`go test ./internal/worker ./internal/taskrunner/executors`).
  The new worker window executor is also exercised against real local FFmpeg/
  ffprobe output, including a cross-job warm-cache hit with timeline rebinding.
  SQLite FINALIZE claims pass 10 concurrent repetitions with exactly one
  successful claimant, and FINALIZE handler tests pass. Full-module
  verification and performance measurements remain open.
- [ ] Run the required full-module verification and cold/warm/retry
  performance measurements. Record task counts, bytes by host, disk reads,
  encoded frames, cache-hit ratio, `created→ready`, `ready→first byte` and
  `first byte→published` p50/p95.
- [ ] Review the complete diff and commit the cleanup and roadmap changes.
- [ ] Push the finished source once the implementation is ready, wait for
  worker-image certification, and record its immutable digest.
- [ ] Mark this roadmap and the associated work objective complete only
  after the image is certified and the P0–P3 acceptance criteria are met.
