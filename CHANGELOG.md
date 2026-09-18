## [Unreleased] - 2026-09-18

### Performance — progressive early upload and submit-time prefetch

- Early progressive uploads now send a 256 KiB first part followed by
  bounded parts up to 2 MiB, honor `ProgressivePartConcurrency`, and compute
  the final SHA-256 while the growing output is being uploaded.
- Newly persisted jobs trigger the existing warm-worker `FutureAssetPlan`
  reconciliation immediately after atomic enqueue, while the durable planner
  remains the retry/reconnect fallback.
- The opt-in chunk-store capability now has its first packet-copy consumer:
  it persists reusable chunk payloads and a worker-local manifest while
  leaving manifest-first delivery disabled until the Master/edge contract is
  landed.
- Worker image release target: `v1.4.37`.

### Correctness — early upload, task DAG, and batch intake hardening

- A missing early-upload plan now falls back after a bounded three-second
  wait; growing-file waiters use generation channels instead of parked
  condition-variable goroutines, and progress propagation is event-driven.
- Early uploads reserve the durable output-spool identity and journal path;
  normal declaration still stamps the commit/target needed for resumable
  retry. Master-stream abort now deletes the remote session and staging data.
- Fan-out task creation is transactional on SQLite, dependency rewrites
  revalidate the complete graph inside the same transaction, and concat
  telemetry uses the mutually-exclusive maximum rather than a double-counting
  sum. Batch usage stats count only accepted items.
- Added the legacy task-schema repair migration and removed the duplicate
  file stat from the master-stream upload path.

### Added — 100x scale track: batch plan dedupe, persistent task DAG, chunk factory (W5 precursor), chaos scenario 20

Four features from the 100x velocity plan (docs/SCALE-IMPROVEMENT-PLAN.md,
docs/100-percent-plan/04) landed as atomic commits, each verified with the
full-module gate:

1. **Batch plan dedupe (#17)** — `POST /api/v1/jobs/batch` now collapses
   fingerprint-identical items: after `NormalizeCanonicalRecipe`, each item's
   render identity is SHA-256'd over every render-relevant field EXCEPT
   idempotency_key, video_name, publications/delivery_plan (a differing
   destination is a second PUBLICATION of the same render, not a second
   render) and placement_pin_worker_id. A dedup item reuses the anchor item's
   job_id (`status:"dedup"`, `deduped_of` = anchor index) instead of
   enqueueing a byte-identical render. A failed enqueue never becomes an
   anchor. Response gains `summary` {total, accepted, deduped, rejected,
   conflict, failed}. Wire schema single-sourced in `internal/apiwire` and
   regenerated via `cmd/api-schema-gen -apply`.
   Evidence: `go test ./internal/handlers/server/pipeline/ -run 'Batch|Dedupe' -count=1`
   green; dedupe ledger `batch_plan_dedupe_test.go` covers fingerprint
   stability (delivery-plan-only variants dedupe), failed-anchor exclusion,
   and handler wiring.

2. **Persistent task DAG (#15)** — migration `176_task_dependencies.sql`
   adds the `depends_on` edge list to the canonical `tasks` table
   (previously `Task.DependsOn` lived only in the in-memory model, so
   dependency gating did not survive a master restart and multi-task jobs
   were impossible under `idx_tasks_job_id_unique`). SQLiteTaskRepository
   persists/reads `depends_on`; `taskgraph.ValidateTaskGraph` adds fail-closed
   cycle detection + unknown-dependency rejection at enqueue; `TickReadiness`
   propagates terminal-FAILED dependencies (anti-zombie: a doomed PENDING
   task is failed by the sweep instead of waiting forever).
   `ExpandService` (taskgraph) is the fan-out enqueue primitive: one job →
   N tasks with validated edges, persisted atomically.
   Evidence: `go test ./internal/taskgraph/ ./internal/store/
   -run 'TaskGraph|Depend|Expand' -count=1` green including the SQLite
   round-trip integration test (expand → TickReadiness → root terminal →
   dependent READY).

3. **Chunk factory, W5 precursor (#14)** —
   `RemoteCodex/native/worker-agent-go/internal/chunkfactory`: keyframe-
   aligned, content-addressed chunk planner + store. Chunk identity = SHA-256
   over {asset_key, source window, profile, chunk_index, identity_version};
   chunks START only on keyframes (the packet-copy mux rule); writes are
   atomic temp+rename and idempotent. Wired into the worker composition root
   behind `chunk_store_enabled` / `VELOX_CHUNK_STORE_ENABLED` (default off):
   capability is DISABLED by default, READY when enabled, MISCONFIGURED —
   fail-closed exit 1 — when enabled but the root cannot be created
   (AGENTS.md §6 state machine; no enabled-with-a-stub path).
   Evidence: `go test ./internal/chunkfactory/` green (keyframe alignment,
   identity stability, idempotent Put); worker bootstrap builds clean.

4. **Chaos scenario 20, W8 (#16)** —
   `tests/e2e/recovery-matrix/scenarios/20-dag-kill-worker-propagation.sh`:
   kill a worker mid-DAG and verify (a) FAILED root propagates to dependent
   PENDING tasks via the anti-zombie sweep (transitive closure over
   `depends_on`), (b) no PENDING task sits behind a terminal-FAILED
   dependency (DAG-P1), (c) no dangling edges (DAG-P2). DAG-P1/P2 are now
   formal invariant functions in `invariants.sh` (invokable by every
   scenario); `run.sh` enumerates scenarios 01-20.
   Evidence: scenario 20 runs 12/12 PASS against a seeded fixture DB; also
   executable against a live server when `API_URL` is provided.

### Fixed — readiness for multi-task jobs and legacy-upgrade fixtures

- `SQLiteTaskRepository.GetByJobID` is deterministic under multi-task jobs
  (stable `ORDER BY created_at, task_id`) instead of relying on insert order.
- Migration `176` `ALTER TABLE` is tolerant of a pre-existing column and of
  fixtures that never created `tasks` (pattern 022; runner formalized in
  `migrations/apply.go`), so legacy-upgrade fixtures that seed history
  through earlier migrations keep passing.
- Worker test fixtures that hand-build the `tasks` table now include
  `depends_on` (AGENTS.md §4 fixture pattern).

Gate evidence: `DataServer` `go vet ./...` 0, `go build ./...` 0,
`go test ./internal/taskgraph/ ./internal/store/ ./internal/handlers/server/pipeline/
./internal/apiwire/ -count=1` green; worker module `go build ./...` 0,
`go vet` on touched packages 0, `go test ./internal/worker/
./internal/chunkfactory/ ./pkg/config/` green.

### Removal — unwired derived-assets resolver + unwired shared placement contract (~667 LOC)

Full removal per ADR 0008 §(b) point 1 — **both conditions fail**, so this is
not a soft-deprecation: **C1** (external callers) zero verified — the
`derivedassets` resolver and the `shared/placement` contract were imported by no
file outside their own directory; **C2** (reachable from outside the repo)
fails — `derivedassets` is an `internal/` package, not importable from outside
its module, and `velox-shared` resolves in-tree through `go.work` + `replace`,
never as a published module; `shared/placement` is not a public HTTP route,
exported module symbol, or public CLI command.

Deleted: `RemoteCodex/native/worker-agent-go/internal/derivedassets/resolver.go`,
`.../resolver_test.go`, and `shared/placement/placement.go` — 3 files, 298
production + 369 test LOC.

**Why removal and not wiring.** `derivedassets` was a complete MISS-side
orchestrator (verify-then-promote over `workercache.DerivedAssetStore`, with a
fail-closed size + SHA-256 re-check) but no production call site ever built a
`Resolver`: the native path never routed through it. `shared/placement` was
created by PR #7 to lift the placement type contracts out of the duplicated
worker-side costmodel so both modules would import one source of truth; that
migration never landed, so the duplication it was meant to delete still exists
and the package only added a second, unused declaration of `ResourceClass` and
`TemporalMode`.

Gate evidence: `scripts/ci/pre-removal-verify.sh` (AGENTS.md §1) green —
`go vet`, `go build`, `go test -count=1 ./...` all 0 over the full `DataServer`
module (test wall clock 264s, no pre-existing failure surfaced). Because the
removal touched the `shared` and `worker-agent-go` modules and not only
`DataServer`, the gate was extended with full-module `go vet ./...` +
`go build ./...` on both remaining modules plus a full-module
`go test -count=1 ./...` on `worker-agent-go` — all green, with zero residual
references (`derivedassets` / `velox-shared/placement` grep clean).

#### Candidates rejected after verification (deliberately NOT removed)

- `shared/contract/payloadfield/payloadfield_gen.go` has no Go importer but is
the **guarded output of `contractgen -check`**: `scripts/ci/check-contract-schema.sh`
regenerates it and fails on any drift. Removing it would break the contract gate.
- `worker-agent-go/internal/jobperf` has no Go importer but is an explicit
exclusion in `scripts/ci/check-telemetry-architecture.sh` and a CHANGELOG-cited
fact source — live policy, not dead code.
- `DataServer/internal/integration_test` is real integration coverage named only
in a commented-out line of `.github/workflows/no-youtube-regression.yml`, i.e. a
wiring gap rather than dead code; it stays until it is wired into a target or
retired together with the five production comments that cite it as a
golden-assertion anchor.
- The five committed `*.pb.go` files (7,929 LOC) are regenerable via
`scripts/gen-proto.sh`, but that script requires `protoc` + Go plugins on `PATH`
and fails loudly when they are missing. Dropping them from the tree would make
`go build ./...` — the AGENTS.md §1 gate itself — depend on an external
toolchain, so they stay committed.

### Cleanup — orphaned benchmark evidence dumps (~5,935 LOC)

`docs/benchmarks/evidence/phase2-engine-delta-2026-08-17/run-current-engine.json`
(2,480 LOC) and
`docs/benchmarks/evidence/phase3-read-amplification-2026-08-17/run-read-amp-fix.json`
(3,455 LOC) were referenced by no document, script, workflow or Go file — the
only `grep` hits were the substring "read-amp" inside comments in
`pkg/performance/{compare_runs.go,gate_tiers_test.go}`. Both removed, with their
now-empty directories.

The phase0 and phase1 dumps were **kept**: they are cited as supporting evidence
by five committed reports (`phase0-perf-report`, `phase0-reference-profiling`,
`phase0-priority1-decision`, `receipts/phase0-receipt`,
`phase1-zero-spawn-vs-ffmpeg`).

### Removal — unwired native frame compositor (~1,411 LOC)

Full removal per ADR 0008 §(b) point 1 — **both conditions fail**, so this is
not a soft-deprecation: **C1** (external callers) zero verified — `FrameGraph`
and `PixelKernelRegistry` were constructed by no production code, only by their
own translation units and their tests; **C2** (reachable from outside the repo)
fails — engine-internal C++ symbols, not a public HTTP route, exported Go
symbol, published module, or public CLI command.

Deleted: `src/render/{frame_overlay,frame_graph,kernel_registry}.cpp`, their
three headers under `include/velox/render/`, and the three tests
(`tests/test_frame_{overlay,overlay_simd,graph}.cpp`) — 9 files, 773 production
+ 638 test LOC. Both now-empty directories removed; CMake unwired
(`VELOX_FRAME_OVERLAY_TEST_SOURCES` in `cmake/Dependencies.cmake`, three test
targets in `cmake/Tests.cmake`); the engine `README.md` source tree no longer
lists a `src/render/` block (it had also drifted: `frame_backend.cpp` was
listed but does not exist).

**Why removal and not wiring.** An earlier audit proposed connecting this
compositor to its first caller, since the AVX2 overlay kernel was real and
bit-exact tested. That is obsolete: compositing and overlays are owned by
**Chronon** (GPU, headless lambda) and this machine performs no overlay work.
Keeping an unwired AVX2 kernel alongside a permanently unreachable
`FrameGraph` is dead code with a maintenance and a false-capability cost — it
reads as "the native engine can composite" when it cannot.

**The fail-closed boundary stays, and is now the canonical statement of
ownership.** The V1 and V2 parsers still reject editorial `layers` at the
parser boundary instead of accepting a plan whose overlays would silently
disappear from the output; both messages now name Chronon as the owner of
compositing, and both carry a comment forbidding a native compositor from being
wired there. The rejection tests are unchanged and still green
(`test_render_plan_v2.cpp` asserts behaviour, not stderr text).

Gate evidence: fresh out-of-tree `cmake` configure + full `ninja` build (567
targets, exit 0) with **zero** references to the removed symbols in the
generated build graph; `ctest` 21/22; `scripts/ci/pre-removal-verify.sh`
(AGENTS.md §1) green — `go vet`, `go build`, `go test -count=1 ./...` all 0 over
the full `DataServer` module. LOC and no-binaries gates green.

#### Finding — pre-existing C++ test failure, NOT attributable to this removal

`packet_components_tests` fails on
`FAIL: complete certified container metadata skips stream-info discovery`.
Attribution verified by construction: the **pre-removal** binary left in
`build/` (compiled before this change, still carrying the old
targets) fails with the **identical** message, and the target links only
`tests/test_packet_components.cpp` + ffmpeg — no file touched here. Tracked as a
followup for the packet/container metadata path, not a blocker.

Unrelated pre-existing gate failure (`check-architecture.sh`, BUILD_INFO
version drift) is unchanged and already tracked by the fMP4 rollout entry in
this file; regenerating it now would stamp the drift commit from a dirty tree.

### Video hot path: segment-worker override no longer oversubscribes the host

`ComputeNativeRenderBudget` divides one render's CPU budget across concurrent
renders and then decides how many clips run in flight (segment workers) and how
many threads each gets. The native engine treats `VELOX_NATIVE_SEGMENT_WORKERS`
as authoritative and clamps it to 8, but the worker-side budget ignored the
knob: it always split the threads for its own computed two-worker default.

An operator who raised the knob therefore got more clips in flight **each
budgeted as if it were one of two** — e.g. eight workers on a 15-core budget
still carrying the two-worker thread share, i.e. a deeply oversubscribed host.
The budget now resolves the same knob itself and re-divides the SAME per-render
budget across the requested worker count, clamped by both the engine cap (8)
and the render's own usable cores, with garbage/zero/negative values falling
back to the computed default.

- `pkg/video/pipeline/native_budget.go` — operator override honoured and
  re-split; the computed default is intentionally unchanged (see below).
- Tests pin the contract, including the engine's real admission rule that one
  segment claims `max(decoder_threads, encoder_threads)` and NOT their sum
  (`render_engine_timeline.cpp`: `SegmentResourceClaim{max(...), 0}`) — the sum
  would over-report the reservation.
- `deploy/runtime/worker.env.example` documents the knob as the per-host,
  certified lever.

**The default is deliberately NOT widened.** More in-flight clips also multiply
frame-pool memory, and the trade only exists as a measurement: the repo's perf
doctrine puts distribution budgets behind tier 2
(`docs/performance-gates.md`) on a dedicated host, and parallelism changes go
through `docs/100-percent-plan/parallelism-certification.md`. The lever is now
safe to raise per host; the default stays conservative until a benchmark run
says otherwise.

#### Correction to the earlier hot-path reading of this migration

Two claims made while reviewing the fMP4 work were wrong and are corrected
here, so nobody optimises the wrong code:

1. **The sequential loop in `renderLegacyTimeline` is NOT the video path.**
   `render_engine_orchestrator.cpp` rejects a video timeline without an explicit
   renderer (`video_renderer_mode_required`) and routes real video work to
   `renderCopyOnly` / `renderMixed`, both of which are packet-copy based with
   overlapped source opens (`media_packet_sessions.cpp`, up to 8 concurrent).
   The remaining sequential loop only covers timelines with NO video sources
   (image/colour segments).
2. **`VELOX_NATIVE_SEGMENT_WORKERS` was already authoritative** in the engine
   (`setEnvIfAbsent` in `engine_process.go`: "explicit operator environment
   remains authoritative"). The two-worker cap was the computed default, not a
   hard ceiling — the real defect was that overriding it did not re-split the
   threads (fixed above).

Remaining, measured-not-guessed levers for video throughput, in order:

| Lever | Where | Note |
|---|---|---|
| `VELOX_NATIVE_SEGMENT_WORKERS` 3-8 on wide hosts | this release | now safe; certify per host |
| Artifact staging is always NVMe (`ARTIFACT_FINAL` in `pkg/storage/resolver.go`): a full write+read of the final file before upload | next | fMP4 is append-only, so the mux output can stream into the progressive upload instead |
| GPU: filter backend is CPU-only (`frame_pipeline_filter.hpp`), codec pinned to `libx264` | feature | NVENC/NVDEC facts are collected but placement ignores them by design (ADR 0009); target already written down: `frames_downloaded_from_gpu == 0` (`jobperf/tracker.go`) |
| `VELOX_PROGRESSIVE_PART_CONCURRENCY` (default 4) and the 8 MiB part size | tuning | env-only today |
| Route more jobs through packet copy by normalising assets once at ingest | strategy | `copy_only` needs keyframe-safety + an identical media signature |

### fMP4 rollout control plane: canonical gate rollout + producer-job acceptance

Closing the two rollout/control-plane gaps reported for the fMP4 final-mux
migration. The native mux work was already on `main`; what was missing was a
sanctioned way to move the admission flag onto ALREADY-INSTALLED workers, a
way to prove the flag is live there, and a producer-job acceptance harness.

- **Worker env mutator** — `deploy/runtime/velox-worker-set-config` accepts
  `--fmp4-stream-profile 0|1` (the fMP4 admission gate) and now preserves any
  managed knob that was not requested. The previous `awk` dropped unrequested
  managed keys, so an fMP4-only rollout would have silently erased a worker's
  `VELOX_AUDIO_MIX_STRATEGY` / `VELOX_AUDIO_MIX_PROFILE` settings. Atomic
  rewrite, service restart, readiness wait and file rollback are unchanged.
- **Master control plane** — the allowlisted worker-config operation carries
  `fmp4_stream_profile` end to end: `fleet.WorkerConfigPayload` +
  `WorkerConfigExecutor` command construction, `MutationRequest` validation
  (`0|1`, and "at least one setting" still enforced), and the
  `fleet_operations` audit payload. This is the only sanctioned way to change
  the flag on an installed worker: `deploy/runtime/worker.env.example` seeds
  fresh hosts only, and hand-editing `worker.env` is forbidden by
  `docs/operations/worker-rollout-paths.md` §5.
- **fleetctl** — `worker-config set <worker_id> --fmp4-stream-profile 0|1`
  (both `--flag value` and `--flag=value` forms), with the help text and
  `deploy/fleetctl/README.md` allowlist table updated.
- **Rollout verification** — new read-only `scripts/ops/verify-fmp4-rollout.sh`
  reports, per worker, the value in `/etc/velox-worker/worker.env` AND the
  effective value inside the running container, classifying each worker as
  `READY` / `DISABLED` / `MISCONFIGURED` (drift between file and container, or
  a value the Go gate does not recognise — which it otherwise treats as
  disabled, silently). Exit `0` all READY, `1` rollout incomplete, `2`
  usage/SSH failure. This is the check that distinguishes "flag added to the
  template" from "flag live on the worker".
- **Producer-job acceptance** — new `scripts/ops/verify-fmp4-producer-job.sh`
  submits (or adopts) a REAL producer job through the documented M2M intake
  with a producer-owned `CompiledRenderPlanV2`, then proves the three values:
  (A) `output.profile_id=velox-h264-fmp4-stream-v1` on the submitted plan,
  (B) `artifact.safe_offset_bytes > 0` observed while the render was still
  running (`artifact.finalized != 1`), and (C) `moof` present in the published
  artifact. Exit `0` PASS, `1` FAIL, `2` precondition, `3` INCONCLUSIVE (a
  check could not be observed — never a silent pass; `--allow-artifact-only`
  makes the check-B waiver explicit).
- **Tests** — offline contracts for every new surface: helper allowlist /
  atomic rewrite / cross-knob preservation / rollback
  (`scripts/ci/test-worker-set-config-offline.sh`, 7 checks), the rollout
  verifier's state matrix with mocked ssh/sudo/docker
  (`scripts/ci/test-verify-fmp4-rollout-offline.sh`, 9 checks), and the
  acceptance harness against a local mock Master
  (`scripts/ci/test-verify-fmp4-producer-job-offline.sh`, 5 checks); plus Go
  tests for the executor command, the API validation boundary and the audit
  payload (`fleet`, `handlers/server/api`, `cmd/fleetctl`).
- **Docs** — rollout + acceptance runbook in
  `docs/operations/REMOTE-M2M-JOB-OPERATIONS.md` (with the two remaining
  acceptance checkboxes and the tracked follow-ups: artifact scratch staging
  still file-backed, and the static fleet list used by `--fleet`), a new
  "runtime capability flags" section in
  `docs/operations/worker-rollout-paths.md`, and the fresh-host vs
  already-installed distinction in `deploy/runtime/README.md`.

#### Gate findings surfaced while verifying this work (pre-existing, tracked)

Per the §4 rule in `AGENTS.md` (findings are not blockers for unrelated work,
but must be tracked), the verification run surfaced one failure that this
change did not introduce:

- `scripts/ci/check-architecture.sh` fails on `main` with
  `BUILD_INFO.json version drift: RemoteCodex/BUILD_INFO.json version=v1.4.21
  vs VERSION.txt v1.4.33`. Attribution: `7219a1a0 chore(release): prepare
  worker v1.4.33` bumped `VERSION.txt` without regenerating
  `RemoteCodex/BUILD_INFO.json` (last regenerated by `88ca842d` for
  v1.4.21). Neither file is touched by this change; `git diff HEAD --stat`
  for both is empty. Remediation (release owner, at release time, so the
  recorded `git_commit` matches the commit that contains the regenerated
  file): `./scripts/generate-build-info.sh`, then re-run
  `scripts/ci/check-architecture.sh`. Deliberately NOT regenerated here:
  the generator stamps `git_commit` from HEAD, which with a dirty tree would
  record a commit that does not contain the change. `make verify` is
  therefore red on `main` for this one pre-existing reason.

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
