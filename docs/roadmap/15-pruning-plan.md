# 15 — Pruning plan for architectural duplicates

> **Verdict (verbatim from the user-pasted audit).** La repo **non è piena di
> copy-paste casuale**, ma contiene diverse duplicazioni architetturali
> importanti. Le due aree più pesanti sono (1) Jobs/Queue e (2) YouTube. Ci
> sono poi ridondanze più semplici da eliminare in bootstrap, moduli,
> Drive, asset e worker. La strategia giusta non è un grande refactor
> unico. Servono PR piccole in questo ordine.

This document is the implementation roadmap for those seven small PRs.
Each section is **self-contained**: goals, files to touch in edit
order, tests, risks, and a verification command. After review, each
section can be promoted to its own numbered roadmap file.

## 0. Cross-PR principles

These come from cross-checking the audit against the code and the
`thinker-with-files-gemini` review:

1. **Read paths first, write paths last.** QueryService (PR2) collapses
   purely read-side duplication. The PR3 transactional migration
   (PR5) is the only true high-risk step.
2. **YouTube BEFORE Jobs repo PR3 migration.** Clearing the mental
   overhead of dual `Service`+`Storage` group APIs makes the
   transactional rewrite safer.
3. **Embed cleanups in the PRs that touch the file.** Don't ship a
   "misc P2 sweep" PR. Kill `mu` on `CommandManager` in PR3. Kill `V2`,
   `dataDir`, `metadataJSON`, `ClearCache` in PR4. Centralize
   `os.Getenv` during PR5's config pass.

## 1. Dependency diagram (text form)

```
PR1 ── (independent, lifecycle/safety)
  │
PR2 ── (independent, reads)
  │
PR3 ── (independent, worker singleton) ── bundles CommandManager.mu removal
  │
PR4 ── (depends on PR1's lifecycle fix; bundles *V2/no-op removals)
  │
PR5 ── (depends on PR2's reader consolidation; bundles os.Getenv + store.Job alias)
  │
PR6 ── (depends on PR5's canonical job model risks being absorbed before
        we add payload surface; OK to merge independently)
  │
PR7 ── (depends on PR1, since enqueuer replaces bootstrap global)
```

Cross-PR order constraint: **PR5 must come after PR2**. PR6 can land
before or after PR5 (canonical payload names are independent of the
writer PR). PR7 should land last because it touches the enqueue path
that PR5 may also tighten.

## 2. PR-by-PR plan

### PR15.1 — YouTube/Drive module lifecycle (P0, low risk, fixes a real bug)

**Goal.** Every module receives already-constructed dependencies
through its constructor. `RegisterRoutes` only mounts routes.
Eliminate the Registry's runtime-locked state.

**Bug confirmed.**
- `cmd/server/bootstrap.go:345` builds `ytMod := app.NewYouTubeModule(...)`
- `cmd/server/bootstrap.go:386` reads `ytMod.Service()` and conditionally
  registers the YouTube delivery provider
- `internal/app/youtube.go:24-49` confirms the constructor does **not**
  build the service; it is built inside `RegisterRoutes`
- Therefore: `ytMod.Service()` is `nil` when bootstrap reads it,
  so the delivery provider is never wired.

**Files (edit order).**
1. `internal/app/youtube.go` — change `NewYouTubeModule` to take a
   pre-built `*youtube.Service` (plus `cfg`, `sqliteStore`) and store
   it on the struct. `RegisterRoutes` only mounts HTTP routes.
2. `internal/app/drive.go` — collapse `init()`, `WithSQLiteStore`,
   double-`init()` in `RegisterRoutes`, and `handlers.SetSQLiteStore`
   into one `NewDriveModule(cfg, sqliteStore)` constructor.
3. `cmd/server/bootstrap.go` — build `youtube.Service` and
   `drive.Service` **before** constructing modules, then pass them in.
   The `if ytMod != nil && ytMod.Service() != nil` guard disappears.
4. `internal/app/registry.go` — delete `Registry` struct,
   `sync.RWMutex`, `Name()`, `List()`, `Len()`, `Register()`. Replace
   `RegisterRoutes(router)` with `[]RouteRegistrar`-style slice, or
   simply call each module's `RegisterRoutes` in sequence from
   bootstrap.
5. `internal/app/health.go` — `HealthModule` ↦ a free function
   `health.RegisterRoutes(r *gin.Engine)`.
6. `internal/app/registry_test.go` — rewrite as a `[]RouteRegistrar`
   test.

**Tests.**
- `internal/app/youtube_test.go` — adjust to new constructor; pin
  that `Handlers()`, `Manager()`, `Service()` are non-nil **before**
  `RegisterRoutes` (mirroring the bootstrap fix).
- Add `internal/cmd/server/bootstrap_test_youtube_provider_test.go`
  asserting `deps.deliveryProviders` contains a `*delivery.YouTubeProvider`
  (currently absent — this is what the bug silently breaks).
- `internal/app/registry_test.go` — re-target the new free-function style.

**Risks & mitigations.**
- Low, because tests assert delivery provider presence.
- The `LivestreamModule` (which takes `ytMod.Service`) becomes
  `LivestreamModule.New(cfg, ytService, sqliteStore)`.

**Verification.**
```bash
cd refactored/DataServer
go build ./...
go test ./internal/app/... ./cmd/server/...
go vet ./...
```

---

### PR15.2 — Collapse `queue.QueryService` into `jobs/view.go` (P0, low risk)

**Goal.** Delete `queue/query.go`'s duplicates, route all readers
through `jobs.NewReader(...)` + `jobs/view.go` free functions.

**Files.**
1. **Source of truth (already exists):**
   - `internal/jobs/view.go` — `ToQueueItem`, `ToPayloadMap`,
     `ToFlatMap`, `FormatStats`, `parsePayloadJSON`.
   - `internal/jobs/view_test.go` — wire-shape snapshots already lock
     pre-Batch-3 JSON.
2. **Delete / thin out:**
   - `internal/queue/query.go` — delete `parsePayloadJSON`,
     `domainJobToQueueJob`, `GetJob`, `GetJobPayload`, `GetJobAsMap`,
     `Stats`, `listJobs`, `GetJobsByStatus`, `GetPendingJobs`,
     `GetRunningJobs`, `GetAllJobs`. Keep only the struct if needed
     for select status aliases.
   - `internal/queue/file_queue.go:64,74,79,150,151` — drop
     `query *QueryService` field, `NewFileQueue`'s `query` parameter,
     and `QueryService()` accessor.
3. **Migrate callers** (confirmed via grep):
   - `internal/handlers/server/smoke/smoke_clip_stock_test.go:32,64`
   - `internal/handlers/server/pipeline/pipeline_bridge_test.go:158`
   - `internal/handlers/server/calendar/calendar_test.go:41`
   - `internal/handlers/server/script/handler_test.go:34,183,291`
   - `internal/creatorflow/service_test.go:40,162`
   - `cmd/server/bootstrap_test.go:150,230,285,325-357`
   - Replace `querySvc := queue.NewQueryService(...)` +
     `querySvc.GetJobPayload(...)` with:
     ```go
     reader := jobs.NewReader(store.NewSQLiteJobRepository(db))
     j, _ := reader.Get(ctx, id)
     payload := jobs.ToPayloadMap(j)
     ```

**Tests.**
- Existing `jobs/view_test.go` snapshots guarantee wire shape.
- Add: `internal/jobs/query_caller_test.go` — small suite that
  exercises each former `QueryService` method via
  `NewReader`+`view` and asserts JSON byte-equality.

**Risks.**
- Low: view tests lock the wire format; nothing else in the
  codebase depends on the QueryService type itself outside these
  callers.

**Verification.**
```bash
go build ./...
go test ./internal/jobs/... ./internal/queue/... ./internal/creatorflow/...
```

---

### PR15.3 — Single `CommandManager` (P0, low risk, fixes a race risk)

**Goal.** Build `*workersreg.CommandManager` exactly once in
bootstrap and inject it into both the HTTP `WorkerUpdateHandler`
and the gRPC handler.

**Bug confirmed.**
- `cmd/server/bootstrap.go:212` —
  `cmdMgr := workersreg.NewCommandManager(sqliteStore)`
  (used by `WorkerUpdateHandler`)
- `cmd/server/bootstrap.go:422` —
  `cmdMgr := workersreg.NewCommandManager(deps.sqliteStore)`
  (used by gRPC `serverDeps`)
- Same `*SQLiteStore` ⇒ two `*CommandManager` writers racing on the
  same `worker_commands` table.

**Files.**
1. `cmd/server/bootstrap.go` — create `cmdMgr := workersreg.NewCommandManager(deps.sqliteStore)`
   once; pass it into both `WorkerUpdateHandler` and the gRPC
   `serverDeps` block.
2. `internal/workers/commands.go` — remove any unused `mu sync.RWMutex`
   field on `CommandManager` (audit claim: never used; confirm with grep).
3. `internal/grpcserver/handler.go:50,132` — already accepts
   `*workersreg.CommandManager`; no API change, just confirm
   constructor wiring.
4. `internal/handlers/remote/workers/worker_update.go:26,139,172` —
   drop `CommandManager()` getter if no caller remains.

**Tests.**
- New `cmd/server/bootstrap_test_singleton_cmdmgr_test.go`:
  assert that `workerUpdateHandler.CommandManager()` and
  `grpcDeps.cmdMgr` are the **same pointer**.
- `internal/workers/commands_test.go` — keep; concurrent
  `PushCommand` from two callers on one instance should serialize.

**Verification.**
```bash
go test -race ./internal/workers/... ./cmd/server/...
```

---

### PR15.4 — YouTube one-source-of-truth (P1, medium risk)

**Goal.** Maintain runtime-only YouTube state in memory (OAuth
tokens, clients). All persisted state lives in SQLite and is reached
through one small repository. Delete the scaffolding
`YouTubeRepository` interface that has no implementation.

**Files (edit order).**
1. **Document the split** in the package doc comments:
   - `internal/services/youtube/service.go` —
     `Service` holds ONLY runtime state (`tokens map[string]*oauth2.Token`,
     `clients map[string]*youtube.Service`). No `channels` or `groups`
     collections.
2. **Repository layer** — introduce a small concrete
   `internal/integrations/youtube/repo.go` (single file) with
   `Repository` interface methods: `ListGroups`,
   `CreateGroup(ctx, name, kind)`, `DeleteGroup(ctx, name)`,
   `AddChannelToGroup(ctx, groupID, chID)`,
   `RemoveChannelFromGroup(...)`, `ListChannelsInGroup(ctx, groupID)`.
   Implementation wraps `*store.SQLiteStore` calls
   (`ListYouTubeGroupsV2`, `AddChannelToGroupV2`,
   `RemoveChannelFromGroupV2`, …).
3. **Delete:**
   - `loadGroupsFromSQLite`, `loadCanonicalGroups` from
     `internal/integrations/youtube/groups.go` (they are dead after
     the in-memory map is gone). Fix the null-deref bug implicitly by
     removing the path.
   - `internal/integrations/youtube/storage_groups.go` methods
     (`CreateGroup`, `DeleteGroup`, …) — replaced by `Repository`
     methods.
   - `internal/integrations/youtube/storage_persistence.go`:
     `save`, `SaveData`, `saveAllReconcile`, `syncGroupLocked`,
     memory-vs-DB guards.
   - `internal/integrations/youtube/repository.go` — unfinished
     scaffolding.
   - The `Storage.data.Groups` field on
     `internal/integrations/youtube/storage.go`.
   - `dataDir` param from `NewStorage`, `metadataJSON` param,
     `ClearCache` no-op.
4. **Rename:**
   - Rename `ListYouTubeGroupsV2`, `AddChannelToGroupV2`,
     `RemoveChannelToGroupV2`, `UpsertYouTubeGroupV2`,
     `UpsertYouTubeChannelV2`, etc. Strip the `V2` suffix in
     `internal/store/youtube_groups.go` and
     `sqlite_youtube_entities.go`.
5. **Routes:**
   - `internal/handlers/server/youtube/youtube_groups.go`,
     `youtube_channels.go` — single path through the new
     `Services.youtube.Service` (using `Repository` for persistence).
   - Confirm that `handlers/server/youtube/routes.go:42-45` and the
     `manager/*` paths in `youtube_routes.go:27-28` route through one
     stack.

**Tests.**
- `internal/store/sqlite_youtube_entities_test.go` continues to pass
  after `*V2` rename.
- New `internal/integrations/youtube/repo_test.go` for the new
  repository contract.
- Add a **null-store test** ensuring `Service.AddChannelToGroup`
  returns an explicit error (not a panic) when `repo == nil`.

**Risks.**
- Medium: routes `/api/youtube/groups` and
  `/api/v1/youtube/manager/groups` currently diverge; verify HTTP
  contract coverage in `route_contract_test.go` is preserved.

**Verification.**
```bash
go test ./internal/services/youtube/... ./internal/integrations/youtube/... ./internal/store/...
```

---

### PR15.5 — Promote PR3 commands to canonical `jobs.Writer` (P1, medium-high risk)

**Goal.** Make `jobs.Writer` the only write contract. Drop the
dual `JobRepository` interface, the `PR3Repository` subset, and
most of `job_repository_adapter.go`. Stop the dual-pass in
bootstrap.

**Files (edit order).**
1. **Extend the canonical interface.**
   - `internal/jobs/repository.go` — add to `jobs.Writer`:
     ```go
     Start(ctx context.Context, cmd StartCommand) error
     RenewLease(ctx context.Context, cmd RenewLeaseCommand) error
     RecordRenderFinished(ctx context.Context, cmd RecordRenderFinishedCommand) error
     Fail(ctx context.Context, cmd FailCommand) error   // signature already exists generically; refine
     Cancel(ctx context.Context, cmd CancelCommand) error
     RequeueExpired(ctx context.Context, cmd RequeueCommand) error
     ```
   - Move `StartCommand`, `RenewLeaseCommand`, …,
     `RequeueCommand`, `RecordRenderFinishedCommand`,
     `RequeueResult` from `internal/store/commands.go` and
     `internal/store/jobs_writer_types.go` to
     `internal/jobs/repository.go` (canonical location). Update
     imports.
2. **Implement on the concrete writer.**
   - `internal/store/sqlite_jobs_writer.go`, `sqlite_jobs_writer_pr3.go`
     — implement the new `jobs.Writer` methods directly. Remove the
     `PR3*` method receivers (they become legacy gluing methods, then
     deleted in step 4).
3. **Migrate consumers.**
   - `internal/queue/service.go` —
     `LifecycleService.Start/Fail/Cancel/RequeueExpired` /
     `RenewLease` switch to call `jobsRepo.Start/Fail/Cancel/
     RequeueExpired/RenewLease`.
   - `internal/grpcserver/handler_jobs.go` —
     `store.FailCommand`/`store.RenewLeaseCommand` literals become
     `jobs.FailCommand`/`jobs.RenewLeaseCommand`.
   - `internal/handlers/server/pipeline/pipeline_lifecycle.go` —
     confirm no direct `JobRepository` use remains.
4. **Drop the dual-pass.**
   - `cmd/server/bootstrap.go:217-237` — pass the same
     `*SQLiteJobRepository` once (it already satisfies both
     `jobs.Repository` AND the legacy surface during the migration)
     and delete the legacy interface param of `NewLifecycleService`.
5. **Delete the legacy surface.**
   - `internal/store/jobs_writer_types.go` — delete `JobRepository`
     interface, `PR3Repository` interface, `CreateJobParams`,
     `TransitionParams`, `StartJobParams`, `CompleteJobParams` types
     where they are no longer referenced.
   - `internal/store/sqlite_jobs_writer_pr3.go` — delete (logic moved
     into `sqlite_jobs_writer.go` earlier).
   - `internal/store/job_repository_adapter.go` — delete entirely.
     `toJobsJob`, `toStoreParams` move into `internal/jobs/conversion.go`
     for callers that still build a `jobs.Job` from a database row.
   - `internal/queue/types.go` (or wherever `Job`, `QueueItem`,
     `JobStatus` aliases live) — drop the type aliases now that there
     is one source.
6. **Final cleanup:**
   - Centralize `os.Getenv` reads into `internal/config/env.go` from
     this PR's perspective (this is the natural moment because tests
     stress the env).
   - Drop `store.Job` alias.

**Tests.**
- All existing `sqlite_jobs_writer_pr3_test.go`,
  `sqlite_jobs_writer_repository_test.go`,
  `internal/store/contracts/jobs_repository_contract_test.go`,
  `queue/lifecycle_test.go` tests must continue to pass against the
  new `jobs.Writer` surface.
- Add `TestJobsWriter_Start_HappyPath`, `..._WrongLeaseID`,
  `..._WrongWorkerID`, `..._WrongAttempt`, `..._WrongRevision`,
  `..._AlreadyRunning`, `..._NullAttemptLegacy` to mirror the existing
  `SQLiteJobRepository_*` cases.
- Add `TestQueueLifecycleService_DelegatesToJobsWriter` — a stub
  `jobs.Repository` confirms `LifecycleService.Start` calls
  `jobsRepo.Start` exactly.

**Risks.**
- Medium-high because it touches transactional paths. Mitigated by
  the existing 21-unit-test suite for CAS/NULL revision/TOCTOU/idempotency.

**Verification.**
```bash
go test -race ./internal/jobs/... ./internal/store/... ./internal/queue/... ./internal/grpcserver/...
```

---

### PR15.6 — Canonical job payload names (P1, medium risk)

**Goal.** Internal canonical names. Legacy compatibility lives at
the HTTP edge only.

**Files.**
1. `internal/jobs/payload/normalize.go` — introduce a `Normalize(cmd)`
   function that takes a `CanonicalJobCommand` and returns the
   canonical payload map with one set of keys (`job_id`, `job_run_id`,
   `video_name`, `voiceover_paths`, `parameters`, …).
2. `internal/jobs/payload/legacy_adapter.go` — `AdaptLegacyToCanonical(in)`
   accepts the legacy form (with `id`, `run_id`, `title`,
   `voiceover_path`, `audio_path`) and emits canonical. Lives next to
   the HTTP/JSON parsers.
3. HTTP handlers (the actual edge) — accept legacy names, call
   `AdaptLegacyToCanonical`, then forward canonical names down.
4. AssetService — drop dual writes; only canonical.
5. Extract `applyRewrite(payload, kind, collector, applicator)` from
   `RewriteVoiceoverPayload` and `RewriteSceneImagePayload`. Same
   core algorithm: JSON ↦ map, iterate keys, substitute fields,
   re-marshal. Parameterize the collector and the applicator.

**Tests.**
- `internal/handlers/route_contract_test.go` — pin HTTP contract:
  legacy request still accepted, response uses canonical names only.
- `internal/jobs/payload/normalize_test.go` — round-trip tests.
- `applyRewrite_test.go` — voiceover + scene image share the same
  algorithm test parameterized over both inputs.

**Verification.**
```bash
go test ./internal/jobs/payload/... ./internal/handlers/...
```

---

### PR15.7 — Drive 3-way split + enqueue struct (P2, low risk)

**Goal.** Drive gained three concrete types. Global
`enqueue.SetVoiceoverAssetService` is replaced by `Enqueuer`.

**Files.**
1. **Drive split (3 types, not 4):**
   - `internal/services/drive/catalog.go` — `DriveCatalog` owns
     SQLite queries (`GetDriveFolders`, `DriveFiles`,
     `GetDriveGroups`, `GroupFolders`) **and** folder resolution.
     No HTTP. No tokens.
   - `internal/services/drive/google_client.go` — `GoogleDriveClient`
     wraps only the real Google Drive HTTP API. **No** `CreateDriveFolder`
     synthetic mode. **No** `UploadText` mock URL.
   - `internal/services/drive/token_catalog.go` — `TokenCatalog`
     owns the filesystem token read/write.
   - `internal/services/drive/fake_client.go` — `FakeDriveClient`
     implements the same interface as `GoogleDriveClient`; only
     compiled under `_test.go` builds.
   - `internal/services/drive/service.go` shrinks to a thin
     orchestration type injecting the above three.
2. **Routes:**
   - `internal/handlers/server/drive/handlers.go` — `/api/drive/folders`
     stays; `/api/drive/upload/text` and `/api/drive/folders/create`
     routes **remove** (or move to a test-only flag if absolutely
     needed for sandboxed dev).
3. **Enqueue:**
   - `internal/jobs/enqueue/enqueue.go` — replace
     `SetVoiceoverAssetService`/`GetVoiceoverAssetService` + global
     mutex with:
     ```go
     type Enqueuer struct {
       Queue     JobQueue
       Voiceover *assets.AssetService
     }
     func (e *Enqueuer) EnqueueSceneVideoJob(ctx, payloadMap) (map[string]interface{}, error) { … }
     ```
   - Update callers (search confirms `cmd/server/bootstrap.go:373`,
     `internal/handlers/server/script/handler_test.go:158,164`) to
     build or receive an `*Enqueuer`. Tests pass through the new
     constructor.

**Tests.**
- Drive tests run against `FakeDriveClient` only.
- New `internal/jobs/enqueue/enqueuer_test.go` — no global
  mutation; per-test `*Enqueuer`.

**Risks.**
- Low. Drive fakes were already isolated; their deletion from the
  production code path is a clean break.

**Verification.**
```bash
go test ./internal/services/drive/... ./internal/jobs/enqueue/... ./internal/handlers/server/drive/...
```

---

## 3. Validation strategy (cross-cutting)

After each PR, run:

```bash
cd refactored/DataServer
go vet ./...
go build ./...
go test ./...
go test -race ./internal/jobs/... ./internal/store/... ./internal/queue/...
```

Per-PR gates already documented under verification blocks above.

## 4. Estimated win

| Area              | Lines removed (rough) | States unified       |
| ----------------- | --------------------: | -------------------- |
| PR1 + PR7 (boot)  |             ~150 LOC  | snapshot lifecycle   |
| PR2 (QueryService)|             ~200 LOC  | one job-view API     |
| PR3 (cmd-mgr)     |              ~30 LOC  | one writer           |
| PR4 (YouTube)     |             ~500 LOC  | one YouTube API      |
| PR5 (jobs repo)   |             ~700 LOC  | one jobs write API   |
| PR6 (payload)     |             ~150 LOC  | canonical keys       |
| PR7 (Drive/enq)   |             ~300 LOC  | split services + DI  |

The bigger win is **state**: after PR4/PR5/PR7, three mental
representations of YouTube/groups and two of jobs collapse to
one each, plus the lifecycle and DI graphs no longer carry
hidden globals or race-prone singletons.

## 5. What NOT to merge (audit's closing note)

- `assets.AssetService` (input assets) vs
  `artifacts.Service` (output finalization) — keep distinct.
- `outbox.Registry`, `delivery.Registry`, asset
  `ResolverRegistry` — distinct responsibilities, do not unify.

## 6. Definition of Done (per PR)

1. No new exported type without a comment.
2. No global var added; existing globals removed or shrunk.
3. After PR1/PR2/PR3, no obvious nil-deref or race paths remain.
4. After PR4/PR5, no in-memory map duplicates a SQLite table.
5. PR has its own `TestXxx_*_Sweep` that asserts the legacy type,
   field, or alias is gone (`go vet`-friendly grep).
6. CI green: `go test ./...` + race detector on touched packages.
---

## 7. Size-band policy formalisation (PR-15.7 follow-up)

The per-file byte-band policy is a **complement** to the LOC gate (§ 11 thresholds in `scripts/ci/check-loc-thresholds.sh`). The LOC gate catches LONG files; the size-band gate catches both LINES (extreme files > 50 KiB -- typically unrefactored monoliths) AND SHORTS (extreme files < 1 KiB -- typically stub remnants or accidentally-truncated files).

### Rule

- **Trigger:** any source-tracked file with size > 50 KiB (51 200 bytes) or size < 1 KiB (1 024 bytes).
- **Opt-out:** the file MUST carry an explicit `// size-benchmark: <band>` (Go files) or `# size-benchmark: <band>` (shell files on line >= 2 after the shebang) header.
- **Band validation:** the `<band>` token MUST be a member of the manifest at [`docs/CHANGELOG.md`](../CHANGELOG.md) `### Known size-bands` table. Out-of-manifest tokens fail the lint.

### Walk strategy

- `git ls-files` (source-tracked set only -- `.gitignore` excludes `dist/`, `refactored/`, `node_modules/`, etc.).
- Filter: `*.go`, `*.sh`, `*.bash`, `*.py`. Other extensions (`*.md`, `*.yml`, `*.json`) handled by their respective linters.

### Implementation

- Self-contained script: [`scripts/ci/check-size-band-policy.sh`](../scripts/ci/check-size-band-policy.sh).
- Delegated from `scripts/ci/check-architecture.sh` rule #11 (so `make verify` continues to surface it).
- Wired as a dedicated CI job: `.github/workflows/ci.yml` `size-band-policy` job, with `if: ${{ always() }}` semantics so it surfaces even if other gates fail.

### Tagged artefacts (initial set)

| File | Bytes | Band token | Target band |
| --- | ---: | --- | --- |
| `internal/application/images/smoke_test.go` | 43 020 | `42,2-45 KB` | 42 200 - 45 000 |
| `tests/operational/artlist_live_e2e_verify.sh` | 42 070 | `42-42,2 KB` | 42 000 - 42 200 |
| `cmd/archcheck/scan/percheck_voiceover_alias_ban_test.go` | 42 112 | `42-42,2 KB` | 42 000 - 42 200 |

### Onboarding a new artefact

1. Decide which band your file fits (use an existing band, or define a new one).
2. Update `docs/CHANGELOG.md` `### Known size-bands` table to include the band (single source of truth).
3. Add `// size-benchmark: <band>` (Go) or `# size-benchmark: <band>` (shell, line >= 2 after shebang) to the top of the file.
4. Land the artefact together with its manifest entry.
5. CI job `size-band-policy` will validate the band token against the manifest and exit 0.

### Failure modes

- File > 50 KiB or < 1 KiB, no header: `::error file=path,line=1::size=N bytes (>50 KiB or <1 KiB), no \`// size-benchmark:\` or \`# size-benchmark:\` header`
- File carries header but band not in manifest: `::error file=path,line=1::size=N bytes, band token \`<band>\` not in manifest` (catches typos like `42-43 KB` instead of `42-42,2 KB`).
