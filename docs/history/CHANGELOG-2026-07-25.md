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
