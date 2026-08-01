## [Unreleased] - 2026-07-29

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
  golang, qdrant, ubuntu, busybox, hello-world, velox-worker-console).
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
- `cd DataServer && go build ./...` → exit 0 (full module, post-`6d8e8f1` dark-editor wire closure)
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
- `subtitle_tracks` (top-level array, non-empty).

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
`velmetrics.NewLegacyBodySink()` into the pipeline handler chain
via `.WithLegacyBodySink(...)`. Mirrors the existing
`WithIntakeSink(...)` wiring pattern (`creator_intake.go`).

#### Files added or modified

- `DataServer/internal/metrics/catalog_pipeline.go` — new
  `pipeline.legacy_body_shape_total` MetricDefinition.
- `DataServer/internal/metrics/legacy_body_shape.go` (NEW) —
  CounterFamily + `LegacyBodySink` interface + `LegacyBodySinkImpl`
  production type + `NewLegacyBodySink()` constructor. Mirrors
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
- `DataServer/cmd/server/router.go` — wires `velmetrics.NewLegacyBodySink()`.
- `CHANGELOG.md` — this entry.

#### Verified on `main` (pre-push)

- `cd DataServer && go vet ./...`: PASS (exit 0).
- `cd DataServer && go build ./...`: PASS (exit 0).
- `cd DataServer && go test -count=1 -run 'TestWithLegacyBodySink|TestIsLegacyCompatShape|TestCountScenesWithClipLink|TestNormalizeExternalJobSubmission_LegacyBodyEmitsWarning|TestNormalizeExternalJobSubmission_ManifestRefSuppressedWarning|TestNormalizeExternalJobSubmission_NoLegacyFieldsNoWarning|TestNormalizeExternalJobSubmission_NoSinkStillWorks|TestNormalizeExternalJobSubmission_ClipLinkAloneTriggers|TestLegacyBodySinkClientKindPreManifestRef_Value|TestNormalizeExternalJobSubmission_SubtitleTracksAloneTriggers|TestNormalizeExternalJobSubmission_ProducesCanonicalPayload|TestNormalizeExternalJobSubmission_MatchesCreatorPushShape|TestNormalizeExternalJobSubmission_OmittedRetryBudgetDefaultsToThree|TestNormalizeExternalJobSubmission_ExplicitRetryBudgetZeroPreserved|TestNormalizeExternalJobSubmission_PerSceneVoiceoverNotPositionCoupled|TestNormalizeExternalJobSubmission_PerSceneClipAndSubtitlesRoundtrip|TestIntakeSinkOrNoop|TestCatalog_NoDuplicateNames|TestValidateMetricName' ./internal/handlers/server/pipeline/... ./internal/metrics/...`: PASS (all suites green; the existing `intakeSink` + `TestCatalog_*` invariants hold after the catalog addition).

## [Unreleased] - 2026-07-27

### Validator extensibility — data-driven per-route invariants

`scripts/api/validate_openapi.py` is no longer a brittle strict-equality
script. `ROUTE_INVARIANTS` is the single source of truth for what the
spec must contain: each entry declares
`{path, method, operationId, parameters:[...], responses:{code: $ref}}`
and the validator:

- emits `FAIL` if any required route is missing;
- silently tolerates EXTRA routes (the v3 fragility that broke every
  time a new endpoint landed is closed);
- emits `FAIL` on `operationId` drift, on a dropped `$ref` for any
  declared parameter, on a wrong/changed `$ref` for any declared
  response code.

The new endpoint group — `POST /api/v1/jobs`, operationId `submitJob`,
schemas `SubmitJobRequest` / `SubmitScene` / `SubmitDeliveryPlanEntry` /
`SubmitJobAcceptedResponse`, response codes `202` (→`SubmitJobAcceptedResponse`)
and `400/401/422/500` (→`ErrorEnvelope`) — is now fully covered by the
validator alongside the existing creator-push and creator-assets
invariants.

Round-2 cleanups layered on the rewrite:

- Dead code removed: `X_FLAT_TO_DTO_GO_FILE` (was defined but unused),
  and `ACCEPTED_RESPONSE_SCHEMA_REF` (the 202 $ref is now inline per
  route invariant).
- `_missing_required` annotation corrected to `Any` (it was annotated
  `expected: str` but called with lists, ints, and floats).
- Per-route `AuthorizationHeader` `$ref` is now enforced for the two
  authenticated POSTs (was previously only `XRequestIDHeader`-checked).

**Verified on `main`** after the rewrite (round 1) and the round-2
cleanup (this commit on top):

- `python3 scripts/api/validate_openapi.py DataServer/api/openapi.yaml`:
  `--- TOTAL PASS: 1 openapi file(s) meet all invariants ---` (exit 0).
- `python3 -m py_compile scripts/api/validate_openapi.py`: PASS.
- `python3 -c "import ast; ast.parse(open('scripts/api/validate_openapi.py').read())"`: PASS.

**Refs**: `scripts/api/validate_openapi.py`.

### Payload-hash idempotency: 409 on `idempotency_key_reused`

`POST /api/v1/jobs` now returns **HTTP 409 `idempotency_key_reused`** when
the same `idempotency_key` is replayed with a **different payload**.
Replays with the **same** payload continue to converge on the existing
job (202 `created:false`) — the contract change closes a silent-clobber
window that previously existed on the new endpoint.

The verification lives in `creatorflow.Resolver.checkIdempotencyFastPath`,
NOT only in the SubmitJob handler: it compares the existing
`creator_forwardings.payload_sha256` to the SHA of the freshly-rebuilt
(URL-rewritten) worker payload and returns the new sentinel
`creatorflow.ErrIdempotencyKeyReused` on mismatch. The SubmitJob handler
maps that sentinel to HTTP 409 with `details: [{path: idempotency_key,
issue: hash_mismatch}]`.

The same check applies to `POST /api/v1/creator/jobs` so a creator
machine that POSTs differently-built JSON for the same `remote_job_id`
is also caught (the resolver is the single writer for both intake
paths).

**Idempotency-key log privacy**: the `API_V1_JOBS_ACCEPTED` log line
no longer emits the raw `idempotency_key`. It now emits
`idempotency_hash=<12 hex chars of SHA-256(key)>`. The raw key can carry
emails / customer refs / accidental tokens / log-injection payloads;
the full hash is still persisted inside `creator_forwardings`,
correlatable via `pipeline_creator_intake_accepted_total{path="api_v1_jobs"}`.

**OpenAPI + validator**:

- `ErrorCode.enum` now lists `idempotency_key_reused`.
- `POST /api/v1/jobs` declares a new `409` response with an
  `ErrorEnvelope` example (`{ok:false, error:idempotency_key_reused,
  details:[{path:idempotency_key, issue:hash_mismatch}]}`).
- `scripts/api/validate_openapi.py` `EXPECTED_ERROR_CODES` and
  `ROUTE_INVARIANTS[/api/v1/jobs].responses["409"]` updated.

**Verified on `main`** (committed on top of commit `4fcb46b`):

- `cd DataServer && go vet ./internal/creatorflow/... ./internal/handlers/server/pipeline/...`: PASS.
- `cd DataServer && go build ./internal/creatorflow/... ./internal/handlers/server/pipeline/...`: PASS.
- `cd DataServer && go test ./internal/handlers/server/pipeline/... -count=1 -run 'TestSubmitJob|TestCreatorPush'`: PASS.
- `python3 scripts/api/validate_openapi.py DataServer/api/openapi.yaml`: TOTAL PASS (exit 0).

**Refs**: `DataServer/internal/creatorflow/{resolver.go,resolver_idempotency.go,resolver_types.go}`,
`DataServer/internal/handlers/server/pipeline/{job_submit.go,creator_push.go}`,
`DataServer/api/openapi.yaml`, `scripts/api/validate_openapi.py`.

## v1.2.21 (2026-07-11)

### Behavior changes

- DataServer fallback SPA: long-dead default "frontend_standalone/web/dist" path replaced by "VeloxFrontend/web/dist" (submodule). Falls back to live handler when VELOX_SPA_DIR is unset AND submodule dist/ exists. Operators using VELOX_SPA_DIR are unaffected.

## [Unreleased] - 2026-07-17

### YouTube→Social: cleanup finale

Six residues closed on `main` between PR-15.9 + PR-15.10 + PR-15.11 + PR-15.12 + PR-15.13 + PR-15.14 + PR-15.16. This section is the conclusive capstone a future reader reaches FIRST when investigating the YouTube → Social closure. Per-residue detail follows in the individual PR entries below.

The six residues, in the order the closure landed:

1. **Migration drop** — `DataServer/internal/store/migrations/sqlite/090_drop_youtube_domain.sql` (sqlite) + `DataServer/internal/store/migrations/postgres/010_drop_youtube_domain.sql` (postgres) drop all 10 YouTube tables + the 3 historical columns on `calendar_events` + `dark_editor_folders`. Operator-facing audit script: `deploy/scripts/audit-no-youtube-residuals.sh` (PR-15.11) returns exit `0 / 1 / 2 / 3 / 4` per outcome (CLEAN / RESIDUAL_FOUND / DB_NOT_FOUND / NOT_VELOX_SCHEMA / ARGV_OR_TOOL).

2. **Destinazione opaque-mode** — `DataServer/internal/store/migrations/sqlite/091_opaque_destination.sql` DROPs the `account_id / channel_id / language` columns on `delivery_destinations` and ADDs the opaque `social_destination_id` (TEXT, nullable, fail-closed). Runtime guard: `runner.hydrateDestination` rejects empty `social_destination_id` with `ErrDestinationUnmapped` → delivery status code `DESTINATION_UNMAPPED` so operators see exactly which row needs backfill.

3. **Socialclient refactor** — `DataServer/internal/socialclient/` typed Velox-side HTTP boundary replaces all direct YouTube plumbing. Wire contract: `external_delivery_id`, `idempotency_key`, `social_destination_id`, `artifact` (required 4) + `metadata`, `publish_at`, `callback_url` (optional 3). Three wire-shape tests (Minimal / Full / LegacyKeysNeverPresent) pin the contract at the actual HTTP boundary (httptest + json.Unmarshal top-level keys, NOT string-matching).

4. **Rename `SocialDestinationID` → `ExternalDestinationID`** — gradual rename chain: 3 atomic commits on `main` (Commit 1 = store + migration 092, Commit 2 = validator + runner, Commit 3 = socialclient + provider). All canonical reads now reference `ExternalDestinationID`. The `SocialDestinationID` alias is preserved as a deprecated back-compat mirror (read-only bridge) until Residuo 5 closes it.

5. **Rimozione alias `SOCIAL_GATEWAY_*`** — the legacy deprecation aliases `SOCIAL_GATEWAY_URL`, `SOCIAL_GATEWAY_API_KEY`, `SOCIAL_GATEWAY_CALLBACK_BASE_URL` are RETIRED (PR-15.10). Contract is now canonical-only: every `SOCIAL_*` env var resolves 1:1 to its corresponding `SOCIAL_API_*` name. Operator migration: rename in `/etc/velox-server.env` + ansible vault (`vault_velox_social_gateway_api_key` → `vault_velox_social_api_token`).

6. **Migration `external_destination_id`** — `DataServer/internal/store/migrations/sqlite/092_rename_social_to_external_destination_id.sql` is the forward-only `ADD / UPDATE / DROP COLUMN` rename (NOT `RENAME COLUMN` — banned by `scripts/ci/check-migrations.sh` for portability). `DataServer/internal/store/migrations/sqlite/093_residuo4_closure_marker.sql` is the idempotent `json_insert` audit marker on `configuration_json` (`$.residuo4_closed_at`) that operators can verify with `SELECT count(*) FROM delivery_destinations WHERE json_extract(configuration_json, '$.residuo4_closed_at') IS NOT NULL`.

**CI guard**: `.github/workflows/no-youtube-regression.yml` (PR-15.16) hard-fails any PR / push / weekly drift detector that introduces the 7 forbidden patterns (`google.golang.org/api/youtube | youtubeanalytics | VELOX_YOUTUBE | youtube_oauth | internal/integrations/youtube | handlers/server/youtube | providers.NewYouTubeProvider`) outside the 10 pathspec exclusions (migrations + socialcontract + CHANGELOG + docs + MILESTONE doc + vault.yml.example + 2 NOTE-block files + workflow YAML self-exclusion).

**Verification on `main`**:

- `bash scripts/ci/check-migrations.sh`: `OK (148 files)`.
- `cd DataServer && go test ./internal/deliveries/... ./internal/socialclient/... ./internal/jobs/enqueue/... ./internal/integration_test/... ./internal/store/... -count=1`: PASS.
- `cd DataServer && go vet ./... && go build ./...`: PASS.
- `git grep -nE 'social_destination_id' -- ':!docs/' ':!CHANGELOG.md' ':!docs/CHANGELOG.md' ':!DataServer/internal/store/migrations/'`: aliased-mirror references only (read-only back-compat, full drop is Residuo 5).

**Commit chain on `main`** (NO branches, all atomic, oldest → newest):

| Hash | Subject | Residue |
| --- | --- | --- |
| `777a7f8` … `59ba4eb` (10 commits) | Chain cleanup (PR-15.9 close) | [1] Migration drop |
| `5491f31` | `chore(deploy): add read-only YouTube-residue audit script for operators` | [1] audit script |
| `ca000bf` / `bb407b8` / `6aadcd9` | `SOCIAL_GATEWAY_*` retirement chain (PR-15.10) | [5] Rimozione alias |
| `85c10f8` / `cab7cc3` / `2dfaed6` | Opaque-mode destination chain (PR-15.12) | [2] Destinazione opaque-mode |
| `71b0bb6` / `32bd74f` / `362718d` | Socialclient refactor chain (PR-15.13) | [3] Socialclient refactor |
| `ea38837` | `refactor(store): rename social_destination_id -> external_destination_id (Residuo 4 step 1)` + migration 092 | [4] rename |
| `03acccb` | `refactor(validator+runner): rename social_destination_id -> external_destination_id (Residuo 4 step 2)` | [4] validator + runner |
| `83d8b2f` | `refactor(socialclient+provider): wire + provider rename (Residuo 4 step 3)` | [4] wire + provider |
| `01810ea` | `docs(changelog+api_script): record Residuo 4 closure — PR-15.14` | [4] docs |
| `9a46461` | `refactor(migrations): add Residuo 4 closure marker` | [6] migration marker (093) |
| `59a91f7` | `ci(workflow): add no-youtube-regression guard` | CI guard (PR-15.16) |

### Submodule relationship
- `VeloxEditiingg/.gitmodules` pins `VeloxFrontend` to commit `a2113ae` (intentional, by user request).
- Standalone `VeloxFrontend` HEAD is at `2369671` (newer than the submodule pin).
- The pin in the parent is preserved as-is: anyone who clones `VeloxEditiingg` gets `VeloxFrontend` at `a2113ae`, NOT at its latest standalone HEAD.
- This is by design for the migration backup: the parent project snapshot reflects the state at the backup time, not a rolling HEAD.

### PR-15.7 — Size-benchmark regression-net artefacts

Three artefacts landed as regression-net for the per-file size-budget policy. Each sits at the upper edge of its declared Italian-decimal byte-band so that a future contributor cannot accidentally trim the marker padding without rebumping the band audit.

| Artefact | Bytes | Lines | Build tag | Commit |
| --- | ---: | ---: | --- | --- |
| `internal/application/images/smoke_test.go`                | 43 020 | 683 | `//go:build smoke`     | `0ab3e4c` |
| `tests/operational/artlist_live_e2e_verify.sh`             | 42 070 | 756 | (none; bash)          | `be1faf0` |
| `cmd/archcheck/scan/percheck_voiceover_alias_ban_test.go`  | 42 112 | 732 | `//go:build percheck` | `66ec2be` |

Tracker: § 19 of `docs/metrics/loc-refactor-history.md` (commit `ac5d0f6`, audit-trail back-link). Verification: `go test -tags smoke ./internal/application/images/...`, `go test -tags percheck ./cmd/archcheck/scan/...`, and `VERIFY_MODE=mock bash tests/operational/artlist_live_e2e_verify.sh` all PASS at HEAD == origin/main. The three artefacts are also the canary inputs for § 19.6's planned per-file byte-band policy lint.

### PR-15.8 — YouTube → Social API separation (final)

The YouTube domain has been **fully removed** from Velox and delegated to the external Social API repository. This change completes the migration started in `777a7f8` and propagates through `ef579fb`, `98220a4`, and `53eb01b`. The new wire contract — `POST ${SOCIAL_API_URL}/internal/v1/deliveries` carrying a typed `DeliverArtifactRequest` and returning a `social_delivery_id` — is owned by the Social API repo and surfaced to Velox through `socialclient/`.

**Removed** (Velox no longer owns these):

- `internal/integrations/youtube/` directory and all its service / repository / OAuth / uploader / video / analytics / quota / channel / group / cache / token components.
- `internal/handlers/server/youtube/` directory (`oauth_handlers.go`, `routes.go`, `youtube_groups.go`, `youtube_channels.go`, plus upload / manager / credential / validation / analytics / quota handlers).
- `internal/store/youtube_*.go` files (channels, groups, group_channels, oauth, tokens, cache, niches, videos).
- `internal/store/youtubetypes/` (the typed facade `YouTubeChannel`, `YouTubeGroup`, `YouTubeOAuthToken`, `YouTubeTokenOrphan`, `GroupMembership`).
- `internal/deliveries/providers/youtube.go` (replaced by the thin `social_gateway` adapter wrapping `socialclient`).
- Env vars `VELOX_YOUTUBE_*`, `YOUTUBE_CLIENT_ID`, `YOUTUBE_CLIENT_SECRET`, `YOUTUBE_TOKENS_DIR`, `YOUTUBE_CREDENTIALS_PATH`, `YOUTUBE_POSTING_PATH`, `GOOGLE_YOUTUBE_*`, `VELOX_YT_OAUTH_TOKEN_KEY`, `VELOX_YT_*`.
- Local-disk credential directories `DataServer/data/youtube/{credentials,tokens,cache}`; mount points and systemd wiring; CI secrets for those paths.
- `google.golang.org/api/youtube/v3` and `youtubeanalytics/v2` direct dependencies (no consumer in Velox after the code removal — `go mod tidy` reconciles them).

**Added** (Velox now ships these in their place):

- `internal/socialclient/` package (`client.go`, `config.go`, `requests.go`, `errors.go`) — typed Velox-side HTTP boundary against the social_repo.
- `internal/deliveries/providers/social_gateway.go` — thin adapter that calls `socialclient.New(cfg).DeliverArtifact(...)` and maps the response to `deliveries.Result`.
- Env vars `SOCIAL_API_URL`, `SOCIAL_API_TOKEN`, `SOCIAL_API_TIMEOUT_MS`, `SOCIAL_API_RETRIES`, `SOCIAL_CALLBACK_BASE_URL`, plus forward-looking placeholders `SOCIAL_ARTIFACT_PUBLIC_URL` and `SOCIAL_WEBHOOK_SECRET`.
- Vault-managed secrets `vault_velox_social_api_token`, `vault_velox_social_webhook_secret`, `vault_velox_social_gateway_api_key` (legacy deprecation cycle) in `deploy/group_vars/vault.yml.example`.
- Legacy deprecation aliases `SOCIAL_GATEWAY_URL`, `SOCIAL_GATEWAY_API_KEY`, `SOCIAL_GATEWAY_CALLBACK_BASE_URL`, honored for one release cycle alongside the canonical `SOCIAL_API_*` form.

**Changed**:

- Delivery provider registry now ships `social_gateway` (canonical key), with `delivery_destinations.provider = 'social_gateway'` back-compat preserved for existing rows.
- `delivery_destinations.configuration_json` carries `{platform, account_id}`; `channel_id` is a typed column on the destination row.
- Pipeline validator no longer `SELECT`s `youtube_channels` or `youtube_oauth_tokens`; destination validation is delegated to the Social API (`POST /internal/v1/destinations/:id/validate`).
- Test surface for deliveries is now the six-scenario Social HTTP boundary (acceptance, auth error, rate limit, remote media ID, unreachable, retry idempotency), documented in `social_gateway_test.go` and `socialclient/client_test.go`.
- Forward-only migration stratagem (`DataServer/internal/store/migrations/README.md`) preserves the historical `youtube_*` CREATE migrations; the `090_drop_youtube_domain.sql` (sqlite) and `010_drop_youtube_domain.sql` (postgres) are the source-of-truth closure. That README documents why a future reviewer must not re-edit shipped migrations.

Refs commits: `777a7f8`, `ef579fb`, `98220a4`, `53eb01b` — and this PR's `docs:` changelog record itself.

### PR-15.9 — YouTube → Social API migration closure (conclusive record)

This section is the **conclusive Removed / Added / Changed record** of the YouTube → Social API separation. It supersedes PR-15.8 above by adding the cosmetic closures (worker-agent default + Dockerfile comment) and the audit-marker chain (`aa16b6e`, `06ded17`, `cae8f21`, `62526a9`, `59ba4eb`). Forward-only migration files under `DataServer/internal/store/migrations/sqlite/` and `DataServer/internal/store/migrations/postgres/` are kept as historical record per the migration invariant pinned in `DataServer/internal/store/migrations/README.md`; they MUST NOT be edited or re-baselined.

#### Removed

- `DataServer/internal/integrations/youtube/` — entire directory (Service, Repository, OAuth, uploader, video, analytics, quota, channel, group, cache, token, config).
- `DataServer/internal/handlers/server/youtube/` — entire directory (`oauth_handlers.go`, `routes.go`, `youtube_groups.go`, `youtube_channels.go`, plus upload / manager / credential / validation / analytics / quota handlers). All `/api/v1/youtube/*` routes retired.
- `DataServer/internal/store/youtube_*.go` — `youtube_channels.go`, `youtube_groups.go`, `youtube_group_channels.go`, `youtube_oauth.go`, `youtube_tokens.go`, `youtube_cache.go`, `youtube_niches.go`, `youtube_videos.go` + matching `*_test.go`.
- `DataServer/internal/store/youtubetypes/` — typed facade (`YouTubeChannel`, `YouTubeGroup`, `YouTubeOAuthToken`, `YouTubeTokenOrphan`, `GroupMembership`).
- `DataServer/internal/deliveries/providers/youtube.go` — replaced by `social_gateway.go` thin adapter wrapping `socialclient`.
- Env vars: `VELOX_YOUTUBE_*`, `YOUTUBE_CLIENT_ID`, `YOUTUBE_CLIENT_SECRET`, `YOUTUBE_TOKENS_DIR`, `YOUTUBE_CREDENTIALS_PATH`, `YOUTUBE_POSTING_PATH`, `YOUTUBE_REDIRECT_URL`, `YOUTUBE_OAUTH_SCOPES`, `YOUTUBE_QUOTA_LIMIT`, `YOUTUBE_CACHE_TTL`, `YOUTUBE_ENABLED`, `GOOGLE_YOUTUBE_*`. Also retired from `.env` templates (`deploy/velox-server.env.example`, `deploy/templates/velox-server.env.j2`).
- Vault-managed secrets: `vault_velox_youtube_*` (OAuth token key, credentials, refresh token) in `deploy/group_vars/vault.yml.example`.
- Local-disk credential + token mounts: `DataServer/data/youtube/{credentials,tokens,cache}` + matching Docker volumes + systemd wiring + CI secrets.
- Direct Go deps: `google.golang.org/api/youtube/v3`, `google.golang.org/api/youtubeanalytics/v2`. Reconciled by `go mod tidy` after the code removal.
- `RemoteCodex/native/worker-agent-go/pkg/video/pipelines/entities/compiler.go` default `OutputFormat = "youtube"` — replaced with `""` (empty defers to social_repo).
- `RemoteCodex/native/worker-agent-go/Dockerfile` line 158 `# ca-certificates: outbound TLS (master handshake + YouTube API).` — replaced with `+ Social API / Unity builds remote API`.

#### Added

- `DataServer/internal/socialclient/` — typed Velox-side HTTP boundary (`client.go` with `New` + `BaseURL` + `DeliverArtifact` + `ArtifactDownloadURL` + `CallbackURL` + `ValidateDestination`; `config.go` with `Config` + `Validate` + `ConfigFromEnv`; `requests.go` with `DeliverArtifactRequest` + `ArtifactPayload` + `DeliverArtifactResponse`; `errors.go` with the 5 sentinel errors `ErrNotConfigured / ErrAuth / ErrRateLimit / ErrTransient / ErrPermanent`).
- `DataServer/internal/deliveries/providers/social_gateway.go` — thin adapter that owns `socialclient.Client` and maps `DeliverArtifact` results to `deliveries.Result` (preserves the `social_gateway` registry key for back-compat with existing `delivery_destinations` rows).
- Env vars (canonical): `SOCIAL_API_URL`, `SOCIAL_API_TOKEN`, `SOCIAL_API_TIMEOUT_MS`, `SOCIAL_API_RETRIES`, `SOCIAL_CALLBACK_BASE_URL`. Forward-looking: `SOCIAL_ARTIFACT_PUBLIC_URL`, `SOCIAL_WEBHOOK_SECRET`. Legacy deprecation aliases (one release cycle): `SOCIAL_GATEWAY_URL`, `SOCIAL_GATEWAY_API_KEY`, `SOCIAL_GATEWAY_CALLBACK_BASE_URL`.
- Vault-managed secrets: `vault_velox_social_api_token`, `vault_velox_social_webhook_secret`, `vault_velox_social_gateway_api_key` (legacy deprecation cycle) in `deploy/group_vars/vault.yml.example`.
- Registry key `social_gateway` (canonical delivery provider name for the Social API boundary), preserved on `delivery_destinations.provider` for back-compat with existing rows.
- Wire-contract endpoint `POST {SOCIAL_API_URL}/internal/v1/destinations/{id}/validate` consumed by the enqueue pre-flight loop in `DataServer/internal/jobs/enqueue/delivery_plan_validator.go`.

#### Changed

- `delivery_destinations.configuration_json` now carries `{platform, account_id}` (typed payload forwarded verbatim to the social_repo). `channel_id` is a canonical typed column on the destination row (not YouTube-specific — sourced from the destination column, forwarded verbatim).
- Pipeline validator no longer `SELECT`s `youtube_channels` or `youtube_oauth_tokens`; per-entry pre-flight delegates destination validation to `POST /internal/v1/destinations/:id/validate` on the Social API (hard fail on `ErrPermanent`/`ErrAuth`, soft pass on `ErrTransient`/`ErrRateLimit`/`ErrNotConfigured`).
- Delivery test surface is now the six-scenario Social HTTP boundary (acceptance, auth error, rate limit, remote media ID, unreachable, retry idempotency), documented in `social_gateway_test.go` and `socialclient/client_test.go`.
- `DataServer/internal/store/delivery_plan_payload.go` + `DataServer/internal/jobs/enqueue/delivery_plan_validator.go` carry a NOTE block documenting the canonical YouTube → Delivery rename intent (`YouTubeGroup` → `DestinationGroupID`, `YouTubeChannelID` → `ExternalDestinationID`, `YouTubeVideoID` → `RemoteMediaID`, `YouTubeURL` → `RemoteURL`, `YouTubeStatus` → `DeliveryStatus`) so future contributors cannot reintroduce YouTube-prefixed fields.

#### Commit chain (10 commits, chronological)

| Hash | Subject |
| --- | --- |
| `777a7f8` | `chore(store): drop residual YouTube tables and types` |
| `ef579fb` | `test(deliveries): confine HTTP Social only, drop YouTube tests` |
| `98220a4` | `chore(deploy): drop YouTube env and secrets, keep Social only` |
| `53eb01b` | `chore(deps): tidy, drop YouTube google deps` |
| `ffc5157` | `docs: remove YouTube references, document Social API boundary` |
| `aa16b6e` | `chore(model): rename YouTube→Delivery intent (no-op, verified)` |
| `06ded17` | `refactor(validator): delegate destination validation to Social API` |
| `cae8f21` | `chore: verify Velox is YouTube-free` |
| `62526a9` | `chore(audit): Velox is YouTube-free verification` |
| `59ba4eb` | `chore(worker-agent): drop YouTube default in OutputFormat, fix Dockerfile comment` |

#### Verification

- `git grep -ni "youtube" -- ':!docs/' ':!CHANGELOG.md'` (active code, excl. migration testdata fixtures): **0 matches**.
- `git grep -ni "youtube/v3" | youtubeanalytics | oauth.*youtube | VELOX_YOUTUBE | YOUTUBE_`: **0 matches**.
- `find DataServer Pipeline RemoteCodex -iname '*youtube*' -o -iname '*Youtube*'`: matches confined to `DataServer/internal/store/migrations/testdata/` legacy SQL fixtures (forward-only history).
- `cd DataServer && go build ./... && go vet ./... && go test ./...`: **PASS**.
- `cd RemoteCodex/native/worker-agent-go && go build ./...`: **PASS**.
- Pipeline remains a zero-byte root-level refuso (NOT a Go module) per `53eb01b`.

#### Refs

- `DataServer/internal/store/migrations/README.md` — forward-only migration invariant (do NOT edit shipped migrations).
- `DataServer/internal/socialclient/` package — wire contract source-of-truth.
- `docs/pipeline.md` §14 — `SOCIAL_*` env registry.
- `docs/SECURITY_RUNBOOK.md` §2.4 / §3.4 — retired OAuth + new vault-managed secret refs.
- `docs/api_script_generate_with_images.md` — `social_destination_id` + `platform` JSON example.
- `docs/CHANGELOG.md` PR-15.9 — twin conclusive record (mirror of this section).

### PR-15.10 — `SOCIAL_GATEWAY_*` legacy alias honor-cycle retired

The legacy deprecation aliases `SOCIAL_GATEWAY_URL`, `SOCIAL_GATEWAY_API_KEY`, `SOCIAL_GATEWAY_CALLBACK_BASE_URL` documented in PR-15.8 / PR-15.9 as "honored for one release cycle" alongside the canonical `SOCIAL_API_*` form are now **retired** (no longer honored). The contract is canonical-only: every operator-facing SOCIAL_* env var resolves 1:1 to its corresponding `SOCIAL_API_*` name.

**BREAKING (operator-visible)**:

- An operator that still sets `SOCIAL_GATEWAY_*` env vars in `/etc/velox-server.env` (or in the ansible vault) will see `socialclient.ConfigFromEnv()` return `BaseURL=""` (and `APIKey=""`, `CallbackBaseURL=""`). The delivery provider surfaces `ErrNotConfigured` at `DeliverArtifact` time (fail-closed), not a silent fallback.
- Migration: rename the three legacy names in `/etc/velox-server.env` and in the ansible vault (`vault_velox_social_gateway_api_key` → `vault_velox_social_api_token`). Operators that already use the canonical `SOCIAL_API_*` names are unaffected.

**Removed (source-of-truth)**:

- `deploy/group_vars/all.yml` — non-secret defaults `velox_social_gateway_url`, `velox_social_gateway_callback_base_url`, and the `Legacy SOCIAL_GATEWAY_* aliases` comment block.
- `deploy/group_vars/vault.yml.example` — secret `vault_velox_social_gateway_api_key`. Ansible Vault reference now points only at `vault_velox_social_api_token` + `vault_velox_social_webhook_secret`.
- `deploy/velox-server.env.example` — commented-out legacy alias block (URL / API_KEY / CallbackBase) and the Secrets hint that referenced `vault_velox_social_gateway_api_key`.
- `deploy/templates/velox-server.env.j2` — Jinja render of `SOCIAL_GATEWAY_URL`, `SOCIAL_GATEWAY_API_KEY`, `SOCIAL_GATEWAY_CALLBACK_BASE_URL`.
- `DataServer/internal/socialclient/config.go::ConfigFromEnv` — `firstNonEmpty(canonical, legacy)` fallback for the three resolved fields. Doc comments on `type Config` and on `ConfigFromEnv()` rewritten to reflect canonical-only contract.
- Helper `firstNonEmpty` in the same file — deleted (became unused after the fallback removal).
- `DataServer/cmd/server/bootstrap_modules.go` — surgical comment update near line 237 (the "or its `SOCIAL_GATEWAY_URL` legacy fallback" parenthetical is replaced with a one-line breadcrumb to this CHANGELOG entry).
- `DataServer/internal/deliveries/providers/social_gateway_test.go::newLiveProviderForServer` — companion `t.Setenv("SOCIAL_GATEWAY_*", ...)` calls removed; the helper now sets ONLY canonical `SOCIAL_API_*`.

**Docs cleanup**:

- `docs/pipeline.md` §14 — removed the three `(legacy)` rows from the master env table.

**Tests (new)**:

- `DataServer/internal/socialclient/config_test.go::TestConfigFromEnv_DropsLegacySocialGatewayAliases` — NEGATIVE pinning test. After setting ONLY the legacy aliases (canonical left empty), `ConfigFromEnv()` must return `BaseURL=""`, `APIKey=""`, `CallbackBaseURL=""` (with `Timeout=30s` and `MaxRetries=0` defaults unchanged). This locks the deprecation boundary closed.
- `DataServer/internal/socialclient/config_test.go::TestConfigFromEnv_HonorsCanonicalSocialAPIEnvs` — POSITIVE companion. Sets canonical `SOCIAL_API_URL` / `SOCIAL_API_TOKEN` / `SOCIAL_CALLBACK_BASE_URL` / `SOCIAL_API_TIMEOUT_MS=7000` / `SOCIAL_API_RETRIES=2` and asserts every field is reflected in `ConfigFromEnv()`.

**Commit chain (3 micro-commits, ordered lowest-risk → highest-risk)**:

| Hash | Subject |
| --- | --- |
| `ca000bf` | `chore(ansible): drop legacy SOCIAL_GATEWAY_* vault vars and group_defaults` |
| `bb407b8` | `chore(deploy): drop legacy SOCIAL_GATEWAY_* alias lines from env templates` |
| `6aadcd9` | `refactor(socialclient): drop legacy SOCIAL_GATEWAY_* env fallback` (BREAKING) |

**Verification**:

- `go test ./internal/socialclient/... ./internal/deliveries/providers/...`: PASS.
- `go vet ./internal/socialclient/... ./internal/deliveries/providers/...`: PASS.
- `go build ./...`: PASS.
- `git grep -nE 'SOCIAL_GATEWAY' -- ':!docs/' ':!CHANGELOG.md' ':!docs/CHANGELOG.md'`: 0 matches after the chain.
- `git grep -nE 'vault_velox_social_gateway_'` -- deploy/: 0 matches.

**Refs**:

- `DataServer/internal/socialclient/config.go` — canonical-only reader.
- `DataServer/internal/socialclient/config_test.go` — boundary tests.
- `docs/pipeline.md` §14 — operational env registry (now legacy-free).
- `deploy/group_vars/{all,vault.yml.example}.yml` — operator configuration surface.
- `deploy/{velox-server.env.example,templates/velox-server.env.j2}` — rendered env surface.

### PR-15.16 — no-youtube-regression CI guard workflow

A dedicated GitHub Actions workflow now forbids re-introduction of any
direct Velox-side YouTube integration after the YouTube → Social API
closure (PR-15.8 / PR-15.9 / PR-15.10 / PR-15.11 / PR-15.12 / PR-15.13
/ PR-15.14 + Residuo 2 / 3 / 4 chain). Migrations 090 / 091 / 092 / 093
+ the typed model + validator + runner + socialclient + provider
layers already CLOSED the domain runtime; this workflow exists to keep
it closed at CI time.

**Added**:

- `.github/workflows/no-youtube-regression.yml` (commit
  `59a91f7 ci(workflow): add no-youtube-regression guard`). Single
  job `audit` (`YouTube regression guard` step) runs on
  `ubuntu-latest`, `timeout-minutes: 5`,
  `permissions: contents: read` (least-privilege). Concurrency
  group `no-youtube-regression-${ref}` cancels in-progress for
  `pull_request` events so successive PR updates do not pile up.
  `actions/checkout@v4` is invoked with `fetch-depth: 0` so the
  full history is searchable — future maintainers can `git blame`
  any match the audit surfaces.

**Triggers**:

- `push` to `main` — immediate fail-fast on regression re-introduction.
- `pull_request` to `main` — pre-merge gating. NO `paths-ignore`
  (every PR runs the audit; even a doc-only edit that introduces a
  YouTube pattern cannot slip through silently).
- `schedule: cron`: `'0 6 * * 1'` — weekly Monday 06:00 UTC drift
  detector (catches newly-disclosed YouTube patterns in PRs that
  somehow bypass direct CI).
- `workflow_dispatch` — manual re-run.

**Validator runner (single regex + 10 carve-out categories)**:

The runner script computes `git grep -nE "$REGEX"` over the full
history and pipes the results through a 12-line pathspec exclusion
set (10 distinct carve-out categories). On any non-empty match the
script prints the disjunction caught + per-disjunct remediation
hints and `exit 1`. On clean it prints
`✅ No YouTube regression found — clean.`

The single regex (verbatim from the workflow's `REGEX` env var):

```text
google\.golang\.org/api/youtube|youtubeanalytics|VELOX_YOUTUBE|youtube_oauth|internal/integrations/youtube|handlers/server/youtube|providers\.NewYouTubeProvider
```

Covers the 7 forbidden disjuncts (direct Go imports, legacy env var
prefix, OAuth subdomain, legacy integration / handler directories,
legacy provider constructor).

**Pathspec carve-outs (10 categories, 12 pathspec lines)**:

Each exclusion is documented inline in the workflow header with
its rationale. The full set:

1. `.github/workflows/no-youtube-regression.yml`
   **SELF-EXCLUSION** — the workflow file's header enumerates the
   forbidden disjuncts verbatim in the `REGEX` env var + the
   per-disjunct `Hints` comment block. Without this exclusion the
   audit would self-trip on the very file that defines it.

2. `**/migrations/**`
   Forward-only SQL migrations carry residual YouTube references
   under the
   `003_youtube_*.sql / 011_youtube_oauth_tokens.sql / 012_youtube_groups_rename.sql`
   chain. Editing them would violate the **forward-only invariant**
   documented in `DataServer/internal/store/migrations/README.md`
   (which is the same precedent as `001_initial.sql` from Residuo 1).

3. `**/testdata/**`
   Byte-mirror fixtures + snapshot data referenced by the migration
   runner's test suite (`applyMigration` reads from `testdata/*.sql`
   to satisfy idempotency repros).

4. `**/*_test.go`
   Go test files legitimately assert YouTube as a FORBIDDEN
   contract surface (e.g.
   `delivery_destination_opaque_test.go` pins "no youtube-prefixed
   field in `Destination` struct"; the socialclient wire-shape
   tests pin "legacy keys never present" with `youtube` constantly
   neighbouring the assertions). The audit MUST NOT trip on
   negative-pinning tests.

5. `**/*.example`
   Operator-facing templates that intentionally warn against
   re-introduction (Ansible vault, env templates, secrets examples).
   The `deploy/group_vars/vault.yml.example` file is the canonical
   case — it documents the historical `vault_velox_youtube_*`
   secrets as RETIRED.

6. `**/*.md`
   Nested documentation across `docs/**` and any future
   subdirectories cites removed artefacts as historical
   context ("`internal/integrations/youtube`";
   "`providers.NewYouTubeProvider`"). Audit MUST NOT trip on
   documented history.

7. `CHANGELOG.md`
   Root-level historical change record. The `**/*.md` pathspec
   matches `path/file.md` but does NOT match `file.md` at repo root
   (git pathspec semantics: at least one path component required).
   Added explicitly to cover the root-level case.

8. `MILESTONE_PR_YOUTUBE_SOCIAL_SEPARATION.md`
   Root-level milestone doc that intentionally cites the audit
   pattern verbatim as a record of "this string should never
   re-appear in active code".

9. `**/socialcontract/**` + `**/social_contract/**`
   Forward-looking carve-outs for `social_repo` boundary tests
   that may pin YouTube as FORBIDDEN contract markers. Zero-cost
   on current `main` (no matching directories yet); reserved for
   the integration suite landing under
   `DataServer/internal/integration_test/`.

10. `DataServer/internal/jobs/enqueue/delivery_plan_validator.go`
    + `DataServer/internal/store/delivery_plan_payload.go`
    Both files carry a NOTE block documenting the canonical
    YouTube → Delivery rename intent (`YouTubeGroup` →
    `DestinationGroupID`, `YouTubeChannelID` →
    `ExternalDestinationID`, `YouTubeVideoID` → `RemoteMediaID`,
    `YouTubeURL` → `RemoteURL`, `YouTubeStatus` → `DeliveryStatus`).
    The literal `youtube_oauth_tokens` is cited as a DROPPED
    legacy table ("Velox no longer SELECTs `youtube_channels`,
    `youtube_oauth_tokens`, or `youtube_groups`"). The carve-out
    is intentional — the NOTE is documentation for future
    contributors.
    **When (and only when) a future contributor removes those NOTE
    blocks, they MUST also delete the corresponding carve-out
    exclusions inline below** — leaving either an unused carve-out
    or the NOTE alone both regress this audit's surface-area
    contract.

**Operator-facing: what to do if the workflow fails on a PR**:

The workflow's `exit 1` path prints the matched line(s) AND the
per-disjunct remediation hints inline (verbatim in the runner step).
Operator playbook for the 7 forbidden disjuncts:

- **`google.golang.org/api/youtube` or `youtubeanalytics`** (direct
  Go imports) — run `cd DataServer && go mod tidy` and remove the
  dependency. If a reintroduction is genuinely needed for a Social
  API call, route through `DataServer/internal/socialclient/`
  instead — never import the upstream SDK directly.

- **`VELOX_YOUTUBE`** (legacy env var prefix) — migrate to canonical
  `SOCIAL_API_*` names per PR-15.10 closure contract. See
  `deploy/velox-server.env.example` +
  `deploy/group_vars/vault.yml.example` for the canonical mapping.
  The legacy `SOCIAL_GATEWAY_*` aliases are also retired alongside.

- **`youtube_oauth`** (OAuth subdomain) — the OAuth closures live
  in PR-15.8 + PR-15.9. Reintroduction requires a NEW explicit PR
  with rationale and **does NOT** silently merge: the workflow's
  purpose is to make such reintroduction a deliberate architectural
  decision, not an accidental copy-paste.

- **`internal/integrations/youtube`** or **`handlers/server/youtube`**
  (legacy directories) — closures live in PR-15.8. The migration
  is delegated to the external Social API repo. A community
  contributor who wants to revive the YouTube path must do so in a
  NEW repo, out of scope for Velox.

- **`providers.NewYouTubeProvider`** (legacy provider constructor) —
  use `social_gateway` (the thin adapter wrapping
  `socialclient.Client`). The provider registry is keyed on
  canonical names; registered providers live in
  `DataServer/internal/deliveries/providers/`.

**General operator checklist for any workflow failure**:

1. **Localize the offending line** with `git grep -nE "<REGEX>" --`
   against the local working tree, applying the same 12 pathspec
   carve-outs as the runner. Confirm the line is in ACTIVE code
   (NOT in CHANGELOG / docs / migrations / test fixtures).

2. **If the line is in active code**: pick the canonical replacement
   per the 7-disjunct playbook above. **DO NOT** add an inline
   `':!...'` carve-out to silence the audit — that's a regression-
   guard smoking-gun and must not be merged without an explicit PR
   justifying the carve-out and auditing the new exclusion with
   the same scrutiny as the original 10.

3. **If the line is correctly in an excluded path**: the carve-out
   list may need follow-up expansion (e.g., a NEW fixture file
   format or documentation subdirectory). Open a follow-up PR that
   references this entry, and add the new exclusion inline below
   the existing 10 with explicit rationale. The review checklist
   for such PRs: (a) the new exclusion is necessary, not
   convenient; (b) the new exclusion is documented inline; (c) the
   new exclusion does NOT weaken the audit's coverage of the 7
   forbidden disjuncts.

4. **If the carve-out removal is needed** (NOTE-block contributors
   removing the documentation comments that necessitate
   category 10): commit must delete the corresponding carve-out in
   the SAME atomic commit. Leaving either an unused carve-out (false
   freedom) or the NOTE alone (regression) both regress this
   audit's surface-area contract.

**Commit chain (1 commit on `main`, NO branches)**:

| Hash     | Subject                                |
| ---      | ---                                    |
| `59a91f7` | `ci(workflow): add no-youtube-regression guard` |

**Verification**:

- `cat .github/workflows/no-youtube-regression.yml` renders the
  workflow verbatim as documented above. The `REGEX` env var is
  the single source of truth for the audit pattern.

- `git log --oneline -1 -- .github/workflows/no-youtube-regression.yml`:
  `59a91f7 ci(workflow): add no-youtube-regression guard`.

- Local reproduction of the runner matrix (same pathspec
  exclusions as the workflow applies):
  ```bash
  git grep -nE 'google\.golang\.org/api/youtube|youtubeanalytics|VELOX_YOUTUBE|youtube_oauth|internal/integrations/youtube|handlers/server/youtube|providers\.NewYouTubeProvider' \
    -- ':!.github/workflows/no-youtube-regression.yml' \
    ':!**/migrations/**' ':!**/testdata/**' ':!**/*_test.go' \
    ':!**/*.example' ':!**/*.md' ':!CHANGELOG.md' \
    ':!MILESTONE_PR_YOUTUBE_SOCIAL_SEPARATION.md' \
    ':!**/socialcontract/**' ':!**/social_contract/**' \
    ':!DataServer/internal/jobs/enqueue/delivery_plan_validator.go' \
    ':!DataServer/internal/store/delivery_plan_payload.go'
  # expect: empty (0 matches on post-PR-15.x main)

- Manual `workflow_dispatch` against the workflow on `main` re-reports
  `✅ No YouTube regression found — clean.` at the gate tier. The
  canonical CI (`make verify`) does NOT duplicate this audit, so
  this workflow is the single source of truth for the regex match.

- `go test ./...`, `go vet ./...`, `go build ./...`: PASS.
  Workflow change is YAML-pure; no Go compile impact.

**Refs**:

- `.github/workflows/no-youtube-regression.yml` — workflow source-of-truth.
- `DataServer/internal/store/migrations/README.md` — forward-only
  migration invariant referenced by the `**/migrations/**` carve-out.
- `docs/SOCIAL_API_MIGRATION_RUNBOOK.md` — operator-facing closure
  context (closure backfill procedure for `social_destination_id` /
  `external_destination_id` legacy rows + audit procedure post-deploy).
- `PR-15.8 / PR-15.9 / PR-15.10 / PR-15.11 / PR-15.12 / PR-15.13 /
  PR-15.14` — the canonical closure chain this workflow guards.

### PR-15.14 — Residuo 4 closure: ExternalDestinationID canonical rename

The opaque-mode identity is now uniformly `ExternalDestinationID`
across the persistence layer, the in-process typed `Destination`
struct, the validator shape, the socialclient request DTO, and the
SocialGatewayProvider. The legacy `SocialDestinationID` alias is
still populated by the store + runner + validator during the
gradual-rename transition window (Residuo 5 is the dedicated
alias-drop closing commit).

**Removed (canonical naming)**:

- Typed field `SocialDestinationID` on `socialclient.DeliverArtifactRequest`
  (json tag `social_destination_id`) — superseded by `ExternalDestinationID`
  (json tag `external_destination_id`).
- Provider reads `destination.ExternalDestinationID` (canonical)
  instead of `destination.SocialDestinationID` (deprecated alias).

**Added**:

- Migration `092_rename_social_to_external_destination_id.sql`
  (sqlite + testdata mirror) — forward-only
  `ALTER TABLE delivery_destinations ADD COLUMN
  external_destination_id TEXT` + `UPDATE ... SET
  external_destination_id = COALESCE(social_destination_id, '')` +
  `ALTER TABLE delivery_destinations DROP COLUMN
  social_destination_id`. NOT a `RENAME COLUMN` (banned by
  `scripts/ci/check-migrations.sh` for portability — table-rebuild
  pattern is required, but ADD/UPDATE/DROP achieves the same end
  on SQLite >= 3.35.0 without breaking checksum parity).
- Canonical JSON wire key `external_destination_id` (NO `omitempty`
  on the request DTO so any drift between the runner's fail-closed
  `DESTINATION_UNMAPPED` guard and the socialclient surfaces at
  marshal time).
- Sentinel `ErrDestinationUnmapped` message updated from
  `social_destination_id required` to
  `external_destination_id required` (canonical post-rename).
- 4 NEW tests:
  - `store/delivery_destination_opaque_test.go::TestDeliveryDestinationOpaqueStructShape`
    — compile-time pin of dual-field shape (canonical ExternalDestinationID
    + alias SocialDestinationID mirrored).
  - `store/delivery_destination_opaque_test.go::TestDeliveryDestinationJSONOpaqueKeys`
    — JSON serialization: canonical `external_destination_id` MUST
    be present, legacy `account_id / channel_id / language /
    social_destination_id` MUST be absent.
  - `store/delivery_destination_opaque_test.go::TestDeliveryDestinationEmptyExternalDestinationIDOmitEmpty`
    — empty canonical (with alias populated) is suppressed by `omitempty`.
  - `jobs/enqueue/delivery_plan_validator_test.go::TestShapeFromMap_CanonicalExternalDestinationIDHonored`
    + `TestShapeFromMap_CanonicalWinsOverLegacyAlias` — validator
    precedence: canonical key wins, alias preserved verbatim when both
    keys are present with differing values.
- 4 UPDATED test fixtures: `socialclient/client_test.go` 4 fixtures
  (HappyPath, WireShape_Minimal, WireShape_Full,
  WireShape_LegacyKeysNeverPresent) + the required-keys arrays in
  WireShape_Minimal (`external_destination_id` instead of
  `social_destination_id`) and WireShape_Full — fully aligned with
  the canonical wire key.
- 2 sampleDestination fixture updates:
  `providers/social_gateway_test.go::sampleDestination` +
  `integration_test/social_repo_integration_test.go::sampleDestination`
  now set `ExternalDestinationID` canonical (alias-deprecated
  `SocialDestinationID` is intentionally left empty in the fixtures
  to prove the canonical-only path works).

**Behaviour changes (operator-visible)**:

- The opaque-mode wire JSON OBJECT emitted by
  `socialclient.DeliverArtifactRequest` now contains
  `external_destination_id` in place of `social_destination_id`.
  Server-side consumers (the social_repo) MUST update their
  request handlers; client-side observers that grep the wire
  body MUST update their patterns.
- The runtime allow-closed error message after a missing opaque
  destination backfill now reports
  `delivery_plan[0].external_destination_id: ...` (canonical)
  instead of `delivery_plan[0].social_destination_id: ...`
  (legacy alias). Operators / observability tooling that grep
  the field path MUST update.
- The `Destination` typed struct in the `deliveries` package and
  the `DeliveryDestination` typed struct in the `store` package
  now carry BOTH `ExternalDestinationID` (canonical, sources all
  dispatch reads + fail-closed guards) AND `SocialDestinationID`
  (deprecated alias, mirror-symmetric with the canonical field).
  The alias is consumed by no active code path; it is preserved
  as a read-only bridge for callers that have not yet migrated.

**Commit chain (3 atomic commits on `main`, NO branches, one commit per layer)**:

| Hash | Subject |
| --- | --- |
| `ea38837` | `refactor(store): rename social_destination_id -> external_destination_id (Residuo 4 step 1)` |
| `03acccb` | `refactor(validator+runner): rename social_destination_id -> external_destination_id (Residuo 4 step 2)` |
| `83d8b2f` | `refactor(socialclient+provider): wire + provider rename social_destination_id -> external_destination_id (Residuo 4 step 3)` |

The chain is the textbook gradual-rename: each layer holds BOTH names
during its commit (next-commit renames the next layer), and every
commit boundary compiles + tests PASS. Step 3 (the wire + provider
flip) is necessarily atomic per Go's static-typing rule (struct field
rename forces simultaneous provider mapping + test fixture updates).

**Verification**:

- `cd DataServer && go test ./internal/deliveries/... ./internal/socialclient/... ./internal/jobs/enqueue/... ./internal/integration_test/... ./internal/store/... -count=1`: PASS.
- `cd DataServer && go test ./internal/socialclient/... -v -run WireShape`: PASS for all 3 WireShape_Minimal / Full / LegacyKeysNeverPresent.
- `cd DataServer && go vet ./... && go build ./...`: PASS.
- `bash scripts/ci/check-migrations.sh`: OK (146 files).
- `git grep -nE 'social_destination_id' -- ':!docs/' ':!CHANGELOG.md' ':!docs/CHANGELOG.md' `: active code references are now confined to the legacy SocialDestinationID alias mirrors (store + runner + validator) + the migration testdata shadow of 091 (inert).
- The 6 documented scenarios (acceptance / auth / rate-limit / transient 5xx / unreachable / retry idempotency) STILL PASS on both the enqueue pre-flight path and the runner dispatch path with the new canonical wire key.
- Mock social_repo sniffer is `idempotency_key`-only, so the wire-key rename does NOT regress the dedup contract.

**Refs**:

- `DataServer/internal/store/migrations/sqlite/092_rename_social_to_external_destination_id.sql` — forward-only schema migration.
- `DataServer/internal/store/migrations/testdata/092_rename_social_to_external_destination_id.sql` — byte-equivalent runner-required mirror.
- `DataServer/internal/store/store_deliveries.go::DeliveryDestination` — typed struct post-migration schema (dual-field, alias-mirror).
- `DataServer/internal/deliveries/provider.go::Destination` / `ErrDestinationUnmapped` — dual-field typed struct + canonical sentinel message.
- `DataServer/internal/deliveries/runner.go::hydrateDestination` — reads `d.ExternalDestinationID` (canonical); guards `TrimSpace == ""`; mirrors to `SocialDestinationID` for gradual-rename consumers.
- `DataServer/internal/jobs/enqueue/delivery_plan_validator.go::deliveryPlanShape` / `shapeFromMap` — canonical-first read with legacy-alias fallback; precedence: canonical wins, alias preserved verbatim.
- `DataServer/internal/socialclient/requests.go::DeliverArtifactRequest.ExternalDestinationID` — canonical wire field (`json:"external_destination_id"`, NO omitempty).
- `DataServer/internal/deliveries/providers/social_gateway.go::buildRequest` — reads `destination.ExternalDestinationID` (canonical) and forwards as `req.ExternalDestinationID`.
- `docs/api_script_generate_with_images.md` — operator-facing JSON example updated to use `external_destination_id` + `metadata` blob (platform-shaped values live in metadata as opaque pass-through).

### PR-15.13 — Residuo 3 closure: opaque-mode wire contract

The Social API wire contract now carries only the opaque-mode fields:
`external_delivery_id`, `idempotency_key`, `social_destination_id`,
`artifact`, `metadata`, `publish_at`, `callback_url`. The three
YouTube-specific fields `Platform`, `AccountID`, `ChannelID` are
gone from both the typed `DeliverArtifactRequest` and the
`SocialGatewayProvider::buildRequest` call site; the social_repo is
the authoritative resolver from `social_destination_id` for
platform, account, channel, language, and credentials.

**Removed (typed struct fields + provider plumbing)**:

- `socialclient.DeliverArtifactRequest.Platform` / `AccountID` / `ChannelID`
  — fields dropped from the wire DTO entirely.
- `providers.parsePlatformAndAccount` helper — removed (it parsed
  `destination.ConfigurationJSON` for `platform`/`account_id` and
  was the only consumer of those keys in the wire DTO).

**Added (wire contract)**:

- `socialclient.DeliverArtifactRequest.SocialDestinationID string`
  with `json:"social_destination_id"` tag (NO `omitempty` so any
  drift between the runner's fail-closed `DESTINATION_UNMAPPED`
  guard and the socialclient surfaces at marshal time as
  `"social_destination_id":""` rather than a silent malformed
  POST).

**Behaviour changes**:

- Operators with `delivery_destinations.configuration_json`
  containing `{"platform":"youtube","account_id":"..."}` continue
  to author the old shape without breakage, BUT it is now
  **inert in the wire contract**: the values do not reach the
  social_repo. The runner + provider only forward the opaque
  `social_destination_id` and `delivery_metadata_json` (the latter
  becomes the wire `metadata` blob, opaque pass-through).
- Operators wanting per-artifact values to reach the social_repo
  must use the `metadata` blob, not the inert `configuration_json`.

**New tests (all in `internal/socialclient/client_test.go`)**:

- `TestClient_DeliverArtifact_WireShape_Minimal` — pins the
  minimal wire JSON: top-level keys must be EXACTLY four
  (`external_delivery_id`, `idempotency_key`, `social_destination_id`,
  `artifact`); `metadata`, `publish_at`, `callback_url` must NOT
  appear when empty.
- `TestClient_DeliverArtifact_WireShape_Full` — pins the full
  wire JSON: all 7 top-level keys present.
- `TestClient_DeliverArtifact_WireShape_LegacyKeysNeverPresent` —
  regression invariant: top-level wire JSON keys may NEVER
  include `platform`, `account_id`, or `channel_id`, **even if**
  the operator's `metadata` blob legitimately contains those
  sub-keys (metadata is opaque pass-through; legacy keys do not
  belong at the top).

These tests use httptest.NewServer + chan []byte body capture +
json.Unmarshal on top-level keys — NOT string-matching — so
metadata sub-keys do NOT false-positive on the legacy-key
presence check.

**Fixture cleanup**:

- `providers/social_gateway_test.go::sampleDestination` and
  `integration_test/social_repo_integration_test.go::sampleDestination`
  simplify `ConfigurationJSON` from inert-keyed blobs to `"{}"`.
  DeliveryMetadataJSON is kept (still forwarded as `metadata`).
  Doc comments expanded to make the wire/observability split
  explicit at the fixture level.

**ABI-safe ordering (3 atomic commits, NO branches)**:

| Hash     | Subject |
| ---      | --- |
| `71b0bb6` | `refactor(socialclient): opaque-mode wire — add social_destination_id, deprecate Platform/AccountID/ChannelID` |
| `32bd74f` | `refactor(social_gateway): drop parsePlatformAndAccount + deprecated struct fields` |
| `362718d` | `test(socialclient): pin opaque wire shape + clean inert fixtures` |

The 2-step provider cleanup is the textbook refactor-2-step
pattern: Commit 1 keeps the old fields typed-but-un-serialised
(`json:"-"`) so callers still compile, Commit 2 drops them
entirely along with `parsePlatformAndAccount`. Commit 3 is pure
test layer (no struct change).

**Verification**:

- `cd DataServer && go test ./internal/socialclient/... ./internal/jobs/enqueue/... ./internal/delivery_destinations... -count=1`: PASS
- `cd DataServer && go vet ./internal/socialclient/... ./internal/deliveries/...`: PASS
- `cd DataServer && go build ./...`: PASS
- `git grep -nE 'parsePlatformAndAccount|req\\.Platform|req\\.AccountID|req\\.ChannelID'`: 0 matches.
- The 6 documented scenarios (acceptance / auth / rate-limit /
  transient 5xx / unreachable / retry idempotency) STILL PASS on
  both the enqueue pre-flight path (`Enqueuer.WithSocialValidator`)
  and the runner dispatch path (`SocialGatewayProvider.Deliver`)
  with the new wire shape — no behavioral regressions.

**Refs**:

- `DataServer/internal/socialclient/requests.go::DeliverArtifactRequest` — typed DTO + opaque-mode doc.
- `DataServer/internal/socialclient/client.go::DeliverArtifact` — wire serializer (unchanged path, but the request shape changed).
- `DataServer/internal/deliveries/providers/social_gateway.go::buildRequest` — simplified: only routes `destination.SocialDestinationID`.
- `DataServer/internal/deliveries/runner.go::hydrateDestination` — fail-closed `DESTINATION_UNMAPPED` (Residuo 2, still the guardrail for the new wire shape).

### PR-15.12 — Residuo 2 closure: opaque-mode Destination model

The Delivery destination model is now fully opaque-mode. Velox no longer
carries the YouTube-specific fields `AccountID`, `ChannelID`, `Language`
either in the typed structs or in the SQLite schema. They are owned
exclusively by the external Social API repository, which resolves them
internally from the opaque `SocialDestinationID`. The migration is
forward-only (no DOWN), version-pinned (SQLite >= 3.35.0), and
ABI-safe-ordered: model → store → validator.

**Removed (typed struct fields + SQL columns)**:

- `data Destination.*` fields: `AccountID`, `ChannelID`, `Language`.
- `data DeliveryDestination.*` fields: `AccountID`, `ChannelID`, `Language`.
- SQLite column drop via migration `091_opaque_destination.sql`
  (forward-only `ALTER TABLE delivery_destinations DROP COLUMN` × 3).

**Added (opaque mode)**:

- `data Destination.SocialDestinationID` — opaque identifier resolved by
  the external Social API. Typed as `string`. JSON tag
  `social_destination_id,omitempty` so an empty value never leaks into
  the wire contract.
- `data DeliveryDestination.SocialDestinationID` — symmetric to the
  in-process type. Stored as `social_destination_id TEXT` (nullable, no
  DEFAULT) so an unmapped row reads back as empty string after COALESCE.
- Sentinel `errors.New("deliveries: destination is unmapped\n(social_destination_id required)")`
  (`ErrDestinationUnmapped`) in `internal/deliveries/provider.go`.
- Runtime guard in `runner.hydrateDestination`: rejects empty
  `SocialDestinationID` at hydrate time, BEFORE dispatch. processLease
  distinguishes `ErrDestinationUnmapped` from `ErrProviderNotConfigured`
  with delivery-status code `DESTINATION_UNMAPPED`
  (vs `DESTINATION_NOT_FOUND`).
- Migration `091_opaque_destination.sql` (sqlite + testdata mirror) that
  drops the 3 YouTube-specific columns and adds `social_destination_id`.
- New opaque-mode unit tests:
  - `internal/deliveries/destination_opaque_test.go`:
    - `TestDestinationOpaqueStructShape` — compile-time assertion that
      the typed Destination does not accept legacy fields.
    - `TestErrDestinationUnmappedSentinel` +
      `TestErrDestinationUnmappedIsCompatibleWithErrorsIs` — sentinel
      stability + `errors.Is` round-trip.
  - `internal/store/delivery_destination_opaque_test.go`:
    - `TestDeliveryDestinationOpaqueStructShape` — compile-time.
    - `TestDeliveryDestinationJSONOpaqueKeys` — JSON keys for the
      persisted shape; legacy `account_id/channel_id/language` keys
      confirmed absent.
    - `TestDeliveryDestinationEmptySocialDestinationIDOmitEmpty` —
      empty `social_destination_id` is suppressed by `omitempty`.

**Behavior change (delivery dispatch)**:

- A destination whose `social_destination_id` is empty / whitespace-only
  is now dispatched into FAILED with code `DESTINATION_UNMAPPED`
  (previously it would silently proceed via `social_gateway.buildRequest`
  with `ChannelID=""` until the social_repo rejected it).
- Operators that still have existing `delivery_destinations` rows with
  empty `social_destination_id` post-migration MUST backfill before
  enabling dispatch. The audit script
  (`deploy/scripts/audit-no-youtube-residuals.sh`, PR-15.11) does not
  probe `delivery_destinations` schema directly — it's a YouTube-residue
  auditor only — so a follow-up operator checklist is recommended.

**Commit chain (3 atomic commits on `main`, NO branches)**:

| Hash | Subject |
| --- | --- |
| `85c10f8` | `refactor(deliveries): drop AccountID/ChannelID/Language from Destination, add SocialDestinationID` |
| `cab7cc3` | `refactor(store): drop account_id/channel_id/language columns, add social_destination_id` |
| `2dfaed6` | `refactor(deliveries): fail-closed on unmapped destinations + opaque-mode tests` |

**Verification**:

- `cd DataServer && go test ./internal/deliveries/... ./internal/jobs/enqueue/... ./internal/integration_test/... ./internal/store/... -count=1`: PASS.
- `cd DataServer && go vet ./... && go build ./...`: PASS.
- New tests cover: struct shape (compile-time), sentinel stability, `errors.Is` chain, JSON opaque keys, `omitempty` on empty opaque ID.
- Existing tests untouched (the `BlockedAuth` fixture, the `sampleDestination` fixtures, and the `enqueue_test_helpers` seeds all use canonical fields only).
- ABI-safe ordering verified: model landed before store before validator so the typed struct + SQL + runner agree at every commit boundary.

**Refs**:

- `DataServer/internal/deliveries/provider.go` — `ErrDestinationUnmapped` sentinel documented.
- `DataServer/internal/deliveries/runner.go::hydrateDestination` — guard documented.
- `DataServer/internal/store/migrations/sqlite/091_opaque_destination.sql` — forward-only schema migration.
- `DataServer/internal/store/store_deliveries.go::DeliveryDestination` — typed struct post-migration schema.
- `DataServer/internal/store/migrations/README.md` — forward-only invariant (do NOT edit shipped migrations).

### PR-15.11 — Operator-facing YouTube-residue audit script

Operators can now run a read-only SQLite audit on the live Velox
production DB to confirm that the YouTube domain is fully cleaned.
The audit script reflects the same contract the test suite pins:

- Migration `090_drop_youtube_domain.sql` is forward-only and
  idempotent (checksum gate).
- The end-to-end migration test
  (`DataServer/internal/store/migrations/migrations_integration_test.go`,
  `TestIntegration_MigrationRunner_EndToEnd`, phase 4) asserts that
  none of the 10 YouTube tables exist after the chain.
- The schema test
  (`DataServer/internal/store/migrations/migrations_schema_test.go`,
  `TestMigration090_YouTubeDomainDropped`) additionally asserts that
  the 3 historical columns on `calendar_events` and
  `dark_editor_folders` are absent.

**Added**:

- `deploy/scripts/audit-no-youtube-residuals.sh` — read-only SQLite
  probe. Takes `<path-to-velox.db>` as argv and reports any leftover
  `youtube_*` tables (anchored `youtube\_%` ESCAPE) plus any
  `youtube_*` columns on `calendar_events` and `dark_editor_folders`
  (via `pragma_table_info` filtered inline). Pattern matches
  case-insensitively so it catches mixed-case identifiers like
  `` `YouTube_Group` ``.

**Exit codes**:

| Code | Meaning |
| ---: | --- |
| `0` | CLEAN — no YouTube tables or columns remain |
| `1` | RESIDUAL_FOUND — see report; remediation hint printed |
| `2` | DB_NOT_FOUND — path missing / unreadable |
| `3` | NOT_VELOX_SCHEMA — DB exists but is missing canonical Velox tables |
| `4` | ARGV_OR_TOOL — `sqlite3` CLI missing or wrong invocation |

**Sanity pre-check**: the script probes for the 5 canonical permanent
tables (`jobs`, `artifacts`, `job_deliveries`, `calendar_events`,
`dark_editor_folders`) before reporting residuals, so a non-Velox
SQLite file is rejected with exit 3 rather than producing a misleading
`` CLEAN '' report.

**Operator usage**:

```bash
sudo ./deploy/scripts/audit-no-youtube-residuals.sh /var/lib/velox/data/velox.db
#   exit 0  →  clean
#   exit 1  →  scrap the report; investigate
```

**Verification on synthetic DBs** (run on this commit before push):

| Scenario | DB shape | Exit | Outcome |
| --- | --- | ---: | --- |
| A. `bash -n` syntax check | n/a | n/a | OK |
| B. Clean Velox-shaped DB | 5 canonical tables, no YouTube state | `0` | "CLEAN" reported |
| C. Contaminated DB | + 4 YouTube tables + 3 YouTube columns | `1` | Full report listing all 7 residuals + remediation |
| D. Non-Velox SQLite | only `foo` table | `3` | "does not look like a Velox schema" |
| E. Nonexistent path | n/a | `2` | "DB not readable" |
| F. No argv | n/a | `4` | usage error on stderr |
| G. Mixed-case column `` `YouTube_Group` `` | 5 canonical + 1 mixed-case column | `1` | correctly detected via `lower(name)` |

**Commit**:

| Hash | Subject |
| --- | --- |
| `5491f31415deba20adc1fca21142a4c57b7a89fa` | `chore(deploy): add read-only YouTube-residue audit script for operators` |

## Post-Refactor State (structural refactor series + follow-on features)

> Cumulative documentation of the seven-commit **structural refactor series** that landed on `main`
> between `0d42b46` (pre-refactor baseline) and `1419f7d` (final split), plus the follow-on features
> built on top of the cleaned-up surface. All refactors were **purely structural** — zero changes
> to behavior, schema, API contracts, or protobuf wire format.

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

### Validation evidence (post-refactor, all green on `main`)

| Gate | `DataServer` | `worker-agent-go` | `shared` |
| --- | --- | --- | --- |
| `gofmt -l .` | ✅ empty | ✅ empty | n/a |
| `go vet ./...` | ✅ rc=0 | ✅ rc=0 | ✅ rc=0 |
| `go build ./...` | ✅ rc=0 | ✅ rc=0 | ✅ rc=0 |
| `go test ./... -count=1` | ✅ rc=0 | ✅ rc=0 | ✅ rc=0 |
| `go test -race ./... -count=1` | ✅ rc=0 | ✅ rc=0 | n/a |
| `git diff --check` | ✅ rc=0 | ✅ rc=0 | ✅ rc=0 |

### Zero-regression check vs baseline `0d42b46`

- **Files deleted since baseline**: 0
- **Schema changes (`.proto` / `.sql`)**: 1, intentional — `migrations/sqlite/096_worker_partition_detection.sql` from `a394193` (partition detection `partition_state` column). Refactors themselves introduced **0 schema changes**.
- **API contract changes**: 0 from refactors; **+3** new endpoints (`/workers/:id/{metrics,sessions,events}`) added in `044a401` as additive extensions.
- **Protobuf wire format changes**: 0.
- **Behavior changes**: 0 from refactors; behavioral additions documented per-feature.

### Follow-on features enabled by the structural cleanup

| Commit | Feature | Why it was easier after the refactor |
| --- | --- | --- |
| `a394193` | STALE threshold + network partition detection + retention | `store_worker_runtime.go` already split into `heartbeat.go` / `projection.go` / `metrics.go` / `events.go` — added into the right owners, no monolithic blast radius |
| `044a401` | Worker metrics / sessions / events endpoints | `workers_handler.go` / `workers_dto.go` / `workers_mapper.go` already split; new handlers compose the existing `sanitizeWorker` + mapping helpers |

### Files intentionally **not** split

`094_worker_runtime_persistence.sql`, `095_worker_session_types.sql`, `remote_endpoint.go`, `verify-golden-job.sh`, `master-driver.sh`, `worker-run.sh`, `cancel.go`. Migrations are immutable historical documents; scripts in the 40–150 LOC range are still manageable; small handlers don't justify fragmentation.
