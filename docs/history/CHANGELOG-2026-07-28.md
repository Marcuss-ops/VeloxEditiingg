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
