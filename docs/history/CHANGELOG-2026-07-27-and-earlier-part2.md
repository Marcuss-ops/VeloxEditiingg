# Velox Changelog Archive — 2026-07-27 and Earlier, Part 2

> Continuation of [part 1](CHANGELOG-2026-07-27-and-earlier.md), covering the
> PR-15.14 → PR-15.11 residue-closure entries and the PR-15.16 CI guard.
> and remain in their original chronological order.

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

<a id="pr-1513-residuo-3-closure-opaque-mode-wire-contract"></a>
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

<a id="pr-1512-residuo-2-closure-opaque-mode-destination-model"></a>
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

<a id="pr-1511-operator-facing-youtube-residue-audit-script"></a>
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
  the 3 historical columns on `calendar_events` are absent. The shipped
  sqlite 090 also retains its historical cleanup of
  `dark_editor_folders.youtube_group` when that legacy table is present;
  the legacy editor tables themselves are covered separately by migration
  128 and are not an active runtime surface.

**Added**:

- `deploy/scripts/audit-no-youtube-residuals.sh` — read-only SQLite
  probe. Takes `<path-to-velox.db>` as argv and reports any leftover
  `youtube_*` tables (anchored `youtube\_%` ESCAPE) plus any
  `youtube_*` columns on `calendar_events`
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
| `4` | ARGV_OR_TOOL — `sqlite3` CLI missing or wrong invocation |**Sanity pre-check**: the script probes for the 4 canonical permanent
 tables (`jobs`, `artifacts`, `job_deliveries`, `calendar_events`) before
 reporting residuals, so a non-Velox SQLite file is rejected with exit 3
 rather than producing a misleading `` CLEAN '' report.

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


---

The PR-15.16 CI-guard entry continues in
[part 3](CHANGELOG-2026-07-27-and-earlier-part3.md).
