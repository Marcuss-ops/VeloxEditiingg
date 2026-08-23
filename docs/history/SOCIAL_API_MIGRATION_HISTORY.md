# Social API Migration Historical Record

> Historical commit anchors and closure notes moved from
> [`SOCIAL_API_MIGRATION_RUNBOOK.md`](../SOCIAL_API_MIGRATION_RUNBOOK.md).
> Current operator procedures remain in the runbook; this document preserves
> the historical changelog mapping and follow-up record.

<a id="5-5-changelog-anchors"></a>

### 5.5 CHANGELOG anchors

Anchors currently shipping in CHANGELOG.md on `main` (verified by
`grep -nE 'PR-15\.(1[0-9])' CHANGELOG.md`):

* **PR-15.10** — residuo 5 (Rimozione alias `SOCIAL_GATEWAY_*`)
* **PR-15.11** — Migration drop (closure of YouTube-domain;
  `090_drop_youtube_domain.sql` + `091_opaque_destination.sql`)
* **PR-15.12** — Residuo 2 closure (opaque-mode Destination model;
  `runner.hydrateDestination` fail-closed guard)
* **PR-15.13** — Residuo 3 closure (socialclient refactor; removed
  top-level `Platform` / `AccountID` / `ChannelID`)
* **PR-15.14** — Residuo 4 closure (canonical rename
  `SocialDestinationID → ExternalDestinationID` via migration 092)
* **PR-15.16** — Residuo 6 closure (`external_destination_id`
  migration / `channel_id` retirement)
* **PR-15.17** — Runbook §0.1/§0.2/§0.3 emission (5-commit
  runbook-promotion chain promoting the Velox→Social API migration
  runbook to a complete cross-repo operator map covering
  `SOCIAL_API_*` env-var bootstrap (§0.1), the 4 channel-readiness
  prerequisites (§0.2), and sender-side `destination_id` selection
  (§0.3)). Round-1 through round-4 commits align §0.2.2 triage +
  §0.3.4 catalog-verdict list with the canonical `target_resolver.go`
  taxonomy and pin the `platform_accounts.status` enum to
  `user.go:49-72` (the canonical 8-value enum declaration).
  Cross-checks: canonical status strings (`BLOCKED_AUTH`,
  `TARGET_NOT_AVAILABLE`, `active`, `reauth_required`); canonical
  path/file anchors (`runner.go:499-500`,
  `store_deliveries.go::BatchDeliveryDestinationsStatus`,
  `delivery_plan_validator.go::validateDeliveryDestinationTx`,
  `InstaeditLogin/internal/deliveries/target_resolver.go:184-188`).
  Operator follow-ups closed: speculative function-name drift
  (`CheckWorkspace` / `VerifyActive`) and non-canonical triage codes
  (`binding_disabled`, `account_inactive`) REMOVED in favor of the
  canonical taxonomy.
  - `422e5c1`  `docs(runbook): add §0.1/§0.2/§0.3 (env bootstrap, channel prerequisites, sender-side destination_id selection)`
  - `cdec3c7`  `docs(runbook): replace speculative function names + paths with verified canonicals in §0.2`
  - `fb1f663`  `docs(runbook): align §0.2.2 triage with canonical taxonomy (round-2)`
  - `736e1ee`  `docs(runbook): align 0.3.4 catalog-verdict list with canonical taxonomy (round-3)`
  - `74973df`  `docs(runbook): pin platform_accounts.status enum to user.go:49-72 + correct 8 canonical values (round-4)`

Operator follow-up (work landed on `main` but NOT yet anchored in
CHANGELOG.md — track via commit hash until the next CHANGELOG
rebase assigns a PR-N.NN anchor):

* **Fail-closed coverage gap test:**
  - `e4c5b58`  `test(deliveries): close Residuo 2 fail-closed coverage gap`
  - `39be2d0`  `test(deliveries): fix build break + panic in fakeProvider`
* **Opaque-wire CI guard (round-1, round-3 fix-ups):**
  - `1927b8b`  `ci(workflow): add opaque-wire Residuo 3 guard`
  - `bf3b845`  `ci(workflow): harden opaque-wire Residuo 3 guard`
* **Residuo 5 (alias removal `SOCIAL_GATEWAY_*`):** DONE — closed in PR-15.10 retirement
  chain on `main` (commits `ca000bf` / `bb407b8` / `6aadcd9`). Canonical-only env contract
  is in force: `socialclient.ConfigFromEnv()` honors ONLY `SOCIAL_API_URL`,
  `SOCIAL_API_TOKEN`, `SOCIAL_API_TIMEOUT_MS`, `SOCIAL_CALLBACK_BASE_URL`.
  The deprecation-cycle aliases
  `SOCIAL_GATEWAY_URL` / `SOCIAL_GATEWAY_API_KEY` /
  `SOCIAL_GATEWAY_CALLBACK_BASE_URL` are NOT honored — operators still carrying
  these in `/etc/velox-server.env` (or the retired
  `vault_velox_social_gateway_api_key` alias) MUST rename to the canonical form. The negative-pinning test
  `TestConfigFromEnv_DropsLegacySocialGatewayAliases` and its companion
  `TestConfigFromEnv_HonorsCanonicalSocialAPIEnvs` (both in
  `DataServer/internal/socialclient/config_test.go`) lock the boundary closed.
  Operator-visible safety net: `deploy/validate-master-env.sh` warns when any
  `SOCIAL_GATEWAY_*` alias is still set in the master env file, AND
  `DataServer/cmd/server/bootstrap_modules.go` emits a one-line
  `[BOOTSTRAP][SOCIALCLIENT] WARN` at every master boot pointing at the canonical rename.
