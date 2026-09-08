# Velox → Social API Migration Runbook (Residuo 2 + Residuo 4 closure)

> **Audience:** SRE / on-call. **Scope:** All operator procedures on the
> Velox SQLite database and Socialclient wire-contract that arise
> following the YouTube → Social closure (PR-15.11 through PR-15.16,
> Migrations 090 → 093).
> **Owner:** Velox core platform. **Review cadence:** every
> PR-touching-deploy change touching `delivery_destinations`,
> `configuration_json`, `socialclient`, or `social_gateway`.

This runbook is the canonical operator map for the Velox → Social
closure. It covers three procedures that SRE on-call should be able
to run blindfolded:

* **§1 Back-filling `external_destination_id`** for
  `delivery_destinations` rows that pre-date Migration 091 + 092.
* **§2 Migrating legacy `configuration_json`** (which may carry
  pre-091 `platform` / `account_id` sub-keys) into the opaque
  post-closure model where platform-shaped intent lives at job-level
  `metadata`.
* **§3 Post-deploy audit** that catches residue on a live database,
  drift in the wire shape, and dispatch-time unmapped destinations.

Every SQL and bash snippet in this runbook is grounded in the
on-disk artifacts cited inline. Where line numbers are referenced
(e.g. `runner.go:499-500`), they match the source at the time of
this document's authoring — see §5 for the CHANGELOG / PR mapping.

---

## 0. Context — what closed and what is still operator-owned

The YouTube → Social closure removed OAuth, channel, token, quota,
publishing state, and platform-specific configuration FROM Velox and
delegated those concerns to the external Social API repository. From
the Velox-side schema and typed-shape side, this materialised as a
four-step migration chain:

| Migration | What it does | Forward-only? |
|---|---|---|
| `090_drop_youtube_domain.sql` (sqlite) / `023_drop_youtube_domain.sql` (postgres) | DROPs all 10 `youtube_*` tables + the 2 historical `youtube_*` columns on `calendar_events` | YES |
| `091_opaque_destination.sql` | DROPs `account_id` / `channel_id` / `language` from `delivery_destinations`; ADDs `social_destination_id TEXT` (nullable, fail-closed) | YES |
| `092_rename_social_to_external_destination_id.sql` | ADDs `external_destination_id TEXT`; `UPDATE SET external_destination_id = COALESCE(social_destination_id, '')`; DROPs `social_destination_id` | YES |
| `093_residuo4_closure_marker.sql` | Idempotent `json_insert` of `$.residuo4_closed_at` ISO-8601 string into `configuration_json` for every row whose JSON is well-formed | YES (idempotent) |

After Migration 091 + 092 land, a `delivery_destinations` row is
structurally defined by:

```text
destination_id, provider, external_destination_id (opaque),
folder_id, name, enabled, configuration_json,
created_at, updated_at
```

A row whose `external_destination_id` is empty / whitespace-only
**cannot dispatch** — the runner fails-closed with sentinel error
`ErrDestinationUnmapped` at `runner.go:499-500`, mapped to status
code `DESTINATION_UNMAPPED` at `runner.go:280-288`, persisted to
`job_deliveries.last_error_code` by `MarkDeliveryFailed`.

The opaque `external_destination_id` is resolved server-side by the
external Social API into (platform, account, channel, language,
credentials). Velox carries NO knowledge of those downstream fields.

---

## 0.1 Bootstrap order for `SOCIAL_API_*` env vars (procedure 0)

Operators bringing up a fresh Velox Master, or rebuilding after a
disaster, MUST configure the `SOCIAL_API_*` env vars on the master
in this exact order. Out-of-order bootstrap leaves the
`socialclient` package partially initialized and triggers the
negative-pinning tests in `DataServer/internal/socialclient/config_test.go`
on the very first request.

### 0.1.1 Variable inventory (canonical, post-Residuo-5)

The canonical contract — enforced by `socialclient.ConfigFromEnv()`
(`DataServer/internal/socialclient/config.go:104-108`) and locked by
`TestConfigFromEnv_HonorsCanonicalSocialAPIEnvs` — reads **only**
the following variables:

| Variable | Default | Purpose |
|---|---|---|
| `SOCIAL_API_URL` | (none — required) | Base URL of the external Social API (e.g. `https://instaedit.example.com`). Read at `config.go:104`. |
| `SOCIAL_API_TOKEN` | (none — required) | Bearer sent as `Authorization: Bearer <token>` on every Velox→Social call. Read at `config.go:105`. |
| `SOCIAL_API_TIMEOUT_MS` | `30000` | Per-request timeout (ms) for Velox→Social HTTP calls. Default 30s. |
| `SOCIAL_CALLBACK_BASE_URL` | (none — required) | Public base URL the Social API calls back to (e.g. `https://velox.example.com`). Used for webhook delivery. |

**No other `SOCIAL_*` variable is honored.** In particular, the
deprecated one-release-cycle aliases from PR-15.10
(`SOCIAL_GATEWAY_URL`, `SOCIAL_GATEWAY_API_KEY`,
`SOCIAL_GATEWAY_CALLBACK_BASE_URL`) are **dropped** at parse time;
see the negative-pinning test
`TestConfigFromEnv_DropsLegacySocialGatewayAliases` and the
operator-visible warning emitted by `DataServer/cmd/server/bootstrap_modules.go`
at every master boot.

### 0.1.2 Bootstrap order (mandatory)

```text
Step 1 — SOCIAL_API_URL
Step 2 — SOCIAL_API_TOKEN
Step 3 — SOCIAL_API_TIMEOUT_MS
Step 4 — SOCIAL_CALLBACK_BASE_URL
Step 5 — verify (see §0.1.3)
Step 6 — restart the master
```

1. **`SOCIAL_API_URL`** first. Without it, the `socialclient`
   `Config{BaseURL:""}` returns immediately on every call and the
   master boot logs `[BOOTSTRAP][SOCIALCLIENT] WARN: SOCIAL_API_URL
   is unset — Velox will skip Social delivery` (see
   `bootstrap_modules.go:211-217`). Setting the URL alone is
   necessary but not sufficient.

2. **`SOCIAL_API_TOKEN`** second. The token authenticates Velox to
   the Social API; the URL alone would be rejected with `401` on
   the very first probe. **Rotate via the OpenBao KV leaf
   `velox/production/master/social-api-token`** (see `SECURITY_RUNBOOK.md`
   §2.4 / §3.4); never hand-edit `/etc/velox-server.env` outside the
   resolver-driven deploy path (`resolve-master-env.sh`).

3. **`SOCIAL_API_TIMEOUT_MS`** third. Default 30s is the
   cross-repo-tested ceiling for chunked artifact delivery. Operators
   who lower this must verify the downstream timeout on the Social
   API side is strictly greater to avoid spurious `504` from
   intermediate proxies.

5. **`SOCIAL_CALLBACK_BASE_URL`** fifth. This is the public base URL
   the Social API calls back to. It MUST match the URL the Social
   API has configured in its `velox_callback_base_url` setting,
   otherwise webhook deliveries will be rejected with HTTP 403
   from the master.

6. **Verify** before restart (see §0.1.3).

7. **Restart the master** (`systemctl restart velox-server` or
   `velox-server systemd unit`). The env vars are read at process
   start; an in-place reload requires a full restart.

### 0.1.3 Pre-restart verification

Run `deploy/validate-master-env.sh` (see `deploy/validate-master-env.sh:251-271`)
to confirm every canonical variable is set, no `CHANGE_ME_*` placeholders
remain, and `SOCIAL_API_URL` parses as a valid `https://` URL:

```bash
sudo -u velox bash -c 'source /etc/velox-server.env && \
    /opt/velox/current/deploy/validate-master-env.sh'
```

**Expected output envelope:**

* `OK — SOCIAL_API_URL=https://instaedit.example.com` (or similar)
* `OK — SOCIAL_API_TOKEN is set (redacted)`
* `WARN` only when the URL is `http://` and the master is behind a
  VPN/front-door TLS terminator (acceptable in dev, never in prod).
* `FAIL` on a `CHANGE_ME_*` placeholder, missing value, malformed
  URL, or any of the deprecated `SOCIAL_GATEWAY_*` aliases still
  set.

If `FAIL`, the master will not boot cleanly. Re-edit
`/etc/velox-server.env` (preferably via the ansible-vault deploy
path) and re-run §0.1.3.

### 0.1.4 Post-restart invariant

After the master boots with all 5 canonical variables set, the
following must hold:

* `/var/log/velox/server.log` contains exactly one
  `[BOOTSTRAP][SOCIALCLIENT] OK: 5/5 canonical SOCIAL_API_* envs honored`
  line.
* `TestConfigFromEnv_HonorsCanonicalSocialAPIEnvs` and
  `TestConfigFromEnv_DropsLegacySocialGatewayAliases` continue to
  pass on `main` (CI gate `main-baseline.yml`).
* The deprecated `SOCIAL_GATEWAY_*` aliases are NOT present in
  `/etc/velox-server.env` (the warning from §0.1.1 must not appear).

If any of these invariants breaks, re-run §0.1.3 and re-roll the
master env vars.

---

## 0.2 Channel prerequisites for a publishable destination (procedure 0b)

A channel surfaced by `POST /api/v1/publishing/targets` is
**publishable** — i.e. usable as `delivery_plan[0].destination_id`
on a render job — **iff all four conditions** below hold
simultaneously. Each condition is enforced by a different layer of
the cross-repo contract:

| # | Condition | Enforced by | Failure surface in `/publishing/targets` |
|---|---|---|---|
| 1 | **Workspace binding enabled** for the channel's owning workspace | `InstaeditLogin/internal/deliveries/target_resolver.go::checkAccountEligibility` (binding gate — step 4 of the eligibility gate) | absence from the catalog (no `can_post=true` row surfaces) |
| 2 | **Platform account active** (status enum = `active`, not `paused` / `revoked` / `disconnected`) | same `checkAccountEligibility` (status gate — step 3 of the eligibility gate) | row dropped from catalog, or `can_post=false` |
| 3 | **OAuth valid** (access token not expired, not revoked, refresh-token chain not broken) | same `checkAccountEligibility` (reauth_required gate — step 1 of the eligibility gate; dual-signal: status enum OR `reauth_required_at` timestamp). Token-freshness itself is verified at the WORKER boundary via `internal/services/youtube_validate.go`. | `status="reauth_required"`, `target_error_code="BLOCKED_AUTH"` |
| 4 | **External destination enabled** (the linked row in `delivery_destinations` has `enabled=true` AND a non-empty `external_destination_id`) | Velox: `DataServer/internal/store/store_deliveries.go::BatchDeliveryDestinationsStatus` (NEW 3-state handler pre-flight, replaces the collapsed 2-state `BatchDeliveryDestinationsExistAndEnabled`) AND `DataServer/internal/store/delivery_plan_validator.go::validateDeliveryDestinationTx` (in-tx atomic creator gate, called from `DataServer/internal/store/atomic_job_task.go::insertDeliveryPlanTx`; wraps `ErrDestinationDisabled` typed sentinel for `errors.Is`) | §0.3.4 item 4 split — **two distinct surfaces** so operator dashboards can disambiguate: (i) Velox-side enqueue-time `details[].target_error_code=BLOCKED_VELOX_DISABLED` (row exists but `enabled=0` on Velox); (ii) catalog-side top-level `error.code=BLOCKED_NO_PUBLISHABLE_CHANNEL` (InstaeditLogin catalog yielded ≥1 entry but zero satisfy `can_post=true AND capabilities.upload_video=true`). Canonical code constants live at `DataServer/internal/handlers/server/pipeline/publishing_error_codes.go`. |

`checkAccountEligibility` (defined at
`InstaeditLogin/internal/deliveries/target_resolver.go:615`) is the
SINGLE canonical gate shared by the SavedDestination,
DirectTarget, and `ListWorkspaceTargets` paths in the catalog
resolver (line 286 + 433, and `target_catalog.go:103`). Operators
MUST NOT re-derive condition 1/2/3 in the sender; trust the
`can_post` boolean surfaced by `/publishing/targets`.

**Condition 4 is enforced by Velox itself** at enqueue time
through the canonical `validateDeliveryDestinationTx` inside the
atomic creator's INSERT transaction. A `delivery_destinations`
row whose `enabled` flips to `false` between catalog discovery and
job submission causes Velox to reject the enqueue with
`destination_id %q is globally disabled`, NOT to silently dispatch.

Conditions 1-3 are also surfaced through the per-row
`target_error_code` taxonomy (see
`InstaeditLogin/internal/deliveries/target_resolver.go:184-188`):
`TARGET_NOT_AVAILABLE` and `BLOCKED_AUTH` are the only two
catalog-sourced error codes a sender should match on. The other
codes the resolver emits (`ACCOUNT_INACTIVE`, `DEST_DISABLED`,
etc.) were speculation in earlier drafts of this section and have
been REMOVED in favor of the canonical taxonomy.

### 0.2.1 Diagnostic SQL — verify all 4 conditions on the master

```sql
-- Condition 4 check: every destination linked to an InstaEdit provider
-- must have enabled=1 AND a non-empty external_destination_id.
SELECT destination_id,
       provider,
       external_destination_id,
       enabled,
       json_extract(configuration_json, '$.residuo4_closed_at') AS r4_marker
FROM delivery_destinations
WHERE provider IN ('instaedit_social_gateway', 'social_gateway')
  AND (
    enabled != 1
    OR external_destination_id IS NULL
    OR TRIM(external_destination_id) = ''
  );
```

**Expected post-healthy-install envelope:** empty result set. A
non-zero count indicates a row whose condition-4 prerequisite
failed — these channels will surface as `can_post=false` in
`/publishing/targets` until an operator flips `enabled=1` (or
re-syncs the catalog via `POST /api/v1/admin/destinations/sync`).

Conditions 1/2/3 are checked from the InstaEdit side via:

```bash
curl -fsS \
  -H "Authorization: Bearer ${INSTAEDIT_ADMIN_TOKEN}" \
  "${INSTAEDIT_BASE_URL}/api/v1/internal/workspaces/<workspace_id>/channels/publishable"
```

The InstaEdit response must report `count == expected_count` and
no entry with `block_reason` set.

### 0.2.2 Failure triage cheat-sheet

The canonical catalog-sourced error codes (declared at
`InstaeditLogin/internal/deliveries/target_resolver.go:184-188`)
are exactly **two**:

* `TARGET_NOT_AVAILABLE` — the eligibility gate rejected the row
  (covers conditions 1, 2, and 3 collapsed; the resolver does NOT
  emit a distinct status enum per condition).
* `BLOCKED_AUTH` — emitted specifically when condition 3
  (OAuth reauth_required dual-signal) is the failing one; the row
  also carries the `status="reauth_required"` enum value on the
  underlying `platform_accounts` row (see
  `InstaeditLogin/internal/models/user.go:51`).

The triage cheat-sheet for a sender or operator is therefore:

| Catalog verdict | Failing condition(s) | Operator action |
|---|---|---|
| `target_error_code="BLOCKED_AUTH"` (row also has `status="reauth_required"`) | 3 (OAuth) | Re-consent the OAuth flow; the catalog entry re-flips to `active` once the new refresh-token is healthy |
| `target_error_code="TARGET_NOT_AVAILABLE"` AND the underlying `platform_accounts.status` is one of `paused` / `revoked` / `disconnected` / `error` / `expired` / `pending_authorization` | 2 (account inactive) | Re-activate the platform account or delete the channel entry |
| `target_error_code="TARGET_NOT_AVAILABLE"` AND the underlying workspace binding row is soft-disabled | 1 (binding) | Enable the workspace binding in the InstaEdit admin console; re-sync catalog |
| Row missing from `targets[]` entirely | 1+2+3 all failed simultaneously OR the channel was deleted upstream | Remove from sender allow-list |
| Velox enqueue-rejected with `details[].target_error_code=BLOCKED_VELOX_DISABLED` (Velox-side `delivery_destinations.enabled=false` for the producer-selected `destination_id`) | 4 (Velox-side, enqueue-time) — see §0.3.4 NIT-2 split | `UPDATE delivery_destinations SET enabled = 1 WHERE destination_id = '...'` OR re-sync the catalog via `POST /api/v1/admin/destinations/sync`. **Distinct** from the catalog-side row above: this case means the producer picked a `destination_id` Velox still had enabled, but it flipped to `enabled=0` between catalog discovery and job submission. Canonical code constant: `DataServer/internal/handlers/server/pipeline/publishing_error_codes.go::BlockedCodeVeloxDisabled`. |
| Velox `POST /api/v1/publishing/targets` returns top-level `error.code=BLOCKED_NO_PUBLISHABLE_CHANNEL` (catalog yielded ≥1 row but zero satisfy `can_post=true AND capabilities.upload_video=true`) | 4 (catalog-side, discovery-time) — see §0.3.4 NIT-2 split | Inspect the per-row `target_error_code` on each element of `targets[]` (still populated — the producer gets the per-row diagnostic AND the top-level summary) and act per the §0.2 chart above. `block_reason` on the top-level error lists workspace/platform so dashboards can group alerts. Canonical code constant: `DataServer/internal/handlers/server/pipeline/publishing_error_codes.go::BlockedCodeNoPublishableChannel`. |

**Note (drift pinned by the §0.2 commit):** the previous draft of
this triage table listed `binding_disabled` and `account_inactive`
as catalog status strings. Those strings do not exist in the
canonical `target_resolver.go` taxonomy; they were speculation
from the §0.2 commit (`422e5c1`) and have been REMOVED. The
resolver surfaces ONE binary `target_error_code` enum
(`TARGET_NOT_AVAILABLE` OR `BLOCKED_AUTH`) and the underlying
`platform_accounts.status` enum (`active` vs `reauth_required`
being the two values the resolver actively maps onto).

---

## 0.3 Sender-side `destination_id` selection criteria (procedure 0c)

A trusted sender (e.g. `PipelineGen` or any `creatorflow` caller)
calls `POST /api/v1/publishing/targets` and selects **exactly one**
target from the response. The selection criteria are operator-enforced
in the sender's code; Velox cannot reject an enqueue that picks a
non-publishable target because the enqueue path is downstream of
selection. Operators MUST encode the §0.2 conditions in the
sender-side predicate.

### 0.3.1 Canonical selection predicate

```text
pick the FIRST target in the response array that satisfies:

  can_post == true                                       (boolean AND)
  AND destination_id is non-empty AND not null            (string presence)
  AND capabilities.upload_video == true                   (capability bit)

Rejects: targets with can_post=false (any of conditions 1-4 failed in §0.2);
         targets with empty/null destination_id (catalog shape drift);
         targets with capabilities.upload_video=false (platform lacks upload scope).
```

Field roles:

* `destination_id` (string) — the Velox-side opaque ID. This is the
  ONLY field the sender copies into `delivery_plan[0].destination_id`
  on the subsequent `POST /api/v1/jobs`. Display-only fields MUST
  NOT be propagated.
* `external_destination_id` (string) — the InstaEdit-side opaque ID.
  Display-only / audit-trail; never propagated to the job payload.
* `channel_id` / `channel_name` (strings) — display-only.
  **Routing uses the opaque IDs only**, so a display-name change
  upstream never breaks dispatch.
* `platform` / `platform_account_id` — the human-friendly hint. The
  sender MAY log these for audit, but MUST NOT include them in
  `delivery_plan[].metadata` (the §0.3.2 metadata hygiene rule).

### 0.3.2 Metadata hygiene — what NOT to copy into `delivery_plan[].metadata`

Senders MUST NOT mirror the following fields into
`delivery_plan[].metadata`:

| Forbidden in metadata | Why |
|---|---|
| `platform` | Opaque-mode contract (see `socialclient/requests.go` § wire-shape); repeated platforms create a side-channel that bypasses the canonical opaque wire. |
| `account_id` / `channel_id` / `language` | Same — Velox's resolver never reads them; the external Social API owns these resolutions. |
| `destination_id` (echo) | The destination is selected via `delivery_plan[0].destination_id`, NOT via metadata. Echoing it produces a confusing audit trail. |

`delivery_plan[].metadata` is for **per-delivery editorial intent**
(title, description, tags, privacy_status, final_privacy,
require_thumbnail, publish_at). See
`docs/publishing-job-payload.md` §2 for the canonical metadata
contract.

### 0.3.3 Smoke verification

The cross-repo smoke `scripts/e2e/publishing_flow_smoke.sh`
embeds the §0.3.1 predicate as a `jq` filter on the
`/publishing/targets` response. Operators adapting a new sender
SHOULD lift the predicate from the smoke unchanged rather than
re-deriving it. The smoke exits with code `5` if no target
satisfies the predicate — the same signal a sender should raise
when no publishable channel exists for the workspace.

### 0.3.4 Failure mode: zero targets satisfy §0.3.1

**NIT-2 split (this commit):** the zero-match catalog case and the
Velox-side enqueue-rejected case are emitted as TWO DISTINCT
canonical error envelopes so operator dashboards can disambiguate
the remediation:

| Surface | Code | Trigger | Owner |
|---|---|---|---|
| Top-level `error` on `POST /api/v1/publishing/targets` response | `BLOCKED_NO_PUBLISHABLE_CHANNEL` | InstaeditLogin catalog yielded ≥1 row but zero satisfy `can_post=true AND capabilities.upload_video=true` | Velox (publisher has no usable candidate to surface) |
| `details[].target_error_code` on `POST /api/v1/jobs` 422 response | `BLOCKED_VELOX_DISABLED` | Producer picked a `destination_id` that exists in `delivery_destinations` but `enabled=0` (catalog was stale at enqueue-time) | Velox (state-of-the-world flipped between catalog and enqueue) |
| `details[].target_error_code` on `POST /api/v1/jobs` 422 response | `DESTINATION_NOT_FOUND` | Producer picked (or fabricated) a `destination_id` never in `delivery_destinations` | sender (stale-id bug — must re-pick from `/publishing/targets`) |

Canonical code constants are defined at
`DataServer/internal/handlers/server/pipeline/publishing_error_codes.go`.
The CI gate `ci-opaque-wire.yml` already enforces the wire-shape;
the §0.3.4 split adds operator-triage granularity on top.

If `POST /api/v1/publishing/targets` returns `targets[]` where no
entry satisfies the §0.3.1 predicate, the sender MUST:

1. Log the full `targets[]` response at `WARN` (after redacting
   `channel_id` if it carries PII under the workspace's data policy).
2. If the response carries a top-level `error.code` (the
   NIT-2 `BLOCKED_NO_PUBLISHABLE_CHANNEL` verdict), surface it
   verbatim to the operator console — do not collapse it into a
   generic "no channels" message because the distinct code is the
   triage key the dashboard group-by relies on.
3. Surface the **canonical catalog verdict** in addition (per-row
   `target_error_code`), drawn from the resolver taxonomy at
   `InstaeditLogin/internal/deliveries/target_resolver.go:184-188`:
   `target_error_code="BLOCKED_AUTH"` (OAuth reauth) or
   `target_error_code="TARGET_NOT_AVAILABLE"` (conditions 1+2 collapsed);
   complemented by the underlying `platform_accounts.status` enum
   value (see `InstaeditLogin/internal/models/user.go:49-72` for
   the canonical declaration): `active`, `reauth_required`,
   `revoked`, `disconnected`, `expired`, `error`,
   `pending_authorization`, `suspended`. The resolver itself only
   branches on `active` vs `reauth_required` (the dual-signal gate
   at `target_resolver.go:618-628`); the other 6 enum values are
   forwarded to the operator console for diagnostic granularity.
   The non-canonical strings `binding_disabled` / `account_inactive`
   were drift in earlier drafts of §0.3.4 and have been REMOVED
   (see §0.2.2 for the round-2 alignment rationale).
4. **Refuse to invent a default channel.** The system MUST NOT
   pick the first row, a similar-named row, or any account from a
   different workspace. Routing via opaque IDs makes silent
   selection catastrophic — a typo in `delivery_plan[0].destination_id`
   dispatches to the wrong account.
5. For condition 4 Velox-side enqueue-rejection
   (`target_error_code=BLOCKED_VELOX_DISABLED`), the canonical
   operator signal is the runner error envelope
   `destination_id %q is globally disabled` (see §0.2 and
   `DataServer/internal/store/delivery_plan_validator.go::validateDeliveryDestinationTx`,
   which wraps the typed sentinel `ErrDestinationDisabled` via `%w`
   to enable `errors.Is` mapping). The DISTINCT
   `DESTINATION_NOT_FOUND` code covers the unknown-id case.

The error envelope a sender surfaces on the §0.3.1 zero-match
case is `BLOCKED_NO_PUBLISHABLE_CHANNEL` with the
`block_reason="no target with can_post=true AND capabilities.upload_video=true"`
(top-level on the catalog response). The DISTINCT
`BLOCKED_VELOX_DISABLED` envelope is surfaced on the enqueue
422 response when the producer picks a destination whose Velox
side row flipped to `enabled=0`. Senders MUST NOT collapse these
two codes into a single field; they are decoupled by design.

---


---

Procedures 1–3 (back-fill, configuration migration, post-deploy audit), the
quick reference, cross-references, operator gotchas, and the update protocol
continue in [SOCIAL_API_MIGRATION_RUNBOOK_PART2.md](SOCIAL_API_MIGRATION_RUNBOOK_PART2.md).
