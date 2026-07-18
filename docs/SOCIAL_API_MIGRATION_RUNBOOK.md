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
| `090_drop_youtube_domain.sql` (sqlite) / `010_drop_youtube_domain.sql` (postgres) | DROPs all 10 `youtube_*` tables + the 3 historical `youtube_*` columns on `calendar_events` / `dark_editor_folders` | YES |
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

## 1. Back-filling `external_destination_id` for legacy rows (procedure a)

### 1.1 Prerequisites

* Write access to the Velox SQLite database file (typically
  `/var/lib/velox/data/velox.db` on production).
* Read-only access to the external Social API repository's
  destination-mapping table to look up the opaque IDs.
* `sqlite3` CLI ≥ 3.35 installed (for ALTER + `json_extract`
  reliability).

### 1.2 Triage unmapped rows

Run:

```sql
SELECT destination_id,
       provider,
       name,
       length(configuration_json) AS cfg_bytes,
       json_extract(configuration_json, '$.residuo4_closed_at') AS r4_marker
FROM delivery_destinations
WHERE external_destination_id IS NULL
   OR TRIM(external_destination_id) = '';
```

**Expected output envelope:**

* On a fresh DB post-`093`, the result set is EMPTY. Every row that
  had a `social_destination_id` pre-091 was migrated verbatim by
  Migration 092's `UPDATE SET` clause. Empty result = healthy.
* On a legacy DB from before Migration 091 + 092, the result set is
  empty only if a pre-closure export → re-import cycle was
  deliberately executed.
* On a legacy DB without backfill, the result set lists every row
  whose platform-shaped state was lost when `account_id` /
  `channel_id` / `language` were dropped by Migration 091. **Every
  row in this set is at risk.**

### 1.3 Resolve opacity

For each row, look up the canonical mapping in the external Social
API. The mapping key is
`(legacy_provider, legacy_account_id, legacy_channel_id)` → `external_destination_id`.
If the external repository does not have a mapping, the destination
is **gone permanently** — Migration 091 was forward-only by design
(see CHANGELOG PR-15.12 § "Residuo 2 closure: opaque-mode
Destination model": "Velox no longer recognises any of those three
columns; their absence is a feature, not a bug.").

> **Caveat — STRANDED ROWS.** A row whose mapping was never
> established in the Social API cannot be back-filled from inside
> Velox. Operators must coordinate with the Social API repo
> maintainers to either (a) seed the mapping there first, then run
> §1.4, or (b) prune the row via `DELETE` if business policy permits.

### 1.4 Back-fill a row

For each unmapped row, execute a single-row UPDATE inside an
explicit transaction so a mis-typed operator entry can be rolled
back without side-effects:

```sql
BEGIN IMMEDIATE;

UPDATE delivery_destinations
SET external_destination_id = '<external-destination-id-from-social-repo>'
WHERE destination_id = '<row-destination_id-from-§1.2>';

-- Verify the change before COMMIT: the row MUST report a
-- non-empty external_destination_id after the UPDATE.
SELECT destination_id,
       TRIM(external_destination_id) AS new_external_destination_id
FROM delivery_destinations
WHERE destination_id = '<row-destination_id-from-§1.2>';

COMMIT;
```

`BEGIN IMMEDIATE` acquires a reserved lock so concurrent
`runner.hydrateDestination` ticks cannot observe a half-applied row
state (the runner reads `external_destination_id` directly).

### 1.5 Verify clean

Re-run the §1.2 detection query. Expected: empty result set.

Also re-run the §3.4 `last_error_code` metric check. Expected: zero
NEW `DESTINATION_UNMAPPED` rows since the §1.4 update window.

### 1.6 Rollback

**None possible** for failed back-fills — Migration 091 was
forward-only. A bad `external_destination_id` value can only be
corrected by another `UPDATE SET` overwriting it (no schema
rollback path exists). The deprecated `SocialDestinationID` struct field alias
in `store/store_deliveries.go` has been **removed entirely from
typed structs** as of Residuo 5 closure (commit `348084a`) — see
the Cutover block below for the current operator-facing contract.
Historical read-back-compat mirror logic and the
`dest.SocialDestinationID = dest.ExternalDestinationID` line are
no longer present in the canonical source tree.

#### Cutover — alias window for `social_destination_id` is closed

As of the universal deployment of Migration 092
(`092_rename_social_to_external_destination_id.sql`), the alias
window for the pre-Migration-092 column name
`social_destination_id` is closed. Operators running
pre-Migration-092 configs MUST upgrade before cutting traffic to
the post-Residuo-4 Velox runtime.

Upgrade procedure for operators on a pre-092 DB:

  1. Apply Migration 092 via the standard Velox migration runner
     (idempotent `UPDATE SET external_destination_id =
     COALESCE(social_destination_id, '')` clause then DROPs
     `social_destination_id`). The runner is SHA-256-checksum-pinned
     on file content; tampering with the historical `.sql` triggers
     checksum-integrity failure on next boot.
  2. Verify the cutover took effect:
       - §3.2 closure-marker pass (`$.residuo4_closed_at` count vs.
         `total_rows` must be 100% on every row whose JSON is
         well-formed).
       - §3.4 `DESTINATION_UNMAPPED` rate trending to zero day-over-
         day once the §1 back-fill cycle is complete.
  3. Post-cutover invariant: the deprecated `SocialDestinationID`
     struct field alias has been removed entirely from typed structs
     (Residuo 5 closure, commit `348084a`). Operators MUST NOT
     reintroduce the alias field — the runner no longer reads it,
     and reintroducing it can only mask schema drift.

Any new code or migration referencing `social_destination_id` MUST
be redirected to `external_destination_id` per §1.4 + §3.5. The
only mentions of `social_destination_id` in the on-disk artifacts
after cutover are checksum-pinned SQL files (migration 092) and
historical CHANGELOG anchors from the closure chain (PR-15.11 /
PR-15.12 / PR-15.13 / PR-15.14 / PR-15.16). Operator-facing
reintroduction of the alias is hard-prevented by the structural-
drift gate `tests/e2e/recovery-matrix/scenarios/19-pipeline-md-stale-field-grep.sh`
(extends to the runbook via the grep-family pattern on
`parsePlatformAndAccount`).

---

## 2. Migrating legacy `configuration_json` (procedure b)

### 2.1 Why this section exists

Pre-Migration 091 schema (see `sqlite/022_split_deliveries.sql`
line 78) created `delivery_destinations.configuration_json` as a
flexible JSON blob. Operators populated it with platform-shaped
intent such as:

```json
{
  "platform": "youtube",
  "account_id": "act_legacy_xxx",
  "channel_id": "UC_legacy_yyy",
  "language": "en"
}
```

Post-closure (Migration 091 onwards), Velox **does not read those
keys** from `configuration_json`. They are still physically present
in the column but are opaque to Velox. Platform-shaped intent now
lives in **job-level `metadata`** at delivery-request time (the
`metadata map[string]any` field on
`socialclient.DeliverArtifactRequest`, verified at
`DataServer/internal/socialclient/requests.go` and pinned by the
negative-pinning test
`TestClient_DeliverArtifact_WireShape_LegacyKeysNeverPresent`).

The external Social API resolves `external_destination_id` into the
authoritative (platform, account, channel, credentials) mapping.
Velox does not need to repeat that mapping on the destination row.

### 2.2 Inventory legacy shapes

Survey what legacy platform-shaped data is currently sitting in
`configuration_json`:

```sql
SELECT destination_id,
       provider,
       external_destination_id,
       json_extract(configuration_json, '$.platform')   AS legacy_platform,
       json_extract(configuration_json, '$.account_id') AS legacy_account_id,
       json_extract(configuration_json, '$.channel_id') AS legacy_channel_id,
       json_extract(configuration_json, '$.language')   AS legacy_language,
       json_extract(configuration_json, '$.residuo4_closed_at') AS r4_marker
FROM delivery_destinations
WHERE json_valid(configuration_json) = 1
  AND (
        json_extract(configuration_json, '$.platform')   IS NOT NULL
     OR json_extract(configuration_json, '$.account_id') IS NOT NULL
     OR json_extract(configuration_json, '$.channel_id') IS NOT NULL
     OR json_extract(configuration_json, '$.language')   IS NOT NULL
  );
```

A row in this result set is a **legacy pending row**. Whether to
scrub (§2.3) or preserve (§2.4) is an operator decision per the
table below.

| Signal | Action |
|---|---|
| External Social API has full `(legacy_platform, legacy_account_id, legacy_channel_id) → external_destination_id` mapping seeded and confirmed authoritative | **SCRUB** (§2.3) |
| External Social API is still resolving against `external_destination_id → legacy-account` as a transition lookup (dual-write window) | **PRESERVE** (§2.4) |
| SOC2 / regulatory policy requires the pre-091 state preserved for forensics | **PRESERVE** (§2.4) |
| `$.residuo4_closed_at` is set (Migration 093 has run) AND `$.platform`, `$.account_id` are present | Migration 093 marker is set without scrubbing: §2.3 NORMALLY recommended |

### 2.3 Scrub legacy sub-keys (decision: clean-up)

The clean-up path uses SQLite's `json_remove`, which is
non-destructive to sub-keys other than those listed. SQLite ≥ 3.38
exposes `json_remove` reliably. Velox's `go-sqlite3 v1.14.15+`
baseline ships ≥ 3.38.

```sql
BEGIN IMMEDIATE;

UPDATE delivery_destinations
SET configuration_json = json_remove(
      configuration_json,
      '$.platform',
      '$.account_id',
      '$.channel_id',
      '$.language'
)
WHERE json_valid(configuration_json) = 1
  AND (
        json_extract(configuration_json, '$.platform')   IS NOT NULL
     OR json_extract(configuration_json, '$.account_id') IS NOT NULL
     OR json_extract(configuration_json, '$.channel_id') IS NOT NULL
     OR json_extract(configuration_json, '$.language')   IS NOT NULL
  );

COMMIT;
```

`json_remove` does NOT touch sub-keys other than those listed
(e.g. an operator-managed `$.privacy` or `$.tags` survives).
Re-running §2.2 after this UPDATE must yield an empty result set.

### 2.4 Preserve legacy sub-keys (decision: do not scrub)

When the external Social API lookup is still in transition
(dual-write window) OR when regulatory policy mandates retaining
the pre-091 state, leave the legacy keys in place. The keys
**remain visible** in `configuration_json` but Velox does not
interpret them. The opaque-mode fail-closed contract
(`runner.go:499-500`) is unaffected because dispatch resolution
relies only on the canonical `external_destination_id` column.

**Audit-trail consideration.** When preserving, the
`$.residuo4_closed_at` marker (Migration 093) coexists with the
legacy sub-keys. This is intentional: Migration 093 records the
canonical-rename closure even when platform-shaped sub-keys remain
in the blob for backwards compatibility. To inspect, run §2.2 and
filter for `r4_marker IS NOT NULL AND legacy_platform IS NOT NULL`.

### 2.5 Verify clean

Re-run §2.2 after §2.3. Expected: empty result set on a fully
scrubbed install, or only rows that intentionally preserved per §2.4.

Also confirm:

```sql
-- Rows with the closure marker set:
SELECT count(*) AS r4_marker_rows
FROM delivery_destinations
WHERE json_extract(configuration_json, '$.residuo4_closed_at') IS NOT NULL;

-- Compare against total row count:
SELECT count(*) AS total_rows
FROM delivery_destinations;
```

Expected: `r4_marker_rows == total_rows` on a clean install
(Migration 093 is idempotent; rows never lose the marker once
applied).

---

## 3. Post-deploy audit procedure (procedure c)

The audit procedure composes four checks: a read-only DB residue
probe, a closure-marker pass, a wire-shape dry-run, and a
dispatch-time error-code metric check.

### 3.1 Check 1 — read-only DB residue probe

Run the canonical YouTube-residue audit script on the live DB:

```bash
./deploy/scripts/audit-no-youtube-residuals.sh /var/lib/velox/data/velox.db
```

**Exit codes** (canonical, mirroring the script header comments):

| Exit | Meaning | Operator action |
|---|---|---|
| 0 | `CLEAN` — no `youtube_*` tables or historical `youtube_*` columns remain | None; §3.2 |
| 1 | `RESIDUAL_FOUND` — see reported lines; remediation hint printed at the bottom of stdout | Re-run Velox so Migration 090 re-applies on next boot; reload pod |
| 2 | `DB_NOT_FOUND` — path missing, unreadable, or empty | Verify `VELOX_DATA_DIR` env mounting; verify `velox.db` file is on disk |
| 3 | `NOT_VELOX_SCHEMA` — DB lacks canonical Velox tables (`jobs`, `artifacts`, `job_deliveries`, `calendar_events`, `dark_editor_folders`) | This is not a Velox DB; abort |
| 4 | `ARGV_OR_TOOL` — `sqlite3` CLI missing from PATH or wrong invocation | Install `sqlite3 ≥ 3.16`; verify arg count |

If exit code is non-zero, fix the underlying issue before §3.2.

### 3.2 Check 2 — closure-marker pass

```sql
SELECT
  (SELECT count(*) FROM delivery_destinations)                                AS total_rows,
  (SELECT count(*) FROM delivery_destinations
   WHERE json_extract(configuration_json, '$.residuo4_closed_at') IS NOT NULL
     AND json_valid(configuration_json) = 1)                                  AS r4_marker_rows,
  (SELECT count(*) FROM delivery_destinations WHERE json_valid(configuration_json) = 0) AS malformed_json_rows;
```

**Expected envelope:**

* `r4_marker_rows == total_rows` on a clean install (Migration 093
  is idempotent).
* `malformed_json_rows == 0`. A non-zero value indicates an operator
  write that bypassed JSON validation; the CI gate
  `ci-opaque-wire.yml` does not catch malformed JSON, so this is a
  manual probe.

### 3.3 Check 3 — wire-shape dry-run

The CI workflow `.github/workflows/ci-opaque-wire.yml` is the
authoritative gate. Local dry-run with the exact same regex + the
exact same carve-outs:

```bash
$matches=$(git grep -nE '^[[:space:]]+(Platform|AccountID|ChannelID)[[:space:]]+[A-Za-z*\[]' -- \
    DataServer/internal/socialclient/ \
    ':!.github/workflows/ci-opaque-wire.yml' \
    ':!**/*_test.go' \
    ':!**/testdata/**' \
    ':!**/migrations/**' \
    ':!**/*.md' \
    ':!CHANGELOG.md' \
    ':!docs/**' \
   || true)

if [[ -n "$matches" ]]; then
  echo "FAIL — opaque-wire regression in socialclient/:"
  echo "$matches"
  exit 1
else
  echo "OK — opaque-wire clean."
fi
```

Expected: `OK — opaque-wire clean.` (0 matches). A non-empty
result means CI would fail; see the canonical replacement
(`socialclient.DeliverArtifactRequest.ExternalDestinationID`,
`json:"external_destination_id"` without `omitempty`) documented at
`DataServer/internal/socialclient/requests.go:36` and the
remediation hints in the workflow file header.

### 3.4 Check 4 — dispatch-time destination_unmapped rate

The runner records every fail-closed dispatch into
`job_deliveries.last_error_code`, with the terminal timestamp on
`job_deliveries.completed_at` (see `runner.go:280-288` and the
`MarkDeliveryFailed` SQL UPDATE at
`store/store_deliveries_lease.go:341-356` which sets
`status='FAILED'`, `last_error_code`, `last_error_message`,
`completed_at`). Query the rate of new `DESTINATION_UNMAPPED` rows
since the §1 back-fill window:

```sql
SELECT date(completed_at)             AS day,
       count(*)                         AS unmapped_count
FROM job_deliveries
WHERE last_error_code = 'DESTINATION_UNMAPPED'
  AND status = 'FAILED'
  AND completed_at >= '<TIMESTAMP-STARTING-OF-§1-WINDOW>'
GROUP BY day
ORDER BY day DESC;
```

> **Note (belt-and-suspenders):** the `status = 'FAILED'` filter is
> intentionally redundant with `last_error_code =
> 'DESTINATION_UNMAPPED'` — `MarkDeliveryFailed` at
> `store/store_deliveries_lease.go:341-356` SETs both columns in the
> same UPDATE. Removing the `status = 'FAILED'` clause would still
> match today's data; keeping it makes the audit robust against
> future PENDING-row edge cases (test fixtures that stamp a
> `last_error_code` before the terminal status).

**Expected post-healthy-install envelope:**

* `unmapped_count` trends to zero day-over-day once the §1 back-fill
  cycle is complete.
* A non-zero rate AFTER the window indicates either (a) new
  `delivery_destinations` rows are being created without
  `external_destination_id` populated, or (b) a regression
  regressed the `runner.go:499-500` fail-closed guard.

If (a): trigger an investigation into the destination-creation
caller (`delivery_plan_validator.go:203-205` enforces pre-flight
validation, but creation-side writes may still bypass it).
If (b): the regression test `TestRunnerHydrateDestination_UnmappedRouting_FailsClosed`
in `DataServer/internal/deliveries/runner_destination_unmapped_test.go`
would have caught this — investigate its CI history.

### 3.5 Compose the entire audit as one command

For SRE convenience, the four checks compose into a single bash
script that exits non-zero if any check fails:

```bash
set -uo pipefail

DB_PATH="${VELOX_DB_PATH:-/var/lib/velox/data/velox.db}"

echo "=== Check 1: youtube-residue audit on $DB_PATH ==="
./deploy/scripts/audit-no-youtube-residuals.sh "$DB_PATH" \
  || { echo "FAIL — see exit code above"; exit 1; }

echo
echo "=== Check 2: closure marker pass ==="
sqlite3 "$DB_PATH" <<'SQL'
SELECT 'total='        || (SELECT count(*) FROM delivery_destinations)
     || ' marked='     || (SELECT count(*) FROM delivery_destinations
                             WHERE json_extract(configuration_json, '$.residuo4_closed_at') IS NOT NULL
                               AND json_valid(configuration_json) = 1)
     || ' malformed='  || (SELECT count(*) FROM delivery_destinations WHERE json_valid(configuration_json) = 0);
SQL

echo
echo "=== Check 3: wire-shape dry-run ==="
matches=$(git grep -nE '^[[:space:]]+(Platform|AccountID|ChannelID)[[:space:]]+[A-Za-z*\[]' -- \
    DataServer/internal/socialclient/ \
    ':!.github/workflows/ci-opaque-wire.yml' \
    ':!**/*_test.go' \
    ':!**/testdata/**' \
    ':!**/migrations/**' \
    ':!**/*.md' \
    ':!CHANGELOG.md' \
    ':!docs/**' \
   || true)
if [[ -n "$matches" ]]; then
  echo "FAIL — opaque-wire regression:"
  echo "$matches"
  exit 1
fi
echo "OK — opaque-wire clean."

echo
echo "=== Check 4: dispatch-time DESTINATION_UNMAPPED rate ==="
sqlite3 -separator $'\t' "$DB_PATH" <<'SQL'
SELECT date(completed_at) AS day, count(*) AS unmapped_count
FROM job_deliveries
WHERE last_error_code = 'DESTINATION_UNMAPPED'
  AND status = 'FAILED'
GROUP BY day
ORDER BY day DESC
LIMIT 7;
SQL

echo
echo "=== ALL CHECKS PASS ==="
```

Pin this script into cron on every Velox host
(`/etc/cron.weekly/velox-social-api-audit.sh`) and pipe stdout to
the operator dashboard. The weekly cadence matches the drift
detector schedule baked into
`.github/workflows/ci-opaque-wire.yml` and
`.github/workflows/no-youtube-regression.yml`.

---

## 4. Quick reference

| Need | Action |
|---|---|
| Find unmapped `delivery_destinations` | §1.2 |
| Resolve a mapping against the external Social API | §1.3 |
| Back-fill one row | §1.4 |
| Inventory legacy sub-keys in `configuration_json` | §2.2 |
| Scrub legacy sub-keys | §2.3 |
| YouTube residue on a live DB | §3.1 (`audit-no-youtube-residuals.sh`) |
| Bridge marker presence | §3.2 |
| Wire-shape dry-run (CI gate) | §3.3 |
| `DESTINATION_UNMAPPED` rate | §3.4 |
| All four checks as one command | §3.5 |

---

## 5. Cross-references

### 5.1 Migrations (in application order)

* `DataServer/internal/store/migrations/sqlite/090_drop_youtube_domain.sql`
  — DROPs all `youtube_*` tables + 3 historical columns. Read-only.
* `DataServer/internal/store/migrations/sqlite/091_opaque_destination.sql`
  — DROPs `account_id` / `channel_id` / `language`; ADDs
  `social_destination_id`.
* `DataServer/internal/store/migrations/sqlite/092_rename_social_to_external_destination_id.sql`
  — ADDs `external_destination_id`; `UPDATE SET`; DROPs
  `social_destination_id`.
* `DataServer/internal/store/migrations/sqlite/093_residuo4_closure_marker.sql`
  — Idempotent `json_insert` of `$.residuo4_closed_at`.

### 5.2 Code

* Fail-closed dispatch guard: `runner.go:499-500`
* `DESTINATION_UNMAPPED` status-code mapping: `runner.go:280-288`
* Sentinel `ErrDestinationUnmapped`: `provider.go:51-62`
* Canonical column read: `store/store_deliveries.go::GetDeliveryDestination`
  (`ExternalDestinationID` mapped to `dest.ExternalDestinationID`,
  mirrored into deprecated `dest.SocialDestinationID` at line 180)
* Pre-flight validation: `delivery_plan_validator.go:203-205`
* Wire-shape contract: `DataServer/internal/socialclient/requests.go`
  (full type, including `ExternalDestinationID` field with NO
  `omitempty`)
* Wire-shape regression test (negative-pinning):
  `TestClient_DeliverArtifact_WireShape_LegacyKeysNeverPresent` in
  `DataServer/internal/socialclient/client_test.go`
* Fail-closed coverage gap test:
  `TestRunnerHydrateDestination_UnmappedRouting_FailsClosed` in
  `DataServer/internal/deliveries/runner_destination_unmapped_test.go`
  (commits `e4c5b58` + `39be2d0` — PR anchor pending CHANGELOG rebase)

### 5.3 CI gates

* `.github/workflows/no-youtube-regression.yml` (PR-15.11) —
  forbids re-introduction of direct YouTube-domain imports.
* `.github/workflows/ci-opaque-wire.yml` (commits `1927b8b` + `bf3b845` — PR anchor pending CHANGELOG rebase) — forbids
  re-introduction of top-level `Platform` / `AccountID` /
  `ChannelID` on `socialclient.DeliverArtifactRequest`.

### 5.4 Audit scripts

* `deploy/scripts/audit-no-youtube-residuals.sh` — read-only
  YouTube-residue DB probe. Exit codes 0/1/2/3/4 (CLEAN /
  RESIDUAL_FOUND / DB_NOT_FOUND / NOT_VELOX_SCHEMA / ARGV_OR_TOOL).

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

Operator follow-up (work landed on `main` but NOT yet anchored in
CHANGELOG.md — track via commit hash until the next CHANGELOG
rebase assigns a PR-N.NN anchor):

* **Fail-closed coverage gap test:**
  - `e4c5b58`  `test(deliveries): close Residuo 2 fail-closed coverage gap`
  - `39be2d0`  `test(deliveries): fix build break + panic in fakeProvider`
* **Opaque-wire CI guard (round-1, round-3 fix-ups):**
  - `1927b8b`  `ci(workflow): add opaque-wire Residuo 3 guard`
  - `bf3b845`  `ci(workflow): harden opaque-wire Residuo 3 guard`
* **Residuo 5 (alias removal `SOCIAL_GATEWAY_*`):** pending
  operator-side coordination. When landed, will surface as the
  next PR-N.NN anchor.

---

## 6. Operator gotchas

* **Forward-only invariant.** Migration 091 was forward-only by
  design. There is no `DOWN` migration. Operators cannot
  `rollback` the schema; only overwrite bad data via
  `UPDATE SET external_destination_id = '<correct-value>'`.
* **Hash checksums.** Migration files are checksum-pinned by the
  migrations runner. Do not edit a historical `.sql` file in place;
  any pre-091 + 092 deviation will trigger migration integrity
  failures on next boot.
* **Job-level vs destination-level metadata.** Platform-shaped keys
  in `configuration_json` are destination-level; the actual
  per-delivery platform-shaped intent lives in the delivery payload's
  `metadata` field (NOT on the destination row). If a requestor's
  metadata payload is missing required sub-keys, the external Social
  API may reject the delivery — that is correct behaviour and not a
  Velox issue.
* **`provider` column ≠ platform.** The `delivery_destinations.provider`
  column is Velox-internal ("social_gateway", "drive", etc.) and NEVER
  refers to a social platform. Operators must NOT confuse it with a
  social-platform identifier (e.g. "youtube", "tiktok").
* **`Migration 093` is idempotent but a single-shot signal.** Once
  a row's `$.residuo4_closed_at` is set, manual scrubbing (§2.3) is
  safe but re-running Migration 093 against the same DB is a no-op
  (the WHERE filter excludes already-marked rows). This is intentional:
  the marker is meant as a one-time audit signal, not a recurring
  annotation.

---

## 7. Runbook update protocol

When updating this runbook:

1. Cross-link every line-reference to a file that ships on `main`.
   The CI gate `ci-opaque-wire.yml` does not lint doc hyperlinks —
   this is a manual responsibility.
2. Update §5.5 (CHANGELOG anchors) when a new PR-n.X lands that
   changes the Schema, the runner, the socialclient, or any of the
   carve-out sets.
3. After every PR-merge touching §0–§3, run §3.5 locally on a
   fresh `velox-test.db` and confirm `=== ALL CHECKS PASS ===`.
4. Pin the runbook to a CHANGELOG entry (e.g.
   `docs: SOCIAL_API_MIGRATION_RUNBOOK.md — first emission
   `).
5. The runbook is read-only — operators do not edit migration
   files; they only run SQL / bash / diagnostic queries.
