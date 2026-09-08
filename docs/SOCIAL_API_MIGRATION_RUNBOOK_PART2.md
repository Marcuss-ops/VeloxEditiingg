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
typed structs** as of Residuo 5 closure — see the Cutover block
below for the current operator-facing contract. Historical
read-back-compat mirror logic and the
`dest.SocialDestinationID = dest.ExternalDestinationID` line are
no longer used in canonical code paths; the closing commit for
the Residuo 5 follow-up is the only stable reference for now
(track via `git log --grep 'Residuo 5'` until §5.5 promotes it to
a PR anchor).

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
PR-15.12 / PR-15.13 / PR-15.14 / PR-15.16).Operator-facing reintroduction of the alias field in
`docs/pipeline.md` is gated by
`tests/e2e/recovery-matrix/scenarios/19-pipeline-md-stale-field-grep.sh`
(4 invariants: `external_destination_id` presence, PR-15.13 +
PR-15.14 cross-references, zero `\| \`channel_id\` \|` literal
cells, zero `parsePlatformAndAccount` references). The same
gate pattern is the planned shape for a runbook-specific
scenario 20 [out of scope for this commit].

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


---

The post-deploy audit procedure continues in
[SOCIAL_API_MIGRATION_RUNBOOK_PART3.md](SOCIAL_API_MIGRATION_RUNBOOK_PART3.md);
the quick reference, cross-references, operator gotchas, and update protocol
live there too.
