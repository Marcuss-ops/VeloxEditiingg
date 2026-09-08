# Velox → Social API Migration Runbook — Part 3 (audit + reference)

> Continuation of [part 2](SOCIAL_API_MIGRATION_RUNBOOK_PART2.md).

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
| 3 | `NOT_VELOX_SCHEMA` — DB lacks canonical Velox tables (`jobs`, `artifacts`, `job_deliveries`, `calendar_events`) | This is not a Velox DB; abort |
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
| Order of `SOCIAL_API_*` env vars on a fresh master | §0.1 |
| Verify all 4 channel-prerequisite conditions are healthy | §0.2 |
| Pick a `destination_id` in the sender (selection criteria) | §0.3 |
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

The historical PR-15.x anchor list and follow-up commit record are maintained
in the dedicated [Social API migration historical record](history/SOCIAL_API_MIGRATION_HISTORY.md#5-5-changelog-anchors).
The current procedures above and the operator gotchas below remain in this
runbook; update the historical record when a new migration closure lands.

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
