-- Migration 093: Residuo 4 closure marker (forward-only no-op on operator-controlled data)
--
-- Records an audit marker on every row of `delivery_destinations` whose
-- `configuration_json` is well-formed JSON, expressing that the Residuo 4
-- canonical-rename chain closed:
--
--   * 091_opaque_destination.sql         (DROPs account_id/channel_id/language; ADDs social_destination_id)
--   * 092_rename_social_to_external_destination_id.sql  (ADDs external_destination_id; UPDATEs; DROPs social_destination_id)
--   * 4 atomic code commits on main (Residuo 4 step 1/2/3 + docs+CHANGELOG):
--     ea38837  refactor(store)                          rename social_destination_id -> external_destination_id
--     03acccb  refactor(validator+runner)               canonical-store + alias-mirror
--     83d8b2f  refactor(socialclient+provider)          wire + provider canonical
--     01810ea  docs(changelog+api_script)               PR-15.14 entry + JSON example
--
-- The marker is a sub-key `$.residuo4_closed_at` inserted into
-- `configuration_json` via idempotent json_insert. Operators can audit
-- the closure with:
--
--   SELECT count(*) AS marked
--   FROM delivery_destinations
--   WHERE json_extract(configuration_json, '$.residuo4_closed_at') IS NOT NULL;
--
-- Forward-only (no DOWN). Idempotent: rows whose configuration_json
-- already carries `$.residuo4_closed_at` are skipped, so re-running
-- the migration on an already-applied database is a true no-op.
--
-- Safety:
--   * json_insert is non-destructive to existing keys in the JSON blob.
--   * WHERE clause filters out NULL/empty/non-JSON-valid
--     configuration_json rows, so migration 093 never overwrites
--     operator-controlled keys on a malformed-JSON row.
--   * SQLite 3.38+ (bundled via go-sqlite3 v1.14.15+, the Velox
--     baseline since migration 091) exposes json_valid() for the
--     malformed-row filter.
--
-- Squatting protection: the marker key is namespaced
-- `residuo4_closed_at` and the value is a timestamp ISO-8601 string,
-- so a future contributor cannot accidentally collide with it
-- without observing the JSON-insert pattern below.

UPDATE delivery_destinations
SET configuration_json = json_insert(
    configuration_json,
    '$.residuo4_closed_at',
    json_quote(CURRENT_TIMESTAMP)
)
WHERE configuration_json IS NOT NULL
  AND TRIM(configuration_json) != ''
  AND json_valid(configuration_json) = 1
  AND json_extract(configuration_json, '$.residuo4_closed_at') IS NULL;
