-- Migration 092: rename social_destination_id → external_destination_id
-- (Residuo 4 of YouTube→Social closure, completing the canonical-rename
-- intent committed in PR-15.12 residue section "Refs").
--
-- The opaque-mode identifier forwarded to the social_repo is renamed to
-- a platform-neutral name. The Social-side row remains opaque to
-- Velox; ONLY the column name / typed-field name changes. After this
-- migration the column wire-key in the JSON is `external_destination_id`
-- (canonical) and the legacy `social_destination_id` key disappears
-- from the operator-facing payload schema.
--
-- ABI strategy: the typed struct DeliveryDestination (in
-- `store/store_deliveries.go`) carries BOTH:
--   * `ExternalDestinationID` — canonical, `json:"external_destination_id,omitempty"`
--   * `SocialDestinationID`   — deprecated alias, `json:"-"` until
--     Residuo 5 (final-old-name-drop follow-up) removes it entirely.
-- SQL readers populate both fields from the new column; the alias
-- lets consumers that have NOT yet migrated (validator + delivery_runner
-- + socialclient + provider) continue to compile without changes
-- until their respective commits land.
--
-- DB migration strategy: NOT `ALTER TABLE ... RENAME COLUMN`
-- (banned by `scripts/ci/check-migrations.sh` for portability). We
-- use the same ADD / UPDATE / DROP COLUMN pattern as 091 opaque-mode
-- migration: ADD canonical column, COPY old → new, DROP legacy column.
-- Three steps, all SQLite-supported and forward-only.
--
-- SQLite >= 3.35.0 supports ADD/DROP COLUMN (bundled via
-- go-sqlite3 v1.14.15+, which Velox already uses — see migration 091).
--
-- Forward-only (no DOWN). Existing rows: their `social_destination_id`
-- value is migrated verbatim into the new `external_destination_id`
-- column via UPDATE SET. No data loss.

-- ============================================================
-- Step 1: ADD the canonical RenameTarget column.
-- TEXT, no NOT NULL, no DEFAULT — operators must populate it.
-- For the duration of the migration it is NULL, then UPDATEd, then
-- the legacy column is dropped.
-- ============================================================

ALTER TABLE delivery_destinations
    ADD COLUMN external_destination_id TEXT;

-- ============================================================
-- Step 2: COPY data from the legacy column into the canonical one.
-- COALESCE handles the (rare) NULL rows that pre-existed the opaque
-- mode addition (migration 091 was forward-only, also nullable).
-- ============================================================

UPDATE delivery_destinations
SET external_destination_id = COALESCE(social_destination_id, '');

-- ============================================================
-- Step 3: DROP the legacy column. Order inconsequential (no FK,
-- no index covers it — see migration 022: only `idx_destinations_provider`
-- and `idx_destinations_enabled` exist on this table).
-- ============================================================

ALTER TABLE delivery_destinations DROP COLUMN social_destination_id;
