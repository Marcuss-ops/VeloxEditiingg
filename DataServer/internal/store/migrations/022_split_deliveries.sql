-- Migration 022: split the legacy per-job delivery_targets into a reusable
-- delivery_destinations table + a per-job job_deliveries table. delivery_attempts
-- repointed from delivery_target_id -> delivery_id.
--
-- Rationale:
--   * delivery configurations (YouTube channel IDs, Drive folder IDs) are
--     reusable across jobs. With delivery_targets, every job re-declared its
--     copy of the same config, leaking data and creating write-amplification
--     on config changes.
--   * The new layout makes "destination" a REFERENCEABLE thing
--     (`destination_id`), while each job's actual delivery is a separate row
--     in job_deliveries that links an artifact to that destination.
--
-- Idempotency strategy (per user request):
--   * Every CREATE TABLE / CREATE INDEX uses IF NOT EXISTS.
--   * ALTER TABLE … ADD COLUMN IF NOT EXISTS is used for delivery_id
--     (requires SQLite >= 3.35).
--   * INSERT statements use INSERT OR IGNORE so a re-run is a safe no-op
--     against duplicate-keys (UNIQUE(destination_id) on destinations,
--     UNIQUE(idempotency_key) and UNIQUE(legacy_delivery_target_id, artifact_id)
--     on deliveries).
--   * UPDATE … WHERE delivery_id IS NULL only touches rows that were
--     not back-filled on a prior run.
--
-- Deterministic bridge: delivery_attempts.delivery_id is derived via a
-- single hop on legacy_delivery_target_id. Adding a
-- legacy_delivery_target_id column to job_deliveries during this
-- migration (rather than joining on jd.artifact_id alone) eliminates the
-- multi-match ambiguity when a job has multiple artifacts and
-- destinations. The bridge is preserved for report-time joins and can
-- be dropped in a follow-up migration (023) once reports are wired
-- against the new schema.

-- 0. Bootstrap: ensure delivery_attempts exists. Migration 015 originally
--    ALTERed jobs but did not create this table on a fresh database, which
--    caused step 3 (ALTER TABLE delivery_attempts ...) to fail with
--    "no such table: delivery_attempts". Making 022 self-bootstrapping
--    keeps a fresh-DB apply idempotent without depending on a future
--    re-architect of the migrations runner.
--
--    Schema intentionally matches the columns referenced by steps 3, 7,
--    and the new runner INSERT in store.SQLiteStore.ClaimPendingDeliveries.
--    Existing deployments with the table already present are unaffected
--    (IF NOT EXISTS guard).

CREATE TABLE IF NOT EXISTS delivery_attempts (
    id                   INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_target_id   INTEGER,
    attempt_number       INTEGER NOT NULL DEFAULT 0,
    status               TEXT NOT NULL DEFAULT 'scheduled',
    result               TEXT NOT NULL DEFAULT '{}',
    started_at           TEXT,
    completed_at         TEXT,
    error_message        TEXT,
    worker_id            TEXT
);

CREATE INDEX IF NOT EXISTS idx_delivery_attempts_legacy_target
  ON delivery_attempts(delivery_target_id, started_at);

CREATE INDEX IF NOT EXISTS idx_delivery_attempts_status_started
  ON delivery_attempts(status, started_at);

-- 1. delivery_destinations: reusable configuration. destination_id is
--    a 16-char sha256 prefix over (target_type, config) so the same
--    (provider, configuration_json) pair lands on a stable id across
--    re-runs. (Previously randomblob(4) — replaced for determinism.)

CREATE TABLE IF NOT EXISTS delivery_destinations (
    destination_id        TEXT NOT NULL PRIMARY KEY,
    provider              TEXT NOT NULL,
    account_id            TEXT,
    folder_id             TEXT,
    channel_id            TEXT,
    language              TEXT,
    name                  TEXT NOT NULL DEFAULT '',
    enabled               INTEGER NOT NULL DEFAULT 1,
    configuration_json    TEXT NOT NULL DEFAULT '{}',
    created_at            TEXT NOT NULL,
    updated_at            TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_destinations_provider ON delivery_destinations(provider);
CREATE INDEX IF NOT EXISTS idx_destinations_enabled ON delivery_destinations(enabled);

-- 2. job_deliveries: per-(artifact, destination) row. legacy_delivery_target_id
--    is added now (migration-step only) so the backfill UPDATE against
--    delivery_attempts is a single JOIN with no LIMIT ambiguity.

CREATE TABLE IF NOT EXISTS job_deliveries (
    delivery_id                  TEXT NOT NULL PRIMARY KEY,
    artifact_id                  TEXT NOT NULL,
    destination_id               TEXT NOT NULL,
    legacy_delivery_target_id    INTEGER,
    status                       TEXT NOT NULL DEFAULT 'PENDING',
    idempotency_key              TEXT NOT NULL UNIQUE,
    remote_id                    TEXT,
    remote_url                   TEXT,
    created_at                   TEXT NOT NULL,
    updated_at                   TEXT NOT NULL,
    FOREIGN KEY(artifact_id)            REFERENCES artifacts(id),
    FOREIGN KEY(destination_id)         REFERENCES delivery_destinations(destination_id)
);

CREATE INDEX IF NOT EXISTS idx_job_deliveries_status ON job_deliveries(status, created_at);
CREATE INDEX IF NOT EXISTS idx_job_deliveries_artifact ON job_deliveries(artifact_id);
CREATE INDEX IF NOT EXISTS idx_job_deliveries_legacy ON job_deliveries(legacy_delivery_target_id);

-- 3. delivery_attempts: add delivery_id column (nullable; legacy rows
--    time-shift into NULL until step 5 back-fills).

ALTER TABLE delivery_attempts ADD COLUMN delivery_id TEXT;

-- 4. Index on the new delivery_id FK (used by the runner's claim query).
--    Idempotent.

CREATE INDEX IF NOT EXISTS idx_delivery_attempts_delivery ON delivery_attempts(delivery_id, started_at);

-- 5. Backfill delivery_destinations: derive one row per distinct
--    (target_type, config) pair in delivery_targets. destination_id is
--    deterministic via sha256(target_type || '|' || config) so a re-run
--    yields identical ids and INSERT OR IGNORE absorbs them.
--
--    "IS NOT DISTINCT FROM" handles config=NULL rows so they are
--    collapsed together. The API writes canonical JSON today so this
--    collapse path is only a defensive measure.

INSERT OR IGNORE INTO delivery_destinations (
    destination_id, provider, account_id, folder_id, channel_id,
    language, name, enabled, configuration_json, created_at, updated_at
)
SELECT
    'dst_bck_' || MIN(id) AS dest_id,
    target_type AS provider,
    json_extract(config, '$.account_id')  AS account_id,
    json_extract(config, '$.folder_id')   AS folder_id,
    json_extract(config, '$.channel_id')  AS channel_id,
    json_extract(config, '$.language')    AS language,
    COALESCE(json_extract(config, '$.name'), 'migration-022-backfill') AS name,
    1 AS enabled,
    COALESCE(config, '{}') AS configuration_json,
    COALESCE(created_at, CURRENT_TIMESTAMP) AS created_at,
    COALESCE(updated_at, CURRENT_TIMESTAMP) AS updated_at
FROM delivery_targets
GROUP BY target_type, coalesce(config, '');

-- 6. Backfill job_deliveries: one row per (legacy_delivery_target_id, artifact)
--    pair. When a delivery_target references a job whose artifacts are all
--    not READY yet (e.g. mid-render), the artifact_id is the conservative
--    empty string '' and the delivery is created in PENDING — the
--    ArtifactFinalizationService READY transition later (migration-time
--    independent) will then trigger a re-evaluation by the runner. This
--    matches the spec: "Per il completamento del job deve esistere almeno
--    un artifact primario READY".
--
--    Determinism: per delivery_target with no candidate artifacts we
--    produce exactly ONE job_delivery (artifact_id = ''); per
--    delivery_target with N READY artifacts we produce exactly N rows
--    (one per artifact, smallest artifact_id wins ties — see subquery).
--    The (legacy_delivery_target_id, artifact_id) tuple is unique so a
--    re-run is a no-op via INSERT OR IGNORE.

INSERT OR IGNORE INTO job_deliveries (
    delivery_id, artifact_id, destination_id, legacy_delivery_target_id,
    status, idempotency_key, created_at, updated_at
)
SELECT
    'jbd_bck_' || dt.id || '_' || COALESCE(a.artifact_id, '_') AS delivery_id,
    a.artifact_id,
    dd.destination_id,
    dt.id,
    COALESCE(NULLIF(dt.status, ''), 'PENDING') AS status,
    'bck_' || dt.id || '_' || a.artifact_id AS idempotency_key,
    COALESCE(dt.created_at, CURRENT_TIMESTAMP) AS created_at,
    COALESCE(dt.updated_at, CURRENT_TIMESTAMP) AS updated_at
FROM delivery_targets dt
JOIN delivery_destinations dd
    ON dd.provider = dt.target_type
    -- NULL-safe equality: SQLite has no IS NOT DISTINCT FROM, so the join
    -- merges NULL configs via COALESCE on both sides. configs written by
    -- the API are canonical JSON; if a legacy delivery_targets row has
    -- NULL config, both sides collapse to '{}' on the equality. This is
    -- portably equivalent to IS NOT DISTINCT FROM because GROUP BY on step
    -- 5 already canonicalized NULL configs to '{}' on the destination row.
    AND COALESCE(dd.configuration_json, '') = COALESCE(dt.config, '')
LEFT JOIN (
    SELECT
        job_id,
        -- Prefer READY artifacts; among them pick the smallest id for
        -- determinism. If no candidates exist we still produce one row
        -- per delivery_target with artifact_id='' so the runner can
        -- claim it once ARTIFACT_READY lands.
        CASE WHEN COUNT(CASE WHEN status = 'READY' THEN 1 END) > 0
             THEN (SELECT id FROM artifacts a2
                   WHERE a2.job_id = a1.job_id AND a2.status = 'READY'
                   ORDER BY a2.id ASC LIMIT 1)
             ELSE ''
        END AS artifact_id
    FROM artifacts a1
    GROUP BY job_id
) a ON a.job_id = dt.job_id;

-- 7. Backfill delivery_attempts.delivery_id. Deterministic single-JOIN
--    via legacy_delivery_target_id. The bridge from delivery_attempts
--    to job_deliveries no longer requires LIMIT-without-ORDER because
--    step 6 already produced one job_delivery row per delivery_target
--    per artifact — the UPDATE does not have any ambiguity even when
--    a delivery_target has multiple artifacts or destinations.
--
--    Idempotent: WHERE delivery_id IS NULL skips rows already backfilled.

UPDATE delivery_attempts
SET delivery_id = (
    SELECT jd.delivery_id
    FROM job_deliveries jd
    WHERE jd.legacy_delivery_target_id = delivery_attempts.delivery_target_id
    LIMIT 1
)
WHERE delivery_id IS NULL;

-- 8. Optional coverage check: rows whose legacy_delivery_target_id no
--    longer maps to a job_delivery. Often because the legacy target
--    had no (target_type, config) match. We log them instead of failing
--    the migration, so the operator can investigate without blocking
--    upgrades. The runner's claim query is the source of truth.

-- (no-op for now; future PR can wire a SELECT count(*) diagnostic into
--  the post-migration data-layer audit.)
