-- Durable, explainable identity for every render attempt.
CREATE TABLE IF NOT EXISTS task_attempt_render_fingerprints (
    attempt_id TEXT PRIMARY KEY,
    task_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    render_fingerprint TEXT NOT NULL,
    render_plan_hash TEXT NOT NULL,
    canonical_payload_hash TEXT NOT NULL,
    input_manifest_hash TEXT NOT NULL,
    asset_hashes_json TEXT NOT NULL DEFAULT '[]',
    font_hashes_json TEXT NOT NULL DEFAULT '[]',
    template_version TEXT NOT NULL DEFAULT '',
    engine_version TEXT NOT NULL DEFAULT '',
    ffmpeg_version TEXT NOT NULL DEFAULT '',
    worker_version TEXT NOT NULL DEFAULT '',
    docker_image_digest TEXT NOT NULL DEFAULT '',
    config_hash TEXT NOT NULL DEFAULT '',
    encoder_config_hash TEXT NOT NULL DEFAULT '',
    random_seed INTEGER NOT NULL DEFAULT 0,
    locale TEXT NOT NULL DEFAULT '',
    timezone TEXT NOT NULL DEFAULT '',
    created_at TEXT NOT NULL
    -- The same render may be attempted more than once; the fingerprint is
    -- an identity/index, not a uniqueness constraint across attempts.
);
CREATE INDEX IF NOT EXISTS idx_task_attempt_render_fingerprints_task
    ON task_attempt_render_fingerprints(task_id);
