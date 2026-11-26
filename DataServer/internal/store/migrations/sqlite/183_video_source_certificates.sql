-- Video source evidence describes immutable bytes and must not depend on a
-- prepared-window asset/blob row being present.
CREATE TABLE IF NOT EXISTS video_source_certificates (
    source_reference TEXT PRIMARY KEY,
    source_sha256 TEXT NOT NULL CHECK(length(source_sha256) = 64),
    source_size_bytes INTEGER NOT NULL CHECK(source_size_bytes > 0),
    profile_revision TEXT NOT NULL,
    certificate_json TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_video_source_certificates_identity
    ON video_source_certificates(source_sha256, profile_revision);
