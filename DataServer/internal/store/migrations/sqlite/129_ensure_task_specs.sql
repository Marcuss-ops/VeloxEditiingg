-- Migration 129: ensure task_specs after the retired v40 stub.
--
-- Some historical databases recorded v40 as the Dark Editor project stub
-- rather than the current task_specs migration. The runner accepts that
-- exact legacy checksum, and this idempotent forward migration restores the
-- canonical task_specs table without changing any applied migration.

CREATE TABLE IF NOT EXISTS task_specs (
    task_id        TEXT NOT NULL,
    spec_version   INTEGER NOT NULL DEFAULT 1,
    spec_hash      TEXT NOT NULL DEFAULT '',
    executor_id    TEXT NOT NULL DEFAULT '',
    payload_json   TEXT NOT NULL DEFAULT '{}',
    created_at     TEXT NOT NULL,
    PRIMARY KEY (task_id)
);
