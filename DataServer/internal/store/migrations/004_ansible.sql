-- Migration 004: Ansible persistence
--
-- Replaces ansible_computers.json and ansible_runs.json with structured tables.
-- SSH passwords are NOT stored in plaintext — use secret_ref to reference
-- an external secret store or an encrypted file.
--
-- Legacy tables (ansible_computers) are kept for data migration; they will
-- be dropped in a later cleanup migration.

-- ============================================================
-- ansible_hosts: structured computer inventory (replaces ansible_computers)
-- ============================================================
CREATE TABLE IF NOT EXISTS ansible_hosts (
    host               TEXT PRIMARY KEY,
    ansible_user       TEXT NOT NULL DEFAULT 'pierone',
    ssh_key_path       TEXT NOT NULL DEFAULT '',
    secret_ref         TEXT NOT NULL DEFAULT '',  -- reference to external secret (e.g. file path, vault key)
    enabled            INTEGER NOT NULL DEFAULT 1,
    availability       TEXT NOT NULL DEFAULT '',
    host_group         TEXT NOT NULL DEFAULT '',
    subgroup           TEXT NOT NULL DEFAULT '',
    tags_json          TEXT NOT NULL DEFAULT '[]',
    notes              TEXT NOT NULL DEFAULT '',
    linked_worker_id   TEXT NOT NULL DEFAULT '',
    worker_id          TEXT NOT NULL DEFAULT '',
    last_seen_at       TEXT,
    last_error_at      TEXT,
    last_error_message TEXT NOT NULL DEFAULT '',
    last_linked_at     TEXT,
    last_run_id        TEXT NOT NULL DEFAULT '',
    last_run_action    TEXT NOT NULL DEFAULT '',
    last_run_rc        INTEGER NOT NULL DEFAULT 0,
    last_log_level     TEXT NOT NULL DEFAULT '',
    last_log_message   TEXT NOT NULL DEFAULT '',
    last_log_source    TEXT NOT NULL DEFAULT '',
    created_at         TEXT NOT NULL,
    updated_at         TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ansible_hosts_enabled ON ansible_hosts(enabled);
CREATE INDEX IF NOT EXISTS idx_ansible_hosts_group ON ansible_hosts(host_group);
CREATE INDEX IF NOT EXISTS idx_ansible_hosts_worker ON ansible_hosts(linked_worker_id);

-- ============================================================
-- ansible_runs: run execution history (replaces ansible_runs.json)
-- ============================================================
CREATE TABLE IF NOT EXISTS ansible_runs (
    run_id             TEXT PRIMARY KEY,
    action             TEXT NOT NULL,
    playbook           TEXT NOT NULL DEFAULT '',
    status             TEXT NOT NULL DEFAULT 'pending',
    started_at         INTEGER NOT NULL DEFAULT 0,
    ended_at           INTEGER NOT NULL DEFAULT 0,
    return_code        INTEGER NOT NULL DEFAULT 0,
    commands_json      TEXT NOT NULL DEFAULT '[]',
    output             TEXT NOT NULL DEFAULT '',
    preamble           TEXT NOT NULL DEFAULT '',
    master_url         TEXT NOT NULL DEFAULT '',
    master_url_source  TEXT NOT NULL DEFAULT '',
    created_at         TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_ansible_runs_status ON ansible_runs(status);
CREATE INDEX IF NOT EXISTS idx_ansible_runs_started ON ansible_runs(started_at);

-- ============================================================
-- ansible_run_hosts: many-to-many hosts per run
-- ============================================================
CREATE TABLE IF NOT EXISTS ansible_run_hosts (
    run_id   TEXT NOT NULL,
    host     TEXT NOT NULL,
    PRIMARY KEY (run_id, host),
    FOREIGN KEY (run_id) REFERENCES ansible_runs(run_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_ansible_run_hosts_host ON ansible_run_hosts(host);
