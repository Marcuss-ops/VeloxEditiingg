-- 020: Persistent worker control plane
-- worker_commands: durable command outbox (replaces in-memory CommandManager)
-- worker_sessions: persistent session tokens (replaces in-memory TokenManager)

CREATE TABLE IF NOT EXISTS worker_commands (
  command_id    TEXT PRIMARY KEY,
  worker_id     TEXT NOT NULL,
  command_type  TEXT NOT NULL,
  payload_json  TEXT DEFAULT '{}',
  status        TEXT NOT NULL DEFAULT 'pending',  -- pending | delivered | acked | expired | failed
  sequence_num  INTEGER NOT NULL DEFAULT 0,
  created_at    TEXT NOT NULL,
  delivered_at  TEXT,
  acked_at      TEXT,
  expires_at    TEXT,
  attempt_count INTEGER NOT NULL DEFAULT 0,
  last_error    TEXT,
  idempotency_key TEXT
);

CREATE INDEX IF NOT EXISTS idx_worker_commands_worker_status
  ON worker_commands(worker_id, status);

CREATE INDEX IF NOT EXISTS idx_worker_commands_created
  ON worker_commands(created_at);

CREATE UNIQUE INDEX IF NOT EXISTS idx_worker_commands_idempotent
  ON worker_commands(worker_id, command_type, idempotency_key)
  WHERE idempotency_key IS NOT NULL;

CREATE TABLE IF NOT EXISTS worker_sessions (
  session_id    TEXT PRIMARY KEY,
  worker_id     TEXT NOT NULL,
  token_hash    TEXT NOT NULL,
  ip_address    TEXT,
  created_at    TEXT NOT NULL,
  expires_at    TEXT NOT NULL,
  last_seen     TEXT,
  revoked       INTEGER NOT NULL DEFAULT 0
);

CREATE INDEX IF NOT EXISTS idx_worker_sessions_worker
  ON worker_sessions(worker_id);

CREATE INDEX IF NOT EXISTS idx_worker_sessions_token
  ON worker_sessions(token_hash);

CREATE TABLE IF NOT EXISTS worker_credentials (
  worker_id       TEXT PRIMARY KEY,
  credential_hash TEXT NOT NULL,
  created_at      TEXT NOT NULL,
  rotated_at      TEXT
);

-- Sequence counter per worker (monotonically increasing)
CREATE TABLE IF NOT EXISTS worker_sequences (
  worker_id    TEXT PRIMARY KEY,
  next_seq_num INTEGER NOT NULL DEFAULT 1
);
