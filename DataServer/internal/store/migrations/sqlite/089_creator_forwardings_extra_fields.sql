-- 089_creator_forwardings_extra_fields.sql
--
-- Add the remaining forwarding-tracking columns specified in Area 3:
--   poll_attempts    — how many times the runner has polled the remote
--                       engine for this forwarding (incremented per poll
--                       cycle, separate from attempt_count which tracks
--                       claim/lease cycles).
--   next_poll_at     — when the runner should poll again (RFC3339). Empty
--                       means "immediately". Distinct from next_attempt_at
--                       which gates the claim/lease retry cycle.
--   last_polled_at   — timestamp of the last successful remote poll.
--   last_error_class — typed error class from the remote engine adapter
--                       (VALIDATION / AUTHENTICATION / RATE_LIMIT /
--                       TRANSIENT / PERMANENT / MALFORMED_RESPONSE).
--                       Complements last_error_code (the short string code)
--                       and last_error_message (the human-readable text).
--
-- All four columns use ALTER TABLE ADD COLUMN so existing deployments
-- get an additive migration without rewriting the table. The NOT NULL
-- DEFAULT '' / 0 clauses ensure scans and inserts that pre-date the
-- migration do not need COALESCE guards.

ALTER TABLE creator_forwardings ADD COLUMN poll_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE creator_forwardings ADD COLUMN next_poll_at TEXT NOT NULL DEFAULT '';
ALTER TABLE creator_forwardings ADD COLUMN last_polled_at TEXT NOT NULL DEFAULT '';
ALTER TABLE creator_forwardings ADD COLUMN last_remote_status TEXT NOT NULL DEFAULT '';
ALTER TABLE creator_forwardings ADD COLUMN last_error_class TEXT NOT NULL DEFAULT '';
