-- 154_forwarding_intake_source.sql
--
-- Thread the producer intake source through the durable forwarding row so
-- the async CreatorForwardingRunner can record
-- `pipeline.intake_source_accepted_total{intake_source}` at the point the
-- downstream Job is actually created (forwarding completion), instead of
-- the HTTP layer recording it as an accept-time side-effect.
--
-- Empty is the default for legacy rows and for the synchronous creator-push
-- forwarding path, whose intake source is already recorded by
-- CanonicalJobSubmitter.Submit at HTTP accept time; the runner skips empty
-- sources when recording.
ALTER TABLE creator_forwardings
    ADD COLUMN intake_source TEXT NOT NULL DEFAULT '';
