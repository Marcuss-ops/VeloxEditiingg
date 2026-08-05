-- Migration 131: persist the shared telemetry catalog version per event.
--
-- TaskResult already carries telemetry_schema_version at the report level and
-- on each PhaseTimingDetailed event. Keeping the version on the event row
-- makes worker -> protobuf -> master -> DB audits self-contained and lets
-- operators distinguish taxonomy generations without decoding raw reports.

ALTER TABLE task_execution_events
    ADD COLUMN telemetry_schema_version INTEGER NOT NULL DEFAULT 0;
