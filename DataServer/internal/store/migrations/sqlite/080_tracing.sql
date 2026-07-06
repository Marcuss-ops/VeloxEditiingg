-- 080_tracing.sql
--
-- Step 15 / Scorecard v2: adds OpenTelemetry distributed tracing
-- correlation columns to task_attempts.
--
-- trace_id  — W3C trace ID (32 hex chars) propagated across services
-- span_id   — W3C span ID (16 hex chars) of the parent span that
--             created or dispatched this attempt
--
-- Both DEFAULT '' so older workers that don't emit these fields
-- continue to function without a code change. The trace context
-- is propagated via gRPC metadata (traceparent header) and
-- persisted at attempt creation time (ClaimNextWithAttemptAtomic)
-- and again at report time (IngestTaskResultAtomic).

ALTER TABLE task_attempts
    ADD COLUMN trace_id TEXT NOT NULL DEFAULT '';
ALTER TABLE task_attempts
    ADD COLUMN span_id  TEXT NOT NULL DEFAULT '';
