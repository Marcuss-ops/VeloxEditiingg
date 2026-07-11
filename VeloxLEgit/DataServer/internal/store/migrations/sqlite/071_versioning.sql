-- 071_versioning.sql
--
-- Step 8 / Scorecard v2: versioning columns on task_attempts.
-- Every attempt records the software versions that produced it so
-- operators can trace regressions to specific code/image changes.
--
-- Columns:
--   git_sha             — commit hash of the deployed code
--   worker_version      — semantic version of the worker binary
--   engine_version      — semantic version of the C++ video engine
--   ffmpeg_version      — FFmpeg version string (e.g. "n7.0.2")
--   config_hash         — hash of the worker's runtime config
--   docker_image_digest — OCI image digest of the running container
--
-- All DEFAULT '' so existing rows and older workers that don't emit
-- these fields continue to function without a code change.

ALTER TABLE task_attempts ADD COLUMN git_sha              TEXT NOT NULL DEFAULT '';
ALTER TABLE task_attempts ADD COLUMN worker_version       TEXT NOT NULL DEFAULT '';
ALTER TABLE task_attempts ADD COLUMN engine_version       TEXT NOT NULL DEFAULT '';
ALTER TABLE task_attempts ADD COLUMN ffmpeg_version       TEXT NOT NULL DEFAULT '';
ALTER TABLE task_attempts ADD COLUMN config_hash          TEXT NOT NULL DEFAULT '';
ALTER TABLE task_attempts ADD COLUMN docker_image_digest  TEXT NOT NULL DEFAULT '';
