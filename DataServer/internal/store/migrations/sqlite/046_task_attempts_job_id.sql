-- 046_task_attempts_job_id.sql
--
-- PR #4: Adds job_id column to task_attempts for task-native dispatch.
-- Each TaskAttempt belongs to a Task which belongs to a Job — the job_id
-- column enables direct job-scoped queries without joining through tasks.

ALTER TABLE task_attempts ADD COLUMN job_id TEXT NOT NULL DEFAULT '';
