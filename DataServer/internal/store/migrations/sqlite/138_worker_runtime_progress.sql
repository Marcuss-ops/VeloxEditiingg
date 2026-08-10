-- 138: incremental canonical attempt progress on worker_task_runtime.
-- The heartbeat projection is the existing live Attempt read path; these
-- columns extend that row without introducing a parallel progress tracker.

ALTER TABLE worker_task_runtime ADD COLUMN current_segment INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_task_runtime ADD COLUMN total_segments INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_task_runtime ADD COLUMN frames_encoded INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_task_runtime ADD COLUMN frames_decoded INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_task_runtime ADD COLUMN frames_composited INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_task_runtime ADD COLUMN ffmpeg_speed_x REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_task_runtime ADD COLUMN elapsed_ms INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_task_runtime ADD COLUMN cumulative_metrics_json TEXT NOT NULL DEFAULT '{}';
