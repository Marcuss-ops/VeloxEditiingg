-- Dedicated queryable hardware facts for canonical capacity reports.
ALTER TABLE worker_runtime_snapshots ADD COLUMN physical_cpu_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_runtime_snapshots ADD COLUMN storage_device TEXT NOT NULL DEFAULT '';
ALTER TABLE worker_runtime_snapshots ADD COLUMN gpu_vram_bytes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_runtime_snapshots ADD COLUMN nvenc_available INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_runtime_snapshots ADD COLUMN nvdec_available INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_runtime_snapshots ADD COLUMN qsv_available INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_runtime_snapshots ADD COLUMN ulimit_nofile_soft INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_runtime_snapshots ADD COLUMN ulimit_nofile_hard INTEGER NOT NULL DEFAULT 0;
