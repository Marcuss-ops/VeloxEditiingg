-- Preserve the memory ceiling needed to calculate host memory utilization.
ALTER TABLE worker_resource_samples ADD COLUMN memory_total_bytes INTEGER NOT NULL DEFAULT 0;
