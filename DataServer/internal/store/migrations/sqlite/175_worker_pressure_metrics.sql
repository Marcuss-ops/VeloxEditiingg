-- Persist CFS throttling and PSI pressure observations from worker heartbeats.
-- These are additive host telemetry; older workers continue to write zeros.

ALTER TABLE worker_resource_samples ADD COLUMN cgroup_nr_throttled INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN cgroup_throttled_usec INTEGER NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN cpu_some_pressure_avg10 REAL NOT NULL DEFAULT 0;
ALTER TABLE worker_resource_samples ADD COLUMN io_some_pressure_avg10 REAL NOT NULL DEFAULT 0;
