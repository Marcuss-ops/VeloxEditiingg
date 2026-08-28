-- Persist the elapsed wall clock of the concurrent cohort.  Total wall is
-- the sum of individual jobs and is not a valid throughput denominator.
ALTER TABLE capacity_benchmark_levels ADD COLUMN level_wall_ms INTEGER NOT NULL DEFAULT 0;
