-- Migration 100: Add workspace scoping for the InstaEdit BFF
--
-- The InstaEdit BFF sends a signed JWT carrying workspace_id. Velox
-- stores it on the resources it owns so the BFF can filter by
-- tenant. Columns are nullable so existing rows remain valid and
-- legacy callers (script, pipeline) that do not yet supply a
-- workspace are not broken.

-- Jobs
ALTER TABLE jobs ADD COLUMN workspace_id INTEGER DEFAULT NULL;
CREATE INDEX IF NOT EXISTS idx_jobs_workspace_id ON jobs(workspace_id);

-- Workers
ALTER TABLE workers ADD COLUMN workspace_id INTEGER DEFAULT NULL;
CREATE INDEX IF NOT EXISTS idx_workers_workspace_id ON workers(workspace_id);

-- Assets (generic asset registry)
ALTER TABLE assets ADD COLUMN workspace_id INTEGER DEFAULT NULL;
CREATE INDEX IF NOT EXISTS idx_assets_workspace_id ON assets(workspace_id);
