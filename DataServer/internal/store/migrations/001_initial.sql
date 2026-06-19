-- Migration 001: Initial schema
-- This consolidates all previous inline DDL from sqlite.go, sqlite_darkeditor_schema.go,
-- sqlite_youtube.go, and sqlite_calendar.go into a single versioned migration.

-- Jobs
CREATE TABLE IF NOT EXISTS jobs (
  job_id TEXT PRIMARY KEY,
  status TEXT,
  video_name TEXT,
  project_id TEXT,
  created_at TEXT,
  updated_at TEXT,
  assigned_to TEXT,
  retry_count INTEGER,
  last_error TEXT,
  completed_at TEXT,
  raw_json TEXT NOT NULL,
  migrated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_jobs_status ON jobs(status);
CREATE INDEX IF NOT EXISTS idx_jobs_updated ON jobs(updated_at);

-- Job history
CREATE TABLE IF NOT EXISTS job_history (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL,
  status TEXT,
  event_ts TEXT,
  worker_id TEXT,
  message TEXT,
  raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_job_history_job_id ON job_history(job_id);

-- Job logs
CREATE TABLE IF NOT EXISTS job_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  job_id TEXT NOT NULL,
  log_ts TEXT,
  message TEXT,
  worker_id TEXT,
  is_error INTEGER DEFAULT 0,
  raw_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_job_logs_job_id ON job_logs(job_id);

-- Workers
CREATE TABLE IF NOT EXISTS workers (
  worker_id TEXT PRIMARY KEY,
  worker_name TEXT,
  status TEXT,
  last_heartbeat TEXT,
  schedulable INTEGER,
  drain INTEGER,
  worker_group TEXT,
  raw_json TEXT NOT NULL,
  migrated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_workers_last_hb ON workers(last_heartbeat);

-- Worker flags (revoked, quarantined)
CREATE TABLE IF NOT EXISTS worker_flags (
  worker_id TEXT PRIMARY KEY,
  revoked INTEGER DEFAULT 0,
  quarantined INTEGER DEFAULT 0,
  raw_json TEXT NOT NULL,
  migrated_at TEXT NOT NULL
);

-- Analytics cache
CREATE TABLE IF NOT EXISTS analytics_cache (
  cache_key TEXT PRIMARY KEY,
  ts REAL,
  data_json TEXT NOT NULL,
  migrated_at TEXT NOT NULL
);

-- Drive links
CREATE TABLE IF NOT EXISTS drive_links (
  id TEXT PRIMARY KEY,
  parent_id TEXT,
  name TEXT,
  link TEXT,
  language TEXT,
  created_at TEXT,
  updated_at TEXT,
  raw_json TEXT NOT NULL,
  migrated_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_drive_links_parent ON drive_links(parent_id);

-- Ansible computers
CREATE TABLE IF NOT EXISTS ansible_computers (
  host TEXT PRIMARY KEY,
  raw_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

-- YouTube channel metadata
CREATE TABLE IF NOT EXISTS youtube_channel_metadata (
  channel_id TEXT PRIMARY KEY,
  title TEXT,
  token_path TEXT,
  language TEXT,
  added_date TEXT,
  last_used TEXT,
  raw_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

-- YouTube groups (Service-level upload groups)
CREATE TABLE IF NOT EXISTS youtube_groups (
  name TEXT PRIMARY KEY,
  description TEXT,
  privacy TEXT,
  channels_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

-- YouTube API cache
CREATE TABLE IF NOT EXISTS youtube_api_cache (
  cache_key TEXT PRIMARY KEY,
  timestamp INTEGER NOT NULL,
  data_json TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_youtube_api_cache_ts ON youtube_api_cache(timestamp);

-- YouTube manager channels (Storage-level)
CREATE TABLE IF NOT EXISTS youtube_manager_channels (
  channel_id TEXT PRIMARY KEY,
  group_name TEXT,
  url TEXT,
  title TEXT,
  name TEXT,
  thumbnail TEXT,
  notes TEXT,
  language TEXT,
  keywords_json TEXT,
  added_at TEXT,
  last_sync TEXT,
  view_count INTEGER DEFAULT 0,
  sub_count INTEGER DEFAULT 0,
  raw_json TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

-- YouTube manager groups (Storage-level)
CREATE TABLE IF NOT EXISTS youtube_manager_groups (
  name TEXT PRIMARY KEY,
  created_at TEXT,
  group_type TEXT,
  tracked_niches_json TEXT,
  updated_at TEXT NOT NULL
);

-- Dark Editor: projects
CREATE TABLE IF NOT EXISTS dark_editor_projects (
  id TEXT PRIMARY KEY,
  user_id TEXT,
  name TEXT NOT NULL,
  type TEXT DEFAULT 'project',
  canvas_json TEXT DEFAULT '{}',
  preview_url TEXT,
  is_template INTEGER DEFAULT 0,
  is_public INTEGER DEFAULT 0,
  metadata TEXT DEFAULT '{}',
  folder_id TEXT,
  created_at TEXT,
  updated_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_de_projects_user ON dark_editor_projects(user_id);
CREATE INDEX IF NOT EXISTS idx_de_projects_type ON dark_editor_projects(type);

-- Dark Editor: folders
CREATE TABLE IF NOT EXISTS dark_editor_folders (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  parent_id TEXT,
  drive_folder_id TEXT,
  youtube_group TEXT,
  created_at TEXT,
  updated_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_de_folders_parent ON dark_editor_folders(parent_id);

-- Dark Editor: assets
CREATE TABLE IF NOT EXISTS dark_editor_assets (
  id TEXT PRIMARY KEY,
  project_id TEXT,
  user_id TEXT,
  type TEXT NOT NULL,
  filename TEXT NOT NULL,
  original_filename TEXT,
  storage_path TEXT NOT NULL,
  storage_type TEXT DEFAULT 'local',
  mime_type TEXT,
  size_bytes INTEGER,
  width INTEGER,
  height INTEGER,
  metadata TEXT DEFAULT '{}',
  created_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_de_assets_project ON dark_editor_assets(project_id);

-- Dark Editor: templates
CREATE TABLE IF NOT EXISTS dark_editor_templates (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  category TEXT,
  description TEXT,
  canvas_json TEXT DEFAULT '{}',
  preview_url TEXT,
  is_public INTEGER DEFAULT 0,
  created_by TEXT,
  usage_count INTEGER DEFAULT 0,
  tags TEXT,
  created_at TEXT,
  updated_at TEXT
);

-- Dark Editor: temp files
CREATE TABLE IF NOT EXISTS dark_editor_temp_files (
  id TEXT PRIMARY KEY,
  filename TEXT NOT NULL UNIQUE,
  original_filename TEXT,
  storage_path TEXT NOT NULL,
  mime_type TEXT,
  size_bytes INTEGER,
  expires_at TEXT,
  created_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_de_temp_expires ON dark_editor_temp_files(expires_at);

-- Dark Editor: generations
CREATE TABLE IF NOT EXISTS dark_editor_generations (
  id TEXT PRIMARY KEY,
  user_id TEXT,
  project_id TEXT,
  prompt TEXT NOT NULL,
  negative_prompt TEXT,
  model TEXT DEFAULT 'flux.1-schnell',
  width INTEGER DEFAULT 1024,
  height INTEGER DEFAULT 1024,
  steps INTEGER DEFAULT 4,
  seed INTEGER,
  asset_id TEXT,
  created_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_de_gen_user ON dark_editor_generations(user_id);

-- YouTube channel metrics
CREATE TABLE IF NOT EXISTS youtube_channel_metrics (
  channel_id TEXT NOT NULL,
  ts TEXT NOT NULL,
  subscriber_count INTEGER,
  view_count INTEGER,
  video_count INTEGER,
  estimated_revenue REAL,
  PRIMARY KEY (channel_id, ts)
);

-- YouTube revenue metrics
CREATE TABLE IF NOT EXISTS youtube_revenue_metrics (
  channel_id TEXT NOT NULL,
  date TEXT NOT NULL,
  estimated_revenue REAL,
  currency TEXT DEFAULT 'EUR',
  views INTEGER,
  PRIMARY KEY (channel_id, date)
);

-- YouTube video metrics
CREATE TABLE IF NOT EXISTS youtube_video_metrics (
  video_id TEXT NOT NULL,
  channel_id TEXT NOT NULL,
  date TEXT NOT NULL,
  title TEXT,
  thumbnail_url TEXT,
  views INTEGER,
  revenue REAL,
  PRIMARY KEY (video_id, date)
);

-- YouTube quota usage
CREATE TABLE IF NOT EXISTS youtube_quota_usage (
  date TEXT PRIMARY KEY,
  units_used INTEGER DEFAULT 0
);

-- Calendar events
CREATE TABLE IF NOT EXISTS calendar_events (
  id TEXT PRIMARY KEY,
  external_id TEXT DEFAULT '',
  source TEXT DEFAULT '',
  title TEXT NOT NULL,
  date INTEGER NOT NULL,
  month INTEGER NOT NULL,
  year INTEGER NOT NULL,
  status TEXT DEFAULT 'draft',
  youtube_group TEXT DEFAULT '',
  stock_footage TEXT DEFAULT '[]',
  initial_clips TEXT DEFAULT '[]',
  intermediate_clips TEXT DEFAULT '[]',
  final_clips TEXT DEFAULT '[]',
  voiceover_paths_json TEXT DEFAULT '[]',
  titles_json TEXT DEFAULT '[]',
  script_text TEXT DEFAULT '',
  youtube_links_json TEXT DEFAULT '[]',
  category TEXT DEFAULT '',
  job_id TEXT DEFAULT '',
  job_status TEXT DEFAULT '',
  queued_at TEXT,
  queue_error TEXT DEFAULT '',
  output_video_path TEXT,
  output_video_url TEXT,
  publish_status TEXT,
  created_at TEXT,
  updated_at TEXT
);

-- Schema migrations tracking (created by migration runner if not exists)
CREATE TABLE IF NOT EXISTS schema_migrations (
  version   INTEGER PRIMARY KEY,
  name      TEXT NOT NULL,
  checksum  TEXT NOT NULL,
  applied_at TEXT NOT NULL
);

-- Worker validations
CREATE TABLE IF NOT EXISTS worker_validations (
  worker_id TEXT PRIMARY KEY,
  validation_code TEXT NOT NULL,
  canonical_unit TEXT,
  valid_from TEXT,
  valid_until TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);
