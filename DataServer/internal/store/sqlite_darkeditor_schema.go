package store

func (s *SQLiteStore) initDarkEditorSchema() error {
	ddl := `
CREATE TABLE IF NOT EXISTS dark_editor_projects (
  id TEXT PRIMARY KEY,
  user_id TEXT,
  name TEXT NOT NULL,
  type TEXT DEFAULT 'project',
  canvas_json TEXT NOT NULL DEFAULT '{}',
  preview_url TEXT,
  is_template INTEGER DEFAULT 0,
  is_public INTEGER DEFAULT 0,
  metadata TEXT DEFAULT '{}',
  folder_id TEXT,
  created_at TEXT,
  updated_at TEXT
);
CREATE TABLE IF NOT EXISTS dark_editor_folders (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  parent_id TEXT,
  created_at TEXT,
  updated_at TEXT
);
CREATE INDEX IF NOT EXISTS idx_de_folders_parent ON dark_editor_folders(parent_id);
CREATE INDEX IF NOT EXISTS idx_de_projects_user ON dark_editor_projects(user_id);
CREATE INDEX IF NOT EXISTS idx_de_projects_type ON dark_editor_projects(type);

CREATE TABLE IF NOT EXISTS dark_editor_assets (
  id TEXT PRIMARY KEY,
  project_id TEXT,
  user_id TEXT,
  type TEXT NOT NULL,
  filename TEXT NOT NULL UNIQUE,
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

CREATE TABLE IF NOT EXISTS dark_editor_templates (
  id TEXT PRIMARY KEY,
  name TEXT NOT NULL,
  category TEXT,
  description TEXT,
  canvas_json TEXT NOT NULL DEFAULT '{}',
  preview_url TEXT,
  is_public INTEGER DEFAULT 0,
  created_by TEXT,
  usage_count INTEGER DEFAULT 0,
  tags TEXT DEFAULT '[]',
  created_at TEXT,
  updated_at TEXT
);

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
`
	_, err := s.db.Exec(ddl)
	if err != nil {
		return err
	}
	return s.ensureColumn("dark_editor_projects", "folder_id", "TEXT")
}
