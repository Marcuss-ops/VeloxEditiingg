// PostgreSQL implementation of ProjectStore for enterprise deployments.
// Only active when VELOX_DB_DRIVER=postgres and VELOX_DB_DSN is set.
// SQLite is the default and recommended database for most deployments.
package store

import (
	"database/sql"
	"errors"
	"fmt"
	"log"
	"time"

	_ "github.com/lib/pq"
)

// PostgresProjectStore implements ProjectStore using PostgreSQL
type PostgresProjectStore struct {
	db *sql.DB
}

// NewPostgresProjectStore creates a new PostgreSQL-backed project store
func NewPostgresProjectStore(connStr string) (*PostgresProjectStore, error) {
	if connStr == "" {
		return nil, errors.New("empty postgres connection string")
	}

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open postgres connection: %w", err)
	}

	// Configure connection pool
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(5 * time.Minute)

	// Test connection
	if err := db.Ping(); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("postgres: close after ping failure: %v", closeErr)
		}
		return nil, fmt.Errorf("failed to ping postgres: %w", err)
	}

	store := &PostgresProjectStore{db: db}

	// Run migrations
	if err := store.runMigrations(); err != nil {
		if closeErr := db.Close(); closeErr != nil {
			log.Printf("postgres: close after migration failure: %v", closeErr)
		}
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	return store, nil
}

// NewPostgresProjectStoreFromDB creates a store from an existing DB connection
func NewPostgresProjectStoreFromDB(db *sql.DB) (*PostgresProjectStore, error) {
	if db == nil {
		return nil, errors.New("nil db connection")
	}

	store := &PostgresProjectStore{db: db}
	if err := store.runMigrations(); err != nil {
		return nil, fmt.Errorf("failed to run migrations: %w", err)
	}

	return store, nil
}

// runMigrations executes the schema migrations
func (s *PostgresProjectStore) runMigrations() error {
	// Check if projects table exists
	var exists bool
	err := s.db.QueryRow(`
		SELECT EXISTS (
			SELECT FROM information_schema.tables
			WHERE table_schema = 'public'
			AND table_name = 'projects'
		)
	`).Scan(&exists)
	if err != nil {
		return fmt.Errorf("failed to check table existence: %w", err)
	}

	// Read and execute migration file (embedded in code for simplicity)
	migrationSQL := `
-- Dark Editor V2 - PostgreSQL Schema
-- Migration 001: Initial schema for projects, assets, and templates

-- Enable UUID extension (if not already enabled)
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Users table (for future authentication)
CREATE TABLE IF NOT EXISTS users (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255),
    avatar_url TEXT,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Projects table (main canvas projects)
CREATE TABLE IF NOT EXISTS projects (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    name VARCHAR(255) NOT NULL,
    type VARCHAR(50) DEFAULT 'project',
    canvas_json JSONB NOT NULL DEFAULT '{}',
    preview_url TEXT,
    is_template BOOLEAN DEFAULT FALSE,
    is_public BOOLEAN DEFAULT FALSE,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Assets table (images, generated content, etc.)
CREATE TABLE IF NOT EXISTS assets (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    project_id UUID REFERENCES projects(id) ON DELETE CASCADE,
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    type VARCHAR(50) NOT NULL,
    filename VARCHAR(255) NOT NULL,
    original_filename VARCHAR(255),
    storage_path TEXT NOT NULL,
    storage_type VARCHAR(20) DEFAULT 'local',
    mime_type VARCHAR(100),
    size_bytes BIGINT,
    width INTEGER,
    height INTEGER,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Templates table (reusable project templates)
CREATE TABLE IF NOT EXISTS templates (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name VARCHAR(255) NOT NULL,
    category VARCHAR(100),
    description TEXT,
    canvas_json JSONB NOT NULL DEFAULT '{}',
    preview_url TEXT,
    is_public BOOLEAN DEFAULT FALSE,
    created_by UUID REFERENCES users(id) ON DELETE SET NULL,
    usage_count INTEGER DEFAULT 0,
    tags TEXT[] DEFAULT '{}',
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Temp files table (for tracking temporary uploads)
CREATE TABLE IF NOT EXISTS temp_files (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    filename VARCHAR(255) NOT NULL UNIQUE,
    original_filename VARCHAR(255),
    storage_path TEXT NOT NULL,
    mime_type VARCHAR(100),
    size_bytes BIGINT,
    expires_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() + INTERVAL '24 hours'),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Generation history (AI-generated images)
CREATE TABLE IF NOT EXISTS generation_history (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    user_id UUID REFERENCES users(id) ON DELETE SET NULL,
    project_id UUID REFERENCES projects(id) ON DELETE SET NULL,
    prompt TEXT NOT NULL,
    negative_prompt TEXT,
    model VARCHAR(100) DEFAULT 'flux.1-schnell',
    width INTEGER DEFAULT 1024,
    height INTEGER DEFAULT 1024,
    steps INTEGER DEFAULT 4,
    seed INTEGER,
    asset_id UUID REFERENCES assets(id) ON DELETE SET NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

-- Indexes for performance
CREATE INDEX IF NOT EXISTS idx_projects_user_id ON projects(user_id);
CREATE INDEX IF NOT EXISTS idx_projects_type ON projects(type);
CREATE INDEX IF NOT EXISTS idx_projects_is_template ON projects(is_template);
CREATE INDEX IF NOT EXISTS idx_projects_created_at ON projects(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_projects_updated_at ON projects(updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_assets_project_id ON assets(project_id);
CREATE INDEX IF NOT EXISTS idx_assets_user_id ON assets(user_id);
CREATE INDEX IF NOT EXISTS idx_assets_type ON assets(type);
CREATE INDEX IF NOT EXISTS idx_assets_created_at ON assets(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_templates_category ON templates(category);
CREATE INDEX IF NOT EXISTS idx_templates_is_public ON templates(is_public);
CREATE INDEX IF NOT EXISTS idx_templates_created_at ON templates(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_temp_files_expires_at ON temp_files(expires_at);
CREATE INDEX IF NOT EXISTS idx_temp_files_created_at ON temp_files(created_at DESC);

CREATE INDEX IF NOT EXISTS idx_generation_history_user_id ON generation_history(user_id);
CREATE INDEX IF NOT EXISTS idx_generation_history_project_id ON generation_history(project_id);
CREATE INDEX IF NOT EXISTS idx_generation_history_created_at ON generation_history(created_at DESC);

-- Function to update updated_at timestamp
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ language 'plpgsql';

-- Triggers for updated_at
DROP TRIGGER IF EXISTS update_projects_updated_at ON projects;
CREATE TRIGGER update_projects_updated_at
    BEFORE UPDATE ON projects
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();

DROP TRIGGER IF EXISTS update_templates_updated_at ON templates;
CREATE TRIGGER update_templates_updated_at
    BEFORE UPDATE ON templates
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
`

	_, err = s.db.Exec(migrationSQL)
	if err != nil {
		return fmt.Errorf("failed to execute migration: %w", err)
	}
	return s.ensureFolderSchema()
}

func (s *PostgresProjectStore) ensureFolderSchema() error {
	migrationSQL := `
CREATE TABLE IF NOT EXISTS dark_editor_folders (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    name TEXT NOT NULL,
    parent_id UUID REFERENCES dark_editor_folders(id) ON DELETE SET NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
ALTER TABLE projects ADD COLUMN IF NOT EXISTS folder_id UUID REFERENCES dark_editor_folders(id) ON DELETE SET NULL;
CREATE INDEX IF NOT EXISTS idx_projects_folder_id ON projects(folder_id);
CREATE INDEX IF NOT EXISTS idx_dark_editor_folders_parent ON dark_editor_folders(parent_id);
DROP TRIGGER IF EXISTS update_dark_editor_folders_updated_at ON dark_editor_folders;
CREATE TRIGGER update_dark_editor_folders_updated_at
    BEFORE UPDATE ON dark_editor_folders
    FOR EACH ROW
    EXECUTE FUNCTION update_updated_at_column();
`
	_, err := s.db.Exec(migrationSQL)
	return err
}

// Close closes the database connection
func (s *PostgresProjectStore) Close() error {
	if s == nil || s.db == nil {
		return nil
	}
	return s.db.Close()
}

// Ping tests the database connection
func (s *PostgresProjectStore) Ping() error {
	return s.db.Ping()
}
