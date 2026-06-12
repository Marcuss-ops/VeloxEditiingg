package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CreateProject creates a new project
func (s *PostgresProjectStore) CreateProject(ctx context.Context, project *Project) error {
	if project.ID == "" {
		project.ID = uuid.New().String()
	}
	if project.Type == "" {
		project.Type = "project"
	}
	if project.CanvasJSON == nil {
		project.CanvasJSON = make(map[string]interface{})
	}
	now := time.Now()
	if project.CreatedAt.IsZero() {
		project.CreatedAt = now
	}
	if project.UpdatedAt.IsZero() {
		project.UpdatedAt = now
	}

	canvasJSON, err := json.Marshal(project.CanvasJSON)
	if err != nil {
		return fmt.Errorf("failed to marshal canvas_json: %w", err)
	}

	metadata, err := json.Marshal(project.Metadata)
	if err != nil {
		metadata = []byte("{}")
	}

	query := `
		INSERT INTO projects (id, user_id, name, type, canvas_json, preview_url, is_template, is_public, metadata, folder_id, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`

	var userID interface{}
	if project.UserID != nil {
		userID = *project.UserID
	}
	var folderID interface{}
	if project.FolderID != nil && *project.FolderID != "" {
		folderID = *project.FolderID
	}

	_, err = s.db.ExecContext(ctx, query,
		project.ID, userID, project.Name, project.Type, canvasJSON,
		project.PreviewURL, project.IsTemplate, project.IsPublic,
		metadata, folderID, project.CreatedAt, project.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create project: %w", err)
	}

	return nil
}

// GetProject retrieves a project by ID
func (s *PostgresProjectStore) GetProject(ctx context.Context, id string) (*Project, error) {
	query := `
		SELECT id, user_id, name, type, canvas_json, preview_url, is_template, is_public, metadata, folder_id, created_at, updated_at
		FROM projects WHERE id = $1
	`

	project := &Project{}
	var canvasJSON, metadata []byte
	var userID, folderID sql.NullString

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&project.ID, &userID, &project.Name, &project.Type,
		&canvasJSON, &project.PreviewURL, &project.IsTemplate, &project.IsPublic,
		&metadata, &folderID, &project.CreatedAt, &project.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get project: %w", err)
	}

	if userID.Valid {
		project.UserID = &userID.String
	}

	if len(canvasJSON) > 0 {
		if err := json.Unmarshal(canvasJSON, &project.CanvasJSON); err != nil {
			project.CanvasJSON = make(map[string]interface{})
		}
	}

	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &project.Metadata); err != nil {
			project.Metadata = make(map[string]interface{})
		}
	}
	if folderID.Valid {
		project.FolderID = &folderID.String
	}

	return project, nil
}

// UpdateProject updates an existing project
func (s *PostgresProjectStore) UpdateProject(ctx context.Context, project *Project) error {
	project.UpdatedAt = time.Now()

	canvasJSON, err := json.Marshal(project.CanvasJSON)
	if err != nil {
		return fmt.Errorf("failed to marshal canvas_json: %w", err)
	}

	metadata, err := json.Marshal(project.Metadata)
	if err != nil {
		metadata = []byte("{}")
	}

	query := `
		UPDATE projects SET
			name = $2, type = $3, canvas_json = $4, preview_url = $5,
			is_template = $6, is_public = $7, metadata = $8, folder_id = $9, updated_at = $10
		WHERE id = $1
	`

	var folderID interface{}
	if project.FolderID != nil && *project.FolderID != "" {
		folderID = *project.FolderID
	}

	result, err := s.db.ExecContext(ctx, query,
		project.ID, project.Name, project.Type, canvasJSON,
		project.PreviewURL, project.IsTemplate, project.IsPublic,
		metadata, folderID, project.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to update project: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return errors.New("project not found")
	}

	return nil
}

// DeleteProject deletes a project by ID
func (s *PostgresProjectStore) DeleteProject(ctx context.Context, id string) error {
	query := `DELETE FROM projects WHERE id = $1`
	result, err := s.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete project: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return errors.New("project not found")
	}

	return nil
}

// ListProjects lists projects with optional filtering
func (s *PostgresProjectStore) ListProjects(ctx context.Context, opts ProjectListOptions) ([]*Project, error) {
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Limit > 500 {
		opts.Limit = 500
	}
	if opts.OrderBy == "" {
		opts.OrderBy = "updated_at"
	}
	if opts.OrderDir == "" {
		opts.OrderDir = "desc"
	}

	// Build query
	query := `SELECT id, user_id, name, type, canvas_json, preview_url, is_template, is_public, metadata, folder_id, created_at, updated_at FROM projects WHERE 1=1`
	args := []interface{}{}
	argNum := 1

	if opts.UserID != "" {
		query += fmt.Sprintf(" AND user_id = $%d", argNum)
		args = append(args, opts.UserID)
		argNum++
	}
	if opts.Type != "" {
		query += fmt.Sprintf(" AND type = $%d", argNum)
		args = append(args, opts.Type)
		argNum++
	}
	if opts.IsTemplate != nil {
		query += fmt.Sprintf(" AND is_template = $%d", argNum)
		args = append(args, *opts.IsTemplate)
		argNum++
	}
	if opts.IsPublic != nil {
		query += fmt.Sprintf(" AND is_public = $%d", argNum)
		args = append(args, *opts.IsPublic)
		argNum++
	}

	// Order and pagination
	allowedOrderBy := map[string]bool{"created_at": true, "updated_at": true, "name": true}
	if !allowedOrderBy[opts.OrderBy] {
		opts.OrderBy = "updated_at"
	}
	if opts.OrderDir != "asc" && opts.OrderDir != "desc" {
		opts.OrderDir = "desc"
	}
	query += fmt.Sprintf(" ORDER BY %s %s", opts.OrderBy, opts.OrderDir)
	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argNum, argNum+1)
	args = append(args, opts.Limit, opts.Offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list projects: %w", err)
	}
	defer rows.Close()

	projects := []*Project{}
	for rows.Next() {
		project := &Project{}
		var canvasJSON, metadata []byte
		var userID, folderID sql.NullString

		err := rows.Scan(
			&project.ID, &userID, &project.Name, &project.Type,
			&canvasJSON, &project.PreviewURL, &project.IsTemplate, &project.IsPublic,
			&metadata, &folderID, &project.CreatedAt, &project.UpdatedAt,
		)
		if err != nil {
			continue
		}

		if userID.Valid {
			project.UserID = &userID.String
		}

		if len(canvasJSON) > 0 {
			if err := json.Unmarshal(canvasJSON, &project.CanvasJSON); err != nil {
				project.CanvasJSON = make(map[string]interface{})
			}
		}

		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &project.Metadata); err != nil {
				project.Metadata = make(map[string]interface{})
			}
		}
		if folderID.Valid {
			project.FolderID = &folderID.String
		}

		projects = append(projects, project)
	}

	return projects, nil
}

func (s *PostgresProjectStore) AssignProjectFolder(ctx context.Context, projectID string, folderID *string) error {
	var folder interface{}
	if folderID != nil && *folderID != "" {
		folder = *folderID
	}

	query := `UPDATE projects SET folder_id = $2 WHERE id = $1`
	result, err := s.db.ExecContext(ctx, query, projectID, folder)
	if err != nil {
		return fmt.Errorf("failed to assign folder: %w", err)
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return errors.New("project not found")
	}
	return nil
}
