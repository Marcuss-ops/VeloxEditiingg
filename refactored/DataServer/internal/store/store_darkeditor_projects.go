package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

func (s *SQLiteStore) CreateProject(ctx context.Context, project *Project) error {
	if project.ID == "" {
		project.ID = fmt.Sprintf("proj_%d", time.Now().UnixNano())
	}
	if project.Type == "" {
		project.Type = "project"
	}
	if project.CanvasJSON == nil {
		project.CanvasJSON = make(map[string]interface{})
	}
	if project.CreatedAt.IsZero() {
		project.CreatedAt = time.Now()
	}
	if project.UpdatedAt.IsZero() {
		project.UpdatedAt = time.Now()
	}

	canvasJSON, _ := json.Marshal(project.CanvasJSON)
	metadata, _ := json.Marshal(project.Metadata)

	var folderID interface{}
	if project.FolderID != nil && *project.FolderID != "" {
		folderID = *project.FolderID
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO dark_editor_projects (id, user_id, name, type, canvas_json, preview_url, is_template, is_public, metadata, folder_id, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		project.ID, project.UserID, project.Name, project.Type, string(canvasJSON),
		project.PreviewURL, project.IsTemplate, project.IsPublic,
		string(metadata), folderID, project.CreatedAt.Format(time.RFC3339), project.UpdatedAt.Format(time.RFC3339),
	)
	return err
}

func (s *SQLiteStore) GetProject(ctx context.Context, id string) (*Project, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, user_id, name, type, canvas_json, preview_url, is_template, is_public, metadata, folder_id, created_at, updated_at
		 FROM dark_editor_projects WHERE id = ?`, id)

	project := &Project{}
	var canvasJSON, metadata []byte
	var createdAt, updatedAt string
	var userID, folderID sql.NullString

	err := row.Scan(&project.ID, &userID, &project.Name, &project.Type, &canvasJSON,
		&project.PreviewURL, &project.IsTemplate, &project.IsPublic,
		&metadata, &folderID, &createdAt, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
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
	project.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
	project.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)
	if folderID.Valid {
		project.FolderID = &folderID.String
	}

	return project, nil
}

func (s *SQLiteStore) UpdateProject(ctx context.Context, project *Project) error {
	project.UpdatedAt = time.Now()
	canvasJSON, _ := json.Marshal(project.CanvasJSON)
	metadata, _ := json.Marshal(project.Metadata)

	var folderID interface{}
	if project.FolderID != nil && *project.FolderID != "" {
		folderID = *project.FolderID
	}

	_, err := s.db.ExecContext(ctx,
		`UPDATE dark_editor_projects SET name=?, type=?, canvas_json=?, preview_url=?, is_template=?, is_public=?, metadata=?, folder_id=?, updated_at=?
		 WHERE id = ?`,
		project.Name, project.Type, string(canvasJSON), project.PreviewURL,
		project.IsTemplate, project.IsPublic, string(metadata),
		folderID, project.UpdatedAt.Format(time.RFC3339), project.ID,
	)
	return err
}

func (s *SQLiteStore) DeleteProject(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM dark_editor_projects WHERE id = ?`, id)
	return err
}

func (s *SQLiteStore) ListProjects(ctx context.Context, opts ProjectListOptions) ([]*Project, error) {
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.OrderBy == "" {
		opts.OrderBy = "updated_at"
	}
	if opts.OrderDir == "" {
		opts.OrderDir = "desc"
	}

	query := `SELECT id, user_id, name, type, canvas_json, preview_url, is_template, is_public, metadata, folder_id, created_at, updated_at FROM dark_editor_projects WHERE 1=1`
	args := []interface{}{}

	if opts.UserID != "" {
		query += " AND user_id = ?"
		args = append(args, opts.UserID)
	}
	if opts.Type != "" {
		query += " AND type = ?"
		args = append(args, opts.Type)
	}
	if opts.IsTemplate != nil {
		query += " AND is_template = ?"
		args = append(args, *opts.IsTemplate)
	}
	if opts.IsPublic != nil {
		query += " AND is_public = ?"
		args = append(args, *opts.IsPublic)
	}

	query += fmt.Sprintf(" ORDER BY %s %s LIMIT ? OFFSET ?", opts.OrderBy, opts.OrderDir)
	args = append(args, opts.Limit, opts.Offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	projects := []*Project{}
	for rows.Next() {
		project := &Project{}
		var canvasJSON, metadata []byte
		var createdAt, updatedAt string
		var userID, folderID sql.NullString

		if err := rows.Scan(&project.ID, &userID, &project.Name, &project.Type, &canvasJSON,
			&project.PreviewURL, &project.IsTemplate, &project.IsPublic,
			&metadata, &folderID, &createdAt, &updatedAt); err != nil {
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
		project.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
		project.UpdatedAt, _ = time.Parse(time.RFC3339, updatedAt)
		if folderID.Valid {
			project.FolderID = &folderID.String
		}
		projects = append(projects, project)
	}
	return projects, nil
}

func (s *SQLiteStore) AssignProjectFolder(ctx context.Context, projectID string, folderID *string) error {
	var folder interface{}
	if folderID != nil && *folderID != "" {
		folder = *folderID
	}

	result, err := s.db.ExecContext(ctx, `UPDATE dark_editor_projects SET folder_id = ? WHERE id = ?`, folder, projectID)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("project not found")
	}
	return nil
}
