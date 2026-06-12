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

// CreateTemplate creates a new template
func (s *PostgresProjectStore) CreateTemplate(ctx context.Context, template *Template) error {
	if template.ID == "" {
		template.ID = uuid.New().String()
	}
	if template.CanvasJSON == nil {
		template.CanvasJSON = make(map[string]interface{})
	}
	now := time.Now()
	if template.CreatedAt.IsZero() {
		template.CreatedAt = now
	}
	if template.UpdatedAt.IsZero() {
		template.UpdatedAt = now
	}

	canvasJSON, err := json.Marshal(template.CanvasJSON)
	if err != nil {
		return fmt.Errorf("failed to marshal canvas_json: %w", err)
	}

	query := `
		INSERT INTO templates (id, name, category, description, canvas_json, preview_url, is_public, created_by, usage_count, tags, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`

	var createdBy interface{}
	if template.CreatedBy != nil {
		createdBy = *template.CreatedBy
	}

	_, err = s.db.ExecContext(ctx, query,
		template.ID, template.Name, template.Category, template.Description,
		canvasJSON, template.PreviewURL, template.IsPublic, createdBy,
		template.UsageCount, template.Tags, template.CreatedAt, template.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create template: %w", err)
	}

	return nil
}

// GetTemplate retrieves a template by ID
func (s *PostgresProjectStore) GetTemplate(ctx context.Context, id string) (*Template, error) {
	query := `
		SELECT id, name, category, description, canvas_json, preview_url, is_public, created_by, usage_count, tags, created_at, updated_at
		FROM templates WHERE id = $1
	`

	template := &Template{}
	var canvasJSON []byte
	var createdBy sql.NullString

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&template.ID, &template.Name, &template.Category, &template.Description,
		&canvasJSON, &template.PreviewURL, &template.IsPublic, &createdBy,
		&template.UsageCount, &template.Tags, &template.CreatedAt, &template.UpdatedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get template: %w", err)
	}

	if createdBy.Valid {
		template.CreatedBy = &createdBy.String
	}

	if len(canvasJSON) > 0 {
		if err := json.Unmarshal(canvasJSON, &template.CanvasJSON); err != nil {
			template.CanvasJSON = make(map[string]interface{})
		}
	}

	return template, nil
}

// UpdateTemplate updates an existing template
func (s *PostgresProjectStore) UpdateTemplate(ctx context.Context, template *Template) error {
	template.UpdatedAt = time.Now()

	canvasJSON, err := json.Marshal(template.CanvasJSON)
	if err != nil {
		return fmt.Errorf("failed to marshal canvas_json: %w", err)
	}

	query := `
		UPDATE templates SET
			name = $2, category = $3, description = $4, canvas_json = $5,
			preview_url = $6, is_public = $7, tags = $8, updated_at = $9
		WHERE id = $1
	`

	result, err := s.db.ExecContext(ctx, query,
		template.ID, template.Name, template.Category, template.Description,
		canvasJSON, template.PreviewURL, template.IsPublic, template.Tags, template.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to update template: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return errors.New("template not found")
	}

	return nil
}

// DeleteTemplate deletes a template by ID
func (s *PostgresProjectStore) DeleteTemplate(ctx context.Context, id string) error {
	query := `DELETE FROM templates WHERE id = $1`
	result, err := s.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("failed to delete template: %w", err)
	}

	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return errors.New("template not found")
	}

	return nil
}

// ListTemplates lists templates with optional filtering
func (s *PostgresProjectStore) ListTemplates(ctx context.Context, opts TemplateListOptions) ([]*Template, error) {
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Limit > 500 {
		opts.Limit = 500
	}
	if opts.OrderBy == "" {
		opts.OrderBy = "created_at"
	}
	if opts.OrderDir == "" {
		opts.OrderDir = "desc"
	}

	query := `SELECT id, name, category, description, canvas_json, preview_url, is_public, created_by, usage_count, tags, created_at, updated_at FROM templates WHERE 1=1`
	args := []interface{}{}
	argNum := 1

	if opts.Category != "" {
		query += fmt.Sprintf(" AND category = $%d", argNum)
		args = append(args, opts.Category)
		argNum++
	}
	if opts.IsPublic != nil {
		query += fmt.Sprintf(" AND is_public = $%d", argNum)
		args = append(args, *opts.IsPublic)
		argNum++
	}
	if len(opts.Tags) > 0 {
		query += fmt.Sprintf(" AND tags && $%d", argNum)
		args = append(args, opts.Tags)
		argNum++
	}

	allowedOrderBy := map[string]bool{"created_at": true, "updated_at": true, "name": true, "usage_count": true}
	if !allowedOrderBy[opts.OrderBy] {
		opts.OrderBy = "created_at"
	}
	if opts.OrderDir != "asc" && opts.OrderDir != "desc" {
		opts.OrderDir = "desc"
	}
	query += fmt.Sprintf(" ORDER BY %s %s", opts.OrderBy, opts.OrderDir)
	query += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argNum, argNum+1)
	args = append(args, opts.Limit, opts.Offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list templates: %w", err)
	}
	defer rows.Close()

	templates := []*Template{}
	for rows.Next() {
		template := &Template{}
		var canvasJSON []byte
		var createdBy sql.NullString

		err := rows.Scan(
			&template.ID, &template.Name, &template.Category, &template.Description,
			&canvasJSON, &template.PreviewURL, &template.IsPublic, &createdBy,
			&template.UsageCount, &template.Tags, &template.CreatedAt, &template.UpdatedAt,
		)
		if err != nil {
			continue
		}

		if createdBy.Valid {
			template.CreatedBy = &createdBy.String
		}

		if len(canvasJSON) > 0 {
			if err := json.Unmarshal(canvasJSON, &template.CanvasJSON); err != nil {
				template.CanvasJSON = make(map[string]interface{})
			}
		}

		templates = append(templates, template)
	}

	return templates, nil
}

// IncrementTemplateUsage increments the usage count for a template
func (s *PostgresProjectStore) IncrementTemplateUsage(ctx context.Context, id string) error {
	query := `UPDATE templates SET usage_count = usage_count + 1 WHERE id = $1`
	_, err := s.db.ExecContext(ctx, query, id)
	return err
}
