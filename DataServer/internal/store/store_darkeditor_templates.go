package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

func (s *SQLiteStore) CreateTemplate(ctx context.Context, template *Template) error {
	if template.ID == "" {
		template.ID = fmt.Sprintf("tmpl_%d", time.Now().UnixNano())
	}
	canvasJSON, _ := json.Marshal(template.CanvasJSON)
	tagsJSON, _ := json.Marshal(template.Tags)

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO dark_editor_templates (id, name, category, description, canvas_json, preview_url, is_public, created_by, usage_count, tags, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		template.ID, template.Name, template.Category, template.Description,
		string(canvasJSON), template.PreviewURL, template.IsPublic, template.CreatedBy,
		template.UsageCount, string(tagsJSON), time.Now().Format(time.RFC3339), time.Now().Format(time.RFC3339),
	)
	return err
}

func (s *SQLiteStore) GetTemplate(ctx context.Context, id string) (*Template, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, name, category, description, canvas_json, preview_url, is_public, created_by, usage_count, tags, created_at, updated_at
		 FROM dark_editor_templates WHERE id = ?`, id)

	template := &Template{}
	var canvasJSON, tagsJSON []byte
	var createdBy sql.NullString

	err := row.Scan(&template.ID, &template.Name, &template.Category, &template.Description,
		&canvasJSON, &template.PreviewURL, &template.IsPublic, &createdBy,
		&template.UsageCount, &tagsJSON, &template.CreatedAt, &template.UpdatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if createdBy.Valid {
		template.CreatedBy = &createdBy.String
	}
	if len(canvasJSON) > 0 {
		if err := json.Unmarshal(canvasJSON, &template.CanvasJSON); err != nil {
			template.CanvasJSON = make(map[string]interface{})
		}
	}
	if len(tagsJSON) > 0 {
		if err := json.Unmarshal(tagsJSON, &template.Tags); err != nil {
			template.Tags = []string{}
		}
	}
	return template, nil
}

func (s *SQLiteStore) UpdateTemplate(ctx context.Context, template *Template) error {
	canvasJSON, _ := json.Marshal(template.CanvasJSON)
	tagsJSON, _ := json.Marshal(template.Tags)

	_, err := s.db.ExecContext(ctx,
		`UPDATE dark_editor_templates SET name=?, category=?, description=?, canvas_json=?, preview_url=?, is_public=?, tags=?, updated_at=?
		 WHERE id = ?`,
		template.Name, template.Category, template.Description, string(canvasJSON),
		template.PreviewURL, template.IsPublic, string(tagsJSON),
		time.Now().Format(time.RFC3339), template.ID,
	)
	return err
}

func (s *SQLiteStore) DeleteTemplate(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM dark_editor_templates WHERE id = ?`, id)
	return err
}

func (s *SQLiteStore) ListTemplates(ctx context.Context, opts TemplateListOptions) ([]*Template, error) {
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.OrderBy == "" {
		opts.OrderBy = "created_at"
	}
	if opts.OrderDir == "" {
		opts.OrderDir = "desc"
	}

	query := `SELECT id, name, category, description, canvas_json, preview_url, is_public, created_by, usage_count, tags, created_at, updated_at FROM dark_editor_templates WHERE 1=1`
	args := []interface{}{}

	if opts.Category != "" {
		query += " AND category = ?"
		args = append(args, opts.Category)
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

	templates := []*Template{}
	for rows.Next() {
		template := &Template{}
		var canvasJSON, tagsJSON []byte
		var createdBy sql.NullString

		if err := rows.Scan(&template.ID, &template.Name, &template.Category, &template.Description,
			&canvasJSON, &template.PreviewURL, &template.IsPublic, &createdBy,
			&template.UsageCount, &tagsJSON, &template.CreatedAt, &template.UpdatedAt); err != nil {
			continue
		}

		if createdBy.Valid {
			template.CreatedBy = &createdBy.String
		}
		if len(canvasJSON) > 0 {
			json.Unmarshal(canvasJSON, &template.CanvasJSON)
		}
		if len(tagsJSON) > 0 {
			json.Unmarshal(tagsJSON, &template.Tags)
		}
		templates = append(templates, template)
	}
	return templates, nil
}

func (s *SQLiteStore) IncrementTemplateUsage(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `UPDATE dark_editor_templates SET usage_count = usage_count + 1 WHERE id = ?`, id)
	return err
}
