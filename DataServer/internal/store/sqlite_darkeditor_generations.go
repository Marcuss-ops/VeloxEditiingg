package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"
)

func (s *SQLiteStore) CreateGenerationRecord(ctx context.Context, record *GenerationRecord) error {
	if record.ID == "" {
		record.ID = fmt.Sprintf("gen_%d", time.Now().UnixNano())
	}
	if record.Model == "" {
		record.Model = "flux.1-schnell"
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO dark_editor_generations (id, user_id, project_id, prompt, negative_prompt, model, width, height, steps, seed, asset_id, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		record.ID, record.UserID, record.ProjectID, record.Prompt, record.NegativePrompt,
		record.Model, record.Width, record.Height, record.Steps, record.Seed,
		record.AssetID, time.Now().Format(time.RFC3339),
	)
	return err
}

func (s *SQLiteStore) ListGenerationHistory(ctx context.Context, opts GenerationListOptions) ([]*GenerationRecord, error) {
	if opts.Limit <= 0 {
		opts.Limit = 50
	}

	query := `SELECT id, user_id, project_id, prompt, negative_prompt, model, width, height, steps, seed, asset_id, created_at FROM dark_editor_generations WHERE 1=1`
	args := []interface{}{}

	if opts.UserID != "" {
		query += " AND user_id = ?"
		args = append(args, opts.UserID)
	}
	if opts.ProjectID != "" {
		query += " AND project_id = ?"
		args = append(args, opts.ProjectID)
	}
	if opts.Model != "" {
		query += " AND model = ?"
		args = append(args, opts.Model)
	}

	query += " ORDER BY created_at DESC LIMIT ? OFFSET ?"
	args = append(args, opts.Limit, opts.Offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	records := []*GenerationRecord{}
	for rows.Next() {
		record := &GenerationRecord{}
		var userID, projectID, assetID sql.NullString

		if err := rows.Scan(&record.ID, &userID, &projectID, &record.Prompt, &record.NegativePrompt,
			&record.Model, &record.Width, &record.Height, &record.Steps, &record.Seed,
			&assetID, &record.CreatedAt); err != nil {
			continue
		}

		if userID.Valid {
			record.UserID = &userID.String
		}
		if projectID.Valid {
			record.ProjectID = &projectID.String
		}
		if assetID.Valid {
			record.AssetID = &assetID.String
		}
		records = append(records, record)
	}
	return records, nil
}
