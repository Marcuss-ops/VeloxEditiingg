package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// CreateGenerationRecord creates a new generation history record
func (s *PostgresProjectStore) CreateGenerationRecord(ctx context.Context, record *GenerationRecord) error {
	if record.ID == "" {
		record.ID = uuid.New().String()
	}
	if record.Model == "" {
		record.Model = "flux.1-schnell"
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now()
	}

	query := `
		INSERT INTO generation_history (id, user_id, project_id, prompt, negative_prompt, model, width, height, steps, seed, asset_id, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`

	var userID, projectID, assetID interface{}
	if record.UserID != nil {
		userID = *record.UserID
	}
	if record.ProjectID != nil {
		projectID = *record.ProjectID
	}
	if record.AssetID != nil {
		assetID = *record.AssetID
	}

	_, err := s.db.ExecContext(ctx, query,
		record.ID, userID, projectID, record.Prompt, record.NegativePrompt,
		record.Model, record.Width, record.Height, record.Steps, record.Seed,
		assetID, record.CreatedAt,
	)
	if err != nil {
		return fmt.Errorf("failed to create generation record: %w", err)
	}

	return nil
}

// ListGenerationHistory lists generation history with optional filtering
func (s *PostgresProjectStore) ListGenerationHistory(ctx context.Context, opts GenerationListOptions) ([]*GenerationRecord, error) {
	if opts.Limit <= 0 {
		opts.Limit = 50
	}
	if opts.Limit > 500 {
		opts.Limit = 500
	}

	query := `SELECT id, user_id, project_id, prompt, negative_prompt, model, width, height, steps, seed, asset_id, created_at FROM generation_history WHERE 1=1`
	args := []interface{}{}
	argNum := 1

	if opts.UserID != "" {
		query += fmt.Sprintf(" AND user_id = $%d", argNum)
		args = append(args, opts.UserID)
		argNum++
	}
	if opts.ProjectID != "" {
		query += fmt.Sprintf(" AND project_id = $%d", argNum)
		args = append(args, opts.ProjectID)
		argNum++
	}
	if opts.Model != "" {
		query += fmt.Sprintf(" AND model = $%d", argNum)
		args = append(args, opts.Model)
		argNum++
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", argNum, argNum+1)
	args = append(args, opts.Limit, opts.Offset)

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to list generation history: %w", err)
	}
	defer rows.Close()

	records := []*GenerationRecord{}
	for rows.Next() {
		record := &GenerationRecord{}
		var userID, projectID, assetID sql.NullString

		err := rows.Scan(
			&record.ID, &userID, &projectID, &record.Prompt, &record.NegativePrompt,
			&record.Model, &record.Width, &record.Height, &record.Steps, &record.Seed,
			&assetID, &record.CreatedAt,
		)
		if err != nil {
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
