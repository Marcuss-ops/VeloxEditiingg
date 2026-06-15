package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"
)

func (s *SQLiteStore) ListFolders(ctx context.Context) ([]*Folder, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT id, name, parent_id, drive_folder_id, youtube_group, created_at, updated_at FROM dark_editor_folders ORDER BY created_at ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	folders := []*Folder{}
	for rows.Next() {
		var folder Folder
		var parentID, driveFolderID, youtubeGroup sql.NullString
		var createdAt, updatedAt sql.NullString
		if err := rows.Scan(&folder.ID, &folder.Name, &parentID, &driveFolderID, &youtubeGroup, &createdAt, &updatedAt); err != nil {
			continue
		}
		if parentID.Valid {
			folder.ParentID = &parentID.String
		}
		if driveFolderID.Valid {
			folder.DriveFolderID = &driveFolderID.String
		}
		if youtubeGroup.Valid {
			folder.YouTubeGroup = &youtubeGroup.String
		}
		if createdAt.Valid && createdAt.String != "" {
			if t, err := time.Parse(time.RFC3339, createdAt.String); err == nil {
				folder.CreatedAt = t
			}
		}
		if updatedAt.Valid && updatedAt.String != "" {
			if t, err := time.Parse(time.RFC3339, updatedAt.String); err == nil {
				folder.UpdatedAt = t
			}
		}
		folders = append(folders, &folder)
	}
	return folders, nil
}

func (s *SQLiteStore) GetFolder(ctx context.Context, id string) (*Folder, error) {
	row := s.db.QueryRowContext(ctx, `SELECT id, name, parent_id, drive_folder_id, youtube_group, created_at, updated_at FROM dark_editor_folders WHERE id = ?`, id)
	var folder Folder
	var parentID, driveFolderID, youtubeGroup sql.NullString
	var createdAt, updatedAt sql.NullString
	err := row.Scan(&folder.ID, &folder.Name, &parentID, &driveFolderID, &youtubeGroup, &createdAt, &updatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if parentID.Valid {
		folder.ParentID = &parentID.String
	}
	if driveFolderID.Valid {
		folder.DriveFolderID = &driveFolderID.String
	}
	if youtubeGroup.Valid {
		folder.YouTubeGroup = &youtubeGroup.String
	}
	if createdAt.Valid && createdAt.String != "" {
		if t, err := time.Parse(time.RFC3339, createdAt.String); err == nil {
			folder.CreatedAt = t
		}
	}
	if updatedAt.Valid && updatedAt.String != "" {
		if t, err := time.Parse(time.RFC3339, updatedAt.String); err == nil {
			folder.UpdatedAt = t
		}
	}
	return &folder, nil
}

func (s *SQLiteStore) CreateFolder(ctx context.Context, folder *Folder) error {
	if folder.ID == "" {
		folder.ID = fmt.Sprintf("folder_%d", time.Now().UnixNano())
	}
	if folder.CreatedAt.IsZero() {
		folder.CreatedAt = time.Now()
	}
	folder.UpdatedAt = folder.CreatedAt

	var parentID interface{}
	if folder.ParentID != nil && *folder.ParentID != "" {
		parentID = *folder.ParentID
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO dark_editor_folders (id, name, parent_id, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?)`,
		folder.ID, folder.Name, parentID,
		folder.CreatedAt.Format(time.RFC3339), folder.UpdatedAt.Format(time.RFC3339),
	)
	return err
}

func (s *SQLiteStore) UpdateFolder(ctx context.Context, folder *Folder) error {
	folder.UpdatedAt = time.Now()

	var parentID interface{}
	if folder.ParentID != nil && *folder.ParentID != "" {
		parentID = *folder.ParentID
	}

	result, err := s.db.ExecContext(ctx,
		`UPDATE dark_editor_folders SET name=?, parent_id=?, updated_at=? WHERE id = ?`,
		folder.Name, parentID, folder.UpdatedAt.Format(time.RFC3339), folder.ID,
	)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("folder not found")
	}
	return nil
}

func (s *SQLiteStore) DeleteFolder(ctx context.Context, id string) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.ExecContext(ctx, `UPDATE dark_editor_projects SET folder_id = NULL WHERE folder_id = ?`, id); err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, `DELETE FROM dark_editor_folders WHERE id = ?`, id)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to check rows affected: %w", err)
	}
	if rows == 0 {
		return fmt.Errorf("folder not found")
	}
	return tx.Commit()
}

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

func (s *SQLiteStore) CreateAsset(ctx context.Context, asset *Asset) error {
	if asset.ID == "" {
		asset.ID = fmt.Sprintf("asset_%d", time.Now().UnixNano())
	}
	if asset.StorageType == "" {
		asset.StorageType = "local"
	}
	metadata, _ := json.Marshal(asset.Metadata)

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO dark_editor_assets (id, project_id, user_id, type, filename, original_filename, storage_path, storage_type, mime_type, size_bytes, width, height, metadata, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		asset.ID, asset.ProjectID, asset.UserID, asset.Type, asset.Filename,
		asset.OriginalFilename, asset.StoragePath, asset.StorageType,
		asset.MimeType, asset.SizeBytes, asset.Width, asset.Height,
		string(metadata), time.Now().Format(time.RFC3339),
	)
	return err
}

func (s *SQLiteStore) GetAsset(ctx context.Context, id string) (*Asset, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, project_id, user_id, type, filename, original_filename, storage_path, storage_type, mime_type, size_bytes, width, height, metadata, created_at
		 FROM dark_editor_assets WHERE id = ?`, id)

	asset := &Asset{}
	var metadata []byte
	var projectID, userID sql.NullString

	err := row.Scan(&asset.ID, &projectID, &userID, &asset.Type, &asset.Filename,
		&asset.OriginalFilename, &asset.StoragePath, &asset.StorageType,
		&asset.MimeType, &asset.SizeBytes, &asset.Width, &asset.Height,
		&metadata, &asset.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if projectID.Valid {
		asset.ProjectID = &projectID.String
	}
	if userID.Valid {
		asset.UserID = &userID.String
	}
	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &asset.Metadata); err != nil {
			asset.Metadata = make(map[string]interface{})
		}
	}
	return asset, nil
}

func (s *SQLiteStore) GetAssetByFilename(ctx context.Context, filename string) (*Asset, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, project_id, user_id, type, filename, original_filename, storage_path, storage_type, mime_type, size_bytes, width, height, metadata, created_at
		 FROM dark_editor_assets WHERE filename = ?`, filename)

	asset := &Asset{}
	var metadata []byte
	var projectID, userID sql.NullString

	err := row.Scan(&asset.ID, &projectID, &userID, &asset.Type, &asset.Filename,
		&asset.OriginalFilename, &asset.StoragePath, &asset.StorageType,
		&asset.MimeType, &asset.SizeBytes, &asset.Width, &asset.Height,
		&metadata, &asset.CreatedAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	if projectID.Valid {
		asset.ProjectID = &projectID.String
	}
	if userID.Valid {
		asset.UserID = &userID.String
	}
	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &asset.Metadata); err != nil {
			asset.Metadata = make(map[string]interface{})
		}
	}
	return asset, nil
}

func (s *SQLiteStore) DeleteAsset(ctx context.Context, id string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM dark_editor_assets WHERE id = ?`, id)
	return err
}

func (s *SQLiteStore) ListProjectAssets(ctx context.Context, projectID string) ([]*Asset, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT id, project_id, user_id, type, filename, original_filename, storage_path, storage_type, mime_type, size_bytes, width, height, metadata, created_at
		 FROM dark_editor_assets WHERE project_id = ? ORDER BY created_at DESC`, projectID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	assets := []*Asset{}
	for rows.Next() {
		asset := &Asset{}
		var metadata []byte
		var projectID, userID sql.NullString

		if err := rows.Scan(&asset.ID, &projectID, &userID, &asset.Type, &asset.Filename,
			&asset.OriginalFilename, &asset.StoragePath, &asset.StorageType,
			&asset.MimeType, &asset.SizeBytes, &asset.Width, &asset.Height,
			&metadata, &asset.CreatedAt); err != nil {
			continue
		}

		if projectID.Valid {
			asset.ProjectID = &projectID.String
		}
		if userID.Valid {
			asset.UserID = &userID.String
		}
		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &asset.Metadata); err != nil {
				asset.Metadata = make(map[string]interface{})
			}
		}
		assets = append(assets, asset)
	}
	return assets, nil
}

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

func (s *SQLiteStore) CreateTempFile(ctx context.Context, file *TempFile) error {
	if file.ID == "" {
		file.ID = fmt.Sprintf("tmp_%d", time.Now().UnixNano())
	}
	if file.CreatedAt.IsZero() {
		file.CreatedAt = time.Now()
	}
	if file.ExpiresAt.IsZero() {
		file.ExpiresAt = file.CreatedAt.Add(24 * time.Hour)
	}

	_, err := s.db.ExecContext(ctx,
		`INSERT INTO dark_editor_temp_files (id, filename, original_filename, storage_path, mime_type, size_bytes, expires_at, created_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		file.ID, file.Filename, file.OriginalFilename, file.StoragePath,
		file.MimeType, file.SizeBytes, file.ExpiresAt.Format(time.RFC3339), file.CreatedAt.Format(time.RFC3339),
	)
	return err
}

func (s *SQLiteStore) GetTempFile(ctx context.Context, filename string) (*TempFile, error) {
	row := s.db.QueryRowContext(ctx,
		`SELECT id, filename, original_filename, storage_path, mime_type, size_bytes, expires_at, created_at
		 FROM dark_editor_temp_files WHERE filename = ?`, filename)

	file := &TempFile{}
	var expiresAt, createdAt string

	err := row.Scan(&file.ID, &file.Filename, &file.OriginalFilename, &file.StoragePath,
		&file.MimeType, &file.SizeBytes, &expiresAt, &createdAt)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	file.ExpiresAt, _ = time.Parse(time.RFC3339, expiresAt)
	file.CreatedAt, _ = time.Parse(time.RFC3339, createdAt)
	return file, nil
}

func (s *SQLiteStore) DeleteTempFile(ctx context.Context, filename string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM dark_editor_temp_files WHERE filename = ?`, filename)
	return err
}

func (s *SQLiteStore) CleanupExpiredTempFiles(ctx context.Context) (int64, error) {
	result, err := s.db.ExecContext(ctx, `DELETE FROM dark_editor_temp_files WHERE expires_at < ?`, time.Now().Format(time.RFC3339))
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}
