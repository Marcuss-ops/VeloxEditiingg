package store

import "time"

// Project represents a Dark Editor project.
type Project struct {
	ID         string                 `json:"id"`
	UserID     *string                `json:"user_id,omitempty"`
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	CanvasJSON map[string]interface{} `json:"canvas_json"`
	PreviewURL string                 `json:"preview_url,omitempty"`
	IsTemplate bool                   `json:"is_template"`
	IsPublic   bool                   `json:"is_public"`
	Metadata   map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt  time.Time              `json:"created_at"`
	UpdatedAt  time.Time              `json:"updated_at"`
	FolderID   *string                `json:"folder_id,omitempty"`
}

// Folder represents a Dark Editor project grouping.
type Folder struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	ParentID      *string   `json:"parent_id,omitempty"`
	DriveFolderID *string   `json:"drive_folder_id,omitempty"`
	YouTubeGroup  *string   `json:"youtube_group,omitempty"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
}

// Asset represents a project asset.
type Asset struct {
	ID               string                 `json:"id"`
	ProjectID        *string                `json:"project_id,omitempty"`
	UserID           *string                `json:"user_id,omitempty"`
	Type             string                 `json:"type"`
	Filename         string                 `json:"filename"`
	OriginalFilename string                 `json:"original_filename,omitempty"`
	StoragePath      string                 `json:"storage_path"`
	StorageType      string                 `json:"storage_type"`
	MimeType         string                 `json:"mime_type,omitempty"`
	SizeBytes        int64                  `json:"size_bytes,omitempty"`
	Width            int                    `json:"width,omitempty"`
	Height           int                    `json:"height,omitempty"`
	Metadata         map[string]interface{} `json:"metadata,omitempty"`
	CreatedAt        time.Time              `json:"created_at"`
}

// Template represents a reusable project template.
type Template struct {
	ID          string                 `json:"id"`
	Name        string                 `json:"name"`
	Category    string                 `json:"category,omitempty"`
	Description string                 `json:"description,omitempty"`
	CanvasJSON  map[string]interface{} `json:"canvas_json"`
	PreviewURL  string                 `json:"preview_url,omitempty"`
	IsPublic    bool                   `json:"is_public"`
	CreatedBy   *string                `json:"created_by,omitempty"`
	UsageCount  int                    `json:"usage_count"`
	Tags        []string               `json:"tags,omitempty"`
	CreatedAt   time.Time              `json:"created_at"`
	UpdatedAt   time.Time              `json:"updated_at"`
}

// TempFile represents a temporary uploaded file.
type TempFile struct {
	ID               string    `json:"id"`
	Filename         string    `json:"filename"`
	OriginalFilename string    `json:"original_filename,omitempty"`
	StoragePath      string    `json:"storage_path"`
	MimeType         string    `json:"mime_type,omitempty"`
	SizeBytes        int64     `json:"size_bytes,omitempty"`
	ExpiresAt        time.Time `json:"expires_at"`
	CreatedAt        time.Time `json:"created_at"`
}

// GenerationRecord represents an AI generation history entry.
type GenerationRecord struct {
	ID             string    `json:"id"`
	UserID         *string   `json:"user_id,omitempty"`
	ProjectID      *string   `json:"project_id,omitempty"`
	Prompt         string    `json:"prompt"`
	NegativePrompt string    `json:"negative_prompt,omitempty"`
	Model          string    `json:"model"`
	Width          int       `json:"width"`
	Height         int       `json:"height"`
	Steps          int       `json:"steps"`
	Seed           int       `json:"seed,omitempty"`
	AssetID        *string   `json:"asset_id,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

// ProjectListOptions defines options for listing projects.
type ProjectListOptions struct {
	UserID     string
	Type       string
	IsTemplate *bool
	IsPublic   *bool
	Limit      int
	Offset     int
	OrderBy    string
	OrderDir   string
}

// TemplateListOptions defines options for listing templates.
type TemplateListOptions struct {
	Category string
	IsPublic *bool
	Tags     []string
	Limit    int
	Offset   int
	OrderBy  string
	OrderDir string
}

// GenerationListOptions defines options for listing generation history.
type GenerationListOptions struct {
	UserID    string
	ProjectID string
	Model     string
	Limit     int
	Offset    int
}

// ProgressSnapshot represents a job progress record from the job_progress table.
type ProgressSnapshot struct {
	JobID         string  `json:"job_id"`
	AttemptNumber int     `json:"attempt_number"`
	Percent       float64 `json:"percent"`
	Stage         string  `json:"stage"`
	CurrentItem   int     `json:"current_item"`
	TotalItems    int     `json:"total_items"`
	Message       string  `json:"message"`
}
