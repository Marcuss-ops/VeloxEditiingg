package darkeditor

import "time"

// ============== CORE IMAGE TYPES ==============

// UploadResponse is the response for image upload
type UploadResponse struct {
	Filename string `json:"filename"`
	URL      string `json:"url"`
}

// FilterRequest is the request for applying a filter
type FilterRequest struct {
	Filename   string  `json:"filename"`
	FilterType string  `json:"filter_type"`
	Value      float64 `json:"value"`
}

// FilterResponse is the response for filter operations
type FilterResponse struct {
	Filename string `json:"filename"`
	URL      string `json:"url"`
}

// TransformRequest is the request for image transformation
type TransformRequest struct {
	Filename   string `json:"filename"`
	CropBox    []int  `json:"crop_box,omitempty"`
	ResizeDims []int  `json:"resize_dims,omitempty"`
}

// ExportRequest is the request for image export
type ExportRequest struct {
	Filename string `json:"filename"`
	Format   string `json:"format"`
	Quality  int    `json:"quality"`
}

// GenerateRequest is the request for AI image generation
type GenerateRequest struct {
	Prompt string `json:"prompt"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	Seed   int    `json:"seed"`
	Steps  int    `json:"steps"`
}

// GenerateResponse is the response for AI image generation
type GenerateResponse struct {
	Filename string `json:"filename"`
	URL      string `json:"url"`
	Prompt   string `json:"prompt"`
}

// UpscaleRequest is the request for image upscaling
type UpscaleRequest struct {
	Filename    string `json:"filename"`
	Scale       int    `json:"scale"`
	SaveInPlace bool   `json:"save_in_place"`
}

// UpscaleResponse is the response for image upscaling
type UpscaleResponse struct {
	Filename string `json:"filename"`
	URL      string `json:"url"`
	SavedAt  string `json:"saved_at,omitempty"`
}

// ============== PROJECT TYPES ==============

// SaveProjectRequest is the request for saving a project
type SaveProjectRequest struct {
	Name            string                 `json:"name"`
	CanvasJSON      map[string]interface{} `json:"canvas_json"`
	PreviewFilename string                 `json:"preview_filename"`
	Type            string                 `json:"type"`
	ID              string                 `json:"id,omitempty"`
}

// Project represents a saved project
type Project struct {
	ID         string                 `json:"id"`
	Name       string                 `json:"name"`
	Type       string                 `json:"type"`
	CanvasJSON map[string]interface{} `json:"canvas_json"`
	PreviewURL string                 `json:"preview_url"`
	CreatedAt  time.Time              `json:"created_at"`
	UpdatedAt  time.Time              `json:"updated_at"`
	FolderID   *string                `json:"folder_id,omitempty"`
}

// Folder represents a folder grouping for Dark Editor projects.
type Folder struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	ParentID  *string   `json:"parent_id,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

// CreateFolderRequest is the request for creating a folder
type CreateFolderRequest struct {
	Name     string  `json:"name" binding:"required"`
	ParentID *string `json:"parent_id"`
}

// UpdateFolderRequest is the request for updating a folder
type UpdateFolderRequest struct {
	Name     *string `json:"name"`
	ParentID *string `json:"parent_id"`
}

// AssignProjectFolderRequest is the request for linking a project to a folder
type AssignProjectFolderRequest struct {
	FolderID *string `json:"folder_id"`
}

// ============== BACKGROUND REMOVAL TYPES ==============

// RemoveBackgroundRequest is the request for background removal
type RemoveBackgroundRequest struct {
	Filename     string `json:"filename"`
	Model        string `json:"model"`         // "u2net", "u2netp", "isnet-general-use", etc.
	OutputFormat string `json:"output_format"` // "png", "webp"
	Async        bool   `json:"async"`         // Process asynchronously
}

// RemoveBackgroundResponse is the response for background removal
type RemoveBackgroundResponse struct {
	Filename   string `json:"filename"`
	URL        string `json:"url"`
	Processing bool   `json:"processing,omitempty"`
	TaskID     string `json:"task_id,omitempty"`
}

// BackgroundRemovalStatus represents the status of an async background removal task
type BackgroundRemovalStatus struct {
	TaskID    string    `json:"task_id"`
	Status    string    `json:"status"` // "pending", "processing", "completed", "failed"
	Filename  string    `json:"filename,omitempty"`
	URL       string    `json:"url,omitempty"`
	Error     string    `json:"error,omitempty"`
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at,omitempty"`
}

// ============== LOGGING TYPES ==============

// ClientLogRequest is the request for client-side logging
type ClientLogRequest struct {
	Level    string                 `json:"level"`
	Message  string                 `json:"message"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}
