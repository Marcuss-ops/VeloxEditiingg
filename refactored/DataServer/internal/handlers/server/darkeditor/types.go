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

// YouTubeGrabRequest is the request for YouTube thumbnail grab
type YouTubeGrabRequest struct {
	URL string `json:"url"`
}

// YouTubeGrabResponse is the response for YouTube thumbnail grab
type YouTubeGrabResponse struct {
	Filename string `json:"filename"`
	VideoID  string `json:"video_id"`
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

// ============== YOUTUBE INTEGRATION TYPES ==============

// SetThumbnailRequest is the request for setting a YouTube thumbnail
type SetThumbnailRequest struct {
	VideoID   string `json:"video_id" binding:"required"`
	Filename  string `json:"filename" binding:"required"`
	ChannelID string `json:"channel_id"` // Optional, will use default if not specified
}

// SetThumbnailResponse is the response for setting a YouTube thumbnail
type SetThumbnailResponse struct {
	Success      bool   `json:"success"`
	VideoID      string `json:"video_id"`
	VideoURL     string `json:"video_url"`
	Message      string `json:"message"`
	ThumbnailURL string `json:"thumbnail_url,omitempty"`
}

// GetChannelsResponse is the response for listing available channels
type GetChannelsResponse struct {
	Channels []ChannelInfo `json:"channels"`
}

// ChannelInfo represents a YouTube channel
type ChannelInfo struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Thumbnail string `json:"thumbnail"`
}

// ============== DRIVE INTEGRATION TYPES ==============

// UploadToDriveRequest is the request for uploading to Drive
type UploadToDriveRequest struct {
	Filename       string `json:"filename" binding:"required"`
	FolderName     string `json:"folder_name"`  // Optional: create/use folder with this name
	FolderID       string `json:"folder_id"`    // Optional: use specific folder ID
	ProjectName    string `json:"project_name"` // Optional: for project-based organization
	ShareWithEmail string `json:"share_with"`   // Optional: email to share with
}

// UploadToDriveResponse is the response for Drive upload
type UploadToDriveResponse struct {
	Success     bool   `json:"success"`
	FileID      string `json:"file_id,omitempty"`
	WebViewLink string `json:"web_view_link,omitempty"`
	FolderLink  string `json:"folder_link,omitempty"`
	Message     string `json:"message,omitempty"`
	Error       string `json:"error,omitempty"`
}

// DriveCreateFolderRequest is the request for creating a Drive folder
type DriveCreateFolderRequest struct {
	Name     string `json:"name" binding:"required"`
	ParentID string `json:"parent_id"` // Optional: parent folder ID
}

// DriveCreateFolderResponse is the response for Drive folder creation
type DriveCreateFolderResponse struct {
	Success bool   `json:"success"`
	ID      string `json:"id,omitempty"`
	Name    string `json:"name,omitempty"`
	Message string `json:"message,omitempty"`
	Error   string `json:"error,omitempty"`
}

// ListFilesResponse is the response for listing files
type ListFilesResponse struct {
	Files []FileInfo `json:"files"`
}

// FileInfo represents a file in Drive
type FileInfo struct {
	ID           string `json:"id"`
	Name         string `json:"name"`
	MimeType     string `json:"mime_type"`
	WebViewLink  string `json:"web_view_link"`
	Size         int64  `json:"size"`
	CreatedTime  string `json:"created_time"`
	ModifiedTime string `json:"modified_time"`
}

// ============== LOGGING TYPES ==============

// ClientLogRequest is the request for client-side logging
type ClientLogRequest struct {
	Level    string                 `json:"level"`
	Message  string                 `json:"message"`
	Metadata map[string]interface{} `json:"metadata,omitempty"`
}
