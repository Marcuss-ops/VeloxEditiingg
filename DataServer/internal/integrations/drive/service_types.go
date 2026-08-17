package drive

import (
	"net/http"
	"sync"

	"velox-server/internal/config"
)

// File represents a Google Drive file
type File struct {
	ID          string            `json:"id"`
	Name        string            `json:"name"`
	MimeType    string            `json:"mimeType"`
	Parents     []string          `json:"parents,omitempty"`
	Properties  map[string]string `json:"properties,omitempty"`
	WebViewLink string            `json:"webViewLink,omitempty"`
	IconLink    string            `json:"iconLink,omitempty"`
	Size        int64             `json:"size,omitempty,string"`
	// VideoMediaMetadata is returned by Drive for video assets. Duration is
	// useful when a folder is expanded into canonical stock references before
	// enqueueing a narrated render.
	VideoMediaMetadata struct {
		DurationMillis int64 `json:"durationMillis,omitempty,string"`
	} `json:"videoMediaMetadata,omitempty"`
	CreatedTime  string `json:"createdTime,omitempty"`
	ModifiedTime string `json:"modifiedTime,omitempty"`
	Trashed      bool   `json:"trashed,omitempty"`
}

// Folder represents a Google Drive folder
type Folder struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// UploadResult represents the result of a file upload
type UploadResult struct {
	Success     bool   `json:"success"`
	FileID      string `json:"file_id,omitempty"`
	WebViewLink string `json:"web_view_link,omitempty"`
	FolderLink  string `json:"folder_link,omitempty"`
	Error       string `json:"error,omitempty"`
	// NetworkMS is the time spent in Drive HTTP round-trips (the upload
	// transfer plus resumable init/status queries). LocalBufferMS is the
	// time spent reading the artifact from local disk into the upload
	// buffer. Together they partition the upload wall time so operators can
	// tell a slow network path from local I/O/buffering.
	NetworkMS     int64 `json:"network_ms,omitempty"`
	LocalBufferMS int64 `json:"local_buffer_ms,omitempty"`
}

// Service provides Google Drive API operations
type Service struct {
	oauthCfg     *OAuth2Config
	tokenManager *TokenManager
	httpClient   *http.Client
	mu           sync.RWMutex
	currentToken *Token
}

// ServiceConfig holds configuration for Drive service
type ServiceConfig struct {
	ClientID     string
	ClientSecret string
	RedirectURI  string
	TokensDir    string
	// PublicRESTURL is supplied by the validated application Config. It is
	// used only to derive the OAuth callback when RedirectURI is omitted.
	PublicRESTURL string
	Credentials   config.CredentialsConfig
}
