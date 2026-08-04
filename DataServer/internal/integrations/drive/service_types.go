package drive

import (
	"net/http"
	"sync"
)

// File represents a Google Drive file
type File struct {
	ID          string   `json:"id"`
	Name        string   `json:"name"`
	MimeType    string   `json:"mimeType"`
	Parents     []string `json:"parents,omitempty"`
	WebViewLink string   `json:"webViewLink,omitempty"`
	IconLink    string   `json:"iconLink,omitempty"`
	Size        int64    `json:"size,omitempty,string"`
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
}
