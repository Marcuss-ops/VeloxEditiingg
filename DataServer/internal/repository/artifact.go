package repository

// Artifact is the persistence projection of a job output artifact row.
type Artifact struct {
	ID              string  `json:"id"`
	JobID           string  `json:"job_id"`
	VideoTitle      string  `json:"video_title,omitempty"`
	AttemptID       int     `json:"attempt_id,omitempty"`
	Type            string  `json:"type"`
	StorageProvider string  `json:"storage_provider"`
	StorageKey      string  `json:"storage_key,omitempty"`
	StorageURL      string  `json:"storage_url,omitempty"`
	LocalPath       string  `json:"local_path,omitempty"`
	SHA256          string  `json:"sha256,omitempty"`
	SizeBytes       int64   `json:"size_bytes"`
	DurationSeconds float64 `json:"duration_seconds,omitempty"`
	DurationMs      int64   `json:"duration_ms,omitempty"`
	Status          string  `json:"status"`
	VerifiedAt      string  `json:"verified_at,omitempty"`
	MimeType        string  `json:"mime_type,omitempty"`
	CreatedAt       string  `json:"created_at"`
}
