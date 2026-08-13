package store

// AssetDownloadProgressView is the JSON-ready job-scoped latest state.
type AssetDownloadProgressView struct {
	JobID           string   `json:"job_id"`
	WorkerID        string   `json:"worker_id"`
	AssetKey        string   `json:"asset_key"`
	AssetID         string   `json:"asset_id"`
	Role            string   `json:"role"`
	State           string   `json:"state"`
	BytesDownloaded int64    `json:"bytes_downloaded"`
	BytesTotal      int64    `json:"bytes_total"`
	BytesPerSecond  float64  `json:"bytes_per_second"`
	ETASeconds      int64    `json:"eta_seconds"`
	Attempt         int      `json:"attempt"`
	SharedWaiters   int      `json:"shared_waiters"`
	CacheHit        bool     `json:"cache_hit"`
	UpdatedAt       string   `json:"updated_at"`
	ErrorCode       string   `json:"error_code,omitempty"`
	ErrorDetail     string   `json:"error_detail,omitempty"`
	TaskID          string   `json:"task_id,omitempty"`
	SceneIDs        []string `json:"scene_ids,omitempty"`
}
