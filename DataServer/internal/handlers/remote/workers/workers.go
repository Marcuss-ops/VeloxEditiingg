package workers

import (
	"sync"
	"time"
)

type PendingUpload struct {
	VideoPath  string                 `json:"video_path"`
	WorkerID   string                 `json:"worker_id"`
	JobRunID   string                 `json:"job_run_id"`
	UploadInfo map[string]interface{} `json:"upload_info"`
	ReceivedAt time.Time              `json:"received_at"`
}

type UploadManager struct {
	mu    sync.RWMutex
	files map[string]*PendingUpload
}
