package workers

// Global upload manager instance
var globalUploadManager = NewUploadManager()

// NewUploadManager creates a new upload manager
func NewUploadManager() *UploadManager {
	return &UploadManager{
		files: make(map[string]*PendingUpload),
	}
}

// AddPendingUpload adds a pending upload
func (um *UploadManager) AddPendingUpload(jobID string, upload *PendingUpload) {
	um.mu.Lock()
	defer um.mu.Unlock()
	um.files[jobID] = upload
}

// GetPendingUpload gets a pending upload
func (um *UploadManager) GetPendingUpload(jobID string) *PendingUpload {
	um.mu.RLock()
	defer um.mu.RUnlock()
	return um.files[jobID]
}

// RemovePendingUpload removes a pending upload
func (um *UploadManager) RemovePendingUpload(jobID string) {
	um.mu.Lock()
	defer um.mu.Unlock()
	delete(um.files, jobID)
}

// GetUploadManager returns the global upload manager
func GetUploadManager() *UploadManager {
	return globalUploadManager
}
