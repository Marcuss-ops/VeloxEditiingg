package drive

import (
	driveSvc "velox-server/internal/services/drive"
)

// DriveFolder represents a Drive folder entry
type DriveFolder = driveSvc.DriveFolder

// DriveFoldersResponse is the API response
type DriveFoldersResponse struct {
	Success bool          `json:"success"`
	Folders []DriveFolder `json:"folders"`
	Count   int           `json:"count"`
}
