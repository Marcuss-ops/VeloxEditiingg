package drive

import (
	"fmt"
	"time"
)

// FakeCreateDriveFolder creates a synthetic folder entry (test-only).
func FakeCreateDriveFolder(req CreateDriveFolderRequest) (string, error) {
	newID := fmt.Sprintf("folder_%d", time.Now().UnixNano())
	return newID, nil
}

// FakeUploadText simulates a file upload (test-only).
func FakeUploadText(req UploadTextRequest) (string, error) {
	return fmt.Sprintf("https://drive.google.com/file/d/text_%d/view", time.Now().UnixNano()), nil
}
