package drive

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"mime/multipart"
	"net/http"
	"net/textproto"
	"os"
	"path/filepath"
	"regexp"
	"strings"
)

// UploadFile uploads a file to Drive
func (s *Service) UploadFile(ctx context.Context, filePath string, folderID string) (*UploadResult, error) {
	token, err := s.getToken(ctx)
	if err != nil {
		return nil, err
	}

	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("failed to get file info: %w", err)
	}

	fileName := filepath.Base(filePath)
	_ = fileInfo // silence unused variable warning - can be used for progress reporting

	// Create multipart upload
	var body bytes.Buffer
	writer := multipart.NewWriter(&body)

	// Write metadata part
	meta := map[string]interface{}{
		"name": fileName,
	}
	if folderID != "" {
		meta["parents"] = []string{folderID}
	}

	metaJSON, _ := json.Marshal(meta)
	h := make(textproto.MIMEHeader)
	h.Set("Content-Type", "application/json; charset=UTF-8")
	part, _ := writer.CreatePart(h)
	part.Write(metaJSON)

	// Write file content part
	h = make(textproto.MIMEHeader)
	h.Set("Content-Type", "application/octet-stream")
	part, _ = writer.CreatePart(h)
	io.Copy(part, file)

	writer.Close()

	// Create upload request
	uploadURL := "https://www.googleapis.com/upload/drive/v3/files?uploadType=multipart&fields=id,webViewLink"
	req, err := http.NewRequestWithContext(ctx, "POST", uploadURL, &body)
	if err != nil {
		return nil, fmt.Errorf("failed to create upload request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.AccessToken))
	req.Header.Set("Content-Type", writer.FormDataContentType())

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("upload failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		var errResp map[string]interface{}
		json.NewDecoder(resp.Body).Decode(&errResp)
		return &UploadResult{
			Success: false,
			Error:   fmt.Sprintf("upload failed (%d): %v", resp.StatusCode, errResp),
		}, nil
	}

	var result File
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, fmt.Errorf("failed to decode upload response: %w", err)
	}

	log.Printf("[CLOUD] Uploaded '%s' to Drive (ID: %s)", fileName, result.ID)

	folderLink := ""
	if folderID != "" {
		folderLink = fmt.Sprintf("https://drive.google.com/drive/folders/%s", folderID)
	}

	return &UploadResult{
		Success:     true,
		FileID:      result.ID,
		WebViewLink: result.WebViewLink,
		FolderLink:  folderLink,
	}, nil
}

// DownloadFile downloads a file from Drive
func (s *Service) DownloadFile(ctx context.Context, fileID string, destPath string) error {
	token, err := s.getToken(ctx)
	if err != nil {
		return err
	}

	downloadURL := fmt.Sprintf("https://www.googleapis.com/drive/v3/files/%s?alt=media", fileID)
	req, err := http.NewRequestWithContext(ctx, "GET", downloadURL, nil)
	if err != nil {
		return fmt.Errorf("failed to create download request: %w", err)
	}

	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token.AccessToken))

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("download failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode >= 400 {
		return fmt.Errorf("download failed with status %d", resp.StatusCode)
	}

	// Create destination directory if needed
	if err := os.MkdirAll(filepath.Dir(destPath), 0755); err != nil {
		return fmt.Errorf("failed to create destination directory: %w", err)
	}

	// Create destination file
	out, err := os.Create(destPath)
	if err != nil {
		return fmt.Errorf("failed to create destination file: %w", err)
	}
	defer out.Close()

	// Copy the content
	if _, err := io.Copy(out, resp.Body); err != nil {
		return fmt.Errorf("failed to write file content: %w", err)
	}

	log.Printf("[DRIVE] Downloaded file %s to %s", fileID, destPath)
	return nil
}

// DownloadFilesFromFolder downloads all files from a Drive folder
func (s *Service) DownloadFilesFromFolder(ctx context.Context, folderID string, destDir string) ([]string, error) {
	// Extract folder ID from URL if needed
	if strings.Contains(folderID, "drive.google.com") {
		re := regexp.MustCompile(`folders/([a-zA-Z0-9-_]+)`)
		matches := re.FindStringSubmatch(folderID)
		if len(matches) > 1 {
			folderID = matches[1]
		} else {
			re = regexp.MustCompile(`[?&]id=([a-zA-Z0-9-_]+)`)
			matches = re.FindStringSubmatch(folderID)
			if len(matches) > 1 {
				folderID = matches[1]
			}
		}
	}

	// List files in folder
	files, err := s.ListFiles(ctx, folderID, 0)
	if err != nil {
		return nil, fmt.Errorf("failed to list folder contents: %w", err)
	}

	if err := os.MkdirAll(destDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create destination directory: %w", err)
	}

	var downloadedFiles []string
	for _, file := range files {
		// Skip folders
		if file.MimeType == "application/vnd.google-apps.folder" {
			continue
		}

		destPath := filepath.Join(destDir, file.Name)
		if err := s.DownloadFile(ctx, file.ID, destPath); err != nil {
			log.Printf("[WARN] Failed to download %s: %v", file.Name, err)
			continue
		}
		downloadedFiles = append(downloadedFiles, destPath)
	}

	return downloadedFiles, nil
}

// UploadVideo uploads a video file to a project folder
func (s *Service) UploadVideo(ctx context.Context, filePath string, projectName string, parentFolderID string) (*UploadResult, error) {
	// Get or create project folder
	var folderID string
	if parentFolderID != "" {
		folderID = parentFolderID
	} else {
		folder, err := s.GetOrCreateFolder(ctx, projectName, "")
		if err != nil {
			return nil, fmt.Errorf("failed to get/create project folder: %w", err)
		}
		folderID = folder.ID
	}

	return s.UploadFile(ctx, filePath, folderID)
}
