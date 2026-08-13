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
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"velox-server/internal/credentials"
)

const defaultDownloadMaxBytes int64 = 256 * 1024 * 1024

// UploadFile uploads a file to Drive
func (s *Service) UploadFile(ctx context.Context, filePath string, folderID string, deliveryID string) (*UploadResult, error) {
	folderID = strings.TrimSpace(folderID)
	if folderID == "" {
		return nil, fmt.Errorf("DELIVERY_TARGET_REQUIRED: an explicit Drive destination is required")
	}
	token, err := s.getToken(ctx)
	if err != nil {
		return nil, err
	}
	if existing, err := s.findExistingDelivery(ctx, folderID, deliveryID); err != nil {
		return nil, fmt.Errorf("check existing Drive delivery: %w", err)
	} else if existing != nil {
		log.Printf("[CLOUD] Reusing existing Drive upload for delivery %s (ID: %s)", deliveryID, existing.ID)
		return &UploadResult{
			Success:     true,
			FileID:      existing.ID,
			WebViewLink: existing.WebViewLink,
			FolderLink:  fmt.Sprintf("https://drive.google.com/drive/folders/%s", folderID),
		}, nil
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
		"name":    fileName,
		"parents": []string{folderID},
	}
	// Stamp deliveryID as a public properties key so retries of the same
	// delivery are traceable to the canonical delivery_id without requiring
	// the drive.appdata OAuth scope.
	if deliveryID != "" {
		meta["properties"] = map[string]string{
			"velox_delivery_id": deliveryID,
		}
	}

	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshal upload metadata: %w", err)
	}
	h := make(textproto.MIMEHeader)
	h.Set("Content-Type", "application/json; charset=UTF-8")
	part, err := writer.CreatePart(h)
	if err != nil {
		return nil, fmt.Errorf("create metadata part: %w", err)
	}
	if _, err := part.Write(metaJSON); err != nil {
		return nil, fmt.Errorf("write metadata part: %w", err)
	}

	// Write file content part
	h = make(textproto.MIMEHeader)
	h.Set("Content-Type", "application/octet-stream")
	part, err = writer.CreatePart(h)
	if err != nil {
		return nil, fmt.Errorf("create content part: %w", err)
	}
	if _, err := io.Copy(part, file); err != nil {
		return nil, fmt.Errorf("copy file into upload body: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close multipart body: %w", err)
	}

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
		raw, _ := io.ReadAll(resp.Body)
		return &UploadResult{
			Success: false,
			Error:   fmt.Sprintf("upload failed (%d): %s", resp.StatusCode, credentials.JSON(string(raw))),
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

// findExistingDelivery looks up the marker written by UploadFile before a
// retry creates another remote object. Drive does not provide an idempotency
// key for multipart uploads; the durable delivery ID in properties is the
// application-level idempotency record.
func (s *Service) findExistingDelivery(ctx context.Context, folderID, deliveryID string) (*File, error) {
	deliveryID = strings.TrimSpace(deliveryID)
	if deliveryID == "" {
		return nil, nil
	}
	escape := func(value string) string {
		return strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), `'`, `\'`)
	}
	query := fmt.Sprintf("'%s' in parents and trashed=false and properties has { key='velox_delivery_id' and value='%s' }", escape(folderID), escape(deliveryID))
	fields := "files(id,name,webViewLink,parents,properties)"
	endpoint := fmt.Sprintf("/files?q=%s&pageSize=1&fields=%s", url.QueryEscape(query), url.QueryEscape(fields))
	var result struct {
		Files []File `json:"files"`
	}
	if err := s.doAPIRequest(ctx, http.MethodGet, endpoint, nil, &result); err != nil {
		return nil, err
	}
	if len(result.Files) == 0 {
		return nil, nil
	}
	return &result.Files[0], nil
}

// UploadVideoWithAccessToken performs one upload with the short-lived token
// issued by the central credential vault.
func (s *Service) UploadVideoWithAccessToken(ctx context.Context, filePath, projectName, parentFolderID, deliveryID, accessToken string) (*UploadResult, error) {
	if strings.TrimSpace(accessToken) == "" {
		return nil, fmt.Errorf("drive access token is required")
	}
	return s.UploadVideo(WithAccessToken(ctx, accessToken), filePath, projectName, parentFolderID, deliveryID)
}

// DownloadFile downloads a file from Drive
func (s *Service) DownloadFile(ctx context.Context, fileID string, destPath string) error {
	return s.DownloadFileWithLimit(ctx, fileID, destPath, defaultDownloadMaxBytes)
}

// DownloadFileWithLimit enforces the byte cap while reading the response.
// Checking Content-Length alone is insufficient because an upstream may omit
// it or lie about it.
func (s *Service) DownloadFileWithLimit(ctx context.Context, fileID string, destPath string, maxBytes int64) error {
	if maxBytes <= 0 {
		return fmt.Errorf("download byte limit must be positive")
	}
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

	if resp.ContentLength > maxBytes {
		return fmt.Errorf("download exceeds byte limit")
	}
	// Copy one byte beyond the limit so a lying or missing Content-Length is
	// detected before the destination can be accepted by the asset resolver.
	written, err := io.Copy(out, io.LimitReader(resp.Body, maxBytes+1))
	if err != nil {
		return fmt.Errorf("failed to write file content: %w", err)
	}
	if written > maxBytes {
		return fmt.Errorf("download exceeds byte limit")
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
	var failures []string
	for _, file := range files {
		// Skip folders
		if file.MimeType == "application/vnd.google-apps.folder" {
			continue
		}

		// Drive folders are allowed to contain duplicate names. The Tyson
		// folder does exactly that (10 variants for each clip_00N.mp4), so a
		// basename-only destination would silently overwrite valid assets.
		ext := filepath.Ext(file.Name)
		stem := strings.TrimSuffix(file.Name, ext)
		destPath := filepath.Join(destDir, fmt.Sprintf("%s_%s%s", stem, file.ID, ext))
		if err := s.DownloadFile(ctx, file.ID, destPath); err != nil {
			log.Printf("[WARN] Failed to download %s: %v", file.Name, err)
			failures = append(failures, fmt.Sprintf("%s (%s): %v", file.Name, file.ID, err))
			continue
		}
		downloadedFiles = append(downloadedFiles, destPath)
	}

	if len(failures) > 0 {
		return downloadedFiles, fmt.Errorf("folder download incomplete: %d/%d files failed: %s", len(failures), len(files), strings.Join(failures, "; "))
	}
	return downloadedFiles, nil
}

// UploadVideo uploads a video file below the explicitly selected parent
// folder. An empty parent is a contract violation: Drive must never invent
// a project folder or route output to an implicit root destination.
// deliveryID is passed through from the runner as an idempotency key.
func (s *Service) UploadVideo(ctx context.Context, filePath string, projectName string, parentFolderID string, deliveryID string) (*UploadResult, error) {
	parentFolderID = strings.TrimSpace(parentFolderID)
	if parentFolderID == "" {
		return nil, fmt.Errorf("DELIVERY_TARGET_REQUIRED: an explicit Drive destination is required")
	}

	folderID := parentFolderID
	if strings.TrimSpace(projectName) != "" {
		folder, err := s.GetOrCreateFolder(ctx, projectName, parentFolderID)
		if err != nil {
			return nil, fmt.Errorf("failed to get/create project folder: %w", err)
		}
		folderID = folder.ID
	}
	return s.UploadFile(ctx, filePath, folderID, deliveryID)
}
