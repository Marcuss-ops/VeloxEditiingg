package drive

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
)

const googleDocMimeType = "application/vnd.google-apps.document"

// CreateGoogleDoc creates a native Google Docs document from plain text.
func (s *Service) CreateGoogleDoc(ctx context.Context, title, content, parentID, deliveryID string) (*UploadResult, error) {
	title, content, parentID = strings.TrimSpace(title), strings.TrimSpace(content), strings.TrimSpace(parentID)
	if title == "" || content == "" {
		return nil, fmt.Errorf("google doc requires a title and non-empty content")
	}
	_, err := s.getToken(ctx)
	if err != nil {
		return nil, err
	}
	meta := map[string]interface{}{"name": title, "mimeType": googleDocMimeType}
	if parentID != "" {
		meta["parents"] = []string{parentID}
	}
	if deliveryID = strings.TrimSpace(deliveryID); deliveryID != "" {
		meta["properties"] = map[string]string{"velox_delivery_id": deliveryID}
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return nil, fmt.Errorf("marshal google doc metadata: %w", err)
	}
	var file File
	if err := s.doAPIRequest(ctx, http.MethodPost, "/files?fields=id,name,mimeType,webViewLink,parents", strings.NewReader(string(metaJSON)), &file); err != nil {
		return nil, fmt.Errorf("create google doc metadata: %w", err)
	}
	if file.ID == "" {
		return nil, fmt.Errorf("create google doc metadata returned empty file id")
	}
	batch, err := json.Marshal(map[string]interface{}{"requests": []map[string]interface{}{{
		"insertText": map[string]interface{}{
			"location": map[string]interface{}{"index": 1},
			"text":     content + "\n",
		},
	}}})
	if err != nil {
		return nil, fmt.Errorf("marshal google doc content request: %w", err)
	}
	if err := s.doAPIRequest(ctx, http.MethodPost, "https://docs.googleapis.com/v1/documents/"+file.ID+":batchUpdate", strings.NewReader(string(batch)), nil); err != nil {
		return nil, fmt.Errorf("insert google doc content: %w", err)
	}
	link := file.WebViewLink
	if link == "" && file.ID != "" {
		link = "https://docs.google.com/document/d/" + file.ID + "/edit"
	}
	return &UploadResult{Success: true, FileID: file.ID, WebViewLink: link}, nil
}
