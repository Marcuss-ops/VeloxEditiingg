package pipeline

import (
	"context"
	"fmt"
	"sort"
	"strings"

	driveapi "velox-server/internal/integrations/drive"

	"velox-shared/assetref"
)

const (
	maxStockFolderDepth = 8
	maxStockFolderFiles = 512
)

// StockFolderLister is the narrow Drive capability required to expand a
// folder-backed stock pool. The handler expands folders once at intake; the
// worker then receives ordinary self-contained velox-drive file references.
type StockFolderLister interface {
	ListFiles(ctx context.Context, folderID string, pageSize int) ([]driveapi.File, error)
}

// expandCreatorStockFolders replaces every Drive folder reference inside
// payload.scenes[].stock with the video files found in that folder (including
// nested folders). A folder is an input collection, never a renderable media
// asset, so failing to expand it is a hard intake error rather than a worker
// fallback.
func expandCreatorStockFolders(ctx context.Context, payload map[string]interface{}, lister StockFolderLister) error {
	if payload == nil {
		return nil
	}
	rawScenes, ok := payload["scenes"].([]interface{})
	if !ok {
		return nil
	}
	for sceneIndex, rawScene := range rawScenes {
		scene, ok := rawScene.(map[string]interface{})
		if !ok {
			continue
		}
		rawStock, ok := scene["stock"]
		if !ok {
			continue
		}
		expanded, changed, err := expandStockValue(ctx, rawStock, lister)
		if err != nil {
			return fmt.Errorf("scenes[%d].stock: %w", sceneIndex, err)
		}
		if changed {
			scene["stock"] = expanded
		}
	}
	return nil
}

func expandStockValue(ctx context.Context, raw interface{}, lister StockFolderLister) ([]interface{}, bool, error) {
	var entries []interface{}
	switch value := raw.(type) {
	case []interface{}:
		entries = value
	case map[string]interface{}:
		entries = []interface{}{value}
	default:
		return nil, false, nil
	}

	var expanded []interface{}
	changed := false
	for _, entry := range entries {
		folderURL := stockEntryFolderURL(entry)
		if folderURL == "" {
			expanded = append(expanded, entry)
			continue
		}
		changed = true
		if lister == nil {
			return nil, false, fmt.Errorf("Drive folder %q requires the Drive listing capability", folderURL)
		}
		folderID, err := assetref.ParseDriveFolderID(folderURL)
		if err != nil {
			return nil, false, err
		}
		files, err := listStockFolderVideos(ctx, lister, folderID.String(), 0, make(map[string]struct{}))
		if err != nil {
			return nil, false, err
		}
		if len(files) == 0 {
			return nil, false, fmt.Errorf("Drive folder %q contains no video files", folderURL)
		}
		for _, file := range files {
			asset := map[string]interface{}{
				"drive_file_id": file.ID,
				"url":           "velox-drive://" + file.ID,
			}
			if duration := file.VideoMediaMetadata.DurationMillis; duration > 0 {
				asset["duration_ms"] = duration
			}
			expanded = append(expanded, asset)
		}
	}
	return expanded, changed, nil
}

func stockEntryFolderURL(entry interface{}) string {
	object, ok := entry.(map[string]interface{})
	if !ok {
		return ""
	}
	for _, key := range []string{"url", "folder_link", "drive_link", "link"} {
		value, ok := object[key].(string)
		if !ok {
			continue
		}
		candidate := strings.TrimSpace(value)
		if _, err := assetref.ParseDriveFolderID(candidate); err == nil {
			return candidate
		}
	}
	return ""
}

func listStockFolderVideos(ctx context.Context, lister StockFolderLister, folderID string, depth int, visited map[string]struct{}) ([]driveapi.File, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if depth > maxStockFolderDepth {
		return nil, fmt.Errorf("Drive stock folder nesting exceeds %d levels", maxStockFolderDepth)
	}
	if _, ok := visited[folderID]; ok {
		return nil, nil
	}
	visited[folderID] = struct{}{}
	files, err := lister.ListFiles(ctx, folderID, 50)
	if err != nil {
		return nil, fmt.Errorf("list Drive folder %q: %w", folderID, err)
	}
	sort.SliceStable(files, func(i, j int) bool {
		left, right := strings.ToLower(files[i].Name), strings.ToLower(files[j].Name)
		if left == right {
			return files[i].ID < files[j].ID
		}
		return left < right
	})

	result := make([]driveapi.File, 0, len(files))
	for _, file := range files {
		if strings.TrimSpace(file.ID) == "" {
			continue
		}
		if file.MimeType == "application/vnd.google-apps.folder" {
			nested, err := listStockFolderVideos(ctx, lister, file.ID, depth+1, visited)
			if err != nil {
				return nil, err
			}
			result = append(result, nested...)
			continue
		}
		if !isDriveVideo(file) {
			continue
		}
		result = append(result, file)
		if len(result) > maxStockFolderFiles {
			return nil, fmt.Errorf("Drive stock folder contains more than %d video files", maxStockFolderFiles)
		}
	}
	sort.SliceStable(result, func(i, j int) bool {
		left, right := strings.ToLower(result[i].Name), strings.ToLower(result[j].Name)
		if left == right {
			return result[i].ID < result[j].ID
		}
		return left < right
	})
	return result, nil
}

func isDriveVideo(file driveapi.File) bool {
	mime := strings.ToLower(strings.TrimSpace(file.MimeType))
	if strings.HasPrefix(mime, "video/") {
		return true
	}
	name := strings.ToLower(strings.TrimSpace(file.Name))
	for _, suffix := range []string{".mp4", ".mov", ".m4v", ".webm", ".mkv", ".avi", ".mxf"} {
		if strings.HasSuffix(name, suffix) {
			return true
		}
	}
	return false
}
