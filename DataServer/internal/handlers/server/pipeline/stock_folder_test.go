package pipeline

import (
	"context"
	"testing"

	driveapi "velox-server/internal/integrations/drive"
)

type stockFolderListerStub struct {
	files         map[string][]driveapi.File
	calls         []string
	metadata      map[string]*driveapi.File
	metadataCalls []string
}

func (s *stockFolderListerStub) ListFiles(_ context.Context, folderID string, _ int) ([]driveapi.File, error) {
	s.calls = append(s.calls, folderID)
	return append([]driveapi.File(nil), s.files[folderID]...), nil
}

func (s *stockFolderListerStub) GetFileMetadata(_ context.Context, fileID string) (*driveapi.File, error) {
	s.metadataCalls = append(s.metadataCalls, fileID)
	return s.metadata[fileID], nil
}

func TestExpandCreatorStockFoldersExpandsVideoFilesRecursively(t *testing.T) {
	lister := &stockFolderListerStub{files: map[string][]driveapi.File{
		"root-folder": {
			{ID: "b", Name: "clip_002.mp4", MimeType: "video/mp4", VideoMediaMetadata: struct {
				DurationMillis int64 `json:"durationMillis,omitempty,string"`
			}{DurationMillis: 2300}},
			{ID: "notes", Name: "notes.txt", MimeType: "text/plain"},
			{ID: "nested-folder", Name: "B-roll", MimeType: "application/vnd.google-apps.folder"},
			{ID: "a", Name: "clip_001.mp4", MimeType: "video/mp4", VideoMediaMetadata: struct {
				DurationMillis int64 `json:"durationMillis,omitempty,string"`
			}{DurationMillis: 1700}},
		},
		"nested-folder": {
			{ID: "c", Name: "clip_003.mov", MimeType: "video/quicktime"},
		},
	}}
	payload := map[string]interface{}{
		"scenes": []interface{}{
			map[string]interface{}{
				"scene_id": "mike-tyson",
				"stock": []interface{}{map[string]interface{}{
					"url": "https://drive.google.com/drive/folders/root-folder?usp=sharing",
				}},
			},
		},
	}

	if err := expandCreatorStockFolders(context.Background(), payload, lister); err != nil {
		t.Fatalf("expandCreatorStockFolders: %v", err)
	}
	stock := payload["scenes"].([]interface{})[0].(map[string]interface{})["stock"].([]interface{})
	if len(stock) != 3 {
		t.Fatalf("expanded stock = %d entries, want 3", len(stock))
	}
	first := stock[0].(map[string]interface{})
	if first["drive_file_id"] != "a" || first["url"] != "velox-drive://a" || first["duration_ms"] != int64(1700) {
		t.Fatalf("first expanded stock = %#v", first)
	}
	if got := stock[2].(map[string]interface{})["url"]; got != "velox-drive://c" {
		t.Fatalf("nested stock URL = %#v, want velox-drive://c", got)
	}
	if len(lister.calls) != 2 || lister.calls[0] != "root-folder" || lister.calls[1] != "nested-folder" {
		t.Fatalf("folder calls = %#v, want root then nested", lister.calls)
	}
}

func TestExpandCreatorStockFoldersUsesCachedMetadataFallback(t *testing.T) {
	lister := &stockFolderListerStub{
		files: map[string][]driveapi.File{
			"root-folder": {
				{ID: "a", Name: "clip-a.mp4", MimeType: "video/mp4"},
				{ID: "a", Name: "clip-a-copy.mp4", MimeType: "video/mp4"},
			},
		},
		metadata: map[string]*driveapi.File{
			"a": {ID: "a", VideoMediaMetadata: struct {
				DurationMillis int64 `json:"durationMillis,omitempty,string"`
			}{DurationMillis: 2300}},
		},
	}
	payload := map[string]interface{}{"scenes": []interface{}{map[string]interface{}{
		"stock": []interface{}{map[string]interface{}{"url": "https://drive.google.com/drive/folders/root-folder"}},
	}}}

	if err := expandCreatorStockFolders(context.Background(), payload, lister); err != nil {
		t.Fatalf("expandCreatorStockFolders: %v", err)
	}
	stock := payload["scenes"].([]interface{})[0].(map[string]interface{})["stock"].([]interface{})
	if len(stock) != 2 || stock[0].(map[string]interface{})["duration_ms"] != int64(2300) ||
		stock[1].(map[string]interface{})["duration_ms"] != int64(2300) {
		t.Fatalf("expanded stock metadata = %#v", stock)
	}
	if len(lister.metadataCalls) != 1 || lister.metadataCalls[0] != "a" {
		t.Fatalf("metadata calls = %#v, want one lookup for asset a", lister.metadataCalls)
	}
}

func TestExpandCreatorStockFoldersRequiresLister(t *testing.T) {
	payload := map[string]interface{}{
		"scenes": []interface{}{map[string]interface{}{
			"stock": []interface{}{map[string]interface{}{
				"url": "https://drive.google.com/drive/folders/root-folder",
			}},
		}},
	}
	if err := expandCreatorStockFolders(context.Background(), payload, nil); err == nil {
		t.Fatal("folder stock accepted without Drive listing capability")
	}
}
