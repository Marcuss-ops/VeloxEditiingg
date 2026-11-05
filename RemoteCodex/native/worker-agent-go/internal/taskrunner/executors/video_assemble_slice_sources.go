package executors

import (
	"encoding/json"
	"net/url"
	"strings"

	"velox-shared/contract"
)

// sliceRuntimeDriveFileIDs resolves Drive transport locators from the
// Producer's runtime manifest without changing the hash-bound render plan.
// The runtime manifest is joined to the plan by its stable asset ID/key.
func sliceRuntimeDriveFileIDs(raw interface{}) map[string]string {
	encoded, err := json.Marshal(raw)
	if err != nil || len(encoded) == 0 || string(encoded) == "null" {
		return nil
	}
	var assets []struct {
		AssetID     string `json:"asset_id"`
		AssetKey    string `json:"asset_key"`
		DriveFileID string `json:"drive_file_id"`
		URL         string `json:"url"`
		SourceURI   string `json:"source_uri"`
	}
	if err := json.Unmarshal(encoded, &assets); err != nil {
		return nil
	}
	byIdentity := make(map[string]string, len(assets)*2)
	for _, asset := range assets {
		fileID := strings.TrimSpace(asset.DriveFileID)
		if fileID == "" {
			for _, locator := range []string{asset.URL, asset.SourceURI} {
				u, parseErr := url.Parse(strings.TrimSpace(locator))
				if parseErr != nil || !strings.EqualFold(u.Scheme, "velox-drive") {
					continue
				}
				fileID = strings.TrimSpace(u.Host)
				if fileID == "" {
					fileID = strings.TrimPrefix(strings.TrimSpace(u.Path), "/")
				}
				if fileID != "" {
					break
				}
			}
		}
		if fileID == "" {
			continue
		}
		for _, identity := range []string{asset.AssetID, asset.AssetKey} {
			if identity = strings.TrimSpace(identity); identity != "" {
				byIdentity[identity] = fileID
			}
		}
	}
	return byIdentity
}

func isSliceDriveSource(asset contract.AssetRefV2, runtimeDriveIDs map[string]string) bool {
	if asset.SourceKind == "drive" || strings.HasPrefix(strings.ToLower(strings.TrimSpace(asset.AssetID)), "drive:") {
		return true
	}
	return sliceDriveFileID(asset, runtimeDriveIDs) != ""
}

func sliceDriveFileID(asset contract.AssetRefV2, runtimeDriveIDs map[string]string) string {
	if fileID := strings.TrimSpace(asset.DriveFileID); fileID != "" {
		return fileID
	}
	for _, identity := range []string{asset.AssetID, asset.AssetKey} {
		if fileID := strings.TrimSpace(runtimeDriveIDs[strings.TrimSpace(identity)]); fileID != "" {
			return fileID
		}
	}
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(asset.AssetID)), "drive:") {
		return strings.TrimSpace(asset.AssetID[len("drive:"):])
	}
	return ""
}
