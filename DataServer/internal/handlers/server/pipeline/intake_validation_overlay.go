package pipeline

import (
	"fmt"
	"sort"
	"strings"

	"github.com/gin-gonic/gin"
)

func validateSubmitOverlays(overlays []SubmitOverlay) []gin.H {
	if len(overlays) == 0 {
		return nil
	}
	details := make([]gin.H, 0)
	seen := make(map[string]struct{}, len(overlays))
	replacements := make([]SubmitOverlay, 0)
	for i, overlay := range overlays {
		path := fmt.Sprintf("overlays.%d", i)
		id := strings.TrimSpace(overlay.ID)
		if id == "" {
			details = append(details, gin.H{"path": path + ".id", "issue": "empty"})
		}
		if _, exists := seen[id]; exists && id != "" {
			details = append(details, gin.H{"path": path + ".id", "issue": "duplicate"})
		}
		seen[id] = struct{}{}
		if strings.TrimSpace(overlay.AssetID) == "" && strings.TrimSpace(overlay.DriveFileID) == "" {
			details = append(details, gin.H{"path": path + ".asset_id", "issue": "asset_id_or_drive_file_id_required"})
		}
		if overlay.StartFrame < 0 {
			details = append(details, gin.H{"path": path + ".start_frame", "issue": "out_of_range"})
		}
		if overlay.FrameCount <= 0 {
			details = append(details, gin.H{"path": path + ".frame_count", "issue": "out_of_range"})
		}
		if overlay.Mode != "replace" && overlay.Mode != "composite" {
			details = append(details, gin.H{"path": path + ".mode", "issue": "unsupported_value", "allowed": []string{"replace", "composite"}})
		}
		if overlay.AudioMode != "" && overlay.AudioMode != "preserve_final_audio" {
			details = append(details, gin.H{"path": path + ".audio_mode", "issue": "unsupported_value", "allowed": []string{"preserve_final_audio"}})
		}
		if overlay.URL != "" && !isAcceptedAssetURL(strings.TrimSpace(overlay.URL)) {
			details = append(details, gin.H{"path": path + ".url", "issue": "unsupported_scheme"})
		}
		if overlay.SHA256 != "" && !manifestRefSHA256Regexp.MatchString(strings.TrimSpace(overlay.SHA256)) {
			details = append(details, gin.H{"path": path + ".sha256", "issue": "malformed"})
		}
		if overlay.Mode == "replace" {
			replacements = append(replacements, overlay)
		}
	}
	sort.SliceStable(replacements, func(i, j int) bool {
		if replacements[i].StartFrame != replacements[j].StartFrame {
			return replacements[i].StartFrame < replacements[j].StartFrame
		}
		return replacements[i].ID < replacements[j].ID
	})
	for i := 1; i < len(replacements); i++ {
		if replacements[i].StartFrame < replacements[i-1].StartFrame+replacements[i-1].FrameCount {
			details = append(details, gin.H{"path": fmt.Sprintf("overlays.%s", replacements[i].ID), "issue": "replace_overlap", "overlaps": replacements[i-1].ID})
		}
	}
	return details
}
