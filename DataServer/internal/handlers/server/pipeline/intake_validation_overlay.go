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
		if err := normalizeSubmitOverlayWindow(&overlay); err != nil {
			details = append(details, gin.H{"path": path + ".end_frame", "issue": "invalid_window", "message": err.Error()})
		}
		if overlay.Mode == "composite" {
			details = append(details, gin.H{"path": path + ".mode", "issue": "requires_prepared_video_replacement", "message": "render the composite into a finished video asset and submit it during FINALIZE as visual_replacements"})
		} else if overlay.Mode != "replace" {
			details = append(details, gin.H{"path": path + ".mode", "issue": "unsupported_value", "allowed": []string{"replace"}})
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

func normalizeSubmitOverlayWindow(overlay *SubmitOverlay) error {
	if overlay == nil {
		return fmt.Errorf("overlay is nil")
	}
	if overlay.StartFrame < 0 {
		return fmt.Errorf("start_frame must be >= 0")
	}
	if overlay.EndFrame > 0 {
		if overlay.EndFrame <= overlay.StartFrame {
			return fmt.Errorf("end_frame must be greater than start_frame")
		}
		if overlay.FrameCount > 0 && overlay.StartFrame+overlay.FrameCount != overlay.EndFrame {
			return fmt.Errorf("end_frame must equal start_frame + frame_count")
		}
		overlay.FrameCount = overlay.EndFrame - overlay.StartFrame
		return nil
	}
	if overlay.FrameCount <= 0 {
		return fmt.Errorf("frame_count or end_frame must be positive")
	}
	overlay.EndFrame = overlay.StartFrame + overlay.FrameCount
	if overlay.EndFrame <= overlay.StartFrame {
		return fmt.Errorf("end_frame overflows the frame window")
	}
	return nil
}
