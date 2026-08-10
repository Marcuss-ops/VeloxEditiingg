package pipeline

import (
	"fmt"
	"strings"

	"github.com/gin-gonic/gin"
)

func validateSubmitAudioTracks(req SubmitJobRequest) []gin.H {
	var details []gin.H
	// Per-audio-track validation: at least one of source_url or asset_id
	// must be non-empty (the Master resolves asset_id → source_url before
	// the worker sees the payload). When source_url IS provided, it must
	// match the http(s) + velox-asset:// + velox-drive:// allow-list.
	// Role must be in the closed enum (when supplied). Volume in [0.0, 2.0].
	for i, track := range req.AudioTracks {
		pathPrefix := fmt.Sprintf("audio_tracks.%d", i)
		trimmedURL := strings.TrimSpace(track.SourceURL)
		trimmedAsset := strings.TrimSpace(track.AssetID)
		if trimmedURL == "" && trimmedAsset == "" {
			details = append(details, gin.H{
				"path":  pathPrefix,
				"issue": "empty",
				"hint":  "provide source_url or asset_id",
			})
		} else if trimmedURL != "" && !assetURLRegexp.MatchString(trimmedURL) {
			details = append(details, gin.H{
				"path":     pathPrefix + ".source_url",
				"issue":    "unsupported_scheme",
				"observed": trimmedURL,
				"allowed":  []string{"https://", "http://", "velox-asset://", "velox-drive://"},
			})
		}
		if track.Role != "" && !containsString(audioRoleValues, track.Role) {
			details = append(details, gin.H{
				"path":     pathPrefix + ".role",
				"issue":    "unsupported_value",
				"observed": track.Role,
				"allowed":  audioRoleValues,
			})
		}
		if track.Volume < 0.0 || track.Volume > 2.0 {
			details = append(details, gin.H{
				"path":     pathPrefix + ".volume",
				"issue":    "out_of_range",
				"min":      0.0,
				"max":      2.0,
				"observed": track.Volume,
			})
		}
	}

	return details
}
