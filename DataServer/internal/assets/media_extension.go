package assets

import "strings"

// media_extension.go owns the media-extension inference helper.
// ResolveAndRegister (in registration.go) calls into it to derive
// the on-disk file extension from the source's suggested name or
// MIME type before staging the asset bytes.

// extensionFromName returns the canonical on-disk extension for an
// asset, preferring a name-suffix when available and falling back to a
// MIME-derived extension for the supported media families.
func extensionFromName(name, mimeType string) string {
	name = strings.TrimSpace(name)
	if name != "" {
		if idx := strings.LastIndex(name, "."); idx >= 0 {
			ext := name[idx:]
			if ext != "" {
				return ext
			}
		}
	}
	switch {
	case strings.HasPrefix(mimeType, "audio/mpeg"):
		return ".mp3"
	case strings.HasPrefix(mimeType, "audio/wav"):
		return ".wav"
	case strings.HasPrefix(mimeType, "audio/mp4"), strings.HasPrefix(mimeType, "audio/m4a"):
		return ".m4a"
	case strings.HasPrefix(mimeType, "video/mp4"):
		return ".mp4"
	case strings.HasPrefix(mimeType, "image/jpeg"):
		return ".jpg"
	case strings.HasPrefix(mimeType, "image/png"):
		return ".png"
	case strings.HasPrefix(mimeType, "image/webp"):
		return ".webp"
	}
	return ".bin"
}
