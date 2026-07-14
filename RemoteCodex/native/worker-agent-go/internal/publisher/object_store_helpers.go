package publisher

import (
	"fmt"
	"strings"
)

// isTransientS3Error reports whether an upload error is safe to retry.
func isTransientS3Error(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	for _, frag := range []string{
		"503", "service unavailable", "throttl", "requesttimeout",
		"slow down", "internalerror", "connection reset", "broken pipe",
		"i/o timeout", "deadline exceeded", "temporary failure",
	} {
		if strings.Contains(msg, frag) {
			return true
		}
	}
	for _, frag := range []string{"408 request timeout", "429 too many requests"} {
		if strings.Contains(msg, frag) {
			return true
		}
	}
	return false
}

// parseS3URL accepts s3://bucket/key or https://host/bucket/key.
func parseS3URL(rawURL, fallbackUploadID string) (bucket, key, uploadID string, err error) {
	if strings.HasPrefix(rawURL, "s3://") {
		rest := strings.TrimPrefix(rawURL, "s3://")
		slash := strings.IndexByte(rest, '/')
		if slash < 0 {
			return "", "", "", fmt.Errorf("object-store-multipart: invalid s3 URL %q", rawURL)
		}
		bucket = rest[:slash]
		key = rest[slash+1:]
		uploadID = fallbackUploadID
		return
	}
	if strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "https://") {
		rest := rawURL
		if i := strings.Index(rest, "://"); i >= 0 {
			rest = rest[i+3:]
		}
		if i := strings.IndexByte(rest, '/'); i >= 0 {
			rest = rest[i+1:]
		} else {
			rest = ""
		}
		slash := strings.IndexByte(rest, '/')
		if slash < 0 {
			bucket = rest
			key = ""
		} else {
			bucket = rest[:slash]
			key = rest[slash+1:]
		}
		uploadID = fallbackUploadID
		return
	}
	return "", "", "", fmt.Errorf("object-store-multipart: unrecognized URL scheme: %q", rawURL)
}

// sortParts orders multipart parts by part number for completion.
func sortParts(parts []s3PartSummary) {
	for i := 1; i < len(parts); i++ {
		j := i
		for j > 0 && parts[j-1].PartNumber > parts[j].PartNumber {
			parts[j-1], parts[j] = parts[j], parts[j-1]
			j--
		}
	}
}
