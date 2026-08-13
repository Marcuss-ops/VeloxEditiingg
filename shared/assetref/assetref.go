// Package assetref provides canonical Google Drive URL normalization.
//
// Google Drive exposes the same file through several URL shapes
// (drive.google.com/file/d/<ID>/view, drive.google.com/uc?id=<ID>,
// drive.google.com/open?id=<ID>, etc.). Comparing raw links across worker
// and master would treat the same file as different assets. ParseDriveFileID
// reduces any supported URL to the stable DriveFileID identifier so callers
// can key caches, snapshots and jobs by a single canonical value.
package assetref

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// ErrEmpty indicates the input URL was empty or whitespace-only.
var ErrEmpty = errors.New("assetref: empty Drive URL")

// AssetKey is the canonical logical identity of an input asset. URLs,
// filenames, and provider-specific IDs are normalized at the boundary and
// must not be used as interchangeable cache identities.
type AssetKey string

// ContentHash is the verified content identity of an asset. It is represented
// as lowercase hexadecimal SHA-256 at persistence and transport boundaries.
type ContentHash string

// ParseContentHash validates and canonicalizes a SHA-256 digest.
func ParseContentHash(raw string) (ContentHash, error) {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if len(raw) != 64 {
		return "", fmt.Errorf("assetref: content hash must be 64 hexadecimal characters")
	}
	for _, r := range raw {
		if (r < '0' || r > '9') && (r < 'a' || r > 'f') {
			return "", fmt.Errorf("assetref: content hash is not hexadecimal")
		}
	}
	return ContentHash(raw), nil
}

// String returns the canonical string representation.
func (h ContentHash) String() string { return string(h) }

// String returns the canonical string representation.
func (k AssetKey) String() string { return string(k) }

// Empty reports whether the key has no usable value.
func (k AssetKey) Empty() bool { return strings.TrimSpace(string(k)) == "" }

// NotDriveError indicates the URL does not point at drive.google.com.
type NotDriveError struct {
	URL string
}

func (e *NotDriveError) Error() string {
	return fmt.Sprintf("assetref: not a Google Drive URL: %q", e.URL)
}

// FolderError indicates the URL points to a Drive folder instead of a file.
type FolderError struct {
	URL string
}

func (e *FolderError) Error() string {
	return fmt.Sprintf("assetref: URL points to a Drive folder, not a file: %q", e.URL)
}

// NoIDError indicates the URL was recognized as a Drive file URL but no file
// ID could be extracted.
type NoIDError struct {
	URL string
}

func (e *NoIDError) Error() string {
	return fmt.Sprintf("assetref: Drive file ID not found in URL: %q", e.URL)
}

// DriveFileID is the canonical Google Drive file identifier extracted from a
// supported Drive URL. It is an opaque provider ID and must not be used
// interchangeably with AssetKey or ContentHash: only ParseDriveFileID (the
// single validation boundary) may construct one from a URL. Representing the
// ID as a distinct type (rather than a bare string) prevents a Drive file ID
// from being confused with a local asset ID or a content hash at call sites.
type DriveFileID string

// String returns the canonical string representation.
func (id DriveFileID) String() string { return string(id) }

// Empty reports whether the ID has no usable value.
func (id DriveFileID) Empty() bool { return strings.TrimSpace(string(id)) == "" }

// ParseDriveFileID extracts the canonical Google Drive file ID from a
// supported URL form, preserving the case of the opaque provider ID.
func ParseDriveFileID(rawURL string) (DriveFileID, error) {
	rawURL = strings.TrimSpace(rawURL)
	if rawURL == "" {
		return "", ErrEmpty
	}

	u, err := url.Parse(rawURL)
	if err != nil {
		return "", fmt.Errorf("assetref: parse %q: %w", rawURL, err)
	}

	host := strings.ToLower(u.Host)
	if host != "drive.google.com" && host != "www.drive.google.com" {
		return "", &NotDriveError{URL: rawURL}
	}

	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	for _, p := range parts {
		if strings.EqualFold(p, "folders") {
			return "", &FolderError{URL: rawURL}
		}
	}

	for i := 0; i+2 < len(parts); i++ {
		if strings.EqualFold(parts[i], "file") && strings.EqualFold(parts[i+1], "d") {
			id := strings.TrimSpace(parts[i+2])
			if id != "" {
				return DriveFileID(id), nil
			}
		}
	}

	if id := strings.TrimSpace(u.Query().Get("id")); id != "" {
		return DriveFileID(id), nil
	}

	return "", &NoIDError{URL: rawURL}
}
