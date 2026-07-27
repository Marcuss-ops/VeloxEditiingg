// Package assetref provides canonical Google Drive URL normalization.
//
// Google Drive exposes the same file through several URL shapes
// (drive.google.com/file/d/<ID>/view, drive.google.com/uc?id=<ID>,
// drive.google.com/open?id=<ID>, etc.). Comparing raw links across worker
// and master would treat the same file as different assets. DriveFileID
// reduces any supported URL to the stable file identifier so callers can
// key caches, snapshots and jobs by a single canonical value.
package assetref

import (
	"errors"
	"fmt"
	"net/url"
	"strings"
)

// ErrEmpty indicates the input URL was empty or whitespace-only.
var ErrEmpty = errors.New("assetref: empty Drive URL")

// NotDriveError indicates the URL does not point at drive.google.com.
type NotDriveError struct {
	URL string
}

func (e *NotDriveError) Error() string {
	return fmt.Sprintf("assetref: not a Google Drive URL: %q", e.URL)
}

// FolderError indicates the URL points to a Drive folder instead of a file.
// Drive folders are collections, not assets, and must not be cached.
//
// The URL field is preserved for DEBUG-level logging and metrics; do not
// include these errors in user-facing responses without redaction.
type FolderError struct {
	URL string
}

func (e *FolderError) Error() string {
	return fmt.Sprintf("assetref: URL points to a Drive folder, not a file: %q", e.URL)
}

// NoIDError indicates the URL was recognized as a Drive file URL but no file
// ID could be extracted (malformed path, missing ?id=, empty ID segment).
//
// As with FolderError, the URL field is for internal observability only.
type NoIDError struct {
	URL string
}

func (e *NoIDError) Error() string {
	return fmt.Sprintf("assetref: Drive file ID not found in URL: %q", e.URL)
}

// DriveFileID extracts the canonical Google Drive file ID from any supported
// URL form:
//
//   - https://drive.google.com/file/d/<ID>[/view|preview|edit]
//   - https://drive.google.com/file/d/<ID>?usp=sharing
//   - https://drive.google.com/uc?id=<ID>
//   - https://drive.google.com/u/<N>/uc?id=<ID>
//   - https://drive.google.com/open?id=<ID>
//   - https://drive.google.com/thumbnail?id=<ID>
//
// A trailing /view, /preview, /edit, an arbitrary query string, or surrounding
// whitespace around the URL is tolerated. Folder URLs
// (drive.google.com/drive/folders/<ID>) return a *FolderError — they refer to
// a collection, not a file.
//
// The returned ID is case-preserving: Google Drive IDs are case-sensitive.
// Callers must treat the result as an opaque token.
func DriveFileID(rawURL string) (string, error) {
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

	// strings.Trim removes leading/trailing "/" so that "/file/d/ABC/" and
	// "//file/d/ABC" both yield a usable slice. When the path is "/" or
	// empty, Split returns [""] — a safe no-op for the loops below.
	parts := strings.Split(strings.Trim(u.Path, "/"), "/")
	for _, p := range parts {
		if strings.EqualFold(p, "folders") {
			return "", &FolderError{URL: rawURL}
		}
	}

	// Path form: .../file/d/<ID>[/suffix...] — case-insensitive on "file"+"d".
	for i := 0; i+2 < len(parts); i++ {
		if strings.EqualFold(parts[i], "file") && strings.EqualFold(parts[i+1], "d") {
			id := strings.TrimSpace(parts[i+2])
			if id != "" {
				return id, nil
			}
		}
	}

	// Query form: .../uc?id=<ID>, .../open?id=<ID>, .../thumbnail?id=<ID>.
	if id := strings.TrimSpace(u.Query().Get("id")); id != "" {
		return id, nil
	}

	return "", &NoIDError{URL: rawURL}
}
