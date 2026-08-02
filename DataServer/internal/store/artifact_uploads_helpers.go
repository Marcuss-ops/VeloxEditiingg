// Package store / artifact_uploads_helpers.go
//
// package-level SQL helpers shared by the artifact_uploads sessions and
// chunks files.
//
// nilOrString maps "" -> nil so the column stores NULL rather than "",
// matching the migration's nullable TEXT columns for expected_sha256 /
// received_sha256. Private to the store package. The artifacts package
// owns its own private copy in sqlite_upload_session_writer.go
// (scoped to the BeginUpload paired-insert path).
package store

import "time"

func nilOrString(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

func nilOrStringPtr(p *string) interface{} {
	if p == nil || *p == "" {
		return nil
	}
	return *p
}

func formatTimePtr(p *time.Time) interface{} {
	if p == nil || p.IsZero() {
		return nil
	}
	return p.UTC().Format(time.RFC3339)
}

func parseTimeRFC3339(t *time.Time, raw string) error {
	if raw == "" {
		*t = time.Time{}
		return nil
	}
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return err
	}
	*t = parsed
	return nil
}
