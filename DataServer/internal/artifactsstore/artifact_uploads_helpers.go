package artifactsstore

import (
	"fmt"
	"time"

	"velox-server/internal/persistedtime"
)

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

func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}

func parsePersistedWorkerTimestamp(value, field string) (time.Time, error) {
	parsed, err := persistedtime.Parse(value, field)
	if err != nil {
		return time.Time{}, fmt.Errorf("artifactsstore: %w", err)
	}
	return parsed, nil
}
