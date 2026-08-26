package artifactsstore

import (
	"fmt"
	"time"
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
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339, "2006-01-02 15:04:05"} {
		if parsed, err := time.Parse(layout, value); err == nil {
			return parsed, nil
		}
	}
	return time.Time{}, fmt.Errorf("artifactsstore: invalid %s %q", field, value)
}
