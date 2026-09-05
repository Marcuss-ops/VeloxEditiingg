// Package news / parse_test.go — pins the parsePublishedAt fallback ladder.
package news

import (
	"testing"
)

func TestParsePublishedAt(t *testing.T) {
	cases := []struct {
		raw      string
		wantOK   bool
		wantZero bool
	}{
		{`2026-09-05T10:00:00Z`, true, false},            // RFC3339 (canonical)
		{`2026-09-05T10:00:00.123456789Z`, true, false},  // RFC3339Nano
		{"Fri, 05 Sep 2026 10:00:00 +0000", true, false}, // RFC1123Z
		{"Fri, 05 Sep 2026 10:00:00 UTC", true, false},   // RFC1123
		{"2026-09-05", true, false},                      // date-only
		{"", true, true},                                 // empty = explicitly unknown (zero time, no error)
		{"not-a-date", false, true},                      // garbage = error, zero time
		{"05/09/2026", false, true},                      // unsupported layout = error
	}

	for _, tc := range cases {
		got, err := parsePublishedAt(tc.raw)
		if tc.wantOK && err != nil {
			t.Errorf("parsePublishedAt(%q) unexpected error: %v", tc.raw, err)
			continue
		}
		if !tc.wantOK && err == nil {
			t.Errorf("parsePublishedAt(%q) expected error, got %v", tc.raw, got)
			continue
		}
		if tc.wantZero && !got.IsZero() {
			t.Errorf("parsePublishedAt(%q) = %v, want zero time", tc.raw, got)
		}
		if !tc.wantZero && got.IsZero() {
			t.Errorf("parsePublishedAt(%q) = zero time, want a parsed value", tc.raw)
		}
	}
}
