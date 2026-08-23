package workers

import (
	"encoding/json"
	"testing"
)

func TestInt64FromHeartbeatExtra(t *testing.T) {
	cases := []struct {
		name string
		v    interface{}
		want int64
		ok   bool
	}{
		{"missing", nil, 0, false},
		{"int64", int64(42), 42, true},
		{"int", int(42), 42, true},
		{"int32", int32(42), 42, true},
		{"float64", float64(42), 42, true},
		{"float64-fraction", float64(42.9), 42, true},
		{"string", "42", 42, true},
		{"string-invalid", "abc", 0, false},
		{"json.Number", json.Number("42"), 42, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			extra := map[string]interface{}{}
			if tc.v != nil {
				extra["jobs_completed"] = tc.v
			}
			got, ok := int64FromHeartbeatExtra(extra, "jobs_completed")
			if ok != tc.ok || got != tc.want {
				t.Errorf("int64FromHeartbeatExtra() = (%d, %v), want (%d, %v)", got, ok, tc.want, tc.ok)
			}
		})
	}
}
