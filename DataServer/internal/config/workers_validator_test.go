package config

import (
	"strings"
	"testing"
)

// TestValidateProductionWorkers pins the production allowlist invariants:
//   - non-empty allowlist is required (a stray empty CSV is rejected)
//   - '*' wildcard is rejected (defense in depth — also caught at load time)
//   - blank IDs are rejected (parseCommaList already drops them, but a
//     future direct caller must still fail closed)
//   - duplicate IDs are rejected
//   - any N >= 1 distinct non-wildcard IDs is accepted
func TestValidateProductionWorkers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		ids       []string
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "single explicit id",
			ids:     []string{"velox-worker-1"},
			wantErr: false,
		},
		{
			name:    "two distinct ids",
			ids:     []string{"velox-worker-1", "velox-worker-2"},
			wantErr: false,
		},
		{
			name:    "three distinct ids (any N >= 1 is OK)",
			ids:     []string{"a", "b", "c"},
			wantErr: false,
		},
		{
			name:    "five distinct ids (any N >= 1 is OK)",
			ids:     []string{"a", "b", "c", "d", "e"},
			wantErr: false,
		},
		{
			name:    "two distinct with realistic names",
			ids:     []string{"CHANGE_ME_WORKER_1", "CHANGE_ME_WORKER_2"},
			wantErr: false,
		},
		{
			name:      "empty list (zero-worker prod is forbidden)",
			ids:       []string{},
			wantErr:   true,
			errSubstr: "must not be empty",
		},
		{
			name:      "nil list (zero-worker prod is forbidden)",
			ids:       nil,
			wantErr:   true,
			errSubstr: "must not be empty",
		},
		{
			name:      "duplicate ids",
			ids:       []string{"velox-worker-1", "velox-worker-1"},
			wantErr:   true,
			errSubstr: "duplicate worker ID",
		},
		{
			name:      "blank id alongside a real id is rejected as empty (loop fires before len check)",
			ids:       []string{"", "valid-id"},
			wantErr:   true,
			errSubstr: "must not contain empty IDs",
		},
		{
			name:      "wildcard rejected (defense-in-depth inside validator)",
			ids:       []string{"*", "velox-worker-2"},
			wantErr:   true,
			errSubstr: "must not contain '*'",
		},
		{
			name:      "single wildcard also rejected",
			ids:       []string{"*"},
			wantErr:   true,
			errSubstr: "must not contain '*'",
		},
		{
			name:      "wildcard-only list caught by wildcard guard (size check never reached)",
			ids:       []string{"*", "*"},
			wantErr:   true,
			errSubstr: "must not contain '*'",
		},
	}

	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := ValidateProductionWorkers(tc.ids)
			if (err != nil) != tc.wantErr {
				t.Fatalf("ids=%v err=%v wantErr=%v", tc.ids, err, tc.wantErr)
			}
			if tc.wantErr && tc.errSubstr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.errSubstr) {
					t.Fatalf("ids=%v err=%v does not contain %q",
						tc.ids, err, tc.errSubstr)
				}
			}
		})
	}
}
