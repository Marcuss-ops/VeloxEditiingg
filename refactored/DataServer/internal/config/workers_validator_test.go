package config

import (
	"strings"
	"testing"
)

// TestValidateProductionWorkers pins the canonical two-worker rule.
// The four fail modes are: too few, too many, duplicates, and "empty
// duplicated as same string" (which lands on the unique check, not
// the count check — see the doc comment on ValidateProductionWorkers).
func TestValidateProductionWorkers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name      string
		ids       []string
		wantErr   bool
		errSubstr string
	}{
		{
			name:    "two distinct ids",
			ids:     []string{"velox-worker-1", "velox-worker-2"},
			wantErr: false,
		},
		{
			name:    "two distinct with realistic names",
			ids:     []string{"CHANGE_ME_WORKER_1", "CHANGE_ME_WORKER_2"},
			wantErr: false,
		},
		{
			name:      "single id (one-worker prod is forbidden)",
			ids:       []string{"velox-worker-1"},
			wantErr:   true,
			errSubstr: "production requires exactly 2",
		},
		{
			name:      "empty list (zero-worker prod is forbidden)",
			ids:       []string{},
			wantErr:   true,
			errSubstr: "production requires exactly 2",
		},
		{
			name:      "nil list (zero-worker prod is forbidden)",
			ids:       nil,
			wantErr:   true,
			errSubstr: "production requires exactly 2",
		},
		{
			name:      "three ids (third worker is forbidden)",
			ids:       []string{"a", "b", "c"},
			wantErr:   true,
			errSubstr: "production requires exactly 2",
		},
		{
			name:      "two duplicates (passes len check, fails unique check)",
			ids:       []string{"velox-worker-1", "velox-worker-1"},
			wantErr:   true,
			errSubstr: "production worker IDs must be unique",
		},
		{
			name:      "two empty strings (caught by the unique check, not the count)",
			ids:       []string{"", ""},
			wantErr:   true,
			errSubstr: "production worker IDs must be unique",
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
			name:      "wildcard-only list caught by wildcard guard (count check never reached)",
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

// TestProductionWorkerCount pins the constant to 2 so an inadvertent
// refactor that bumps it to e.g. 3 (without updating the env templates,
// Ansible inventor and group_vars in lockstep) fails this test loudly.
func TestProductionWorkerCount(t *testing.T) {
	t.Parallel()
	if productionWorkerCount != 2 {
		t.Fatalf("productionWorkerCount = %d, want 2 — "+
			"changing the constant requires updating deploy/* and DataServer/data/ansible/* "+
			"templates in lockstep, do NOT relax this without a co-PR",
			productionWorkerCount)
	}
}
