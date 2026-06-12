package worker

import (
	"testing"

	"velox-worker-agent/pkg/api"
)

func TestResolveJobRunID(t *testing.T) {
	cases := []struct {
		name string
		job  *api.Job
		want string
	}{
		{
			name: "direct field",
			job: &api.Job{
				JobRunID: "run-direct",
			},
			want: "run-direct",
		},
		{
			name: "parameters fallback",
			job: &api.Job{
				Parameters: map[string]interface{}{
					"job_run_id": "run-param",
				},
			},
			want: "run-param",
		},
		{
			name: "legacy run_id fallback",
			job: &api.Job{
				Parameters: map[string]interface{}{
					"run_id": "run-legacy",
				},
			},
			want: "run-legacy",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := resolveJobRunID(tc.job); got != tc.want {
				t.Fatalf("resolveJobRunID() = %q, want %q", got, tc.want)
			}
		})
	}
}
