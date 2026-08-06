package grpcserver

import "testing"

// TestMaxParallelJobsFromCapabilities_SingleWire locks the canonical wire
// representation of worker capacity: capabilities.host.max_parallel_jobs.
// The legacy top-level capabilities.max_parallel_jobs mirror was removed
// together with its reader, so a top-level-only shape MUST read as 0 — a
// worker that still publishes the old shape is effectively capacity-unknown
// until the v3-only protocol gate and worker rollout converge.
func TestMaxParallelJobsFromCapabilities_SingleWire(t *testing.T) {
	cases := []struct {
		name string
		caps map[string]interface{}
		want int
	}{
		{name: "nil map", caps: nil, want: 0},
		{name: "empty map", caps: map[string]interface{}{}, want: 0},
		{name: "canonical host shape float64", caps: map[string]interface{}{
			"host": map[string]interface{}{"max_parallel_jobs": float64(4)},
		}, want: 4},
		{name: "canonical host shape int", caps: map[string]interface{}{
			"host": map[string]interface{}{"max_parallel_jobs": 4},
		}, want: 4},
		{name: "canonical host shape int32", caps: map[string]interface{}{
			"host": map[string]interface{}{"max_parallel_jobs": int32(4)},
		}, want: 4},
		{name: "canonical host shape int64", caps: map[string]interface{}{
			"host": map[string]interface{}{"max_parallel_jobs": int64(4)},
		}, want: 4},
		{name: "legacy top-level mirror ONLY is ignored", caps: map[string]interface{}{
			"max_parallel_jobs": float64(8),
		}, want: 0},
		{name: "host wins when both shapes present", caps: map[string]interface{}{
			"max_parallel_jobs": float64(8),
			"host":              map[string]interface{}{"max_parallel_jobs": float64(4)},
		}, want: 4},
		{name: "host block without key", caps: map[string]interface{}{
			"host": map[string]interface{}{"hostname": "w1"},
		}, want: 0},
		{name: "wrong host block type", caps: map[string]interface{}{
			"host": "not-a-map",
		}, want: 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := maxParallelJobsFromCapabilities(tc.caps); got != tc.want {
				t.Fatalf("maxParallelJobsFromCapabilities(%v) = %d, want %d", tc.caps, got, tc.want)
			}
		})
	}
}
