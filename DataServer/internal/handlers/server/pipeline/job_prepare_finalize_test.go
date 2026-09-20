package pipeline

import "testing"

func TestPrepareRuntimeAssetsCompletionRequiresExplicitEmptyListOptIn(t *testing.T) {
	complete := true
	incomplete := false
	cases := []struct {
		name string
		req  SubmitJobRequest
		want bool
	}{
		{name: "omitted", req: SubmitJobRequest{}, want: false},
		{name: "non_empty_assets", req: SubmitJobRequest{RuntimeAssets: []map[string]interface{}{{"asset_id": "a"}}}, want: true},
		{name: "empty_assets_still_pending", req: SubmitJobRequest{RuntimeAssets: []map[string]interface{}{}}, want: false},
		{name: "explicit_empty_complete", req: SubmitJobRequest{RuntimeAssets: []map[string]interface{}{}, RuntimeAssetsComplete: &complete}, want: true},
		{name: "explicit_incomplete_overrides_assets", req: SubmitJobRequest{RuntimeAssets: []map[string]interface{}{{"asset_id": "a"}}, RuntimeAssetsComplete: &incomplete}, want: false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := runtimeAssetsCompleteForPrepare(tc.req)
			if got != tc.want {
				t.Fatalf("runtime assets complete=%v, want %v", got, tc.want)
			}
		})
	}
}
