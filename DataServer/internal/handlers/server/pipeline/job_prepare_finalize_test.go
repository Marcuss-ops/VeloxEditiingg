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
		{name: "runtime_payload_null_still_pending", req: SubmitJobRequest{RuntimePayload: map[string]interface{}{"runtime_assets": nil}}, want: false},
		{name: "runtime_payload_empty_still_pending", req: SubmitJobRequest{RuntimePayload: map[string]interface{}{"runtime_assets": []interface{}{}}}, want: false},
		{name: "runtime_payload_non_empty_assets", req: SubmitJobRequest{RuntimePayload: map[string]interface{}{"runtime_assets": []interface{}{map[string]interface{}{"asset_id": "a"}}}}, want: true},
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

func TestPrepareCompiledPlanWaitsForEveryRuntimeBinding(t *testing.T) {
	complete := true
	req := SubmitJobRequest{
		CompiledRenderPlanJSON: `{"plan_version":2,"final_audio":{"asset_id":"audio-1"},"assets":[{"asset_id":"stock-1"},{"asset_id":"overlay-1"}]}`,
		RuntimeAssetsComplete:  &complete,
		RuntimeAssets: []map[string]interface{}{
			{"asset_id": "audio-1"},
			{"asset_id": "overlay-1"},
		},
	}
	if runtimeAssetsCompleteForPrepare(req) {
		t.Fatal("partial runtime assets must remain pending even when the producer marks them complete")
	}
	req.RuntimeAssets = append(req.RuntimeAssets, map[string]interface{}{"asset_id": "stock-1", "role": "stock"})
	if !runtimeAssetsCompleteForPrepare(req) {
		t.Fatal("all plan assets and final audio are bound; PRE should be single-stage")
	}
}

func TestPrepareStockPoolWaitsForResolvedPoolMember(t *testing.T) {
	complete := true
	req := SubmitJobRequest{
		CompiledRenderPlanJSON: `{"plan_version":2,"stock_pool":{"source_kind":"drive","drive_folder_id":"folder-1","selection":"shuffle"},"final_audio":{"asset_id":"audio-1"},"assets":[]}`,
		RuntimeAssetsComplete:  &complete,
		RuntimeAssets:          []map[string]interface{}{{"asset_id": "audio-1"}},
	}
	if runtimeAssetsCompleteForPrepare(req) {
		t.Fatal("stock_pool must remain pending until the Master resolves at least one folder member")
	}
	req.RuntimeAssets = append(req.RuntimeAssets, map[string]interface{}{"asset_id": "drive-file-1", "drive_file_id": "drive-file-1", "stock_pool_member": true})
	if !runtimeAssetsCompleteForPrepare(req) {
		t.Fatal("resolved stock_pool member and final audio should complete PRE")
	}
}
