package contract

import "testing"

func TestRequiredVideoAssemblyExecutorRoutesPendingRuntimeAssetsToCopy(t *testing.T) {
	payload := map[string]interface{}{
		PayloadKeyCompiledRenderPlanJSON: `{"plan_version":2,"assets":[{"asset_id":"prepared","kind":"prepared_video_fragment"}]}`,
		"runtime_assets_pending":         true,
	}
	if got := RequiredVideoAssemblyExecutor(payload); got != VideoAssemblyCopyExecutorID {
		t.Fatalf("executor = %q, want %q", got, VideoAssemblyCopyExecutorID)
	}
}

func TestRequiredVideoAssemblyExecutorRoutesFinalizedStockRoleToCopy(t *testing.T) {
	payload := map[string]interface{}{
		PayloadKeyCompiledRenderPlanJSON: `{"plan_version":2,"assets":[{"asset_id":"prepared","kind":"prepared_video_fragment"}]}`,
		"runtime_assets":                 []interface{}{map[string]interface{}{"asset_id": "prepared", "role": "stock", "drive_file_id": "drive-1"}},
	}
	if got := RequiredVideoAssemblyExecutor(payload); got != VideoAssemblyCopyExecutorID {
		t.Fatalf("executor = %q, want %q", got, VideoAssemblyCopyExecutorID)
	}
}
