package enqueue

import (
	"testing"

	"velox-shared/contract"
)

func TestResolveInternalExecutorID_UsesDriveSliceExecutorForDriveStock(t *testing.T) {
	payload := map[string]interface{}{
		contract.PayloadKeyCompiledRenderPlanJSON: `{"assets":[{"asset_id":"drive:file-01","kind":"video","source_kind":"drive","drive_file_id":"file-01"}]}`,
		contract.PayloadKeyCompiledRenderPlanSHA:  "present",
	}
	if got := resolveInternalExecutorID(payload); got != canonicalDriveSliceExecutor {
		t.Fatalf("executor = %q, want %q", got, canonicalDriveSliceExecutor)
	}
}

func TestResolveInternalExecutorID_KeepsCertifiedPlansOnCopyExecutor(t *testing.T) {
	payload := map[string]interface{}{
		contract.PayloadKeyCompiledRenderPlanJSON: `{"assets":[{"asset_id":"prepared-01","kind":"prepared_video_fragment"}]}`,
		contract.PayloadKeyCompiledRenderPlanSHA:  "present",
	}
	if got := resolveInternalExecutorID(payload); got != canonicalCompiledPlanExecutor {
		t.Fatalf("executor = %q, want %q", got, canonicalCompiledPlanExecutor)
	}
}
