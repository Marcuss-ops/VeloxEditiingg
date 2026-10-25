package pipeline

import (
	"strings"
	"testing"
)

func TestPrepareRejectsLegacyDriveLinkScenesAndOverlays(t *testing.T) {
	var req SubmitJobRequest
	body := `{"idempotency_key":"drive-link-pre","scenes":[{"text":"scene","duration_seconds":5,"kind":"clip","stock":{"asset_id":"stock-1","drive_file_id":"stock-1","drive_link":"https://drive.google.com/file/d/stock-1/view","size_bytes":1000}}],"overlays":[{"id":"overlay-1","asset_id":"overlay-1","drive_link":"https://drive.google.com/file/d/overlay-1/view","size_bytes":2000,"start_frame":0,"end_frame":24,"mode":"replace","z_index":1,"audio_mode":"preserve_final_audio"}],"runtime_assets":[{"asset_id":"audio-1","drive_link":"https://drive.google.com/file/d/audio-1/view","size_bytes":3000}]}`
	if err := decodeStrictJSON(strings.NewReader(body), &req); err != nil {
		t.Fatalf("decode legacy PREPARE body: %v", err)
	}
	if req.Scenes[0].Stock == nil || req.Scenes[0].Stock.DriveLink == "" {
		t.Fatalf("legacy drive_link payload failed to decode for rejection: %+v", req.Scenes)
	}
	if validation, bad := ValidateSubmitJobRequest(req); !bad || validation == nil {
		t.Fatal("Master must reject scene/overlay Drive-link payloads; producer must submit CompiledRenderPlanV2")
	}
	raw := submitRequestToRawPayload(&req)
	if _, present := raw["scenes"]; present {
		t.Fatal("legacy scenes leaked into the renderer payload")
	}
	if _, present := raw["overlays"]; present {
		t.Fatal("legacy overlays leaked into the renderer payload")
	}
}
