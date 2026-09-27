package pipeline

import (
	"strings"
	"testing"
)

func TestPrepareAcceptsDriveLinkAliasesAndProjectsDownloadSources(t *testing.T) {
	var req SubmitJobRequest
	body := `{"idempotency_key":"drive-link-pre","scenes":[{"text":"scene","duration_seconds":5,"kind":"clip","stock":{"asset_id":"stock-1","drive_file_id":"stock-1","drive_link":"https://drive.google.com/file/d/stock-1/view","size_bytes":1000}}],"overlays":[{"id":"overlay-1","asset_id":"overlay-1","drive_link":"https://drive.google.com/file/d/overlay-1/view","size_bytes":2000,"start_frame":0,"end_frame":24,"mode":"replace","z_index":1,"audio_mode":"preserve_final_audio"}],"runtime_assets":[{"asset_id":"audio-1","drive_link":"https://drive.google.com/file/d/audio-1/view","size_bytes":3000}]}`
	if err := decodeStrictJSON(strings.NewReader(body), &req); err != nil {
		t.Fatalf("decode PREPARE with drive_link fields: %v", err)
	}
	if req.Scenes[0].Stock == nil || req.Scenes[0].Stock.DriveLink == "" {
		t.Fatalf("stock drive_link was not decoded: %+v", req.Scenes[0].Stock)
	}
	if validation, bad := ValidateSubmitJobRequest(req); bad {
		t.Fatalf("stock/overlay drive_link PREPARE rejected: %+v", validation.Details)
	}
	raw := submitRequestToRawPayload(&req)
	scenes := raw["scenes"].([]interface{})
	stock := scenes[0].(map[string]interface{})["stock"].([]interface{})[0].(map[string]interface{})
	if stock["source_uri"] != "https://drive.google.com/uc?export=download&id=stock-1" {
		t.Fatalf("stock source_uri=%v", stock["source_uri"])
	}
	overlays := raw["overlays"].([]interface{})
	if got := overlays[0].(map[string]interface{})["source_uri"]; got != "https://drive.google.com/uc?export=download&id=overlay-1" {
		t.Fatalf("overlay source_uri=%v", got)
	}
	if got := raw["runtime_assets"].([]map[string]interface{})[0]["drive_link"]; got != "https://drive.google.com/file/d/audio-1/view" {
		t.Fatalf("runtime asset drive_link was not preserved: %v", got)
	}
}
