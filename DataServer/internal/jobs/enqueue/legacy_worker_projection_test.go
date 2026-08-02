package enqueue

import (
	"reflect"
	"testing"

	"velox-shared/contract"
)

func TestProjectLegacyWorkerPayload_ProjectsOnlyWorkerCopy(t *testing.T) {
	canonical := map[string]interface{}{
		"payload_contract_version": contract.PayloadContractVersionCanonical,
		"source":                   "pipeline_generate_with_images",
		"video_name":               "Legacy worker projection",
		"script_text":              "Narrated clip",
		"voiceover_paths":          []interface{}{"velox-asset://voice-1"},
		"scenes": []interface{}{
			map[string]interface{}{
				"text":             "Scene 1",
				"clip_link":        "velox-asset://clip-1",
				"duration_seconds": 5.0,
			},
		},
	}
	before := deepCloneForTest(canonical)

	legacy, err := ProjectLegacyWorkerPayload(canonical)
	if err != nil {
		t.Fatalf("ProjectLegacyWorkerPayload: %v", err)
	}
	if got := legacy["payload_contract_version"]; got != contract.PayloadContractVersionLegacy {
		t.Fatalf("legacy payload_contract_version = %v, want %d", got, contract.PayloadContractVersionLegacy)
	}
	if got := legacy["version"]; got != "v1" {
		t.Fatalf("legacy version = %v, want v1", got)
	}
	if _, ok := legacy["items"]; !ok {
		t.Fatalf("legacy payload missing items: %#v", legacy)
	}
	if _, ok := legacy["clips"]; !ok {
		t.Fatalf("legacy payload missing clips: %#v", legacy)
	}
	if _, ok := legacy["video_mode"]; !ok {
		t.Fatalf("legacy payload missing video_mode: %#v", legacy)
	}
	if _, ok := legacy["source"]; ok {
		t.Fatalf("legacy payload leaked lifecycle source metadata: %#v", legacy)
	}
	parameters, ok := legacy["parameters"].(map[string]interface{})
	if !ok {
		t.Fatalf("legacy payload missing render parameters: %#v", legacy)
	}
	if _, ok := parameters["scenes_json"]; !ok {
		t.Fatalf("legacy render parameters missing scenes_json: %#v", parameters)
	}
	if got := canonical["payload_contract_version"]; got != contract.PayloadContractVersionCanonical {
		t.Fatalf("canonical payload version mutated to %v", got)
	}
	if !reflect.DeepEqual(canonical, before) {
		t.Fatalf("canonical payload mutated:\nbefore=%#v\nafter=%#v", before, canonical)
	}
}

func TestProjectLegacyWorkerPayload_Nil(t *testing.T) {
	legacy, err := ProjectLegacyWorkerPayload(nil)
	if err != nil {
		t.Fatalf("ProjectLegacyWorkerPayload(nil): %v", err)
	}
	if legacy != nil {
		t.Fatalf("nil payload projected to %#v, want nil", legacy)
	}
}

func deepCloneForTest(input map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(input))
	for key, value := range input {
		switch nested := value.(type) {
		case []interface{}:
			items := make([]interface{}, len(nested))
			for i, item := range nested {
				if object, ok := item.(map[string]interface{}); ok {
					copyObject := make(map[string]interface{}, len(object))
					for objectKey, objectValue := range object {
						copyObject[objectKey] = objectValue
					}
					items[i] = copyObject
				} else {
					items[i] = item
				}
			}
			out[key] = items
		default:
			out[key] = value
		}
	}
	return out
}
