package pipeline

import (
	"reflect"
	"strings"
	"testing"

	"velox-shared/compatibility"
)

func TestSubmitJobRequestVoiceoverFieldMatchesCompatibilityRegistry(t *testing.T) {
	typeOfRequest := reflect.TypeOf(SubmitJobRequest{})
	field, ok := typeOfRequest.FieldByName("VoiceoverPaths")
	if !ok {
		t.Fatal("SubmitJobRequest.VoiceoverPaths is missing")
	}
	jsonTag := strings.Split(field.Tag.Get("json"), ",")[0]
	if jsonTag != compatibility.VoiceoverPathsKey {
		t.Fatalf("SubmitJobRequest VoiceoverPaths json tag = %q, want %q", jsonTag, compatibility.VoiceoverPathsKey)
	}
}
