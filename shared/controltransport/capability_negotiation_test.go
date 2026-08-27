package controltransport

import "testing"

func TestHasProgressiveUploadCapabilityFailClosed(t *testing.T) {
	for name, capabilities := range map[string]map[string]interface{}{
		"nil":     nil,
		"missing": {},
		"false":   {CapabilityArtifactProgressiveUploadV1: false},
		"wrong":   {CapabilityArtifactProgressiveUploadV1: "true"},
	} {
		if HasProgressiveUploadCapability(capabilities) {
			t.Errorf("%s capabilities must not negotiate progressive upload", name)
		}
	}
	if !HasProgressiveUploadCapability(map[string]interface{}{
		CapabilityArtifactProgressiveUploadV1: true,
	}) {
		t.Fatal("explicit true capability must negotiate progressive upload")
	}
}
