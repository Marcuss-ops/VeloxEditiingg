package artifacts

import "testing"

func TestArtifactStateTerminalSemantics(t *testing.T) {
	for _, state := range []ArtifactState{ArtifactReady, ArtifactQuarantined, ArtifactDeleted, ArtifactFailed} {
		if !state.IsTerminal() {
			t.Errorf("%q should be terminal", state)
		}
	}
	if ArtifactState(ArtifactStaging).IsTerminal() {
		t.Fatal("STAGING should not be terminal")
	}
}
