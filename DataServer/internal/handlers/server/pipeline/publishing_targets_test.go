package pipeline

import "testing"

func TestVeloxDestinationID(t *testing.T) {
	if got := veloxDestinationID(" extdst_01JREADY "); got != "instaedit_extdst_01JREADY" {
		t.Fatalf("veloxDestinationID = %q", got)
	}
}
