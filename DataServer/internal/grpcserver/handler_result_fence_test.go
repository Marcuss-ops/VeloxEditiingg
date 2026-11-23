package grpcserver

import (
	"errors"
	"fmt"
	"testing"
)

func TestIsStalePreparationFence(t *testing.T) {
	if !isStalePreparationFence(errStalePreparationFence) {
		t.Fatal("sentinel must classify as stale")
	}
	if !isStalePreparationFence(fmt.Errorf("commit: %w", errStalePreparationFence)) {
		t.Fatal("wrapped sentinel must classify as stale")
	}
	if isStalePreparationFence(errors.New("database is locked")) {
		t.Fatal("transient error must not classify as stale")
	}
	if isStalePreparationFence(nil) {
		t.Fatal("nil must not classify as stale")
	}
}
