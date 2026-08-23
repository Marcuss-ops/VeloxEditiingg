package fleet

import (
	"context"
	"errors"
	"strings"
	"testing"
	"velox-server/internal/store"
)

func TestUpdate_stepContainerRunning_NilDocker(t *testing.T) {
	backend, _ := stubBackends(t)
	backend.Docker = nil
	e := NewUpdateExecutor(backend)
	if err := e.stepContainerRunning(context.Background(), "wkr-1"); !errors.Is(err, ErrContainerUnhealthy) {
		t.Errorf("nil Docker backend: want ErrContainerUnhealthy sentinel, got %v", err)
	}
}
func TestUpdate_parsePayload_EmptyJSONObject(t *testing.T) {
	e := NewUpdateExecutor(UpdateBackend{})
	op := &store.Operation{Payload: []byte("{}")}
	if _, _, err := e.parsePayload(op); err == nil || !strings.Contains(err.Error(), "payload empty") {
		t.Errorf("{} payload: want payload-empty error, got %v", err)
	}
}
