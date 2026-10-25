package doctor

import (
	"context"
	"fmt"

	"velox-worker-agent/pkg/config"
)

// RegistryValidator checks that the sole V2 packet-copy executor is present.
// RW-PROD-002 §2 item 10.
type RegistryValidator struct {
	Registry ExecutorRegistryView
}

func (v *RegistryValidator) ID() string { return "executors.registry" }

func (v *RegistryValidator) Run(_ context.Context, _ *config.WorkerConfig) Result {
	if v.Registry == nil {
		return fail("executors.registry",
			"executor registry is nil (not wired)",
			"ensure the composition root wires an executor registry before calling doctor")
	}

	descs := v.Registry.Descriptors()
	if len(descs) == 0 {
		return fail("executors.registry",
			"executor registry is empty (no executors registered)",
			"register video.assemble.copy.v1@1 before starting the worker")
	}
	// Check the single supported worker operation.
	hasPacketCopy := false
	for _, d := range descs {
		if d.ID == "video.assemble.copy.v1" && d.Version == 1 {
			hasPacketCopy = true
			break
		}
	}
	if !hasPacketCopy {
		return fail("executors.registry",
			fmt.Sprintf("registry has %d executor(s) but video.assemble.copy.v1@1 is missing", len(descs)),
			"register the packet-copy executor via executors.NewVideoAssembleCopy")
	}

	detail := fmt.Sprintf("%d executor(s) registered, video.assemble.copy.v1@1 present", len(descs))
	return pass("executors.registry", detail)
}
