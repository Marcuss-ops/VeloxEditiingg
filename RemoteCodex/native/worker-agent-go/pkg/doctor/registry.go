package doctor

import (
	"context"
	"fmt"

	"velox-worker-agent/pkg/config"
)

// RegistryValidator checks that the executor registry is non-empty and
// that scene.composite.v1@1 is registered (unless the worker is running
// in the "creator" profile, which intentionally omits video rendering).
// RW-PROD-002 §2 item 10.
type RegistryValidator struct {
	Registry ExecutorRegistryView
	// Profile is the worker profile (e.g. "creator"). When set to
	// "creator", the validator no longer requires scene.composite.v1@1.
	Profile string
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
			"at minimum, MustRegister(scene.composite.v1@1) before starting the worker")
	}

	isCreator := config.IsCreatorProfileValue(v.Profile)
	if isCreator {
		detail := fmt.Sprintf("%d executor(s) registered (creator profile, scene.composite.v1 not required)", len(descs))
		return pass("executors.registry", detail)
	}

	// Check for scene.composite.v1@1 specifically.
	hasSceneComposite := false
	for _, d := range descs {
		if d.ID == "scene.composite.v1" && d.Version == 1 {
			hasSceneComposite = true
			break
		}
	}
	if !hasSceneComposite {
		return fail("executors.registry",
			fmt.Sprintf("registry has %d executor(s) but scene.composite.v1@1 is missing", len(descs)),
			"register scene.composite.v1@1 via executors.NewSceneComposite")
	}

	detail := fmt.Sprintf("%d executor(s) registered, scene.composite.v1@1 present", len(descs))
	return pass("executors.registry", detail)
}
