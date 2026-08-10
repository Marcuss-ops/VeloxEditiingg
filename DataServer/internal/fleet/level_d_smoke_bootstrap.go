package fleet

import "fmt"

// ConfigureLevelDSmokeCapability registers a smoke executor only when its
// complete backend is valid. Production callers must pass development=false;
// an incomplete production composition is a hard configuration error, never a
// registered executor that fails later or a fake success.
func ConfigureLevelDSmokeCapability(registry *ExecutorRegistry, executor *LevelDSmokeExecutor, development bool) (SmokeCapabilityStatus, error) {
	if registry == nil {
		return MisconfiguredSmokeCapability("executor registry is nil"), fmt.Errorf("fleet: Level-D smoke registry is nil")
	}
	if executor == nil {
		return MisconfiguredSmokeCapability("executor is nil"), fmt.Errorf("fleet: Level-D smoke executor is nil")
	}
	if !development {
		if _, isStub := executor.backend.Asset.(*StubAssetResolver); isStub {
			status := MisconfiguredSmokeCapability("StubAssetResolver is development-only")
			return status, fmt.Errorf("fleet: Level-D smoke production registration: %w", ErrSmokeRunnerNotWired)
		}
	}
	if err := executor.ValidateProductionBackends(); err != nil {
		if development {
			return DisabledSmokeCapability("development smoke backend is incomplete"), nil
		}
		return MisconfiguredSmokeCapability(err.Error()), fmt.Errorf("fleet: Level-D smoke production registration: %w", err)
	}
	if err := registry.Register(OperationKindSmoke, executor); err != nil {
		return MisconfiguredSmokeCapability(err.Error()), fmt.Errorf("fleet: register Level-D smoke executor: %w", err)
	}
	return ReadySmokeCapability(), nil
}
