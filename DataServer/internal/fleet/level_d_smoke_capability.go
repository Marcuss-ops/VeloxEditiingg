package fleet

import (
	"errors"
	"fmt"
)

// SmokeCapabilityState describes whether Level-D can be offered by the
// current composition. DISABLED is intentional when the real asset resolver
// is unavailable; no smoke executor or route should be registered then.
type SmokeCapabilityState string

const (
	SmokeCapabilityDisabled      SmokeCapabilityState = "DISABLED"
	SmokeCapabilityReady         SmokeCapabilityState = "READY"
	SmokeCapabilityMisconfigured SmokeCapabilityState = "MISCONFIGURED"
)

// SmokeCapabilityStatus is the readiness-facing Level-D capability verdict.
type SmokeCapabilityStatus struct {
	State  SmokeCapabilityState
	Reason string
}

func DisabledSmokeCapability(reason string) SmokeCapabilityStatus {
	return SmokeCapabilityStatus{State: SmokeCapabilityDisabled, Reason: reason}
}

func ReadySmokeCapability() SmokeCapabilityStatus {
	return SmokeCapabilityStatus{State: SmokeCapabilityReady}
}

func MisconfiguredSmokeCapability(reason string) SmokeCapabilityStatus {
	return SmokeCapabilityStatus{State: SmokeCapabilityMisconfigured, Reason: reason}
}

// Validate enforces a reason for misconfiguration and rejects unknown states.
func (s SmokeCapabilityStatus) Validate() error {
	switch s.State {
	case SmokeCapabilityDisabled, SmokeCapabilityReady:
		return nil
	case SmokeCapabilityMisconfigured:
		if s.Reason == "" {
			return errors.New("fleet: MISCONFIGURED smoke capability has no reason")
		}
		return fmt.Errorf("fleet: %s: %s", s.State, s.Reason)
	default:
		return fmt.Errorf("fleet: unknown smoke capability state %q", s.State)
	}
}

// ReadinessError keeps intentional DISABLED non-blocking while surfacing a
// malformed or requested-but-broken composition as not ready.
func (s SmokeCapabilityStatus) ReadinessError() error {
	if s.State == SmokeCapabilityDisabled || s.State == SmokeCapabilityReady {
		return nil
	}
	return s.Validate()
}
