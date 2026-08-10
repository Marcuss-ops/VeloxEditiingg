package opsalerts

import (
	"errors"
	"fmt"
)

// CapabilityState is the composition state of the fleet alert evaluator.
// DISABLED is intentional: no evaluator runner or alert routes are exposed.
// READY means all required dependencies are wired. MISCONFIGURED means the
// capability was requested but cannot safely evaluate alerts.
type CapabilityState string

const (
	CapabilityDisabled      CapabilityState = "DISABLED"
	CapabilityReady         CapabilityState = "READY"
	CapabilityMisconfigured CapabilityState = "MISCONFIGURED"
)

// CapabilityStatus is the readiness-facing status of opsalerts.
type CapabilityStatus struct {
	State  CapabilityState
	Reason string
}

// DisabledStatus returns the explicit production-off status. Disabled is a
// valid state and therefore does not make the process itself unready.
func DisabledStatus(reason string) CapabilityStatus {
	return CapabilityStatus{State: CapabilityDisabled, Reason: reason}
}

// ReadyStatus returns a fully wired status.
func ReadyStatus() CapabilityStatus {
	return CapabilityStatus{State: CapabilityReady}
}

// MisconfiguredStatus returns a fail-closed status with an operator reason.
func MisconfiguredStatus(reason string) CapabilityStatus {
	return CapabilityStatus{State: CapabilityMisconfigured, Reason: reason}
}

// Validate enforces the closed capability-state contract.
func (s CapabilityStatus) Validate() error {
	switch s.State {
	case CapabilityDisabled, CapabilityReady:
		return nil
	case CapabilityMisconfigured:
		if s.Reason == "" {
			return errors.New("opsalerts: MISCONFIGURED capability has no reason")
		}
		return fmt.Errorf("opsalerts: %s: %s", s.State, s.Reason)
	default:
		return fmt.Errorf("opsalerts: unknown capability state %q", s.State)
	}
}

// ReadinessError returns an error only for a misconfigured capability.
// Intentional DISABLED is safe because its runner and routes are absent.
func (s CapabilityStatus) ReadinessError() error {
	if s.State == CapabilityDisabled || s.State == CapabilityReady {
		return nil
	}
	return s.Validate()
}
