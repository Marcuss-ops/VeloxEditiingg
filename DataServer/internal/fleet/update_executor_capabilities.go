package fleet

import (
	"errors"
	"fmt"
	"strings"
)

// UpdateCapability is the fail-closed boot verdict for the update
// path (AZIONE 2: no "docker client not wired" discovered 30s after
// a POST). Ready is true ONLY when every critical backend is wired:
// SSH, Docker, Cosign, Registry, Deployments, Runtime, Preflight, Image,
// Smoke and Drive.
// Missing lists the names of the absent backends so the operator log,
// the /ready probe and the 503 gate detail all surface the same
// grep-friendly vocabulary.
type UpdateCapability struct {
	Ready   bool
	Missing []string
}

// UpdateCapabilityState is the composition state of the update path.
// DISABLED is intentional (no update executor wired: operator opted out
// or the fleet dependency is absent). READY means every critical backend
// is wired. MISCONFIGURED means the executor exists but a backend is
// missing, and /ready must go red.
type UpdateCapabilityState string

const (
	UpdateCapabilityDisabled      UpdateCapabilityState = "DISABLED"
	UpdateCapabilityReady         UpdateCapabilityState = "READY"
	UpdateCapabilityMisconfigured UpdateCapabilityState = "MISCONFIGURED"
)

// UpdateCapabilityStatus is the readiness-facing update path verdict,
// mirroring the opsalerts and Level-D smoke capability trinity
// (AGENTS.md §6): exactly one of DISABLED / READY / MISCONFIGURED.
type UpdateCapabilityStatus struct {
	State  UpdateCapabilityState
	Reason string
}

// DisabledUpdateCapability returns the explicit production-off status.
// DISABLED is a valid state and therefore does not fail readiness.
func DisabledUpdateCapability(reason string) UpdateCapabilityStatus {
	return UpdateCapabilityStatus{State: UpdateCapabilityDisabled, Reason: reason}
}

// ReadyUpdateCapability returns a fully wired status.
func ReadyUpdateCapability() UpdateCapabilityStatus {
	return UpdateCapabilityStatus{State: UpdateCapabilityReady}
}

// MisconfiguredUpdateCapability returns a fail-closed status with an
// operator reason (the missing backends).
func MisconfiguredUpdateCapability(reason string) UpdateCapabilityStatus {
	return UpdateCapabilityStatus{State: UpdateCapabilityMisconfigured, Reason: reason}
}

// Validate enforces the closed capability-state contract.
func (s UpdateCapabilityStatus) Validate() error {
	switch s.State {
	case UpdateCapabilityDisabled, UpdateCapabilityReady:
		return nil
	case UpdateCapabilityMisconfigured:
		if s.Reason == "" {
			return errors.New("fleet: MISCONFIGURED update capability has no reason")
		}
		return fmt.Errorf("fleet: %s: %s", s.State, s.Reason)
	default:
		return fmt.Errorf("fleet: unknown update capability state %q", s.State)
	}
}

// ReadinessError keeps intentional DISABLED non-blocking while surfacing a
// requested-but-broken update path as not ready.
func (s UpdateCapabilityStatus) ReadinessError() error {
	if s.State == UpdateCapabilityDisabled || s.State == UpdateCapabilityReady {
		return nil
	}
	return s.Validate()
}

// CapabilityStatus derives the trinity verdict from the current wiring. A
// nil executor is DISABLED (no update path composed); a wired executor
// missing backends is MISCONFIGURED; otherwise READY.
func (e *UpdateExecutor) CapabilityStatus() UpdateCapabilityStatus {
	if e == nil {
		return DisabledUpdateCapability("update executor is not wired")
	}
	capability := e.Capability()
	if capability.Ready {
		return ReadyUpdateCapability()
	}
	return MisconfiguredUpdateCapability("missing dependencies: " + strings.Join(capability.Missing, ", "))
}

func (e *UpdateExecutor) AttachRuntimeBackends(smoke BackendSmokeRunner, drive BackendDriveVerifier) error {
	if e == nil {
		return errors.New("update: nil executor")
	}
	if smoke == nil || drive == nil {
		return errors.New("update: fresh smoke and drive verifier are required")
	}
	e.backend.Smoke = smoke
	e.backend.Drive = drive
	return nil
}

func (e *UpdateExecutor) Capability() UpdateCapability {
	if e == nil {
		return UpdateCapability{Missing: []string{"executor"}}
	}
	missing := make([]string, 0, 8)
	if e.backend.SSHCmd == nil {
		missing = append(missing, "ssh")
	}
	if e.backend.Docker == nil {
		missing = append(missing, "docker")
	}
	if e.backend.Deployments == nil {
		missing = append(missing, "deployments")
	}
	if e.backend.Cosign == nil {
		missing = append(missing, "cosign")
	}
	if e.backend.Image == nil {
		missing = append(missing, "image")
	}
	if e.backend.Registry == nil {
		missing = append(missing, "registry")
	}
	if e.backend.Preflight == nil {
		missing = append(missing, "runtime_preflight")
	}
	if e.backend.Smoke == nil {
		missing = append(missing, "smoke")
	}
	if e.backend.Drive == nil {
		missing = append(missing, "drive")
	}
	return UpdateCapability{Ready: len(missing) == 0, Missing: missing}
}

func (e *UpdateExecutor) Ready() bool { return e.Capability().Ready }

func (e *UpdateExecutor) ValidateProductionBackends() error {
	if e == nil {
		return errors.New("update: nil executor")
	}
	capability := e.Capability()
	if !capability.Ready {
		return fmt.Errorf("missing dependencies: %s", strings.Join(capability.Missing, ", "))
	}
	return nil
}
