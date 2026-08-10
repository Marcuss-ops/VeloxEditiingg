package fleet

import (
	"errors"
	"fmt"
	"strings"
)

// UpdateCapability is the fail-closed boot verdict for the update
// path (AZIONE 2: no "docker client not wired" discovered 30s after
// a POST). Ready is true ONLY when every critical backend is wired:
// SSH, Docker, Cosign, Registry, Deployments, Image, Smoke and Drive.
// Missing lists the names of the absent backends so the operator log,
// the /ready probe and the 503 gate detail all surface the same
// grep-friendly vocabulary.
type UpdateCapability struct {
	Ready   bool
	Missing []string
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
