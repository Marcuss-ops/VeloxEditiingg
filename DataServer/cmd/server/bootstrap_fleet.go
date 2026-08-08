package main

// Fleet dependency composition and adapters for the master bootstrap.

import (
	"context"
	"fmt"
	"log"
	"strings"
	"time"

	"velox-server/internal/deploy"
	"velox-server/internal/deploy/cosign"
	"velox-server/internal/fleet"
	"velox-server/internal/handlers/server/api"
	integrationsDrive "velox-server/internal/integrations/drive"
	"velox-server/internal/supervisor"
	workersreg "velox-server/internal/workers"
)

// deployUpdateImageValidator wraps internal/deploy.ValidateImageRef
// for the UpdateExecutor's BackendImageRefValidator surface.
// Kept inline (not promoted to a separate file) because the
// Step 9/15 wiring path needs the type-name at the composition
// root and the shape is single-purpose.
type deployUpdateImageValidator struct{}

// Validate delegates to deploy.ValidateImageRef. The error
// sentinels (ErrEmptyImageRef, ErrMobileImageRef, etc.) flow
// through unmodified so the executor's audit-dashboard grep
// remains stable.
func (deployUpdateImageValidator) Validate(ref string) error {
	return deploy.ValidateImageRef(ref)
}

// newUpdateCosignVerifier returns the production-default Cosign
// verifier for the UpdateExecutor's BackendCosignVerifierIfc
// surface. The ExternalCosignVerifier shells out to the cosign
// CLI; VELOX_SKIP_COSIGN_VERIFY=1 short-circuits via the env
// guard inside the verifier.
func newUpdateCosignVerifier() BackendCosignVerifierIfc {
	return cosign.NewExternalCosignVerifier()
}

// BackendCosignVerifierIfc is the alias type the fleet package
// declares in update_executor.go. We re-declare it here so the
// composition root doesn't have to import the fleet package's
// unexported type. (The two declarations are structurally
// identical — the fleet package's is the canonical interface;
// this satisfies it via structural typing.)
type BackendCosignVerifierIfc interface {
	Verify(ctx context.Context, ref string) error
}

// driveUploaderAdapter adapts integrationsDrive.Service to the
// fleet.BackendDriveUploader interface. Lives in the composition root
// (not the fleet package) to keep fleet decoupled from integrations/drive.
type driveUploaderAdapter struct {
	svc      *integrationsDrive.Service
	folderID string
}

type driveVerifierAdapter struct{ svc *integrationsDrive.Service }

func (a *driveVerifierAdapter) VerifyDelivery(ctx context.Context, driveFileID string, expectedBytes int64) error {
	if a == nil || a.svc == nil {
		return fmt.Errorf("drive verifier: service not configured")
	}
	file, err := a.svc.GetFileMetadata(ctx, driveFileID)
	if err != nil {
		return fmt.Errorf("drive metadata: %w", err)
	}
	if file == nil || file.ID == "" {
		return fmt.Errorf("drive metadata missing file id")
	}
	if expectedBytes > 0 && file.Size != expectedBytes {
		return fmt.Errorf("drive size=%d want=%d", file.Size, expectedBytes)
	}
	return nil
}

// UploadArtifact delegates to the real Drive service's UploadFile.
// The runID is used as the deliveryID for traceability in Drive properties.
// A smoke-specific folder can be configured via VELOX_SMOKE_DRIVE_FOLDER_ID;
// when unset the file is uploaded to the Drive root.
func (a *driveUploaderAdapter) UploadArtifact(ctx context.Context, runID, srcPath string, expectedBytes int64, expectedSHA256 string) (string, error) {
	if a == nil || a.svc == nil {
		return "", fmt.Errorf("drive uploader: service not configured")
	}
	if err := fleet.VerifyLocalArtifactDigest(srcPath, expectedBytes, expectedSHA256); err != nil {
		return "", fmt.Errorf("%w: %v", fleet.ErrDriveUploadFail, err)
	}
	result, err := a.svc.UploadFile(ctx, srcPath, strings.TrimSpace(a.folderID), runID)
	if err != nil {
		return "", fmt.Errorf("%w: %v", fleet.ErrDriveUploadFail, err)
	}
	if !result.Success {
		return "", fmt.Errorf("%w: %s", fleet.ErrDriveUploadFail, result.Error)
	}
	if result.FileID == "" {
		return "", fmt.Errorf("%w: drive returned empty file_id", fleet.ErrDriveUploadFail)
	}
	metadata, err := a.svc.GetFileMetadata(ctx, result.FileID)
	if err != nil {
		return "", fmt.Errorf("%w: post-upload metadata verification: %v", fleet.ErrDriveUploadFail, err)
	}
	if expectedBytes > 0 && metadata.Size != expectedBytes {
		return "", fmt.Errorf("%w: post-upload size=%d want=%d", fleet.ErrDriveUploadFail, metadata.Size, expectedBytes)
	}
	log.Printf("[SMOKE-DRIVE] uploaded %s → Drive file_id=%s (bytes=%d sha256=%s verified)", runID, result.FileID, expectedBytes, expectedSHA256)
	return result.FileID, nil
}

// FleetDep is the Step 4/15 fleet-operator dependency bundle: the
// FleetController (publish + tick + audit bridge), the constructor
// of an AdminOperationsHandler for the audit routes, and the
// ExecutorRegistry reserved for future concrete executor registration.
//
// tickWiredAtBoot records whether the controller is registered in the
// process supervisor. A false value is only valid for partial test
// composition; production boot must set it to true so published
// operations cannot remain QUEUED indefinitely.
type FleetDep struct {
	Controller      *fleet.FleetController
	Registry        *fleet.ExecutorRegistry
	Update          *fleet.UpdateExecutor
	tickWiredAtBoot bool
}

// getHandler returns the api.AdminOperationsHandler wrapping
// Controller, or nil if Controller is absent (e.g. feature
// disabled via the bootstrap). Returns nil-safe on a zero-value
// FleetDep so destructive reads of c.fleet.getHandler() during
// route registration do not panic.
func (f *FleetDep) getHandler() *api.AdminOperationsHandler {
	if f == nil || f.Controller == nil {
		return nil
	}
	return api.NewAdminOperationsHandler(f.Controller)
}

// buildFleet constructs the FleetController + ExecutorRegistry
// when the persistence layer is available. Returns (nil, nil) on
// a persistence-disabled boot (test fixture paths) so the router
// registers the audit route stubs but serving returns 503 via the
// handler's nil-controller guard.
//
// Step 9/15: registers the UpdateExecutor for the `update`
// operation kind, replacing the Step 4/15 noop default. Live
// dependencies (Deployments repo, Cosign verifier, Image
// validator) are wired from the persistence layer; future
// steps (7+/8+/9+) plug in the real SSH client, Docker cli
// wrapper, Smoke runner, and Drive verifier (each nil-tolerant
// today: missing dep fails the Execute call loudly rather than
// silently noops).
//
// opTimeout is bumped to 30min (overriding DefaultOpTimeout's
// 10min) so the forward+rollback cascade for an `update`
// operation has headroom for: cosign verify (30s) +
// docker pull (10min) + compose restart (2min) + container
// running check + /health/ready poll (60s) + master connect
// (30s) + Level D smoke (5min) + Drive verify (60s) +
// RB-only cascade on failure (15min slack).
func buildFleet(p *persistenceDeps, workerRegistry *workersreg.Registry, sharedSSH fleet.BackendSSHClient) (*FleetDep, error) {
	if p == nil || p.SQLite == nil {
		return nil, nil
	}
	registry := fleet.NewExecutorRegistry()
	controller := fleet.NewFleetController(
		p.SQLite,
		registry,
		fleet.DefaultTickInterval,
		30*time.Minute, // opTimeout for forward + rollback cascade
	)

	// Step 9/15 UpdateExecutor — wires the live deps that
	// bootstrap knows about. The registry gater is canonical for
	// both active_tasks polling and the executor-owned drain toggle;
	// no operator-side/document-only drain is required.
	updateBackend := fleet.UpdateBackend{
		SSHCmd:      sharedSSH,
		Docker:      &fleet.SSHWorkerDockerClient{SSH: sharedSSH},
		Deployments: p.SQLite,
		Cosign:      newUpdateCosignVerifier(),
		Image:       deployUpdateImageValidator{},
		Registry:    &fleet.RealRegistryUpdateGater{Reg: workerRegistry},
	}
	updateExecutor := fleet.NewUpdateExecutor(updateBackend)
	registry.Register(fleet.OperationKindUpdate, updateExecutor)
	log.Printf("[BOOTSTRAP] UpdateExecutor registered for kind=%s (SSH/Docker activation wired; fresh Smoke/Drive attach pending)", fleet.OperationKindUpdate)

	return &FleetDep{
		Controller:      controller,
		Registry:        registry,
		Update:          updateExecutor,
		tickWiredAtBoot: false,
	}, nil
}

// registerFleetRunner attaches the FleetController to the already-built
// supervisor. buildSupervisor runs before buildFleet because most runners
// are module dependencies, so registration is intentionally a small
// post-build step. The supervisor owns the goroutine and graceful shutdown;
// the controller owns only its operation tick.
func registerFleetRunner(sup *supervisor.Supervisor, dep *FleetDep) error {
	if sup == nil || dep == nil || dep.Controller == nil {
		return nil
	}
	const maxRetries = 5
	return sup.Register(supervisor.Runner{
		Name:  "fleet-controller",
		Class: supervisor.ClassRestartable,
		Policy: supervisor.RestartPolicy{
			MaxRetries:     maxRetries,
			InitialBackoff: 500 * time.Millisecond,
			MaxBackoff:     30 * time.Second,
			RestartOnPanic: true,
		},
		Run: func(ctx context.Context) error {
			log.Printf("[BOOTSTRAP] FleetController runner started")
			return dep.Controller.Run(ctx)
		},
	})
}
