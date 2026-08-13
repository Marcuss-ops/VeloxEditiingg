// Package preparedassets owns the single master-side admission authority for
// externally rendered, already-encoded video fragments. It deliberately does
// not know whether a producer used overlays, CUDA, Vulkan, or templates.
package preparedassets

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"velox-server/internal/repository"
	"velox-shared/contract"
)

var (
	ErrArtifactNotReady      = errors.New("prepared asset: artifact is not READY")
	ErrArtifactIntegrity     = errors.New("prepared asset: artifact integrity does not match manifest")
	ErrArtifactAlreadyBound  = errors.New("prepared asset: asset key is already bound to another artifact")
	ErrPreparedAssetNotAdmit = errors.New("prepared asset: manifest is not admissible")
)

// ArtifactReader is intentionally the existing artifact authority. Prepared
// assets do not get a second blob registry or a second storage state machine.
type ArtifactReader interface {
	GetByID(context.Context, string) (*repository.Artifact, error)
}

type State string

const (
	StateReady   State = "READY"
	StateWaiting State = "WAITING"
)

type Admission struct {
	State    State
	Manifest contract.PreparedVideoFragmentManifestV1
	Artifact *repository.Artifact
}

type PreparedAssetAdmissionResolver struct {
	artifacts ArtifactReader
	profiles  map[string]contract.CanonicalVideoProfileV1
}

func NewPreparedAssetAdmissionResolver(artifacts ArtifactReader) *PreparedAssetAdmissionResolver {
	profiles := map[string]contract.CanonicalVideoProfileV1{
		contract.CanonicalVideoProfileV1Default.ProfileID: contract.CanonicalVideoProfileV1Default,
	}
	return &PreparedAssetAdmissionResolver{artifacts: artifacts, profiles: profiles}
}

// Resolver is retained as a concise type alias for callers that do not need
// the full contract name.
type Resolver = PreparedAssetAdmissionResolver

func NewResolver(artifacts ArtifactReader) *PreparedAssetAdmissionResolver {
	return NewPreparedAssetAdmissionResolver(artifacts)
}

// Admit validates the producer manifest and binds it to the existing READY
// artifact row. WAITING is returned only when the row is absent or not yet
// READY; callers must keep the final render blocked in that state.
func (r *PreparedAssetAdmissionResolver) Admit(ctx context.Context, manifest contract.PreparedVideoFragmentManifestV1) (Admission, error) {
	if r == nil || r.artifacts == nil {
		return Admission{}, fmt.Errorf("%w: artifact reader is not configured", ErrPreparedAssetNotAdmit)
	}
	if err := manifest.Validate(); err != nil {
		return Admission{}, fmt.Errorf("%w: %v", ErrPreparedAssetNotAdmit, err)
	}
	profile, ok := r.profiles[manifest.ProfileID]
	if !ok {
		return Admission{}, fmt.Errorf("%w: unknown profile %q", ErrPreparedAssetNotAdmit, manifest.ProfileID)
	}
	if err := profile.Validate(); err != nil {
		return Admission{}, fmt.Errorf("%w: %v", ErrPreparedAssetNotAdmit, err)
	}
	artifact, err := r.artifacts.GetByID(ctx, manifest.ArtifactID)
	if err != nil {
		return Admission{}, fmt.Errorf("prepared asset: load artifact %q: %w", manifest.ArtifactID, err)
	}
	if artifact == nil || !strings.EqualFold(strings.TrimSpace(artifact.Status), "READY") {
		return Admission{State: StateWaiting, Manifest: manifest, Artifact: artifact}, ErrArtifactNotReady
	}
	if artifact.SHA256 != manifest.SHA256 || artifact.SizeBytes != manifest.SizeBytes {
		return Admission{}, fmt.Errorf("%w: artifact=%q sha256/size", ErrArtifactIntegrity, manifest.ArtifactID)
	}
	if artifact.Type != "video" && artifact.Type != "prepared_video_fragment" {
		return Admission{}, fmt.Errorf("%w: artifact=%q type=%q is not video", ErrArtifactIntegrity, manifest.ArtifactID, artifact.Type)
	}
	return Admission{State: StateReady, Manifest: manifest, Artifact: artifact}, nil
}

// RegisterPreparedArtifact is the control-plane operation exposed to an
// external producer. Registration does not create a second blob or cache: it
// admits the supplied manifest only after the existing artifact authority has
// committed the referenced READY artifact.
func (r *PreparedAssetAdmissionResolver) RegisterPreparedArtifact(ctx context.Context, manifest contract.PreparedVideoFragmentManifestV1) (Admission, error) {
	return r.Admit(ctx, manifest)
}

// Ready is the fail-closed readiness check used by final-render admission.
func (r *PreparedAssetAdmissionResolver) Ready(ctx context.Context, manifest contract.PreparedVideoFragmentManifestV1) (bool, error) {
	admission, err := r.Admit(ctx, manifest)
	if err != nil {
		return false, err
	}
	return admission.State == StateReady, nil
}

// RequireAllReady is the final-render gate. A single WAITING/invalid
// artifact keeps the render blocked; there is no best-effort partial start.
func (r *PreparedAssetAdmissionResolver) RequireAllReady(ctx context.Context, manifests []contract.PreparedVideoFragmentManifestV1) error {
	for _, manifest := range manifests {
		if _, err := r.Admit(ctx, manifest); err != nil {
			return err
		}
	}
	return nil
}
