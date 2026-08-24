// Package assembly defines the versioned handoff contract between the
// producer computers and Velox. It is intentionally separate from
// JobPayloadV2: the latter is the renderer payload, while this contract
// describes eager preparation and late-bound runtime assets.
package assembly

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	videoContract "velox-shared/contract"
)

const ContractVersion = "velox.assembly.v1"

// CanonicalAssemblyContractVersionV1 is the explicit version constant shared
// by PipelineGen, Velox and RenderingGen. ContractVersion remains as a
// backwards-compatible alias for existing callers.
const CanonicalAssemblyContractVersionV1 = ContractVersion

type DispatchTarget string

const (
	TargetVelox DispatchTarget = "velox"
)

type DispatchMode string

const (
	ModeNever        DispatchMode = "never"
	ModeEager        DispatchMode = "eager"
	ModeWhenComplete DispatchMode = "when_complete"
	ModeManual       DispatchMode = "manual"
)

// DispatchPolicy is the internal, typed form of the external send_to_velox
// convenience flag. New callers should use Mode; the bool is accepted only
// at the boundary and normalized with NormalizeDispatch.
type DispatchPolicy struct {
	Target       DispatchTarget `json:"target"`
	Mode         DispatchMode   `json:"mode"`
	StartPrepare bool           `json:"start_prepare"`
}

// ExternalDispatch is the intentionally small producer-facing shape. It is
// normalized before it reaches the assembly state machine.
type ExternalDispatch struct {
	SendToVelox bool `json:"send_to_velox"`
}

func NormalizeDispatch(in ExternalDispatch) DispatchPolicy {
	if !in.SendToVelox {
		return DispatchPolicy{Target: TargetVelox, Mode: ModeNever}
	}
	return DispatchPolicy{Target: TargetVelox, Mode: ModeEager, StartPrepare: true}
}

type AssetKind string

const (
	KindSourceClip    AssetKind = "source_clip"
	KindVoiceover     AssetKind = "voiceover"
	KindImage         AssetKind = "image"
	KindOverlayAsset  AssetKind = "overlay_asset"
	KindPreparedScene AssetKind = "prepared_scene"
	KindGeneratedClip AssetKind = "generated_clip"
	KindFinalAudio    AssetKind = "final_audio"
)

type AssetAvailability string

const (
	AvailabilityKnown    AssetAvailability = "known"
	AvailabilityRuntime  AssetAvailability = "runtime"
	AvailabilityOptional AssetAvailability = "optional"
)

type AssetProducer string

const (
	ProducerPipelineGen  AssetProducer = "pipelinegen"
	ProducerRenderingGen AssetProducer = "renderinggen"
	ProducerChronon      AssetProducer = "chronon"
)

type AssetState string

const (
	AssetDeclared        AssetState = "declared"
	AssetWaitingProducer AssetState = "waiting_producer"
	AssetMaterializing   AssetState = "materializing"
	AssetReady           AssetState = "ready"
	AssetInvalidated     AssetState = "invalidated"
	AssetFailed          AssetState = "failed"
)

// AssetRequirement identifies bytes independently from their locator. A
// known asset must carry its hash and locator; a runtime asset carries the
// producer identity and receives its locator/hash when published.
type AssetRequirement struct {
	AssetID      string            `json:"asset_id"`
	Kind         AssetKind         `json:"kind"`
	Availability AssetAvailability `json:"availability"`
	Producer     AssetProducer     `json:"producer,omitempty"`
	URL          string            `json:"url,omitempty"`
	SHA256       string            `json:"sha256,omitempty"`
	SizeBytes    int64             `json:"size_bytes,omitempty"`
	MIMEType     string            `json:"mime_type,omitempty"`
	ProfileID    string            `json:"profile_id,omitempty"`
	Required     bool              `json:"required"`
	State        AssetState        `json:"state"`
}

type TimelineItem struct {
	SceneID string `json:"scene_id"`
	AssetID string `json:"asset_id"`
}

type Output struct {
	ProfileID string `json:"profile_id"`
}

type AssemblyState string

const (
	StatePreparing       AssemblyState = "preparing"
	StateWaitingRuntime  AssemblyState = "waiting_runtime_assets"
	StateReadyToFinalize AssemblyState = "ready_to_finalize"
	StateFinalizing      AssemblyState = "finalizing"
	StateCompleted       AssemblyState = "completed"
	StateFailed          AssemblyState = "failed"
	StateCancelled       AssemblyState = "cancelled"
)

// CanonicalAssemblyContractV1 is the final, versioned handoff exchanged by
// PipelineGen, Velox and RenderingGen. It contains control-plane identity,
// the immutable timeline binding, the complete asset state, and the output
// profile; renderer payloads must not be used as a substitute for this type.
type CanonicalAssemblyContractV1 struct {
	ContractVersion  string                                 `json:"contract_version"`
	JobID            string                                 `json:"job_id"`
	TimelineRevision uint64                                 `json:"timeline_revision"`
	TimelineHash     string                                 `json:"timeline_hash"`
	PreparationHash  string                                 `json:"preparation_hash,omitempty"`
	Dispatch         DispatchPolicy                         `json:"dispatch"`
	Assets           []AssetRequirement                     `json:"assets"`
	Timeline         []TimelineItem                         `json:"timeline,omitempty"`
	Output           Output                                 `json:"output"`
	Profile          *videoContract.CanonicalVideoProfileV1 `json:"profile,omitempty"`
	State            AssemblyState                          `json:"state,omitempty"`
}

// AssemblyJobV1 is retained as a source-compatible name for existing Velox
// intake and task-contract callers. Both names are the exact same wire type.
type AssemblyJobV1 = CanonicalAssemblyContractV1

type PublishedArtifact struct {
	JobID            string        `json:"job_id"`
	TimelineRevision uint64        `json:"timeline_revision"`
	AssetID          string        `json:"asset_id"`
	StorageURL       string        `json:"storage_url"`
	SHA256           string        `json:"sha256"`
	SizeBytes        int64         `json:"size_bytes"`
	MIMEType         string        `json:"mime_type,omitempty"`
	ProfileID        string        `json:"profile_id,omitempty"`
	Producer         AssetProducer `json:"producer,omitempty"`
	FrameCount       uint64        `json:"frame_count,omitempty"`
}

type InvalidateArtifact struct {
	JobID            string   `json:"job_id"`
	TimelineRevision uint64   `json:"timeline_revision"`
	AssetIDs         []string `json:"asset_ids"`
}

func (p DispatchPolicy) Valid() bool {
	if p.Target != TargetVelox {
		return false
	}
	switch p.Mode {
	case ModeNever, ModeWhenComplete, ModeManual:
		return !p.StartPrepare
	case ModeEager:
		return p.StartPrepare
	default:
		return false
	}
}

func (a AssetAvailability) Valid() bool {
	return a == AvailabilityKnown || a == AvailabilityRuntime || a == AvailabilityOptional
}

func (s AssemblyState) Valid() bool {
	switch s {
	case StatePreparing, StateWaitingRuntime, StateReadyToFinalize, StateFinalizing, StateCompleted, StateFailed, StateCancelled:
		return true
	default:
		return false
	}
}

func (k AssetKind) Valid() bool {
	switch k {
	case KindSourceClip, KindVoiceover, KindImage, KindOverlayAsset, KindPreparedScene, KindGeneratedClip, KindFinalAudio:
		return true
	default:
		return false
	}
}

func (p AssetProducer) Valid() bool {
	return p == ProducerPipelineGen || p == ProducerRenderingGen || p == ProducerChronon
}

func validSHA256(value string) bool {
	if len(value) == sha256.Size*2 {
		_, err := hex.DecodeString(value)
		return err == nil
	}
	return strings.HasPrefix(value, "sha256:") && len(value) == len("sha256:")+sha256.Size*2 && validSHA256(strings.TrimPrefix(value, "sha256:"))
}

// Validate enforces the wire invariants before a job can enter intake.
// Unknown contract versions and incomplete known assets are rejected.
func (j CanonicalAssemblyContractV1) Validate() error {
	if j.ContractVersion != ContractVersion {
		return fmt.Errorf("assembly: unsupported contract_version %q", j.ContractVersion)
	}
	if strings.TrimSpace(j.JobID) == "" {
		return errors.New("assembly: job_id is required")
	}
	if j.TimelineRevision == 0 {
		return errors.New("assembly: timeline_revision must be greater than zero")
	}
	if strings.TrimSpace(j.TimelineHash) == "" {
		return errors.New("assembly: timeline_hash is required")
	}
	if !j.Dispatch.Valid() {
		return errors.New("assembly: invalid dispatch policy")
	}
	if strings.TrimSpace(j.Output.ProfileID) == "" {
		return errors.New("assembly: output.profile_id is required")
	}
	if j.Profile != nil {
		if err := j.Profile.Validate(); err != nil {
			return fmt.Errorf("assembly: invalid canonical profile: %w", err)
		}
		if j.Profile.ProfileID != j.Output.ProfileID {
			return fmt.Errorf("assembly: profile.profile_id %q does not match output.profile_id %q", j.Profile.ProfileID, j.Output.ProfileID)
		}
	}
	if j.PreparationHash != "" && j.PreparationHash != j.ComputePreparationHash() {
		return errors.New("assembly: preparation_hash does not match contract contents")
	}
	if j.State != "" && !j.State.Valid() {
		return fmt.Errorf("assembly: unknown state %q", j.State)
	}
	seen := make(map[string]struct{}, len(j.Assets))
	for i, asset := range j.Assets {
		if strings.TrimSpace(asset.AssetID) == "" {
			return fmt.Errorf("assembly: assets[%d].asset_id is required", i)
		}
		if _, ok := seen[asset.AssetID]; ok {
			return fmt.Errorf("assembly: duplicate asset_id %q", asset.AssetID)
		}
		seen[asset.AssetID] = struct{}{}
		if !asset.Kind.Valid() || !asset.Availability.Valid() {
			return fmt.Errorf("assembly: assets[%d] has an unknown kind or availability", i)
		}
		if asset.SizeBytes < 0 {
			return fmt.Errorf("assembly: asset %q has negative size_bytes", asset.AssetID)
		}
		if asset.Availability == AvailabilityKnown {
			if strings.TrimSpace(asset.URL) == "" || !validSHA256(asset.SHA256) {
				return fmt.Errorf("assembly: known asset %q requires url and sha256", asset.AssetID)
			}
		} else if asset.Availability == AvailabilityRuntime && !asset.Producer.Valid() {
			return fmt.Errorf("assembly: runtime asset %q requires a known producer", asset.AssetID)
		}
	}
	for i, item := range j.Timeline {
		if strings.TrimSpace(item.SceneID) == "" || strings.TrimSpace(item.AssetID) == "" {
			return fmt.Errorf("assembly: timeline[%d] requires scene_id and asset_id", i)
		}
		if _, ok := seen[item.AssetID]; !ok {
			return fmt.Errorf("assembly: timeline[%d] references undeclared asset %q", i, item.AssetID)
		}
	}
	return nil
}

// DeriveState keeps waiting jobs out of render slots. Optional assets do not
// block finalization; required runtime assets do.
func (j CanonicalAssemblyContractV1) DeriveState() AssemblyState {
	for _, asset := range j.Assets {
		if asset.Required && asset.State != AssetReady {
			if asset.Availability == AvailabilityRuntime {
				return StateWaitingRuntime
			}
			return StatePreparing
		}
	}
	return StateReadyToFinalize
}
