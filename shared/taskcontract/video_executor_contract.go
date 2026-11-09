package taskcontract

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"velox-shared/contract"
)

const (
	// VideoPrepareWindowExecutorID is the worker-side window preparation
	// executor. It resolves and certifies exactly one source window.
	VideoPrepareWindowExecutorID = contract.VideoPrepareWindowExecutorID
	// VideoAssemblyCopyExecutorID is the packet-copy assembly executor.
	VideoAssemblyCopyExecutorID = contract.VideoAssemblyCopyExecutorID
	// VideoAssemblySliceExecutorID is the source-aware assembly executor.
	VideoAssemblySliceExecutorID = contract.VideoAssemblySliceExecutorID
)

// videoPrepareWindowContract declares one source and no prefetch gate. The
// worker acquires, verifies and certifies that source itself; requiring a
// job-wide future-asset plan here was the r67f stall class.
type videoPrepareWindowContract struct{}

func (videoPrepareWindowContract) Validate(spec TaskInput) error {
	var request contract.VideoPrepareWindowV1
	if err := spec.DecodePayload(contract.VideoPrepareWindowPayloadKey, &request); err != nil {
		return err
	}
	return request.Validate()
}

func (videoPrepareWindowContract) RequiredAssets(spec TaskInput) ([]RequiredAsset, error) {
	var request contract.VideoPrepareWindowV1
	if err := spec.DecodePayload(contract.VideoPrepareWindowPayloadKey, &request); err != nil {
		return nil, err
	}
	if err := request.Validate(); err != nil {
		return nil, err
	}
	key := strings.TrimSpace(request.Source.AssetKey)
	if key == "" {
		key = strings.TrimSpace(request.Source.AssetID)
	}
	if key == "" {
		return nil, fmt.Errorf("video.prepare.window.v1: source asset has no key or id")
	}
	// Exactly one declared input. Sibling task assets in the same job are
	// deliberately not part of this executor's dependency set.
	return []RequiredAsset{{
		Key:       key,
		AssetID:   request.Source.AssetID,
		SHA256:    request.Source.SHA256,
		SizeBytes: request.Source.SizeBytes,
		Role:      "video_prepare_source",
	}}, nil
}

func (videoPrepareWindowContract) RequiresPrefetch(TaskInput) bool { return false }

func (videoPrepareWindowContract) CompletionKind() CompletionKind {
	return CompletionPreparedSegment
}

// videoAssemblyContract declares the final assembly inputs and requires the
// strict preparation gate. Assembly consumes only the compiled plan and its
// runtime bindings; raw producer scenes are not its input.
type videoAssemblyContract struct {
	executorID string
}

func (c videoAssemblyContract) Validate(spec TaskInput) error {
	if spec.ExecutorID != c.executorID {
		return fmt.Errorf("taskcontract: %q cannot validate as %q", spec.ExecutorID, c.executorID)
	}
	planJSON, _ := spec.Payload[contract.PayloadKeyCompiledRenderPlanJSON].(string)
	if strings.TrimSpace(planJSON) == "" {
		return fmt.Errorf("%s: compiled render plan is required", c.executorID)
	}
	if _, err := contract.DecodeCompiledRenderPlanV2([]byte(planJSON)); err != nil {
		return fmt.Errorf("%s: invalid compiled render plan: %w", c.executorID, err)
	}
	if got := contract.RequiredVideoAssemblyExecutor(spec.Payload); got != c.executorID {
		return fmt.Errorf("%s: payload routes to %q", c.executorID, got)
	}
	return nil
}

func (c videoAssemblyContract) RequiredAssets(spec TaskInput) ([]RequiredAsset, error) {
	planJSON, _ := spec.Payload[contract.PayloadKeyCompiledRenderPlanJSON].(string)
	if strings.TrimSpace(planJSON) == "" {
		return nil, fmt.Errorf("%s: compiled render plan is required", c.executorID)
	}
	plan, err := contract.DecodeCompiledRenderPlanV2([]byte(planJSON))
	if err != nil {
		return nil, fmt.Errorf("%s: invalid compiled render plan: %w", c.executorID, err)
	}
	assets := make([]RequiredAsset, 0, len(plan.Assets)+2)
	for _, asset := range plan.Assets {
		key := strings.TrimSpace(asset.AssetKey)
		if key == "" {
			key = strings.TrimSpace(asset.AssetID)
		}
		if key == "" {
			continue
		}
		assets = append(assets, RequiredAsset{
			Key:       key,
			AssetID:   asset.AssetID,
			SHA256:    asset.SHA256,
			SizeBytes: asset.SizeBytes,
			Role:      asset.Kind,
		})
	}
	if plan.StockPool == nil && plan.FinalAudio.AssetID != "" {
		assets = append(assets, RequiredAsset{
			Key:       plan.FinalAudio.AssetID,
			AssetID:   plan.FinalAudio.AssetID,
			SHA256:    plan.FinalAudio.SHA256,
			SizeBytes: plan.FinalAudio.SizeBytes,
			Role:      "final_audio",
		})
	}
	assets = append(assets, runtimeRequiredAssets(spec.Payload["runtime_assets"])...)
	if runtimePayload, ok := spec.Payload["runtime_payload"].(map[string]interface{}); ok {
		assets = append(assets, runtimeRequiredAssets(runtimePayload["runtime_assets"])...)
	}
	return canonicalRequiredAssets(assets), nil
}

func (videoAssemblyContract) RequiresPrefetch(TaskInput) bool { return true }

func (videoAssemblyContract) CompletionKind() CompletionKind {
	return CompletionArtifactCommit
}

type runtimeAssetWire struct {
	AssetID   string `json:"asset_id"`
	AssetKey  string `json:"asset_key"`
	SHA256    string `json:"sha256"`
	SizeBytes int64  `json:"size_bytes"`
	Role      string `json:"role"`
	SourceURI string `json:"source_uri"`
}

func runtimeRequiredAssets(raw interface{}) []RequiredAsset {
	if raw == nil {
		return nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil
	}
	var bindings []runtimeAssetWire
	if err := json.Unmarshal(encoded, &bindings); err != nil {
		return nil
	}
	out := make([]RequiredAsset, 0, len(bindings))
	for _, binding := range bindings {
		key := strings.TrimSpace(binding.AssetKey)
		if key == "" {
			key = strings.TrimSpace(binding.AssetID)
		}
		if key == "" {
			continue
		}
		out = append(out, RequiredAsset{
			Key:       key,
			AssetID:   binding.AssetID,
			SHA256:    binding.SHA256,
			SizeBytes: binding.SizeBytes,
			Role:      binding.Role,
			SourceURI: binding.SourceURI,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}
