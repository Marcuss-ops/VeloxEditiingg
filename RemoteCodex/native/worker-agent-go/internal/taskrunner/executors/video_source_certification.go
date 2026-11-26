package executors

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"time"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/runtimeassets"
)

const VideoSourceCertificationID = contract.VideoSourceCertificationExecutorID

// NewVideoSourceCertification creates the artifact-free worker executor for
// complete-source evidence. It owns the complete-source probe and block-proof caches used only for
// certification.
func NewVideoSourceCertification(outputRoot string) executor.Executor {
	inspector := newVideoSourceInspector(outputRoot)
	return &videoSourceCertificationExecutor{inspector: inspector}
}

type videoSourceCertificationExecutor struct {
	inspector *videoSourceInspector
}

func (e *videoSourceCertificationExecutor) Descriptor() executor.Descriptor {
	return executor.Descriptor{
		ID: VideoSourceCertificationID, Version: contract.VideoSourceCertificationVersion,
		InputTypes: []string{"video.source.certification.v1"}, ResourceClass: executor.ResourceCPU,
		Deterministic: true, Cacheable: true, TemporalMode: executor.TemporalGlobal,
	}
}

func (e *videoSourceCertificationExecutor) request(spec executor.TaskSpec) (contract.VideoSourceCertificationV1, error) {
	if spec.ExecutorID != VideoSourceCertificationID {
		return contract.VideoSourceCertificationV1{}, fmt.Errorf("video.source.certify.v1: executor_id must be %q", VideoSourceCertificationID)
	}
	if e == nil || e.inspector == nil {
		return contract.VideoSourceCertificationV1{}, errors.New("video.source.certify.v1: source inspector is unavailable")
	}
	raw, ok := spec.Payload[contract.VideoSourceCertificationPayloadKey]
	if !ok || raw == nil {
		return contract.VideoSourceCertificationV1{}, fmt.Errorf("video.source.certify.v1: %q is required", contract.VideoSourceCertificationPayloadKey)
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return contract.VideoSourceCertificationV1{}, err
	}
	var request contract.VideoSourceCertificationV1
	if err := json.Unmarshal(encoded, &request); err != nil {
		return request, err
	}
	if err := request.Validate(); err != nil {
		return request, err
	}
	if request.OutputProfileID != e.inspector.profile.ProfileID || !contract.SupportsVideoWindowPreparationRevision(e.inspector.profile, e.inspector.quality, request.OutputProfileRevision) {
		return request, errors.New("video.source.certify.v1: canonical profile revision is not enabled")
	}
	return request, nil
}

func (e *videoSourceCertificationExecutor) Validate(spec executor.TaskSpec) error {
	_, err := e.request(spec)
	return err
}

func (e *videoSourceCertificationExecutor) Execute(ctx context.Context, execCtx executor.ExecutionContext, spec executor.TaskSpec) (executor.ExecutionResult, error) {
	started := time.Now().UTC()
	fail := func(code string, err error) (executor.ExecutionResult, error) {
		return executor.ExecutionResult{Status: "failed", ErrorCode: code, ErrorDetail: err.Error(), StartedAt: started, CompletedAt: time.Now().UTC()}, nil
	}
	request, err := e.request(spec)
	if err != nil {
		return fail(contract.VideoSourceCertificationInvalidErrorCode, err)
	}
	bindings, ok := runtimeassets.FromContext(ctx)
	if !ok {
		return fail(contract.VideoSourceCertificationInputMissingErrorCode, errors.New("verified complete source binding is required"))
	}
	source, ok := bindings[request.Source.AssetID]
	if !ok || !source.Verified || source.Sparse || source.Size != request.Source.SizeBytes || !isLowerSHA256(source.SHA256) {
		return fail(contract.VideoSourceCertificationInputInvalidErrorCode, errors.New("source must be complete, verified and match its declared size"))
	}
	if request.Source.SHA256 != "" && source.SHA256 != request.Source.SHA256 {
		return fail(contract.VideoSourceCertificationIdentityMismatchErrorCode, errors.New("resolved source SHA-256 differs from request"))
	}
	info, err := os.Stat(source.Path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != request.Source.SizeBytes {
		if err == nil {
			err = errors.New("source file size changed after resolution")
		}
		return fail(contract.VideoSourceCertificationSourceInvalidErrorCode, err)
	}
	probe, err := e.inspector.probeSource(ctx, source.Path, source.SHA256)
	if err != nil {
		return fail(contract.VideoSourceCertificationProbeFailedErrorCode, err)
	}
	if err := e.inspector.ensureExactSourceFrameCount(ctx, source.Path, source.SHA256, &probe); err != nil {
		return fail(contract.VideoSourceCertificationProbeFailedErrorCode, err)
	}
	asset := request.Source
	asset.SHA256 = source.SHA256
	certificate := sourceCertificateFromProbe(probe, asset, request.OutputProfileID, request.OutputProfileRevision, e.inspector.profile)
	if certificate == nil {
		return fail(contract.VideoSourceCertificationIncompatibleErrorCode, errors.New("complete source facts do not satisfy the canonical certification contract"))
	}
	chunks, err := e.inspector.certifySourceChunks(ctx, source.Path, source.SHA256, source.Size, probe, source.SourceChunks)
	if err != nil {
		return fail(contract.VideoSourceCertificationHashFailedErrorCode, err)
	}
	certificate.Chunks = chunks
	if err := certificate.Validate(); err != nil {
		return fail(contract.VideoSourceCertificationInvalidErrorCode, err)
	}
	metadata, err := json.Marshal(certificate)
	if err != nil {
		return fail(contract.VideoSourceCertificationEncodeFailedErrorCode, err)
	}
	completed := time.Now().UTC()
	return executor.ExecutionResult{
		Status: "succeeded",
		Segments: []executor.SegmentTiming{{
			SceneID: request.Source.AssetID, SourceType: VideoSourceCertificationID,
			DurationMS: float64(completed.Sub(started).Milliseconds()), SourceBytes: source.Size,
			FramesEncoded: 0, Codec: certificate.Evidence.Codec, Status: "succeeded",
			CacheKey: "video-source:" + certificate.SHA256, MetadataJSON: string(metadata),
		}}, StartedAt: started, CompletedAt: completed,
	}, nil
}

var _ executor.Executor = (*videoSourceCertificationExecutor)(nil)
