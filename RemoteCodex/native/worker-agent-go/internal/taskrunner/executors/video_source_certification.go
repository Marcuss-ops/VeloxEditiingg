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
// complete-source evidence. It shares the existing probe and block-proof
// caches with the legacy window preparer implementation.
func NewVideoSourceCertification(outputRoot string) executor.Executor {
	preparer := NewVideoPrepareWindow(outputRoot).(*videoPrepareWindowExecutor)
	return &videoSourceCertificationExecutor{preparer: preparer}
}

type videoSourceCertificationExecutor struct {
	preparer *videoPrepareWindowExecutor
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
	if e == nil || e.preparer == nil {
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
	if request.OutputProfileID != e.preparer.profile.ProfileID || !contract.SupportsVideoWindowPreparationRevision(e.preparer.profile, e.preparer.quality, request.OutputProfileRevision) {
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
		return fail("SOURCE_CERTIFICATION_INVALID", err)
	}
	bindings, ok := runtimeassets.FromContext(ctx)
	if !ok {
		return fail("SOURCE_CERTIFICATION_INPUT_MISSING", errors.New("verified complete source binding is required"))
	}
	source, ok := bindings[request.Source.AssetID]
	if !ok || !source.Verified || source.Sparse || source.Size != request.Source.SizeBytes || !isLowerSHA256(source.SHA256) {
		return fail("SOURCE_CERTIFICATION_INPUT_INVALID", errors.New("source must be complete, verified and match its declared size"))
	}
	if request.Source.SHA256 != "" && source.SHA256 != request.Source.SHA256 {
		return fail("SOURCE_CERTIFICATION_IDENTITY_MISMATCH", errors.New("resolved source SHA-256 differs from request"))
	}
	info, err := os.Stat(source.Path)
	if err != nil || !info.Mode().IsRegular() || info.Size() != request.Source.SizeBytes {
		if err == nil {
			err = errors.New("source file size changed after resolution")
		}
		return fail("SOURCE_CERTIFICATION_SOURCE_INVALID", err)
	}
	probe, err := e.preparer.probeSource(ctx, source.Path, source.SHA256, true)
	if err != nil {
		return fail("SOURCE_CERTIFICATION_PROBE_FAILED", err)
	}
	windowRequest := contract.VideoPrepareWindowV1{
		Source: request.Source, OutputProfileID: request.OutputProfileID,
		OutputProfileRevision: request.OutputProfileRevision,
	}
	windowRequest.Source.SHA256 = source.SHA256
	certificate := sourceCertificateFromProbe(probe, windowRequest, e.preparer.profile)
	if certificate == nil {
		return fail("SOURCE_CERTIFICATION_INCOMPATIBLE", errors.New("complete source facts do not satisfy the canonical certification contract"))
	}
	chunks, err := e.preparer.certifySourceChunks(ctx, source.Path, source.SHA256, source.Size, probe)
	if err != nil {
		return fail("SOURCE_CERTIFICATION_HASH_FAILED", err)
	}
	certificate.Chunks = chunks
	if err := certificate.Validate(); err != nil {
		return fail("SOURCE_CERTIFICATION_INVALID", err)
	}
	metadata, err := json.Marshal(certificate)
	if err != nil {
		return fail("SOURCE_CERTIFICATION_ENCODE_FAILED", err)
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
