package worker

import (
	"fmt"
	"os"
	"path/filepath"

	pb "velox-shared/controltransport/pb"
	"velox-worker-agent/internal/spool"
	"velox-worker-agent/internal/taskrunner"
)

func validateArtifactOutputs(report *taskrunner.TaskExecutionReport) error {
	if report == nil {
		return fmt.Errorf("worker artifact upload: report is nil")
	}
	for _, ref := range report.Outputs {
		if ref.URI == "" || ref.Hash == "" || ref.SizeBytes <= 0 {
			return fmt.Errorf("worker artifact upload: output %q has incomplete local manifest", ref.Type)
		}
		if _, err := os.Stat(ref.URI); err != nil {
			return fmt.Errorf("worker artifact upload: output %q is not readable: %w", ref.Type, err)
		}
	}
	return nil
}

func buildOutputManifests(pte *PendingTaskExecution, report *taskrunner.TaskExecutionReport) []*pb.OutputManifest {
	manifests := make([]*pb.OutputManifest, 0, len(report.Outputs))
	for i, ref := range report.Outputs {
		kind, mime := outputKindAndMime(ref.Type)
		manifests = append(manifests, &pb.OutputManifest{
			OutputKind:     kind,
			LogicalName:    filepath.Base(ref.URI),
			MimeType:       mime,
			SizeBytes:      ref.SizeBytes,
			Sha256:         ref.Hash,
			WorkerSpoolKey: fmt.Sprintf("%s:output:%d", pte.TaskID, i),
		})
	}
	return manifests
}

func outputKindAndMime(refType string) (kind, mime string) {
	switch refType {
	case "render.output":
		return "final_video", "video/mp4"
	case "engine.progress.sidecar", "engine_progress_sidecar":
		return "engine_progress_sidecar", "application/json"
	default:
		return refType, "application/octet-stream"
	}
}

func mimeForOutputKind(kind string) string {
	switch kind {
	case "final_video":
		return "video/mp4"
	case "engine_progress.sidecar":
		return "application/json"
	default:
		return "application/octet-stream"
	}
}

func (w *Worker) outputStorageTier(uri string) spool.StorageTier {
	if w == nil || w.storageResolver == nil {
		return spool.StorageTierNvmeDurable
	}
	if dir := w.storageResolver.Config().ArtifactStaging.Dir; dir != "" && pathWithinRoot(dir, uri) {
		return spool.StorageTierTmpfsVolatile
	}
	return spool.StorageTierNvmeDurable
}
