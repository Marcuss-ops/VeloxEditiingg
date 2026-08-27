package grpcserver

import (
	"context"
	"fmt"

	"velox-server/internal/logging"
	"velox-shared/controltransport"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (h *Handler) checkArtifactProgressiveCapability(workerID string) error {
	sess := h.getSession(workerID)
	if sess == nil {
		return status.Errorf(codes.FailedPrecondition, "progressive artifact upload refused: worker session is unavailable")
	}
	sess.capabilitiesMu.RLock()
	capabilities := make(controltransport.CapabilitySet, len(sess.capabilities))
	copy(capabilities, sess.capabilities)
	sess.capabilitiesMu.RUnlock()
	supported := capabilities.Has(controltransport.CapabilityArtifactProgressiveUploadV1)
	if !supported {
		err := fmt.Errorf("worker %s did not negotiate %s", workerID, controltransport.CapabilityArtifactProgressiveUploadV1)
		logGRPCf(context.Background(), logging.LevelWarn, logging.CodeGRPCArtifactUploadRejected, "[GRPC] %v", err)
		return status.Error(codes.FailedPrecondition, err.Error())
	}
	return nil
}
