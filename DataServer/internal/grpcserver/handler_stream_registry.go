// handler_stream_registry.go — hello → in-memory registry bridge.
//
// The gRPC hello handler stores the worker's capability report in the
// runtime snapshot DB and on the control session, but until this file
// existed it never forwarded the report into the in-memory worker
// registry (the read model behind GET /api/v1/workers). The legacy HTTP
// registration path (handlers/remote/workers/lifecycle/registration.go)
// populated extra["capabilities"], which applyMetadataFields uses to
// derive DeclaredMaxSlots. Without the same bridge on the gRPC hello,
// v3-only workers read task_slots=0 in the API even though they publish
// capabilities.host.max_parallel_jobs=1.
package grpcserver

import (
	"context"

	"velox-server/internal/logging"
	pb "velox-shared/controltransport/pb"
)

// registerHelloCapabilitiesInRegistry forwards the hello-declared
// capability report into the in-memory registry so the read model
// surfaces the canonical worker capacity (task_slots =
// host.max_parallel_jobs). It mirrors the legacy HTTP registration
// adapter (extra["capabilities"]) and is a no-op when the registry is
// not wired or the report is empty.
func (h *Handler) registerHelloCapabilitiesInRegistry(
	ctx context.Context,
	workerID, workerName, peerIP, protocolVersion string,
	caps map[string]interface{},
	hello *pb.Hello,
) {
	if h.registry == nil || len(caps) == 0 {
		return
	}
	extra := map[string]interface{}{
		"capabilities":     caps,
		"code_version":     hello.GetVersion(),
		"bundle_version":   hello.GetBundleVersion(),
		"bundle_hash":      hello.GetBundleHash(),
		"protocol_version": protocolVersion,
		"engine_version":   hello.GetEngineVersion(),
	}
	if err := h.registry.RegisterWorker(ctx, workerID, workerName, peerIP, extra); err != nil {
		logGRPCf(ctx, logging.LevelWarn, logging.CodeGRPCRegistryBridge, "[GRPC] Worker %s registry registration from hello failed: %v", workerID, err)
	}
}
