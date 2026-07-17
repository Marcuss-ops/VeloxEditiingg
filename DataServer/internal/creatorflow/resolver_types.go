package creatorflow

import "fmt"

// ResolveRequest is the typed input for Resolver.Resolve.
//
//   - ForwardingID: optional. When set (the runner path), the resolver
//     treats req.ForwardingID as the existing creator_forwardings row
//     from the runner's lease and UPDATES its payload + source_status
//     before the atomic enqueue. When empty (the handler sync path),
//     the resolver INSERTs a fresh PENDING creator_forwardings row and
//     immediately promotes it to READY_TO_FORWARD via the leaseless
//     MarkCreatorForwardingReadySync transition.
//   - SourceProvider: e.g. "remote_engine". Required.
//   - SourceJobID: the remote engine's job id. Required.
//   - TargetExecutorID: the executor that the Velox Job should route
//     to. Optional; defaults to "scene.composite.v1".
//   - Payload: the raw remote-engine response map. Required; must pass
//     enqueue.ShouldForwardPipelineResult or Resolve returns (nil, nil)
//     ("result not complete — caller should keep polling").
type ResolveRequest struct {
	ForwardingID     string
	SourceProvider   string
	SourceJobID      string
	TargetExecutorID string
	Payload          map[string]interface{}
}

// ResolveOutput is what every caller receives. JobID and ForwardingID
// are guaranteed to be the SAME across the handler and runner paths for
// the same (source_provider, source_job_id, target_executor_id) input.
// Response is the HTTP-flavored envelope (job_id, status, ok, …) that
// the handler returns to the client; the runner ignores it.
type ResolveOutput struct {
	JobID        string
	ForwardingID string
	Response     map[string]interface{}
}

// ErrResolverNotComplete is the sentinel for "payload not complete —
// caller should keep polling". Returned as (nil, ErrResolverNotComplete);
// the caller decides whether nil-output means "early exit" (handler:
// respond 202 polling, runner: mark retry-wait).
var ErrResolverNotComplete = fmt.Errorf("creatorflow: Resolve: payload is not complete enough to forward")
