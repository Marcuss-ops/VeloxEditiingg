package worker

// Package worker provides the core worker orchestration logic.
// Methods have been split into per-responsibility files:
//   - worker_lifecycle.go   — Start, Stop, runSession, isConnectionLevelError
//   - worker_registration.go — buildHello, capabilitiesMap, normalizeOfferedExecutorID, hostInfo
//   - worker_claimloop.go   — receiveLoop and dispatch helpers (sendTaskAccepted/Reject,
//     getIntParam, ConnState/setConnState/newTransport, storePendingTask, takePendingTask)
//   - worker_artifacts.go   — typed pending-msg dispatcher (Artifact Commit Protocol v1)
// Helper types live in worker_types.go; heartbeat/lease-renewal in worker_comms.go;
// command processing in worker_commands.go; persistence in worker_persistence.go.
// Sibling files for transport, asset bridging, job execution, output upload sit
// alongside this orchestrator and share the same `package worker` visibility.
