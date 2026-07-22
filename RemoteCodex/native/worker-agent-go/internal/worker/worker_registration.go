package worker

// worker_registration.go: registration / hello handshake metadata
// — what the worker advertises to the master at Hello time and
// keeps in sync with heartbeat.Extra.capabilities. Single source of
// truth (capabilitiesMap) is reused by both worker_registration.go
// (buildHello) and worker_comms.go (sendHeartbeat); any wire-shape
// change must touch one function. Resource sampling lives behind
// w.sampler (telemetry.Sampler) — see worker_types.go for the field
// definition.
//
// Extracted from worker.go (commit 2c5392e → next).

import (
	"crypto/sha256"
	"encoding/hex"
	"os"
	"runtime"
	"strings"

	"velox-shared/controltransport"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/pkg/api"
)

// Canonical capability keys advertised by the creator profile. These
// must stay aligned with the master's routing keys.
const (
	CapabilityScriptGenerate        = "script.generate"
	CapabilityVoiceoverGenerateItem = "voiceover.generate_item"
	CapabilityImageGenerateGoogle   = "image.generate.google"
)

// buildHello constructs a WorkerHello from the worker configuration.
// PR-3.5: the capability payload is derived EXCLUSIVELY from
// w.capabilitiesMap(hostname) — a single helper also used by
// sendHeartbeat. Any wire-shape change touches one function.
func (w *Worker) buildHello() controltransport.WorkerHello {
	hostname, _ := os.Hostname()

	hello := controltransport.WorkerHello{
		WorkerID:        w.config.WorkerID,
		WorkerName:      w.config.WorkerName,
		Hostname:        hostname,
		Version:         w.version,
		BundleVersion:   w.config.BundleVersion,
		BundleHash:      w.config.BundleHash,
		ProtocolVersion: w.config.ProtocolVersion,
		EngineVersion:   w.config.EngineVersion,
		WorkerClass:     w.config.WorkerClass,
		RolloutGroup:    w.config.RolloutGroup,
		Capabilities:    w.capabilitiesMap(hostname),
	}

	// Compute persistent credential hash if worker secret is configured.
	// Credential = SHA-256(workerID + ":" + workerSecret)
	if w.config.WorkerSecret != "" {
		h := sha256.New()
		h.Write([]byte(w.config.WorkerID + ":" + w.config.WorkerSecret))
		hello.CredentialHash = hex.EncodeToString(h.Sum(nil))
		w.logger.Debug("[AUTH] Credential hash computed for registration")
	}

	return hello
}

// capabilitiesMap is the SINGLE source of truth for the worker's
// capability map. Both buildHello and sendHeartbeat call it; any change
// to wire shape touches one function, not two.
//
// Concurrency invariants:
//   - max_parallel_jobs is sourced ONCE from w.concurrencyLimiter (host
//     block). The top-level mirror reads from the SAME host value, so a
//     ConfigurationUpdate flipped via SetMaxActiveJobs is visible in
//     BOTH locations atomically per capabilitiesMap call.
//   - AsMap emits an empty slice (not nil) when the registry is empty so
//     encoding/json never silently drops the executors key.
//
// Artifact Commit Protocol (Fase 3.7-3.12): the umbrella
// CapabilityArtifactCommitV1 is the load-bearing capability that
// routes the worker to the typed declare/plan/complete/ack path on
// the master. The 4 phase-specific caps are published alongside for
// forward-compat dispatch; the master only consults the umbrella for
// the v1 cutover.
func (w *Worker) capabilitiesMap(hostname string) map[string]interface{} {
	host := w.hostInfo(hostname, w.concurrencyLimiter.MaxActiveJobs())
	report := executor.BuildCapabilityReport(w.executorRegistry, host)
	m := report.AsMap()
	// Top-level mirror of host.max_parallel_jobs for legacy master
	// decoders that don't walk into the host sub-block. Sourced from
	// the SAME host field — both paths MUST stay byte-identical.
	m["max_parallel_jobs"] = host.MaxParallelJobs
	// Artifact Commit Protocol v1: typed declare/plan/complete/ack
	// pipeline. The master consults this capability at dispatch and
	// routes the worker to the typed path only when it is present.
	m[controltransport.CapabilityArtifactCommitV1] = true
	m[controltransport.CapabilityTaskOutputDeclaredV1] = true
	m[controltransport.CapabilityArtifactUploadPlanV1] = true
	m[controltransport.CapabilityArtifactUploadCompletedV1] = true
	m[controltransport.CapabilityTaskCommitAckV1] = true

	// Creator profile: advertise the creative job types the master uses
	// to route script, voiceover and image generation work. Without these
	// keys the master would never schedule creator jobs on this worker.
	if w.config.IsCreatorProfile() {
		m[CapabilityScriptGenerate] = true
		m[CapabilityVoiceoverGenerateItem] = true
		m[CapabilityImageGenerateGoogle] = true
	}
	return m
}

// normalizeOfferedExecutorID strips an accidental "@version" suffix
// from a task offer's executor_id when the master already split the
// version into executor_version. Registry descriptors forbid '@' in
// the base ID, so the last '@' unambiguously identifies the suffix.
func normalizeOfferedExecutorID(id string) string {
	if i := strings.LastIndex(id, "@"); i > 0 {
		return id[:i]
	}
	return id
}

// hostInfo packages the static host-side fields of the capability report.
// All values are pre-shaped so PR-3.6's resource sampler can fill
// RAMBytes / DiskFreeBytes / HasGPU without breaking the wire contract —
// the master will simply start seeing non-zero values.
//
// F4 integration: Host() is consulted lazily on every hostInfo call (cheap
// atomic.Pointer load); the sampler publishes refreshed values from its
// background 5s tick loop. If the sampler hasn't yet booted (pre-tick),
// the related HostInfo fields default to zero — same wire contract the
// master has handled for years (zero == "not yet sampled").
func (w *Worker) hostInfo(hostname string, maxParallel int) api.HostInfo {
	host := api.HostInfo{
		WorkerID:        w.config.WorkerID,
		Hostname:        hostname,
		CPUCount:        runtime.NumCPU(),
		MaxParallelJobs: maxParallel,
	}
	if w.sampler != nil {
		if h := w.sampler.Host(); h != nil {
			host.HasGPU = h.HasGPU
			host.RAMBytes = h.RAMBytes
			host.DiskFreeBytes = h.DiskFreeBytes
		}
	}
	return host
}
