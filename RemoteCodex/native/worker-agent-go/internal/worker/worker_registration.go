package worker

// worker_registration.go: registration / hello handshake metadata
// — what the worker advertises to the master at Hello time and
// keeps in sync with heartbeat.Extra.capabilities. Single source of
// truth (capabilityReport) is reused by both worker_registration.go
// (buildHello) and worker_comms.go (sendHeartbeat); any wire-shape
// change must touch one function. Resource sampling lives behind
// w.sampler (telemetry.Sampler) — see worker_types.go for the field
// definition.
//
// Extracted from worker.go (commit 2c5392e → next).

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"os"
	"runtime"
	"strings"

	"velox-shared/controltransport"
	"velox-worker-agent/internal/executor"
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
// w.capabilitiesMap(hostname), the protobuf-boundary projection of the
// typed capabilityReport. Any wire-shape change touches one function.
func (w *Worker) buildHello() controltransport.WorkerHello {
	hostname, _ := os.Hostname()

	hello := controltransport.WorkerHello{
		WorkerID:        w.config.WorkerID,
		WorkerName:      w.workerName(hostname),
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

// workerName returns the operator-configured display name when present.
// WorkerID remains the immutable routing identity; the derived physical-host
// name is only a compatibility fallback for older configurations.
func (w *Worker) workerName(hostname string) string {
	if name := strings.TrimSpace(w.config.WorkerName); name != "" {
		return name
	}
	return workerDisplayName(hostname)
}

// workerDisplayName is the compatibility fallback for configurations without
// an explicit worker_name. The configured worker_id remains the stable
// routing identity.
func workerDisplayName(hostname string) string {
	if strings.TrimSpace(hostname) == "" {
		hostname, _ = os.Hostname()
	}
	ip := "unknown-ip"
	if conn, err := net.Dial("udp", "8.8.8.8:80"); err == nil {
		if addr, ok := conn.LocalAddr().(*net.UDPAddr); ok && addr.IP != nil {
			ip = addr.IP.String()
		}
		_ = conn.Close()
	} else if interfaces, err := net.Interfaces(); err == nil {
		for _, iface := range interfaces {
			addrs, _ := iface.Addrs()
			for _, addr := range addrs {
				if ipnet, ok := addr.(*net.IPNet); ok && ipnet.IP.To4() != nil && !ipnet.IP.IsLoopback() {
					ip = ipnet.IP.String()
					break
				}
			}
			if ip != "unknown-ip" {
				break
			}
		}
	}
	return fmt.Sprintf("worker_%s_%s", strings.TrimSpace(hostname), strings.ReplaceAll(ip, ":", "_"))
}

// capabilityReport is the typed single source of truth for worker
// capabilities. The map conversion happens only when the protobuf Struct
// transport requires it.
func (w *Worker) capabilityReport(hostname string) controltransport.CapabilityReport {
	report := executor.BuildCapabilityReport(w.executorRegistry, w.hostInfo(hostname, w.concurrencyLimiter.MaxActiveJobs()))
	report.Features = []string{
		controltransport.CapabilityArtifactCommitV1,
		controltransport.CapabilityTaskOutputDeclaredV1,
		controltransport.CapabilityArtifactUploadPlanV1,
		controltransport.CapabilityArtifactUploadCompletedV1,
		controltransport.CapabilityTaskCommitAckV1,
		controltransport.CapabilityCanonicalPayloadV2,
	}
	if w.config.IsCreatorProfile() {
		report.Features = append(report.Features,
			CapabilityScriptGenerate,
			CapabilityVoiceoverGenerateItem,
			CapabilityImageGenerateGoogle,
		)
	}
	if w.clipCache != nil {
		if keys, err := w.clipCache.ReadyKeys(context.Background()); err == nil {
			const maxAdvertisedCacheKeys = 2048
			if len(keys) > maxAdvertisedCacheKeys {
				report.AssetCacheTruncated = true
				keys = keys[:maxAdvertisedCacheKeys]
			}
			report.AssetCacheKeys = keys
		}
	}
	return report
}

// capabilitiesMap is the sole protobuf-boundary projection of the typed
// capability report. Both buildHello and sendHeartbeat call it.
//
// Any internal consumer must use capabilityReport or ExecutorRegistry,
// never inspect this map.
//

// Concurrency invariants:
//   - max_parallel_jobs is sourced ONCE from w.concurrencyLimiter and
//     published in the host block only (capabilities.host.max_parallel_jobs).
//     A ConfigurationUpdate flipped via SetMaxActiveJobs is visible on the
//     next capabilitiesMap call. There is no top-level mirror: only
//     protocol v3 workers are admitted (controltransport.IsSupportedProtocol
//     == ProtocolVersionCurrent) and current masters read the host block.
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
	m := w.capabilityReport(hostname).AsMap()
	// max_parallel_jobs has a SINGLE wire representation:
	// capabilities.host.max_parallel_jobs (see CapabilityReport.AsMap).
	// The legacy top-level mirror was removed — the v3-only protocol gate
	// (controltransport.IsSupportedProtocol) guarantees every admitted
	// worker/master pair can walk into the host sub-block.

	// ReleaseIdentity is the single release certificate shared with the
	// master: the canonical block rides under release_identity while the
	// flat legacy keys keep the master runtime snapshot columns populated.
	// Both hello and heartbeat publish it because they share this map.
	ri := w.loadReleaseIdentity()
	if !ri.IsEmpty() {
		m[controltransport.CapabilityReleaseIdentityKey] = ri.AsCapabilitiesBlock()
		for k, v := range ri.FlatLegacyKeys() {
			m[k] = v
		}
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
func (w *Worker) hostInfo(hostname string, maxParallel int) controltransport.HostInfo {
	host := controltransport.HostInfo{
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
