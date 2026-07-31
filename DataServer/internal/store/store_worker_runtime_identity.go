package store

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

// WorkerRuntimeSnapshot is the canonical runtime identity captured by the
// master when a worker Hello is admitted. The row is immutable after insert;
// attempts reference SnapshotID rather than trusting worker-reported values.
type WorkerRuntimeSnapshot struct {
	SnapshotID        string
	WorkerID          string
	SessionID         string
	Hostname          string
	NodeID            string
	WorkerName        string
	WorkerClass       string
	RolloutGroup      string
	GitSHA            string
	WorkerVersion     string
	BundleVersion     string
	BundleHash        string
	EngineVersion     string
	FFmpegVersion     string
	ProtocolVersion   string
	ConfigHash        string
	DockerImageDigest string
	CPUModel          string
	LogicalCPUCount   int
	EffectiveCPUCount int
	CPUQuota          float64
	TotalMemoryBytes  int64
	GPUModel          string
	GPUDriver         string
	KernelVersion     string
	OSRelease         string
	StorageClass      string
	CapabilitiesJSON  string
	ConnectedAt       time.Time
}

// GetOrCreateWorkerRuntimeSnapshot returns the immutable snapshot for a
// worker/session pair, creating it if necessary. INSERT OR IGNORE makes a
// retried Hello safe; an existing row is never updated with a later Hello.
func (s *SQLiteStore) GetOrCreateWorkerRuntimeSnapshot(snapshot WorkerRuntimeSnapshot) (*WorkerRuntimeSnapshot, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("worker runtime snapshot: store not initialized")
	}
	if snapshot.WorkerID == "" || snapshot.SessionID == "" {
		return nil, fmt.Errorf("worker runtime snapshot: worker_id and session_id are required")
	}
	if snapshot.SnapshotID == "" {
		snapshot.SnapshotID = uuid.NewString()
	}
	if snapshot.ConnectedAt.IsZero() {
		snapshot.ConnectedAt = time.Now().UTC()
	}
	if snapshot.CapabilitiesJSON == "" {
		snapshot.CapabilitiesJSON = "{}"
	}
	if !json.Valid([]byte(snapshot.CapabilitiesJSON)) {
		return nil, fmt.Errorf("worker runtime snapshot: capabilities_json must be valid JSON")
	}

	connectedAt := snapshot.ConnectedAt.UTC().Format(time.RFC3339Nano)
	_, err := s.db.Exec(`
		INSERT OR IGNORE INTO worker_runtime_snapshots (
			snapshot_id, worker_id, session_id,
			hostname, node_id, worker_name, worker_class, rollout_group,
			git_sha, worker_version, bundle_version, bundle_hash,
			engine_version, ffmpeg_version, protocol_version,
			config_hash, docker_image_digest, cpu_model,
			logical_cpu_count, effective_cpu_count, cpu_quota,
			total_memory_bytes, gpu_model, gpu_driver, kernel_version,
			os_release, storage_class, capabilities_json, connected_at
		) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		snapshot.SnapshotID, snapshot.WorkerID, snapshot.SessionID,
		snapshot.Hostname, snapshot.NodeID, snapshot.WorkerName,
		snapshot.WorkerClass, snapshot.RolloutGroup,
		snapshot.GitSHA, snapshot.WorkerVersion, snapshot.BundleVersion,
		snapshot.BundleHash, snapshot.EngineVersion, snapshot.FFmpegVersion,
		snapshot.ProtocolVersion, snapshot.ConfigHash, snapshot.DockerImageDigest,
		snapshot.CPUModel, snapshot.LogicalCPUCount, snapshot.EffectiveCPUCount,
		snapshot.CPUQuota, snapshot.TotalMemoryBytes, snapshot.GPUModel,
		snapshot.GPUDriver, snapshot.KernelVersion, snapshot.OSRelease,
		snapshot.StorageClass, snapshot.CapabilitiesJSON, connectedAt,
	)
	if err != nil {
		return nil, fmt.Errorf("worker runtime snapshot insert: %w", err)
	}

	return s.GetWorkerRuntimeSnapshotBySession(snapshot.WorkerID, snapshot.SessionID)
}

// GetWorkerRuntimeSnapshotBySession returns the immutable snapshot linked to
// a worker's exact gRPC session.
func (s *SQLiteStore) GetWorkerRuntimeSnapshotBySession(workerID, sessionID string) (*WorkerRuntimeSnapshot, error) {
	if s == nil || s.db == nil {
		return nil, fmt.Errorf("worker runtime snapshot: store not initialized")
	}
	var snapshot WorkerRuntimeSnapshot
	var connectedAt string
	var nodeID, workerName, workerClass, rolloutGroup sql.NullString
	var gitSHA, workerVersion, bundleVersion, bundleHash sql.NullString
	var engineVersion, ffmpegVersion, protocolVersion sql.NullString
	var configHash, dockerImageDigest, cpuModel sql.NullString
	var gpuModel, gpuDriver, kernelVersion, osRelease, storageClass sql.NullString
	var capabilitiesJSON sql.NullString
	var disconnectedAt sql.NullString
	_ = disconnectedAt // reserved for a future lifecycle read model

	err := s.db.QueryRow(`
		SELECT snapshot_id, worker_id, session_id, hostname,
		       node_id, worker_name, worker_class, rollout_group,
		       git_sha, worker_version, bundle_version, bundle_hash,
		       engine_version, ffmpeg_version, protocol_version,
		       config_hash, docker_image_digest, cpu_model,
		       logical_cpu_count, effective_cpu_count, cpu_quota,
		       total_memory_bytes, gpu_model, gpu_driver, kernel_version,
		       os_release, storage_class, capabilities_json, connected_at
		  FROM worker_runtime_snapshots
		 WHERE worker_id = ? AND session_id = ?`, workerID, sessionID).Scan(
		&snapshot.SnapshotID, &snapshot.WorkerID, &snapshot.SessionID,
		&snapshot.Hostname, &nodeID, &workerName, &workerClass, &rolloutGroup,
		&gitSHA, &workerVersion, &bundleVersion, &bundleHash,
		&engineVersion, &ffmpegVersion, &protocolVersion,
		&configHash, &dockerImageDigest, &cpuModel,
		&snapshot.LogicalCPUCount, &snapshot.EffectiveCPUCount, &snapshot.CPUQuota,
		&snapshot.TotalMemoryBytes, &gpuModel, &gpuDriver, &kernelVersion,
		&osRelease, &storageClass, &capabilitiesJSON, &connectedAt,
	)
	if err == sql.ErrNoRows {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("worker runtime snapshot lookup: %w", err)
	}

	snapshot.NodeID = nodeID.String
	snapshot.WorkerName = workerName.String
	snapshot.WorkerClass = workerClass.String
	snapshot.RolloutGroup = rolloutGroup.String
	snapshot.GitSHA = gitSHA.String
	snapshot.WorkerVersion = workerVersion.String
	snapshot.BundleVersion = bundleVersion.String
	snapshot.BundleHash = bundleHash.String
	snapshot.EngineVersion = engineVersion.String
	snapshot.FFmpegVersion = ffmpegVersion.String
	snapshot.ProtocolVersion = protocolVersion.String
	snapshot.ConfigHash = configHash.String
	snapshot.DockerImageDigest = dockerImageDigest.String
	snapshot.CPUModel = cpuModel.String
	snapshot.GPUModel = gpuModel.String
	snapshot.GPUDriver = gpuDriver.String
	snapshot.KernelVersion = kernelVersion.String
	snapshot.OSRelease = osRelease.String
	snapshot.StorageClass = storageClass.String
	snapshot.CapabilitiesJSON = capabilitiesJSON.String
	snapshot.ConnectedAt, _ = time.Parse(time.RFC3339Nano, connectedAt)
	return &snapshot, nil
}
