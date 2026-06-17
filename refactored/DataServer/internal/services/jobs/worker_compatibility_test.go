package jobs

import (
	"context"
	"testing"

	"velox-server/internal/config"
	"velox-server/internal/workers"
)

func TestCheckWorkerCompatibility_ValidWorker(t *testing.T) {
	s := &Service{cfg: &config.Config{Workers: config.WorkersConfig{VersionNumber: "v1.0.6"}}}
	worker := &workers.WorkerInfo{
		WorkerID:        "test-worker",
		ProtocolVersion: workers.DefaultWorkerProtocolVersion,
		Capabilities: map[string]interface{}{
			"supported_job_types": []interface{}{"health_check", "process_video", "process_audio"},
		},
	}
	if reason := s.checkWorkerCompatibility(context.Background(), worker, ""); reason != "" {
		t.Fatalf("expected compatible, got: %s", reason)
	}
}

func TestCheckWorkerCompatibility_NilWorker(t *testing.T) {
	s := &Service{cfg: &config.Config{}}
	if reason := s.checkWorkerCompatibility(context.Background(), nil, ""); reason == "" {
		t.Fatal("expected rejection for nil worker")
	}
}

func TestCheckWorkerCompatibility_MissingProtocolVersion(t *testing.T) {
	s := &Service{cfg: &config.Config{}}
	worker := &workers.WorkerInfo{
		WorkerID:        "test-worker",
		ProtocolVersion: "",
		Capabilities:    map[string]interface{}{"supported_job_types": []interface{}{"health_check"}},
	}
	if reason := s.checkWorkerCompatibility(context.Background(), worker, ""); reason == "" {
		t.Fatal("expected rejection for missing protocol_version")
	}
}

func TestCheckWorkerCompatibility_ProtocolVersionMismatch(t *testing.T) {
	s := &Service{cfg: &config.Config{}}
	worker := &workers.WorkerInfo{
		WorkerID:        "test-worker",
		ProtocolVersion: "2025-01-legacy-v1",
		Capabilities:    map[string]interface{}{"supported_job_types": []interface{}{"health_check"}},
	}
	if reason := s.checkWorkerCompatibility(context.Background(), worker, ""); reason == "" {
		t.Fatal("expected rejection for protocol_version mismatch")
	}
}

func TestCheckWorkerCompatibility_MissingCapabilities(t *testing.T) {
	s := &Service{cfg: &config.Config{}}
	worker := &workers.WorkerInfo{
		WorkerID:        "test-worker",
		ProtocolVersion: workers.DefaultWorkerProtocolVersion,
		Capabilities:    nil,
	}
	if reason := s.checkWorkerCompatibility(context.Background(), worker, ""); reason == "" {
		t.Fatal("expected rejection for missing capabilities")
	}
}

func TestCheckWorkerCompatibility_UnsupportedJobType(t *testing.T) {
	s := &Service{cfg: &config.Config{}}
	worker := &workers.WorkerInfo{
		WorkerID:        "test-worker",
		ProtocolVersion: workers.DefaultWorkerProtocolVersion,
		Capabilities: map[string]interface{}{
			"supported_job_types": []interface{}{"health_check"},
		},
	}
	if reason := s.checkWorkerCompatibility(context.Background(), worker, "process_video"); reason == "" {
		t.Fatal("expected rejection for unsupported job type")
	}
}

func TestCheckWorkerCompatibility_SupportedJobType(t *testing.T) {
	s := &Service{cfg: &config.Config{}}
	worker := &workers.WorkerInfo{
		WorkerID:        "test-worker",
		ProtocolVersion: workers.DefaultWorkerProtocolVersion,
		Capabilities: map[string]interface{}{
			"supported_job_types": []interface{}{"health_check", "process_video"},
		},
	}
	if reason := s.checkWorkerCompatibility(context.Background(), worker, "process_video"); reason != "" {
		t.Fatalf("expected compatible, got: %s", reason)
	}
}

func TestCheckWorkerCompatibility_EmptyJobTypeSkipsCheck(t *testing.T) {
	s := &Service{cfg: &config.Config{}}
	worker := &workers.WorkerInfo{
		WorkerID:        "test-worker",
		ProtocolVersion: workers.DefaultWorkerProtocolVersion,
		Capabilities: map[string]interface{}{
			"supported_job_types": []interface{}{"health_check"},
		},
	}
	if reason := s.checkWorkerCompatibility(context.Background(), worker, ""); reason != "" {
		t.Fatalf("expected compatible with empty job type, got: %s", reason)
	}
}

func TestCheckWorkerCompatibility_EmptySupportedJobTypesSkipsCheck(t *testing.T) {
	s := &Service{cfg: &config.Config{}}
	worker := &workers.WorkerInfo{
		WorkerID:        "test-worker",
		ProtocolVersion: workers.DefaultWorkerProtocolVersion,
		Capabilities: map[string]interface{}{
			"other_key": "value",
		},
	}
	if reason := s.checkWorkerCompatibility(context.Background(), worker, "process_video"); reason != "" {
		t.Fatalf("expected compatible when no supported_job_types defined, got: %s", reason)
	}
}

func TestCheckWorkerCompatibility_MissingBundleHash(t *testing.T) {
	s := &Service{cfg: &config.Config{}, masterBundleHash: "abc123"}
	worker := &workers.WorkerInfo{
		WorkerID:        "test-worker",
		ProtocolVersion: workers.DefaultWorkerProtocolVersion,
		BundleHash:      "",
		Capabilities:    map[string]interface{}{"supported_job_types": []interface{}{"health_check"}},
	}
	if reason := s.checkWorkerCompatibility(context.Background(), worker, ""); reason != "" {
		t.Fatalf("expected compatible (warning only), got: %s", reason)
	}
}

func TestCheckWorkerCompatibility_BundleHashMismatch(t *testing.T) {
	s := &Service{cfg: &config.Config{}, masterBundleHash: "abc123"}
	worker := &workers.WorkerInfo{
		WorkerID:        "test-worker",
		ProtocolVersion: workers.DefaultWorkerProtocolVersion,
		BundleHash:      "wrong_hash",
		Capabilities:    map[string]interface{}{"supported_job_types": []interface{}{"health_check"}},
	}
	if reason := s.checkWorkerCompatibility(context.Background(), worker, ""); reason != "" {
		t.Fatalf("expected compatible (warning only), got: %s", reason)
	}
}

func TestCheckWorkerCompatibility_BundleHashMatch(t *testing.T) {
	s := &Service{cfg: &config.Config{}, masterBundleHash: "abc123"}
	worker := &workers.WorkerInfo{
		WorkerID:        "test-worker",
		ProtocolVersion: workers.DefaultWorkerProtocolVersion,
		BundleHash:      "abc123",
		Capabilities:    map[string]interface{}{"supported_job_types": []interface{}{"health_check"}},
	}
	if reason := s.checkWorkerCompatibility(context.Background(), worker, ""); reason != "" {
		t.Fatalf("expected compatible with matching bundle_hash, got: %s", reason)
	}
}

func TestCheckWorkerCompatibility_NoMasterHashSkipsCheck(t *testing.T) {
	s := &Service{cfg: &config.Config{}, masterBundleHash: ""}
	worker := &workers.WorkerInfo{
		WorkerID:        "test-worker",
		ProtocolVersion: workers.DefaultWorkerProtocolVersion,
		BundleHash:      "",
		Capabilities:    map[string]interface{}{"supported_job_types": []interface{}{"health_check"}},
	}
	if reason := s.checkWorkerCompatibility(context.Background(), worker, ""); reason != "" {
		t.Fatalf("expected compatible when master has no bundle_hash, got: %s", reason)
	}
}
