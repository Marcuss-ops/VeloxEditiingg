package observability

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// ProductionDoctor evaluates the operator-critical fleet invariants from the
// same worker read model used by the admin API. It reports UNKNOWN when an
// agent has not published a dimension; UNKNOWN is intentionally unhealthy in
// production so missing telemetry cannot masquerade as readiness.
func (s *Service) ProductionDoctor(_ context.Context) (*ProductionDoctorResult, error) {
	if s == nil || s.workers == nil {
		return nil, fmt.Errorf("observability: worker reader not configured")
	}
	workers, err := s.workers.ListWorkers()
	if err != nil {
		return nil, fmt.Errorf("observability: list workers: %w", err)
	}
	result := &ProductionDoctorResult{
		Environment: "production",
		GeneratedAt: time.Now().UTC().Format(time.RFC3339Nano),
		Healthy:     true,
		Checks:      make([]DoctorCheck, 0),
	}
	if len(workers) == 0 {
		result.Healthy = false
		result.Checks = append(result.Checks, DoctorCheck{Check: "fleet", Status: "FAIL", Detail: "no registered workers"})
		return result, nil
	}
	for _, worker := range workers {
		id, _ := worker["worker_id"].(string)
		name, _ := worker["worker_name"].(string)
		check := func(kind, status, detail string) {
			result.Checks = append(result.Checks, DoctorCheck{WorkerID: id, Name: name, Check: kind, Status: status, Detail: detail})
			if status == "FAIL" || status == "UNKNOWN" {
				result.Healthy = false
			}
		}
		if id == "" {
			check("identity", "FAIL", "worker_id is missing")
		} else {
			check("identity", "PASS", "worker_id is present")
		}
		status, _ := worker["status"].(string)
		if status == "CONNECTED" || status == "DRAINING" {
			check("connection", "PASS", status)
		} else {
			check("connection", "FAIL", "worker status="+status)
		}
		readinessStatus, detail := readinessVerdict(worker["readiness"])
		check("readiness", readinessStatus, detail)
		desired, _ := worker["target_digest"].(string)
		running, _ := worker["image_digest"].(string)
		desiredCanonical := normalizeDigest(desired)
		runningCanonical := normalizeDigest(running)
		switch {
		case desiredCanonical == "" || runningCanonical == "":
			check("digest", "UNKNOWN", "desired or running digest is not reported")
		case desiredCanonical != runningCanonical:
			check("digest", "FAIL", "desired="+desired+" running="+running)
		default:
			check("digest", "PASS", "running digest matches desired digest")
		}
	}
	return result, nil
}

// normalizeDigest compares the immutable content digest rather than its
// transport representation. The Master stores target_digest as a pinned
// image reference while workers commonly report image_digest as bare
// sha256:<hex>; both representations identify the same image.
func normalizeDigest(raw string) string {
	digest := strings.TrimSpace(raw)
	if at := strings.LastIndexByte(digest, '@'); at >= 0 {
		digest = digest[at+1:]
	}
	return strings.TrimPrefix(digest, "sha256:")
}

func readinessVerdict(raw any) (string, string) {
	readiness, ok := raw.(map[string]any)
	if !ok || len(readiness) == 0 {
		return "UNKNOWN", "worker has not published a readiness snapshot"
	}
	for _, key := range []string{"status", "state", "readiness"} {
		if value, ok := readiness[key].(string); ok {
			switch strings.ToUpper(strings.TrimSpace(value)) {
			case "READY", "HEALTHY", "PASS":
				return "PASS", key + "=" + value
			case "NOT_READY", "NOT-READY", "UNHEALTHY", "FAIL":
				return "FAIL", key + "=" + value
			}
		}
	}
	for _, key := range []string{"ready", "cache_protection_ready", "cache_ready", "blob_ready", "bootstrapped"} {
		if value, ok := readiness[key].(bool); ok && !value {
			return "FAIL", key + "=false"
		}
	}
	return "PASS", "readiness snapshot present"
}
