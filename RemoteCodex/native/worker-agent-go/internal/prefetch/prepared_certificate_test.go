package prefetch

import (
	"testing"
	"time"
)

func TestPreparedJobCertificate_FullLineage(t *testing.T) {
	pj := PreparedJob{
		JobID:         "job-B",
		TaskID:        "task-B",
		TaskRevision:  4,
		WorkerID:      "host_57_131_20_173",
		ReservationID: "future:host_57_131_20_173:task-B",
		PlanID:        "future:host_57_131_20_173",
		PlanVersion:   92,
		State:         PreparationStatePrepared,
		PreparedAt:    time.Date(2026, 8, 28, 12, 0, 0, 0, time.UTC),
		Assets: map[string]PreparedAssetMetadata{
			"video": {AssetKey: "video", SHA256: "aaa", SizeBytes: 500_000_000},
			"audio": {AssetKey: "audio", SHA256: "bbb", SizeBytes: 10_000_000},
		},
	}

	cert := pj.Certificate()

	if cert.WorkerID != "host_57_131_20_173" {
		t.Fatalf("WorkerID = %q, want host_57_131_20_173", cert.WorkerID)
	}
	if cert.ReservationID != "future:host_57_131_20_173:task-B" {
		t.Fatalf("ReservationID = %q", cert.ReservationID)
	}
	if cert.PlanID != "future:host_57_131_20_173" {
		t.Fatalf("PlanID = %q", cert.PlanID)
	}
	if cert.PlanVersion != 92 {
		t.Fatalf("PlanVersion = %d, want 92", cert.PlanVersion)
	}
	if cert.TaskRevision != 4 {
		t.Fatalf("TaskRevision = %d, want 4", cert.TaskRevision)
	}
	if cert.PreparedAt.IsZero() {
		t.Fatal("PreparedAt must not be zero")
	}
	if cert.AssetsRequired != 2 {
		t.Fatalf("AssetsRequired = %d, want 2", cert.AssetsRequired)
	}
	if cert.AssetsPrepared != 2 {
		t.Fatalf("AssetsPrepared = %d, want 2", cert.AssetsPrepared)
	}
	if cert.PreparedBytes != 510_000_000 {
		t.Fatalf("PreparedBytes = %d, want 510000000", cert.PreparedBytes)
	}
}

func TestVerifyClaim_HappyPath(t *testing.T) {
	cert := PreparedJobCertificate{
		WorkerID:       "host_57_131_20_173",
		TaskRevision:   4,
		AssetsRequired: 2,
		AssetsPrepared: 2,
	}
	ok, reason := cert.VerifyClaim("host_57_131_20_173", 4)
	if !ok {
		t.Fatalf("VerifyClaim failed: %s", reason)
	}
}

func TestVerifyClaim_WorkerMismatch(t *testing.T) {
	cert := PreparedJobCertificate{
		WorkerID:       "host_57_131_20_173",
		TaskRevision:   4,
		AssetsRequired: 2,
		AssetsPrepared: 2,
	}
	ok, reason := cert.VerifyClaim("host_57_129_132_133", 4)
	if ok {
		t.Fatal("VerifyClaim must fail on worker mismatch")
	}
	if reason == "" {
		t.Fatal("reason must not be empty on failure")
	}
	t.Logf("correctly rejected: %s", reason)
}

func TestVerifyClaim_RevisionMismatch(t *testing.T) {
	cert := PreparedJobCertificate{
		WorkerID:       "host_57_131_20_173",
		TaskRevision:   4,
		AssetsRequired: 2,
		AssetsPrepared: 2,
	}
	ok, reason := cert.VerifyClaim("host_57_131_20_173", 5)
	if ok {
		t.Fatal("VerifyClaim must fail on revision mismatch")
	}
	t.Logf("correctly rejected: %s", reason)
}

func TestVerifyClaim_AssetsIncomplete(t *testing.T) {
	cert := PreparedJobCertificate{
		WorkerID:       "host_57_131_20_173",
		TaskRevision:   4,
		AssetsRequired: 2,
		AssetsPrepared: 1, // incomplete
	}
	ok, reason := cert.VerifyClaim("host_57_131_20_173", 4)
	if ok {
		t.Fatal("VerifyClaim must fail when assets are incomplete")
	}
	t.Logf("correctly rejected: %s", reason)
}

func TestVerifyClaim_ZeroCertificateRejected(t *testing.T) {
	cert := PreparedJobCertificate{}
	ok, reason := cert.VerifyClaim("worker-1", 1)
	if ok {
		t.Fatal("VerifyClaim must fail for zero-value certificate")
	}
	if reason == "" {
		t.Fatal("reason must not be empty for zero certificate")
	}
}

func TestVerifyClaim_SkipsWorkerCheckWhenEmpty(t *testing.T) {
	cert := PreparedJobCertificate{
		ReservationID:  "future:worker-1:task-1",
		TaskRevision:   4,
		AssetsRequired: 2,
		AssetsPrepared: 2,
	}
	// WorkerID is empty → skip worker check
	ok, reason := cert.VerifyClaim("any-worker", 4)
	if !ok {
		t.Fatalf("VerifyClaim should skip worker check when cert WorkerID is empty: %s", reason)
	}
}

func TestVerifyClaim_SkipsRevisionCheckWhenZero(t *testing.T) {
	cert := PreparedJobCertificate{
		WorkerID:       "host_57_131_20_173",
		AssetsRequired: 2,
		AssetsPrepared: 2,
	}
	// TaskRevision is 0 → skip revision check
	ok, reason := cert.VerifyClaim("host_57_131_20_173", 99)
	if !ok {
		t.Fatalf("VerifyClaim should skip revision check when cert TaskRevision is 0: %s", reason)
	}
}

func TestPreparedJob_CertificatePreservesWorkerID(t *testing.T) {
	pj := PreparedJob{
		JobID:    "job-1",
		TaskID:   "task-1",
		WorkerID: "worker-CERT",
		Assets: map[string]PreparedAssetMetadata{
			"a": {AssetKey: "a", SizeBytes: 100},
		},
	}
	cert := pj.Certificate()
	if cert.WorkerID != "worker-CERT" {
		t.Fatalf("Certificate WorkerID = %q, want worker-CERT", cert.WorkerID)
	}
}

func TestCertificateZeroAssetsNotRejected(t *testing.T) {
	// Zero assets required → no rejection (task without assets).
	cert := PreparedJobCertificate{
		WorkerID:       "worker-1",
		TaskRevision:   1,
		AssetsRequired: 0,
		AssetsPrepared: 0,
	}
	ok, reason := cert.VerifyClaim("worker-1", 1)
	if !ok {
		t.Fatalf("VerifyClaim should pass for zero assets: %s", reason)
	}
}
