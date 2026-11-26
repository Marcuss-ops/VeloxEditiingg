package completion

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"velox-shared/contract"
)

// A committed DAG child must not roll back just because a sibling has not
// completed. The final task will perform the job-level promotion later.
func TestCommitAttemptDoesNotRequireSiblingTasksDone(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	fence := validFence("task-dag-child", "attempt-dag-child")
	jobID := "job-dag-child"
	seedCompleteUploadFixture(t, db, "up-dag-child", "art-dag-child", jobID, strings.Repeat("a", 64))
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`INSERT INTO tasks (task_id,job_id,status,revision,created_at,updated_at) VALUES ('task-dag-sibling',?,'READY',1,?,?)`, jobID, now, now); err != nil {
		t.Fatalf("seed sibling task: %v", err)
	}
	if _, err := c.DeclareOutputs(context.Background(), DeclareOutputsCommand{Fence: fence, JobID: jobID, OutputManifests: validManifests()}); err != nil {
		t.Fatalf("DeclareOutputs: %v", err)
	}
	commitID := scheduleRowReady(t, db, fence, "art-dag-child")
	if _, err := db.Exec(`UPDATE jobs SET status='AWAITING_ARTIFACT' WHERE job_id=?`, jobID); err != nil {
		t.Fatalf("set artifact gate: %v", err)
	}
	if _, err := c.CommitAttempt(context.Background(), commitID); err != nil {
		t.Fatalf("child commit should succeed while sibling is READY: %v", err)
	}
	var taskStatus, jobStatus string
	if err := db.QueryRow(`SELECT status FROM tasks WHERE task_id='task-dag-child'`).Scan(&taskStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM jobs WHERE job_id=?`, jobID).Scan(&jobStatus); err != nil {
		t.Fatal(err)
	}
	if taskStatus != "SUCCEEDED" || jobStatus == "SUCCEEDED" {
		t.Fatalf("child/job status = %s/%s; want SUCCEEDED/nonterminal", taskStatus, jobStatus)
	}
}

func TestCommitPreparedWindowClosesFencedInternalTask(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	jobID := "job-prep-commit"
	seedCompleteUploadFixture(t, db, "up-prep-commit", "art-prep-commit", jobID, strings.Repeat("a", 64))
	taskID, attemptID := "task-prep-commit", "attempt-prep-commit"
	f := validFence(taskID, attemptID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`UPDATE tasks SET executor_id=?,winning_attempt_terminal_pending=1 WHERE task_id=?`, contract.VideoPrepareWindowExecutorID, taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE task_attempts SET status='SUCCEEDED' WHERE id=?`, attemptID); err != nil {
		t.Fatal(err)
	}
	sourceSHA := strings.Repeat("b", 64)
	outputSHA := strings.Repeat("c", 64)
	extraSHA := strings.Repeat("d", 64)
	profile := contract.CanonicalVideoProfileV1Default
	revision, err := contract.VideoWindowPreparationRevision(profile, contract.CanonicalPreparationQualityPolicyDefault)
	if err != nil {
		t.Fatal(err)
	}
	req := contract.VideoPrepareWindowV1{Version: 1, Source: contract.AssetRefV2{AssetID: "source", Kind: "video", SHA256: sourceSHA, SizeBytes: 123}, SourceStartUS: 10, SourceDurationUS: 5000000, OutputFrameCount: 120, TimelineRevision: 1, TimelineSHA256: strings.Repeat("e", 64), TimelineStartFrame: 0, OutputProfileID: profile.ProfileID, OutputProfileRevision: revision}
	identity, err := contract.VideoPrepareWindowCacheIdentity(req)
	if err != nil {
		t.Fatal(err)
	}
	cert := contract.VideoPrepareWindowCertificateV1{SourceSHA256: sourceSHA, OutputSHA256: outputSHA, OutputSizeBytes: 10, ClosedGOPAttested: true, PacketCopyUsed: true, OutputEvidence: contract.VideoWindowOutputEvidenceV1{Codec: "h264", ExtradataSHA256: extraSHA}}
	certJSON, _ := json.Marshal(cert)
	sourceCertificate := contract.VideoSourceCertificateV1{
		Version: 1, SHA256: sourceSHA, SizeBytes: req.Source.SizeBytes, ProfileID: profile.ProfileID,
		PreparationRevision: revision, DurationUS: 5_000_000, KeyframesUS: []int64{0},
		Evidence: contract.VideoWindowOutputEvidenceV1{
			Codec: profile.Codec, Profile: strings.ToLower(profile.CodecProfile), Level: 40,
			Width: profile.Width, Height: profile.Height, PixelFormat: profile.PixelFormat,
			FPSNum: profile.FPSNum, FPSDen: profile.FPSDen, TimeBaseNum: profile.TimeBaseNum,
			TimeBaseDen: profile.TimeBaseDen, FrameCount: 120, FirstFrameKeyframe: true,
			ExtradataSHA256: contract.CanonicalVideoExtradataSHA256,
		},
	}
	if err := sourceCertificate.Validate(); err != nil {
		t.Fatalf("source certificate fixture: %v", err)
	}
	sourceCertificateJSON := mustJSON(t, sourceCertificate)
	if _, err := db.Exec(`INSERT INTO task_specs(task_id,executor_id,payload_json,created_at) VALUES(?,?,?,?)`, taskID, contract.VideoPrepareWindowExecutorID, mustJSON(t, map[string]any{contract.VideoPrepareWindowPayloadKey: req}), now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO assets(asset_id,kind,status,sha256,size_bytes,storage_provider,storage_key,metadata_json,created_at,verified_at) VALUES('prep-asset','video_segment','READY',?,10,'local','blob/prep','{}',?,?)`, outputSHA, now, now); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO asset_sources(source_id,asset_id,source_type,source_reference,metadata_json,created_at) VALUES('prep-source','prep-asset','worker_prepared','prepared-window:'||?,?,?)`, identity, string(certJSON), now); err != nil {
		t.Fatal(err)
	}
	coord := c.(PreparedWindowCoordinator)
	id, err := coord.CommitPreparedWindow(context.Background(), PreparedWindowCommitCommand{Fence: f, JobID: jobID, Identity: identity, AssetID: "prep-asset", SHA256: outputSHA, SizeBytes: 10, ExtradataSHA256: extraSHA, SourceCertificateJSON: sourceCertificateJSON})
	if err != nil {
		t.Fatal(err)
	}
	var taskStatus, commitStatus string
	var required int
	if err := db.QueryRow(`SELECT status FROM tasks WHERE task_id=?`, taskID).Scan(&taskStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status,required_output_count FROM attempt_commits WHERE commit_id=?`, id).Scan(&commitStatus, &required); err != nil {
		t.Fatal(err)
	}
	if taskStatus != "SUCCEEDED" || commitStatus != "COMMITTED" || required != 0 {
		t.Fatalf("task=%s commit=%s required_outputs=%d", taskStatus, commitStatus, required)
	}
	var savedSourceCertificate string
	if err := db.QueryRow(`SELECT certificate_json FROM video_source_certificates WHERE source_sha256=?`, sourceSHA).Scan(&savedSourceCertificate); err != nil || savedSourceCertificate != sourceCertificateJSON {
		t.Fatalf("source certificate was not committed with task: certificate=%s err=%v", savedSourceCertificate, err)
	}
	_, err = coord.CommitPreparedWindow(context.Background(), PreparedWindowCommitCommand{Fence: f, JobID: jobID, Identity: identity, AssetID: "prep-asset", SHA256: outputSHA, SizeBytes: 10, ExtradataSHA256: extraSHA, SourceCertificateJSON: sourceCertificateJSON})
	if err != nil {
		t.Fatalf("idempotent replay: %v", err)
	}
}

func TestCommitSourceCertificateNeedsNoPreparedAssetAndIsFenced(t *testing.T) {
	db := openCoordinatorTestDB(t)
	c := newTestCoordinator(db)
	jobID := "job-source-cert-commit"
	seedCompleteUploadFixture(t, db, "up-source-cert", "art-source-cert", jobID, strings.Repeat("a", 64))
	taskID, attemptID := "task-source-cert-commit", "attempt-source-cert-commit"
	f := validFence(taskID, attemptID)
	now := time.Now().UTC().Format(time.RFC3339Nano)
	if _, err := db.Exec(`UPDATE tasks SET executor_id=?,winning_attempt_terminal_pending=1 WHERE task_id=?`, contract.VideoSourceCertificationExecutorID, taskID); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE task_attempts SET status='SUCCEEDED' WHERE id=?`, attemptID); err != nil {
		t.Fatal(err)
	}
	profile := contract.CanonicalVideoProfileV1Default
	revision, err := contract.VideoWindowPreparationRevision(profile, contract.CanonicalPreparationQualityPolicyDefault)
	if err != nil {
		t.Fatal(err)
	}
	request := contract.VideoSourceCertificationV1{
		Version:         contract.VideoSourceCertificationVersion,
		Source:          contract.AssetRefV2{AssetID: "source", SourceKind: "drive", DriveFileID: "drive-source", Kind: "video", SizeBytes: 123},
		OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
	}
	if _, err := db.Exec(`INSERT INTO task_specs(task_id,executor_id,payload_json,created_at) VALUES(?,?,?,?)`, taskID, contract.VideoSourceCertificationExecutorID, mustJSON(t, map[string]any{contract.VideoSourceCertificationPayloadKey: request}), now); err != nil {
		t.Fatal(err)
	}
	certificate := contract.VideoSourceCertificateV1{
		Chunks:  &contract.VideoSourceChunksV1{ChunkSize: contract.VideoSourceChunkSize, SHA256: []string{strings.Repeat("c", 64)}},
		Version: 1, SHA256: strings.Repeat("b", 64), SizeBytes: request.Source.SizeBytes,
		ProfileID: profile.ProfileID, PreparationRevision: revision, DurationUS: 5_000_000, KeyframesUS: []int64{0},
		Evidence: contract.VideoWindowOutputEvidenceV1{
			Codec: profile.Codec, Profile: strings.ToLower(profile.CodecProfile), Level: 40,
			Width: profile.Width, Height: profile.Height, PixelFormat: profile.PixelFormat,
			FPSNum: profile.FPSNum, FPSDen: profile.FPSDen, TimeBaseNum: profile.TimeBaseNum,
			TimeBaseDen: profile.TimeBaseDen, FrameCount: 120, FirstFrameKeyframe: true,
			ExtradataSHA256: contract.CanonicalVideoExtradataSHA256,
		},
	}
	if err := certificate.Validate(); err != nil {
		t.Fatalf("source certificate fixture: %v", err)
	}
	coordinator := c.(SourceCertificateCoordinator)
	command := SourceCertificateCommitCommand{Fence: f, JobID: jobID, CertificateJSON: mustJSON(t, certificate)}
	commitID, err := coordinator.CommitSourceCertificate(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	var taskStatus, commitStatus string
	if err := db.QueryRow(`SELECT status FROM tasks WHERE task_id=?`, taskID).Scan(&taskStatus); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT status FROM attempt_commits WHERE commit_id=?`, commitID).Scan(&commitStatus); err != nil {
		t.Fatal(err)
	}
	var count int
	if err := db.QueryRow(`SELECT COUNT(*) FROM assets`).Scan(&count); err != nil || count != 0 {
		t.Fatalf("certificate-only completion created an asset blob row: count=%d err=%v", count, err)
	}
	var certificateJSON string
	if err := db.QueryRow(`SELECT certificate_json FROM video_source_certificates WHERE source_sha256=?`, certificate.SHA256).Scan(&certificateJSON); err != nil || certificateJSON != command.CertificateJSON {
		t.Fatalf("certificate registry row = %s err=%v", certificateJSON, err)
	}
	if taskStatus != "SUCCEEDED" || commitStatus != "COMMITTED" {
		t.Fatalf("task/commit status = %s/%s", taskStatus, commitStatus)
	}
	if _, err := coordinator.CommitSourceCertificate(context.Background(), command); err != nil {
		t.Fatalf("idempotent certificate completion replay: %v", err)
	}
	stale := command
	stale.Fence.LeaseID = "different-lease"
	stale.CertificateJSON = mustJSON(t, contract.VideoSourceCertificateV1{Version: 1})
	if _, err := coordinator.CommitSourceCertificate(context.Background(), stale); err == nil {
		t.Fatal("stale source-certificate result was accepted")
	}
}

func mustJSON(t *testing.T, v any) string {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}
