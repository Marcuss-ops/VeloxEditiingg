package executors

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	osexec "os/exec"
	"path/filepath"
	"strconv"
	"testing"

	"velox-shared/contract"
	"velox-worker-agent/internal/executor"
	"velox-worker-agent/internal/runtimeassets"
)

func TestVideoSourceCertificationReturnsEvidenceWithoutMediaOutput(t *testing.T) {
	if _, err := osexec.LookPath("ffmpeg"); err != nil {
		t.Skip("ffmpeg is not installed")
	}
	if _, err := osexec.LookPath("ffprobe"); err != nil {
		t.Skip("ffprobe is not installed")
	}
	profile := contract.CanonicalVideoProfileV1Default
	quality := contract.CanonicalPreparationQualityPolicyDefault
	revision, err := contract.VideoWindowPreparationRevision(profile, quality)
	if err != nil {
		t.Fatal(err)
	}
	root := t.TempDir()
	sourcePath := filepath.Join(root, "complete-source.mp4")
	args := []string{"-hide_banner", "-loglevel", "error", "-f", "lavfi", "-i",
		fmt.Sprintf("color=c=black:s=%dx%d:r=%d/%d:d=4", profile.Width, profile.Height, profile.FPSNum, profile.FPSDen),
		"-an", "-vf", contract.CanonicalVideoScaleFilter(profile)}
	args = append(args, contract.CanonicalVideoEncodeArgs(profile, quality)...)
	args = append(args, "-video_track_timescale", strconv.Itoa(profile.TimeBaseDen), "-y", sourcePath)
	if output, err := osexec.Command("ffmpeg", args...).CombinedOutput(); err != nil {
		t.Skipf("ffmpeg cannot create canonical fixture: %v: %s", err, output)
	}
	data, err := os.ReadFile(sourcePath)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(data)
	sha := hex.EncodeToString(digest[:])
	request := contract.VideoSourceCertificationV1{
		Version:         contract.VideoSourceCertificationVersion,
		Source:          contract.AssetRefV2{AssetID: "source-cert-fixture", SHA256: sha, SizeBytes: int64(len(data)), Kind: "video"},
		OutputProfileID: profile.ProfileID, OutputProfileRevision: revision,
	}
	workerExecutor := NewVideoSourceCertification(filepath.Join(root, "prepared-windows"))
	spec := executor.TaskSpec{JobID: "job-source-cert", ExecutorID: VideoSourceCertificationID, Payload: map[string]interface{}{contract.VideoSourceCertificationPayloadKey: request}}
	ctx := runtimeassets.WithBindings(context.Background(), runtimeassets.Bindings{
		request.Source.AssetID: {AssetID: request.Source.AssetID, Path: sourcePath, SHA256: sha, Size: int64(len(data)), Verified: true},
	})
	result, err := workerExecutor.Execute(ctx, nil, spec)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if result.Status != "succeeded" || len(result.Outputs) != 0 || len(result.Segments) != 1 || result.Segments[0].SourceType != VideoSourceCertificationID {
		t.Fatalf("source certification did not return an artifact-free result: %+v", result)
	}
	var certificate contract.VideoSourceCertificateV1
	if err := json.Unmarshal([]byte(result.Segments[0].MetadataJSON), &certificate); err != nil {
		t.Fatal(err)
	}
	if err := certificate.Validate(); err != nil || certificate.SHA256 != sha || certificate.SizeBytes != int64(len(data)) || certificate.Evidence.FrameCount != 96 || certificate.Chunks == nil {
		t.Fatalf("invalid complete source certificate: %+v err=%v", certificate, err)
	}
	if certificate.Chunks.Validate(int64(len(data))) != nil {
		t.Fatalf("invalid block integrity proof: %+v", certificate.Chunks)
	}
	if workerExecutor.Descriptor().OutputTypes != nil {
		t.Fatalf("source certification declares media outputs: %#v", workerExecutor.Descriptor().OutputTypes)
	}
}
