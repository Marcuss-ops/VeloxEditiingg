package assets

import (
	"context"
	"testing"

	videoContract "velox-shared/contract"
)

type recordingVideoRunner struct {
	probeOutput []byte
	commands    []videoCommand
}

type videoCommand struct {
	name string
	args []string
}

func (r *recordingVideoRunner) Run(_ context.Context, name string, args ...string) ([]byte, error) {
	r.commands = append(r.commands, videoCommand{name: name, args: append([]string(nil), args...)})
	if name == "ffprobe" {
		return r.probeOutput, nil
	}
	return nil, nil
}

func TestVideoTrimmerPreparationRevisionTracksAcceptanceAndProfile(t *testing.T) {
	profile := videoContract.CanonicalVideoProfileV1Default
	quality := videoContract.CanonicalPreparationQualityPolicyDefault
	base := newVideoWindowVerifierForTest(&recordingVideoRunner{})
	if got := base.preparationRevision(); got == "" {
		t.Fatal("preparation revision is empty")
	}
	quality.VideoCRF = 24
	changedQuality := newVideoWindowVerifierWithPolicy(&recordingVideoRunner{}, profile, quality)
	if changedQuality.preparationRevision() != base.preparationRevision() {
		t.Fatal("encoder-only CRF change must not invalidate packet-copy preparation evidence")
	}
	quality.VideoMaxRate = "3M"
	changedAcceptance := newVideoWindowVerifierWithPolicy(&recordingVideoRunner{}, profile, quality)
	if changedAcceptance.preparationRevision() == base.preparationRevision() {
		t.Fatal("source acceptance bitrate change did not invalidate preparation revision")
	}
	profile.GOPSize++
	changedProfile := newVideoWindowVerifierWithPolicy(&recordingVideoRunner{}, profile, videoContract.CanonicalPreparationQualityPolicyDefault)
	if changedProfile.preparationRevision() == base.preparationRevision() {
		t.Fatal("canonical profile change did not invalidate preparation revision")
	}
}

func TestVideoTrimmerProbeReadsCanonicalMediaEvidence(t *testing.T) {
	runner := &recordingVideoRunner{probeOutput: []byte(`{"streams":[{"codec_type":"video","codec_name":"h264","profile":"High","level":40,"width":1920,"height":1080,"r_frame_rate":"24/1","time_base":"1/90000","pix_fmt":"yuv420p","duration":"2","nb_frames":"48","has_b_frames":0,"extradata_hash":"SHA256:abc"}],"frames":[{"media_type":"video","key_frame":1,"best_effort_timestamp_time":"0"}],"format":{"duration":"2"}}`)}
	probe, err := newVideoWindowVerifierForTest(runner).probe(context.Background(), "/window.mp4")
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if probe.VideoCodec != "h264" || probe.FrameCount != 48 || probe.ExtradataSHA256 != "abc" || len(probe.Keyframes) != 1 {
		t.Fatalf("probe evidence = %+v", probe)
	}
	if len(runner.commands) != 1 || runner.commands[0].name != "ffprobe" {
		t.Fatalf("commands = %+v, want one ffprobe", runner.commands)
	}
}

func TestVideoTrimmerNoLongerInvokesEncoder(t *testing.T) {
	runner := &recordingVideoRunner{}
	trimmer := newVideoWindowVerifierForTest(runner)
	if _, err := trimmer.probe(context.Background(), ""); err == nil {
		t.Fatal("empty path unexpectedly accepted")
	}
	for _, command := range runner.commands {
		if command.name == "ffmpeg" {
			t.Fatalf("probe invoked encoder: %+v", command)
		}
	}
}
