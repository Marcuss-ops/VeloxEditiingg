package grpcserver

import (
	"testing"

	"google.golang.org/protobuf/types/known/structpb"
	pb "velox-shared/controltransport/pb"
)

func TestHandleHeartbeatKeepsCommitSeparateFromSoftwareVersion(t *testing.T) {
	h := minimalHandlerForHeartbeat(t, nil)
	sess := &workerSession{workerID: "w-release", sessionID: "s-release"}
	h.mu.Lock()
	h.sessions["s-release"] = sess
	h.workerSessions["w-release"] = "s-release"
	h.mu.Unlock()

	extra, err := structpb.NewStruct(map[string]any{
		"capabilities": map[string]any{
			"git_sha":        "abc1234",
			"worker_version": "v1.4.54",
			"ffmpeg_version": "7.1.1",
			"release_identity": map[string]any{
				"source_commit":     "abc1234",
				"software_version":  "v1.4.54",
				"capability_schema": 1,
			},
		},
	})
	if err != nil {
		t.Fatalf("build heartbeat extra: %v", err)
	}
	h.handleHeartbeat("w-release", "s-release", &pb.Heartbeat{
		CodeVersion:   "v1.4.54",
		EngineVersion: "chronon-v2",
		Extra:         extra,
	})

	if got := sess.gitSHA.Load(); got != "abc1234" {
		t.Fatalf("git_sha = %v, want certificate source commit abc1234", got)
	}
	if got := sess.workerVersion.Load(); got != "v1.4.54" {
		t.Fatalf("worker_version = %v, want v1.4.54", got)
	}
	if got := sess.engineVersion.Load(); got != "chronon-v2" {
		t.Fatalf("engine_version = %v, want chronon-v2", got)
	}
	if got := sess.ffmpegVersion.Load(); got != "7.1.1" {
		t.Fatalf("ffmpeg_version = %v, want 7.1.1", got)
	}
}

func TestHandleHeartbeatDoesNotLabelSoftwareVersionAsGitCommit(t *testing.T) {
	h := minimalHandlerForHeartbeat(t, nil)
	sess := &workerSession{workerID: "w-release-empty", sessionID: "s-release-empty"}
	h.mu.Lock()
	h.sessions["s-release-empty"] = sess
	h.workerSessions["w-release-empty"] = "s-release-empty"
	h.mu.Unlock()
	h.handleHeartbeat("w-release-empty", "s-release-empty", &pb.Heartbeat{CodeVersion: "v1.4.54"})

	if got := sess.gitSHA.Load(); got != nil {
		t.Fatalf("git_sha = %v, want unset when no source commit was advertised", got)
	}
	if got := sess.workerVersion.Load(); got != "v1.4.54" {
		t.Fatalf("worker_version = %v, want fallback v1.4.54", got)
	}
}
