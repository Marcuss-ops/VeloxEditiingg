package fleet

import (
	"context"
	"reflect"
	"strings"
	"testing"

	"velox-shared/identity"
)

type recordingSSH struct {
	commands []string
	output   string
	err      error
}

func (s *recordingSSH) Run(_ context.Context, workerID, command string) (string, error) {
	s.commands = append(s.commands, workerID+"\x00"+command)
	return s.output, s.err
}

func TestSSHWorkerDockerClient_RejectsNonDigestBeforeSSH(t *testing.T) {
	ssh := &recordingSSH{}
	client := &SSHWorkerDockerClient{SSH: ssh}

	if _, err := client.ActivateImage(context.Background(), "worker-1", "ghcr.io/acme/worker:latest"); err == nil {
		t.Fatal("ActivateImage accepted mutable image tag")
	}
	if len(ssh.commands) != 0 {
		t.Fatalf("SSH calls = %d, want 0 for invalid image ref", len(ssh.commands))
	}
}

func TestSSHWorkerDockerClient_UsesFixedActivationHelper(t *testing.T) {
	ssh := &recordingSSH{output: "activated"}
	client := &SSHWorkerDockerClient{SSH: ssh}
	image := "ghcr.io/marcuss-ops/velox-worker@sha256:" + strings.Repeat("a", 64)

	got, err := client.ActivateImage(context.Background(), "worker-1", image)
	if err != nil {
		t.Fatalf("ActivateImage returned error: %v", err)
	}
	if got != "activated" {
		t.Fatalf("ActivateImage output = %q, want activated", got)
	}
	want := "worker-1\x00sudo -n /usr/local/sbin/velox-worker-activate-image " + image
	if len(ssh.commands) != 1 || ssh.commands[0] != want {
		t.Fatalf("SSH command = %q, want %q", ssh.commands, want)
	}
}

func TestSecureSSHClient_UsesHardenedSSHArguments(t *testing.T) {
	reg := NewWorkerRegistry()
	if err := reg.AddWorker(WorkerRegistryEntry{
		WorkerID: identity.ParseWorkerID("worker-1"),
		Host:     "10.0.0.10",
		SSHUser:  "velox",
		SSHPort:  2222,
	}); err != nil {
		t.Fatal(err)
	}
	client := NewSecureSSHClient(reg, "/run/secrets/worker.key", "/run/secrets/known_hosts")
	entry := reg.GetWorker("worker-1")
	got := client.baseSSHArgs(entry)
	want := []string{
		"-i", "/run/secrets/worker.key",
		"-o", "StrictHostKeyChecking=yes",
		"-o", "UserKnownHostsFile=/run/secrets/known_hosts",
		"-o", "ConnectTimeout=10",
		"-o", "BatchMode=yes",
		"-o", "LogLevel=ERROR",
		"-p", "2222",
		"velox@10.0.0.10",
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("hardened SSH args = %#v, want %#v", got, want)
	}
}

func TestSecureSSHClient_RejectsUnsafeCommandsAndAllowsActivationScriptPath(t *testing.T) {
	client := NewSecureSSHClient(NewWorkerRegistry(), "/key", "/known_hosts")
	if _, err := client.Run(context.Background(), "worker-1", "docker pull image && systemctl restart velox-worker.service"); err == nil {
		t.Fatal("SecureSSHClient.Run accepted a composed shell command")
	}
	if _, err := client.Run(context.Background(), "worker-1", "printf 'safe\\nunsafe'"); err == nil {
		t.Fatal("SecureSSHClient.Run accepted a command containing a newline")
	}
	if err := validateScriptPath("/usr/local/sbin/velox-worker-activate-image"); err != nil {
		t.Fatalf("activation helper path rejected: %v", err)
	}
}

func TestSecureSSHClient_NilRegistryFailsClosed(t *testing.T) {
	var client *SecureSSHClient
	if _, err := client.Run(context.Background(), "worker-1", "true"); err == nil {
		t.Fatal("nil SecureSSHClient unexpectedly attempted a command")
	}
}
