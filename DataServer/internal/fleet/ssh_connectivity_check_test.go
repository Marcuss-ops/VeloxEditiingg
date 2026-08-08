package fleet

import (
	"context"
	"testing"
)

// TestWorkerSSHStatus_Predicates pins the consolidated verdicts used by
// the CLI + handler (Ready / Result) — pure logic, no SSH involved.
func TestWorkerSSHStatus_Predicates(t *testing.T) {
	ready := WorkerSSHStatus{WorkerID: "w", SSH: "PASS", HostKey: "PASS", Sudo: "PASS"}
	if !ready.Ready() || ready.Result() != "READY" {
		t.Errorf("all-PASS worker expected Ready/READY, got %v/%q", ready.Ready(), ready.Result())
	}
	noSudo := WorkerSSHStatus{SSH: "PASS", HostKey: "PASS", Sudo: "FAIL"}
	if noSudo.Result() != "NOT-READY" {
		t.Errorf("sudo FAIL must be NOT-READY")
	}
	noKey := WorkerSSHStatus{SSH: "PASS", HostKey: "FAIL", Sudo: "PASS"}
	if noKey.Result() != "NOT-READY" {
		t.Errorf("hostkey FAIL must be NOT-READY")
	}
	noSSH := WorkerSSHStatus{SSH: "FAIL", HostKey: "SKIP", Sudo: "SKIP"}
	if noSSH.Result() != "NOT-READY" {
		t.Errorf("ssh FAIL must be NOT-READY")
	}
}

// TestSSHConnectivityCheck_NilRegistry returns an empty slice (nil-
// tolerant, mirroring the rest of the fleet surface) rather than a panic.
func TestSSHConnectivityCheck_NilRegistry(t *testing.T) {
	if got := SSHConnectivityCheck(context.Background(), nil, "", "", nil); got != nil {
		t.Fatalf("nil registry must return nil slice, got %#v", got)
	}
}
