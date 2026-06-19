package workers

import (
	"testing"
	"time"
)

func TestCommandManagerPushAndGet(t *testing.T) {
	cm := NewCommandManager(nil)

	cm.PushCommand("w1", "restart_worker", nil)

	cmds := cm.GetPendingCommands("w1")
	if len(cmds) != 0 {
		t.Fatalf("expected 0 commands with nil store, got %d", len(cmds))
	}
}

func TestCommandManagerIdempotent(t *testing.T) {
	cm := NewCommandManager(nil)

	cm.PushCommand("w1", "restart_worker", nil)
	cm.PushCommand("w1", "restart_worker", nil)

	cmds := cm.GetPendingCommands("w1")
	if len(cmds) != 0 {
		t.Fatalf("expected 0 commands with nil store, got %d", len(cmds))
	}
}

func TestTokenManagerNilStore(t *testing.T) {
	tm := NewTokenManager(nil)

	token := tm.GenerateToken("w1")
	if token == "" {
		t.Fatal("expected non-empty token")
	}

	_, ok := tm.ValidateToken(token)
	if ok {
		t.Error("expected nil store to reject all tokens")
	}
}

func TestTokenManagerExpired(t *testing.T) {
	tm := NewTokenManager(nil)

	token := tm.GenerateToken("w1")

	// With nil store, validate always fails
	_, ok := tm.ValidateToken(token)
	if ok {
		t.Error("expected nil store to reject tokens")
	}
}

func TestTokenManagerRevoke(t *testing.T) {
	tm := NewTokenManager(nil)

	token := tm.GenerateToken("w1")
	tm.RevokeToken(token)

	_, ok := tm.ValidateToken(token)
	if ok {
		t.Error("expected revoked token to be invalid")
	}
}

func TestTokenManagerInvalidToken(t *testing.T) {
	tm := NewTokenManager(nil)

	_, ok := tm.ValidateToken("nonexistent")
	if ok {
		t.Error("expected nonexistent token to be invalid")
	}
}

func TestTokenManagerRevokeWorkerTokens(t *testing.T) {
	tm := NewTokenManager(nil)

	tm.GenerateToken("w1")
	tm.RevokeWorkerTokens("w1")
}

func TestWorkerCommandToJSON(t *testing.T) {
	cmd := &WorkerCommand{
		CommandID: "cmd-1",
		Type:      "restart_worker",
		Command:   "restart_worker",
		Timestamp: time.Now().UTC().Format(time.RFC3339),
	}
	data := cmd.ToJSON()
	if len(data) == 0 {
		t.Error("expected non-empty JSON")
	}
}

func TestCommandManager_AckCommandByID_NilStore(t *testing.T) {
	cm := NewCommandManager(nil)
	err := cm.AckCommandByID("w1", "cmd-nonexistent")
	if err == nil {
		t.Error("expected error from AckCommandByID with nil store")
	}
	if err.Error() != "no store" {
		t.Errorf("expected 'no store' error, got %q", err.Error())
	}
}

func TestCommandManager_PushCommand_ReturnsCommandID(t *testing.T) {
	cm := NewCommandManager(nil)
	cmdID := cm.PushCommand("w1", "drain", map[string]interface{}{"reason": "test"})
	if cmdID == "" {
		t.Error("expected non-empty command_id")
	}
}

func TestCommandManager_GetPendingCommands_NilStore(t *testing.T) {
	cm := NewCommandManager(nil)
	cm.PushCommand("w1", "restart_worker", nil)
	cmds := cm.GetPendingCommands("w1")
	if len(cmds) != 0 {
		t.Fatalf("expected 0 commands with nil store, got %d", len(cmds))
	}
}
