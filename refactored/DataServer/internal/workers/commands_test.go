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

func TestCommandManagerAckCommand(t *testing.T) {
	cm := NewCommandManager(nil)

	cm.PushCommand("w1", "restart_worker", nil)
	cm.AckCommand("w1", "restart_worker")

	cmds := cm.GetPendingCommands("w1")
	if len(cmds) != 0 {
		t.Fatalf("expected 0 pending commands, got %d", len(cmds))
	}
}

func TestCommandManagerAckNonexistent(t *testing.T) {
	cm := NewCommandManager(nil)

	cm.AckCommand("w1", "nonexistent")

	cmds := cm.GetPendingCommands("w1")
	if len(cmds) != 0 {
		t.Fatalf("expected 0 commands, got %d", len(cmds))
	}
}

func TestCommandManagerGetAckTimeNonexistent(t *testing.T) {
	cm := NewCommandManager(nil)

	ackTime := cm.GetAckTime("w1", "nonexistent")
	if ackTime != nil {
		t.Error("expected nil ack time for nonexistent command")
	}
}

func TestUpdateManagerRequestAndAck(t *testing.T) {
	um := NewUpdateManager()

	um.RequestUpdate("w1", "v1.0")

	update := um.GetPendingUpdate("w1")
	if update == nil {
		t.Fatal("expected pending update")
	}
	if update.Version != "v1.0" {
		t.Errorf("expected version v1.0, got %s", update.Version)
	}
	if update.Ack {
		t.Error("expected update to not be acked")
	}

	um.AckUpdate("w1", "v1.0")
	update = um.GetPendingUpdate("w1")
	if !update.Ack {
		t.Error("expected update to be acked")
	}
	if update.AckVersion != "v1.0" {
		t.Errorf("expected ack version v1.0, got %s", update.AckVersion)
	}
}

func TestUpdateManagerClearUpdate(t *testing.T) {
	um := NewUpdateManager()

	um.RequestUpdate("w1", "v1.0")
	um.ClearUpdate("w1")

	update := um.GetPendingUpdate("w1")
	if update != nil {
		t.Error("expected nil after clear")
	}
}

func TestUpdateManagerMultipleWorkers(t *testing.T) {
	um := NewUpdateManager()

	um.RequestUpdate("w1", "v1.0")
	um.RequestUpdate("w2", "v2.0")

	update1 := um.GetPendingUpdate("w1")
	update2 := um.GetPendingUpdate("w2")

	if update1 == nil || update1.Version != "v1.0" {
		t.Error("expected w1 update v1.0")
	}
	if update2 == nil || update2.Version != "v2.0" {
		t.Error("expected w2 update v2.0")
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
