package workers

import (
	"testing"
	"time"
)

func TestCommandManagerPushAndGet(t *testing.T) {
	cm := NewCommandManager()

	cm.PushCommand("w1", "restart_worker", nil)

	cmds := cm.GetPendingCommands("w1")
	if len(cmds) != 1 {
		t.Fatalf("expected 1 pending command, got %d", len(cmds))
	}
	if cmds[0].Type != "restart_worker" {
		t.Errorf("expected command type restart_worker, got %s", cmds[0].Type)
	}
}

func TestCommandManagerIdempotent(t *testing.T) {
	cm := NewCommandManager()

	cm.PushCommand("w1", "restart_worker", nil)
	cm.PushCommand("w1", "restart_worker", nil) // duplicate

	cmds := cm.GetPendingCommands("w1")
	if len(cmds) != 1 {
		t.Fatalf("expected 1 pending command (idempotent), got %d", len(cmds))
	}
}

func TestCommandManagerAckCommand(t *testing.T) {
	cm := NewCommandManager()

	cm.PushCommand("w1", "restart_worker", nil)
	cm.AckCommand("w1", "restart_worker")

	cmds := cm.GetPendingCommands("w1")
	if len(cmds) != 0 {
		t.Fatalf("expected 0 pending commands after ack, got %d", len(cmds))
	}

	// Check ack time was recorded
	ackTime := cm.GetAckTime("w1", "restart_worker")
	if ackTime == nil {
		t.Error("expected ack time to be recorded")
	}
}

func TestCommandManagerMultipleCommands(t *testing.T) {
	cm := NewCommandManager()

	cm.PushCommand("w1", "restart_worker", nil)
	cm.PushCommand("w1", "update_code", map[string]interface{}{"version": "v1.0"})

	cmds := cm.GetPendingCommands("w1")
	if len(cmds) != 2 {
		t.Fatalf("expected 2 pending commands, got %d", len(cmds))
	}

	// Ack one
	cm.AckCommand("w1", "restart_worker")
	cmds = cm.GetPendingCommands("w1")
	if len(cmds) != 1 {
		t.Fatalf("expected 1 pending command after ack, got %d", len(cmds))
	}
	if cmds[0].Type != "update_code" {
		t.Errorf("expected remaining command update_code, got %s", cmds[0].Type)
	}
}

func TestCommandManagerMultipleWorkers(t *testing.T) {
	cm := NewCommandManager()

	cm.PushCommand("w1", "restart_worker", nil)
	cm.PushCommand("w2", "update_code", nil)

	cmds1 := cm.GetPendingCommands("w1")
	cmds2 := cm.GetPendingCommands("w2")

	if len(cmds1) != 1 {
		t.Errorf("expected 1 command for w1, got %d", len(cmds1))
	}
	if len(cmds2) != 1 {
		t.Errorf("expected 1 command for w2, got %d", len(cmds2))
	}
}

func TestCommandManagerNoCommands(t *testing.T) {
	cm := NewCommandManager()

	cmds := cm.GetPendingCommands("nonexistent")
	if len(cmds) != 0 {
		t.Fatalf("expected 0 commands for nonexistent worker, got %d", len(cmds))
	}
}

func TestCommandManagerAckNonexistent(t *testing.T) {
	cm := NewCommandManager()

	// Should not panic
	cm.AckCommand("w1", "nonexistent")

	cmds := cm.GetPendingCommands("w1")
	if len(cmds) != 0 {
		t.Fatalf("expected 0 commands, got %d", len(cmds))
	}
}

func TestCommandManagerGetAckTimeNonexistent(t *testing.T) {
	cm := NewCommandManager()

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

func TestTokenManagerGenerateAndValidate(t *testing.T) {
	tm := NewTokenManager()

	token := tm.GenerateToken("w1")
	if token == "" {
		t.Fatal("expected non-empty token")
	}

	workerID, ok := tm.ValidateToken(token)
	if !ok {
		t.Fatal("expected token to be valid")
	}
	if workerID != "w1" {
		t.Errorf("expected worker ID w1, got %s", workerID)
	}
}

func TestTokenManagerExpired(t *testing.T) {
	tm := NewTokenManager()

	token := tm.GenerateToken("w1")

	// Manually expire the token
	tm.mu.Lock()
	tm.tokens[token].ExpiresAt = time.Now().Add(-time.Hour)
	tm.mu.Unlock()

	_, ok := tm.ValidateToken(token)
	if ok {
		t.Error("expected expired token to be invalid")
	}
}

func TestTokenManagerRevoke(t *testing.T) {
	tm := NewTokenManager()

	token := tm.GenerateToken("w1")
	tm.RevokeToken(token)

	_, ok := tm.ValidateToken(token)
	if ok {
		t.Error("expected revoked token to be invalid")
	}
}

func TestTokenManagerInvalidToken(t *testing.T) {
	tm := NewTokenManager()

	_, ok := tm.ValidateToken("nonexistent")
	if ok {
		t.Error("expected nonexistent token to be invalid")
	}
}
