package workers

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"velox-server/internal/store"
)

// WorkerCommand represents a command to be executed by a worker.
type WorkerCommand struct {
	CommandID   string                 `json:"command_id"`
	Type        string                 `json:"type"`
	Command     string                 `json:"command"`
	Timestamp   string                 `json:"timestamp"`
	Params      map[string]interface{} `json:"params,omitempty"`
	SequenceNum int64                  `json:"sequence_num,omitempty"`
}

// CommandManager handles pending commands for workers, backed by SQLite.
//
// Single-source-of-truth invariant (Phase 4.4+):
//   - Commands are persisted in `worker_commands` (SQLite) — the only path.
//   - Acknowledgements are by command_id (AckCommandByID) — the legacy
//     "by type" path is removed: AckCommand(workerID, cmdType) was a footgun
//     because two pending commands of the same type on the same worker could
//     be ACK'd by the wrong worker. AckCommandByID is tied to the exact
//     command_id and is only callable by the owning worker.
//   - GetAckTime is removed — callers should query worker_commands directly
//     if they need ack timestamps.
//
// PR15.3: removed the unused `mu sync.RWMutex` field. SQLite operations
// are already serialized by SQLiteStore's own connection pool; an
// additional Go-side mutex was dead weight and a magnet for false
// "deadlock-free" assumptions.
type CommandManager struct {
	store *store.SQLiteStore
}

// NewCommandManager creates a SQLite-backed command manager.
func NewCommandManager(dbStore *store.SQLiteStore) *CommandManager {
	return &CommandManager{store: dbStore}
}

// PushCommand adds a command for a worker. Returns the command_id.
func (cm *CommandManager) PushCommand(workerID string, cmdType string, params map[string]interface{}) string {
	commandID := fmt.Sprintf("cmd-%s-%s-%d", workerID, cmdType, time.Now().UnixNano())

	if cm == nil || cm.store == nil {
		return commandID
	}
	commandID, err := cm.PushCommandWithError(workerID, cmdType, params)
	if err != nil {
		registryLog.ErrorWithMsg("cmd.push.fail", "Failed to persist command",
			map[string]interface{}{"worker_id": workerID, "type": cmdType, "err": err.Error()})
		return ""
	}
	return commandID
}

// PushCommandWithError adds a command to the durable worker command outbox.
// Unlike the compatibility wrapper PushCommand, it fails when the store is
// unavailable or when the duplicate-check/persist operation cannot complete;
// HTTP/control-plane callers must use this method before acknowledging that a
// command was queued.
func (cm *CommandManager) PushCommandWithError(workerID string, cmdType string, params map[string]interface{}) (string, error) {
	if cm == nil || cm.store == nil {
		return "", fmt.Errorf("worker command store is not configured")
	}
	commandID := fmt.Sprintf("cmd-%s-%s-%d", workerID, cmdType, time.Now().UnixNano())

	// Idempotent: skip if same type already pending
	ok, err := cm.store.HasPendingCommand(workerID, cmdType, commandID)
	if err != nil {
		return "", fmt.Errorf("check pending command: %w", err)
	}
	if ok {
		return commandID, nil
	}

	cmd := &store.PersistedCommand{
		CommandID:      commandID,
		WorkerID:       workerID,
		CommandType:    cmdType,
		Payload:        params,
		Status:         "pending",
		CreatedAt:      time.Now().UTC(),
		ExpiresAt:      timePtr(time.Now().UTC().Add(24 * time.Hour)),
		IdempotencyKey: commandID,
	}

	if _, err := cm.store.InsertCommand(cmd); err != nil {
		return "", fmt.Errorf("persist command: %w", err)
	}

	return commandID, nil
}

// GetPendingCommands returns all pending commands for a worker.
func (cm *CommandManager) GetPendingCommands(workerID string) []WorkerCommand {
	if cm.store == nil {
		return []WorkerCommand{}
	}

	persisted, err := cm.store.GetPendingCommands(workerID)
	if err != nil {
		registryLog.ErrorWithMsg("cmd.get.fail", "Failed to get pending commands",
			map[string]interface{}{"worker_id": workerID, "err": err.Error()})
		return []WorkerCommand{}
	}

	result := make([]WorkerCommand, 0, len(persisted))
	for _, p := range persisted {
		result = append(result, WorkerCommand{
			CommandID:   p.CommandID,
			Type:        p.CommandType,
			Command:     p.CommandType,
			Timestamp:   p.CreatedAt.Format(time.RFC3339),
			Params:      p.Payload,
			SequenceNum: p.SequenceNum,
		})
	}
	return result
}

// GetPendingCommandsAndMarkDelivered fetches all pending commands for a
// worker and marks each one as delivered (pending → delivered, delivered_at
// populated, attempt_count incremented). The returned commands reflect their
// pre-delivery state (the worker still needs to process them).
//
// This is the preferred method for HTTP command polling — it closes the
// visibility gap between "the master fetched the command" and "the worker
// received it", making the command lifecycle observable:
//
//	pending → delivered → acked
//	pending → delivered → expired (timeout)
func (cm *CommandManager) GetPendingCommandsAndMarkDelivered(workerID string) []WorkerCommand {
	cmds := cm.GetPendingCommands(workerID)

	for _, cmd := range cmds {
		if cmd.CommandID != "" {
			if err := cm.MarkCommandDelivered(cmd.CommandID); err != nil {
				registryLog.ErrorWithMsg("cmd.markdelivered.fail",
					"Failed to mark command delivered",
					map[string]interface{}{
						"command_id": cmd.CommandID,
						"worker_id":  workerID,
						"err":        err.Error(),
					})
			}
		}
	}

	return cmds
}

// AckCommandByID marks a specific command as acknowledged, scoped to its owning worker.
// The workerID prevents workers from ACKing commands owned by other workers.
//
// This is the ONLY surviving ACK path — the type-based fallback was removed in
// Phase 4.5 because it allowed a worker to ack the wrong command when two
// pending commands of the same type coexisted on the same worker.
func (cm *CommandManager) AckCommandByID(workerID, commandID string) error {
	if cm.store == nil {
		return fmt.Errorf("no store")
	}
	return cm.store.AckCommandByID(workerID, commandID)
}

// MarkCommandDelivered marks a single command as delivered (pending → delivered)
// by its command_id. The caller is responsible for only marking commands that
// were successfully sent on the stream.
func (cm *CommandManager) MarkCommandDelivered(commandID string) error {
	if cm.store == nil {
		return fmt.Errorf("no store")
	}
	return cm.store.MarkCommandDelivered(commandID)
}

// WorkerToken represents a temporary authentication token (kept for response shape).
type WorkerToken struct {
	WorkerID  string    `json:"worker_id"`
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
}

// TokenManager handles worker authentication tokens, backed by SQLite sessions.
//
// PR15.3: removed the unused `mu sync.RWMutex` field for the same reason
// as CommandManager (SQLite serializes access).
type TokenManager struct {
	store *store.SQLiteStore
}

// NewTokenManager creates a SQLite-backed token manager.
func NewTokenManager(dbStore *store.SQLiteStore) *TokenManager {
	return &TokenManager{store: dbStore}
}

// GenerateToken creates a new session token for a worker and persists it.
func (tm *TokenManager) GenerateToken(workerID string) string {
	token, err := tm.GenerateTokenWithError(workerID)
	if err != nil {
		registryLog.ErrorWithMsg("token.gen.fail", "Failed to generate worker session token",
			map[string]interface{}{"worker_id": workerID, "err": err.Error()})
		return ""
	}
	return token
}

// GenerateTokenWithError creates and durably persists a worker session token.
// A configured store is authoritative: callers must not receive a usable
// token when either the worker row or session row could not be persisted.
// With a nil store it preserves the in-memory test/dev token behavior; such a
// token cannot validate because all validation paths require a store.
func (tm *TokenManager) GenerateTokenWithError(workerID string) (string, error) {
	if tm == nil {
		return "", fmt.Errorf("token manager is nil")
	}
	token := generateRandomToken()
	tokenHash := store.HashCredential(token)
	sessionID := fmt.Sprintf("sess-%s-%d", workerID, time.Now().UnixNano())

	if tm.store == nil {
		return token, nil
	}

	if err := tm.store.EnsureWorkerRecord(workerID); err != nil {
		return "", fmt.Errorf("bootstrap worker record: %w", err)
	}
	sess := &store.PersistedSession{
		SessionID:   sessionID,
		WorkerID:    workerID,
		SessionType: "asset",
		TokenHash:   tokenHash,
		ExpiresAt:   time.Now().UTC().Add(time.Hour),
	}
	if err := tm.store.InsertSession(sess); err != nil {
		return "", fmt.Errorf("persist session: %w", err)
	}
	return token, nil
}

// ValidateWorkerCommandToken checks if a worker command token is valid and
// returns the associated worker ID. Renamed from ValidateToken to eliminate
// ambiguity with social OAuth token validators.
func (tm *TokenManager) ValidateWorkerCommandToken(token string) (string, bool) {
	if tm.store == nil || token == "" {
		return "", false
	}

	tokenHash := store.HashCredential(token)
	sess, err := tm.store.ValidateSession(tokenHash)
	if err != nil || sess == nil {
		return "", false
	}
	return sess.WorkerID, true
}

// ValidateWorkerCredentialToken validates the persistent credential hash used
// by the mTLS control-plane worker for authenticated data-plane requests.
// Unlike a command/session token, this credential is bound to the declared
// worker ID and is not accepted without that binding.
func (tm *TokenManager) ValidateWorkerCredentialToken(workerID, credentialHash string) (bool, error) {
	if tm == nil || tm.store == nil || strings.TrimSpace(workerID) == "" || strings.TrimSpace(credentialHash) == "" {
		return false, nil
	}
	return tm.store.ValidateWorkerCredential(workerID, credentialHash)
}

// RevokeToken revokes a token by revoking its session.
func (tm *TokenManager) RevokeToken(token string) {
	if tm.store == nil || token == "" {
		return
	}
	tokenHash := store.HashCredential(token)
	sess, err := tm.store.ValidateSession(tokenHash)
	if err == nil && sess != nil {
		_ = tm.store.RevokeSession(sess.SessionID)
	}
}

// RevokeWorkerTokens revokes all tokens for a worker.
func (tm *TokenManager) RevokeWorkerTokens(workerID string) {
	if tm.store != nil {
		_ = tm.store.RevokeWorkerSessions(workerID)
	}
}

// generateRandomToken generates a cryptographically secure random token
func generateRandomToken() string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 32)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		if err != nil {
			b[i] = chars[i%len(chars)]
		} else {
			b[i] = chars[n.Int64()]
		}
	}
	return string(b)
}

// ToJSON converts WorkerCommand to JSON
func (c *WorkerCommand) ToJSON() []byte {
	data, _ := json.Marshal(c)
	return data
}

func timePtr(t time.Time) *time.Time {
	return &t
}
