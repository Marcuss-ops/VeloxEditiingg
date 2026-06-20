package workers

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"math/big"
	"sync"
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
type CommandManager struct {
	mu    sync.RWMutex
	store *store.SQLiteStore
}

// NewCommandManager creates a SQLite-backed command manager.
func NewCommandManager(dbStore *store.SQLiteStore) *CommandManager {
	return &CommandManager{store: dbStore}
}

// PushCommand adds a command for a worker. Returns the command_id.
func (cm *CommandManager) PushCommand(workerID string, cmdType string, params map[string]interface{}) string {
	commandID := fmt.Sprintf("cmd-%s-%s-%d", workerID, cmdType, time.Now().UnixNano())

	if cm.store == nil {
		return commandID
	}

	// Idempotent: skip if same type already pending
	if ok, _ := cm.store.HasPendingCommand(workerID, cmdType, commandID); ok {
		return commandID
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
		registryLog.ErrorWithMsg("cmd.push.fail", "Failed to persist command",
			map[string]interface{}{"worker_id": workerID, "type": cmdType, "err": err.Error()})
	}

	return commandID
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
type TokenManager struct {
	mu    sync.RWMutex
	store *store.SQLiteStore
}

// NewTokenManager creates a SQLite-backed token manager.
func NewTokenManager(dbStore *store.SQLiteStore) *TokenManager {
	return &TokenManager{store: dbStore}
}

// GenerateToken creates a new session token for a worker and persists it.
func (tm *TokenManager) GenerateToken(workerID string) string {
	token := generateRandomToken()
	tokenHash := store.HashCredential(token)
	sessionID := fmt.Sprintf("sess-%s-%d", workerID, time.Now().UnixNano())

	if tm.store != nil {
		sess := &store.PersistedSession{
			SessionID: sessionID,
			WorkerID:  workerID,
			TokenHash: tokenHash,
			ExpiresAt: time.Now().UTC().Add(time.Hour),
		}
		if err := tm.store.InsertSession(sess); err != nil {
			registryLog.ErrorWithMsg("token.gen.fail", "Failed to persist session",
				map[string]interface{}{"worker_id": workerID, "err": err.Error()})
		}
	}

	return token
}

// ValidateWorkerCommandToken checks if a worker command token is valid and
// returns the associated worker ID. Renamed from ValidateToken to eliminate
// ambiguity with YouTube OAuth token validators.
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
