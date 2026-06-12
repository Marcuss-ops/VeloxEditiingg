package workers

import (
	"crypto/rand"
	"encoding/json"
	"math/big"
	"sync"
	"time"
)

// WorkerCommand represents a command to be executed by a worker
type WorkerCommand struct {
	Type      string                 `json:"type"`
	Command   string                 `json:"command"`
	Timestamp string                 `json:"timestamp"`
	Params    map[string]interface{} `json:"params,omitempty"`
}

// CommandManager handles pending commands for workers
type CommandManager struct {
	mu         sync.RWMutex
	pending    map[string][]WorkerCommand      // worker_id -> commands
	ackTracker map[string]map[string]time.Time // worker_id -> command_type -> ack_time
}

// NewCommandManager creates a new command manager
func NewCommandManager() *CommandManager {
	return &CommandManager{
		pending:    make(map[string][]WorkerCommand),
		ackTracker: make(map[string]map[string]time.Time),
	}
}

// PushCommand adds a command for a worker
func (cm *CommandManager) PushCommand(workerID string, cmdType string, params map[string]interface{}) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	cmd := WorkerCommand{
		Type:      cmdType,
		Command:   cmdType,
		Timestamp: time.Now().UTC().Format(time.RFC3339),
		Params:    params,
	}

	// Check if command already exists (idempotent)
	for _, existing := range cm.pending[workerID] {
		if existing.Type == cmdType {
			return // Already pending
		}
	}

	cm.pending[workerID] = append(cm.pending[workerID], cmd)
}

// GetPendingCommands returns all pending commands for a worker
func (cm *CommandManager) GetPendingCommands(workerID string) []WorkerCommand {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	cmds := cm.pending[workerID]
	if cmds == nil {
		return []WorkerCommand{}
	}

	// Return a copy
	result := make([]WorkerCommand, len(cmds))
	copy(result, cmds)
	return result
}

// AckCommand marks a command as acknowledged and removes it
func (cm *CommandManager) AckCommand(workerID string, cmdType string) {
	cm.mu.Lock()
	defer cm.mu.Unlock()

	// Remove from pending
	var remaining []WorkerCommand
	for _, cmd := range cm.pending[workerID] {
		if cmd.Type != cmdType {
			remaining = append(remaining, cmd)
		}
	}
	cm.pending[workerID] = remaining

	// Track ack time
	if cm.ackTracker[workerID] == nil {
		cm.ackTracker[workerID] = make(map[string]time.Time)
	}
	cm.ackTracker[workerID][cmdType] = time.Now()
}

// GetAckTime returns when a command was acknowledged
func (cm *CommandManager) GetAckTime(workerID string, cmdType string) *time.Time {
	cm.mu.RLock()
	defer cm.mu.RUnlock()

	if cm.ackTracker[workerID] == nil {
		return nil
	}
	if t, ok := cm.ackTracker[workerID][cmdType]; ok {
		return &t
	}
	return nil
}

// PendingUpdate tracks pending code updates for workers
type PendingUpdate struct {
	WorkerID    string    `json:"worker_id"`
	Version     string    `json:"version"`
	RequestedAt time.Time `json:"requested_at"`
	Ack         bool      `json:"ack"`
	AckVersion  string    `json:"ack_version,omitempty"`
	AckTime     time.Time `json:"ack_time,omitempty"`
}

// UpdateManager handles pending code updates for workers
type UpdateManager struct {
	mu      sync.RWMutex
	pending map[string]*PendingUpdate // worker_id -> update
}

// NewUpdateManager creates a new update manager
func NewUpdateManager() *UpdateManager {
	return &UpdateManager{
		pending: make(map[string]*PendingUpdate),
	}
}

// RequestUpdate schedules a code update for a worker
func (um *UpdateManager) RequestUpdate(workerID string, version string) {
	um.mu.Lock()
	defer um.mu.Unlock()

	um.pending[workerID] = &PendingUpdate{
		WorkerID:    workerID,
		Version:     version,
		RequestedAt: time.Now(),
		Ack:         false,
	}
}

// GetPendingUpdate returns the pending update for a worker
func (um *UpdateManager) GetPendingUpdate(workerID string) *PendingUpdate {
	um.mu.RLock()
	defer um.mu.RUnlock()

	return um.pending[workerID]
}

// AckUpdate marks an update as acknowledged
func (um *UpdateManager) AckUpdate(workerID string, ackVersion string) {
	um.mu.Lock()
	defer um.mu.Unlock()

	if update, ok := um.pending[workerID]; ok {
		update.Ack = true
		update.AckVersion = ackVersion
		update.AckTime = time.Now()
	}
}

// ClearUpdate removes a pending update
func (um *UpdateManager) ClearUpdate(workerID string) {
	um.mu.Lock()
	defer um.mu.Unlock()

	delete(um.pending, workerID)
}

// WorkerToken represents a temporary authentication token
type WorkerToken struct {
	WorkerID  string    `json:"worker_id"`
	Token     string    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
	CreatedAt time.Time `json:"created_at"`
}

// TokenManager handles worker authentication tokens
type TokenManager struct {
	mu     sync.RWMutex
	tokens map[string]*WorkerToken // token -> WorkerToken
}

// NewTokenManager creates a new token manager
func NewTokenManager() *TokenManager {
	return &TokenManager{
		tokens: make(map[string]*WorkerToken),
	}
}

// GenerateToken creates a new token for a worker
func (tm *TokenManager) GenerateToken(workerID string) string {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	// Generate random token (in production use crypto/rand)
	token := generateRandomToken()

	tm.tokens[token] = &WorkerToken{
		WorkerID:  workerID,
		Token:     token,
		ExpiresAt: time.Now().Add(time.Hour),
		CreatedAt: time.Now(),
	}

	return token
}

// ValidateToken checks if a token is valid and returns the worker ID
func (tm *TokenManager) ValidateToken(token string) (string, bool) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	t, ok := tm.tokens[token]
	if !ok {
		return "", false
	}

	// Check expiration
	if time.Now().After(t.ExpiresAt) {
		delete(tm.tokens, token)
		return "", false
	}

	return t.WorkerID, true
}

// RevokeToken removes a token
func (tm *TokenManager) RevokeToken(token string) {
	tm.mu.Lock()
	defer tm.mu.Unlock()

	delete(tm.tokens, token)
}

// generateRandomToken generates a cryptographically secure random token
func generateRandomToken() string {
	const chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
	b := make([]byte, 32)
	for i := range b {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(chars))))
		if err != nil {
			// Fallback: use a simpler approach if crypto/rand fails (should never happen)
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
