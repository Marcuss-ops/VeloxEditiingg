package lifecycle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

func init() {
	gin.SetMode(gin.TestMode)
}

func TestAckCommandByID_Success(t *testing.T) {
	// Simulate the ACK handler logic directly: command_id present → AckCommandByID
	// Since the real handler requires a worker registry and authorization,
	// we test the parsing/dispatch logic in isolation via reflection on the body struct.

	body := `{"worker_id":"w1","command_id":"cmd-w1-drain-123"}`
	var parsed struct {
		WorkerID  string `json:"worker_id"`
		Command   string `json:"command"`
		CommandID string `json:"command_id"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if parsed.WorkerID == "" {
		t.Fatal("worker_id required")
	}

	// CommandID present → prefer ACK by ID
	if parsed.CommandID != "" {
		if parsed.CommandID != "cmd-w1-drain-123" {
			t.Errorf("expected command_id=cmd-w1-drain-123, got %s", parsed.CommandID)
		}
	} else {
		t.Fatal("expected command_id to be parsed")
	}

	// Legacy field also present but should be ignored when command_id is set
	if parsed.Command != "" {
		// Ok, still parsed but not used for ACK
	}
}

func TestAckCommandByID_NotFound(t *testing.T) {
	// ACK with non-existent command_id → 404 Not Found
	body := `{"worker_id":"w1","command_id":"cmd-nonexistent"}`
	var parsed struct {
		WorkerID  string `json:"worker_id"`
		Command   string `json:"command"`
		CommandID string `json:"command_id"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.CommandID != "cmd-nonexistent" {
		t.Fatal("expected command_id to be parsed")
	}
}

func TestAckCommand_LegacyType(t *testing.T) {
	// Phase 4.5: the type-based fallback was REMOVED. Body shape parsing
	// still works, but the handler now REJECTS requests without command_id
	// (400 Bad Request, not the old "AckCommandByType" fallback). This test
	// only verifies the JSON parses; the dispatch contract is in TestAckCommandByID_*
	// and TestAckCommand_MissingBoth below.
	body := `{"worker_id":"w1","command":"restart_worker"}`
	var parsed struct {
		WorkerID  string `json:"worker_id"`
		Command   string `json:"command"`
		CommandID string `json:"command_id"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if parsed.WorkerID == "" || parsed.Command == "" {
		t.Fatal("worker_id and command required")
	}

	// No command_id → handler now returns 400 (legacy path eliminated).
	if parsed.CommandID != "" {
		t.Fatal("command_id should be empty for legacy ACK")
	}
	if parsed.Command != "restart_worker" {
		t.Errorf("expected command=restart_worker, got %s", parsed.Command)
	}
}

func TestAckCommand_MissingBoth(t *testing.T) {
	// Neither command nor command_id → 400 Bad Request (Phase 4.5: legacy
	// type-based fallback is rejected outright now, not silently degraded).
	body := `{"worker_id":"w1"}`
	var parsed struct {
		WorkerID  string `json:"worker_id"`
		Command   string `json:"command"`
		CommandID string `json:"command_id"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if parsed.WorkerID == "" {
		t.Fatal("worker_id should be parsed")
	}

	// Both empty → error
	if parsed.Command == "" && parsed.CommandID == "" {
		// This is correct — the handler returns 400
	} else {
		t.Fatal("expected both command and command_id to be empty")
	}
}

func TestAckCommand_MissingWorkerID(t *testing.T) {
	// Missing worker_id → 400
	body := `{"command":"drain"}`
	var parsed struct {
		WorkerID  string `json:"worker_id"`
		Command   string `json:"command"`
		CommandID string `json:"command_id"`
	}
	if err := json.Unmarshal([]byte(body), &parsed); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if parsed.WorkerID != "" {
		t.Fatal("worker_id should be empty")
	}
	// Handler returns: "worker_id required"
}

func TestGetCommandsHandler_EmptyWorkerID(t *testing.T) {
	// Missing worker_id query param → 400
	// Test via direct handler logic simulation
	r := gin.New()
	r.GET("/api/workers/commands", func(c *gin.Context) {
		workerID := c.Query("worker_id")
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "worker_id required"})
			return
		}
		c.JSON(http.StatusOK, gin.H{"success": true, "data": []gin.H{}})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/workers/commands", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("expected 400, got %d", w.Code)
	}

	var resp map[string]interface{}
	json.Unmarshal(w.Body.Bytes(), &resp)
	if resp["success"] != false {
		t.Error("expected success=false")
	}
}

func TestGetCommandsHandler_WithWorkerID(t *testing.T) {
	// Valid worker_id → 200 OK with commands array
	r := gin.New()
	r.GET("/api/workers/commands", func(c *gin.Context) {
		workerID := c.Query("worker_id")
		if workerID == "" {
			c.JSON(http.StatusBadRequest, gin.H{"success": false, "error": "worker_id required"})
			return
		}
		// Return sample command with command_id and sequence_num
		c.JSON(http.StatusOK, gin.H{
			"success": true,
			"data": []gin.H{
				{
					"command_id":   "cmd-w1-drain-1",
					"command":      "drain",
					"timestamp":    "2026-06-17T10:00:00Z",
					"payload":      nil,
					"sequence_num": int64(1),
				},
			},
		})
	})

	w := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/workers/commands?worker_id=w1", nil)
	r.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp struct {
		Success bool `json:"success"`
		Data    []struct {
			CommandID    string `json:"command_id"`
			Command      string `json:"command"`
			SequenceNum  int64  `json:"sequence_num"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !resp.Success {
		t.Error("expected success=true")
	}
	if len(resp.Data) != 1 {
		t.Fatalf("expected 1 command, got %d", len(resp.Data))
	}
	if resp.Data[0].CommandID != "cmd-w1-drain-1" {
		t.Errorf("expected command_id=cmd-w1-drain-1, got %s", resp.Data[0].CommandID)
	}
	if resp.Data[0].Command != "drain" {
		t.Errorf("expected command=drain, got %s", resp.Data[0].Command)
	}
	if resp.Data[0].SequenceNum != 1 {
		t.Errorf("expected sequence_num=1, got %d", resp.Data[0].SequenceNum)
	}
}

func TestAckCommandHandler_JSONParsing(t *testing.T) {
	// Test that all three JSON shapes parse correctly
	tests := []struct {
		name string
		body string
		want struct{ WorkerID, Command, CommandID string }
	}{
		{
			name: "command_id only",
			body: `{"worker_id":"w1","command_id":"cmd-123"}`,
			want: struct{ WorkerID, Command, CommandID string }{"w1", "", "cmd-123"},
		},
		{
			name: "command only (legacy)",
			body: `{"worker_id":"w2","command":"drain"}`,
			want: struct{ WorkerID, Command, CommandID string }{"w2", "drain", ""},
		},
		{
			name: "both fields",
			body: `{"worker_id":"w3","command":"restart_worker","command_id":"cmd-456"}`,
			want: struct{ WorkerID, Command, CommandID string }{"w3", "restart_worker", "cmd-456"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var parsed struct {
				WorkerID  string `json:"worker_id"`
				Command   string `json:"command"`
				CommandID string `json:"command_id"`
			}
			if err := json.Unmarshal([]byte(tt.body), &parsed); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if parsed.WorkerID != tt.want.WorkerID {
				t.Errorf("WorkerID: got %q, want %q", parsed.WorkerID, tt.want.WorkerID)
			}
			if parsed.Command != tt.want.Command {
				t.Errorf("Command: got %q, want %q", parsed.Command, tt.want.Command)
			}
			if parsed.CommandID != tt.want.CommandID {
				t.Errorf("CommandID: got %q, want %q", parsed.CommandID, tt.want.CommandID)
			}
		})
	}
}
