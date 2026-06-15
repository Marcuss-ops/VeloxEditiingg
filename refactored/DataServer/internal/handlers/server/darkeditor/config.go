package darkeditor

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ============================================================================
// LOGGER
// ============================================================================

// LogLevel represents log severity
type LogLevel string

const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info"
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

// LogEntry represents a single log entry
type LogEntry struct {
	Timestamp time.Time              `json:"timestamp"`
	Level     LogLevel               `json:"level"`
	Message   string                 `json:"message"`
	Metadata  map[string]interface{} `json:"metadata,omitempty"`
	Source    string                 `json:"source,omitempty"` // "server" or "client"
}

// Logger manages persistent logging
type Logger struct {
	logFile    *os.File
	logPath    string
	maxSize    int64 // max file size in bytes
	mu         sync.Mutex
	entries    []LogEntry // in-memory buffer
	maxEntries int        // max entries to keep in memory
}

// NewLogger creates a new logger
func NewLogger(logDir string, maxSize int64) (*Logger, error) {
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return nil, err
	}

	logPath := filepath.Join(logDir, "dark_editor.log")
	file, err := os.OpenFile(logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}

	return &Logger{
		logFile:    file,
		logPath:    logPath,
		maxSize:    maxSize,
		entries:    make([]LogEntry, 0, 1000),
		maxEntries: 1000,
	}, nil
}

// Log writes a log entry
func (l *Logger) Log(level LogLevel, message string, metadata map[string]interface{}, source string) {
	entry := LogEntry{
		Timestamp: time.Now(),
		Level:     level,
		Message:   message,
		Metadata:  metadata,
		Source:    source,
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	// Add to in-memory buffer
	l.entries = append(l.entries, entry)
	if len(l.entries) > l.maxEntries {
		l.entries = l.entries[len(l.entries)-l.maxEntries:]
	}

	// Write to file
	data, _ := json.Marshal(entry)
	l.logFile.Write(data)
	l.logFile.Write([]byte("\n"))

	// Rotate if needed
	l.checkRotation()
}

// GetEntries returns recent log entries
func (l *Logger) GetEntries(limit int, level LogLevel) []LogEntry {
	l.mu.Lock()
	defer l.mu.Unlock()

	var result []LogEntry
	for i := len(l.entries) - 1; i >= 0 && len(result) < limit; i-- {
		if level == "" || l.entries[i].Level == level {
			result = append(result, l.entries[i])
		}
	}
	return result
}

// checkRotation rotates log file if too large
func (l *Logger) checkRotation() {
	info, err := l.logFile.Stat()
	if err != nil {
		return
	}

	if info.Size() > l.maxSize {
		l.logFile.Close()

		// Rename current file
		backupPath := l.logPath + "." + time.Now().Format("20060102-150405")
		os.Rename(l.logPath, backupPath)

		// Create new file
		l.logFile, _ = os.OpenFile(l.logPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	}
}

// Close closes the logger
func (l *Logger) Close() error {
	if l.logFile != nil {
		return l.logFile.Close()
	}
	return nil
}

// ============================================================================
// CONFIG & HANDLER
// ============================================================================

// Config holds configuration for the dark editor handlers
type Config struct {
	TempDir       string // Directory for temporary files
	ProjectsDir   string // Directory for projects (fallback)
	LogDir        string // Directory for log files
	NVIDIAAPIKey  string // NVIDIA API key for AI generation
	MaxUploadSize int64  // Max upload size in bytes
	MaxLogSize    int64  // Max log file size in bytes (default: 10MB)
}

// Handler holds the dark editor handlers
type Handler struct {
	cfg    *Config
	logger *Logger // Persistent logger
}

// NewHandler creates a new dark editor handler
func NewHandler(cfg *Config) *Handler {
	if cfg.MaxUploadSize == 0 {
		cfg.MaxUploadSize = 50 * 1024 * 1024 // 50MB default
	}
	if cfg.MaxLogSize == 0 {
		cfg.MaxLogSize = 10 * 1024 * 1024 // 10MB default
	}

	h := &Handler{cfg: cfg}

	// Initialize logger if LogDir is configured
	if cfg.LogDir != "" {
		logger, err := NewLogger(cfg.LogDir, cfg.MaxLogSize)
		if err == nil {
			h.logger = logger
		}
	}

	return h
}

// SetLogger sets the logger (for late initialization)
func (h *Handler) SetLogger(l *Logger) {
	h.logger = l
}

// GetLogger returns the logger
func (h *Handler) GetLogger() *Logger {
	return h.logger
}
