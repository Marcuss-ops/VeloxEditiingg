package logging

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
	"time"
)

// Event represents a structured log event
type Event struct {
	Timestamp time.Time              `json:"timestamp"`
	Level     string                 `json:"level"`
	Code      string                 `json:"code"`
	Component string                 `json:"component,omitempty"`
	Message   string                 `json:"message,omitempty"`
	Fields    map[string]interface{} `json:"fields,omitempty"`
}

// Logger provides structured logging with throttling support
type Logger struct {
	mu         sync.Mutex
	component  string
	throttler  *Throttler
	quiet      bool
	jsonOutput bool
}

// Global default logger
var defaultLogger = &Logger{
	throttler:  NewThrottler(5 * time.Minute),
	quiet:      os.Getenv("VELOX_QUIET_LOGS") == "true",
	jsonOutput: os.Getenv("VELOX_JSON_LOGS") == "true",
}

// NewLogger creates a new logger for a component
func NewLogger(component string) *Logger {
	return &Logger{
		component:  component,
		throttler:  defaultLogger.throttler,
		quiet:      defaultLogger.quiet,
		jsonOutput: defaultLogger.jsonOutput,
	}
}

// SetQuiet enables/disables quiet mode (errors only)
func SetQuiet(quiet bool) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.quiet = quiet
}

// SetJSONOutput enables/disables JSON output
func SetJSONOutput(json bool) {
	defaultLogger.mu.Lock()
	defer defaultLogger.mu.Unlock()
	defaultLogger.jsonOutput = json
}

// Info logs an info-level event
func (l *Logger) Info(code string, fields map[string]interface{}) {
	l.log(LevelInfo, code, "", fields)
}

// InfoWithMsg logs an info-level event with a custom message
func (l *Logger) InfoWithMsg(code, message string, fields map[string]interface{}) {
	l.log(LevelInfo, code, message, fields)
}

// Warn logs a warning-level event
func (l *Logger) Warn(code string, fields map[string]interface{}) {
	l.log(LevelWarn, code, "", fields)
}

// WarnWithMsg logs a warning-level event with a custom message
func (l *Logger) WarnWithMsg(code, message string, fields map[string]interface{}) {
	l.log(LevelWarn, code, message, fields)
}

// Error logs an error-level event
func (l *Logger) Error(code string, fields map[string]interface{}) {
	l.log(LevelError, code, "", fields)
}

// ErrorWithMsg logs an error-level event with a custom message
func (l *Logger) ErrorWithMsg(code, message string, fields map[string]interface{}) {
	l.log(LevelError, code, message, fields)
}

// Debug logs a debug-level event (only if VELOX_DEBUG=true)
func (l *Logger) Debug(code string, fields map[string]interface{}) {
	if os.Getenv("VELOX_DEBUG") != "true" {
		return
	}
	l.log(LevelDebug, code, "", fields)
}

// WarnThrottled logs a warning with throttling (dedup by code+key fields)
// Returns true if logged, false if throttled
func (l *Logger) WarnThrottled(code string, key string, fields map[string]interface{}) bool {
	throttleKey := code + ":" + key
	if !l.throttler.Allow(throttleKey) {
		return false
	}
	l.log(LevelWarn, code, "", fields)
	return true
}

// InfoThrottled logs info with throttling
func (l *Logger) InfoThrottled(code string, key string, fields map[string]interface{}) bool {
	throttleKey := code + ":" + key
	if !l.throttler.Allow(throttleKey) {
		return false
	}
	l.log(LevelInfo, code, "", fields)
	return true
}

// log is the internal logging function
func (l *Logger) log(level, code, message string, fields map[string]interface{}) {
	// In quiet mode, only log errors
	if l.quiet && level != LevelError {
		return
	}

	event := Event{
		Timestamp: time.Now().UTC(),
		Level:     level,
		Code:      code,
		Component: l.component,
		Fields:    fields,
	}

	// Use provided message or get from code description
	if message != "" {
		event.Message = message
	} else {
		event.Message = GetDescription(code)
	}

	if l.jsonOutput {
		l.outputJSON(event)
	} else {
		l.outputHuman(event)
	}
}

// outputJSON outputs the event as JSON
func (l *Logger) outputJSON(event Event) {
	data, err := json.Marshal(event)
	if err != nil {
		log.Printf("{\"error\":\"failed to marshal log event: %v\"}", err)
		return
	}
	log.Println(string(data))
}

// outputHuman outputs the event in human-readable format
func (l *Logger) outputHuman(event Event) {
	// Format: [LEVEL] code message fields...
	base := fmt.Sprintf("[%s] %s %s", event.Level, event.Code, event.Message)

	// Add key fields inline
	var fieldStrs []string
	for k, v := range event.Fields {
		fieldStrs = append(fieldStrs, fmt.Sprintf("%s=%v", k, v))
	}

	if len(fieldStrs) > 0 {
		log.Printf("%s %s", base, joinFields(fieldStrs))
	} else {
		log.Print(base)
	}
}

// joinFields joins field strings
func joinFields(fields []string) string {
	result := ""
	for i, f := range fields {
		if i > 0 {
			result += " "
		}
		result += f
	}
	return result
}

// === Global functions for convenience ===

// Info logs info using default logger
func Info(code string, fields map[string]interface{}) {
	defaultLogger.Info(code, fields)
}

// Warn logs warning using default logger
func Warn(code string, fields map[string]interface{}) {
	defaultLogger.Warn(code, fields)
}

// Error logs error using default logger
func Error(code string, fields map[string]interface{}) {
	defaultLogger.Error(code, fields)
}

// Debug logs debug using default logger
func Debug(code string, fields map[string]interface{}) {
	defaultLogger.Debug(code, fields)
}

// WarnThrottled logs throttled warning using default logger
func WarnThrottled(code, key string, fields map[string]interface{}) bool {
	return defaultLogger.WarnThrottled(code, key, fields)
}

// F is a helper to create fields map
func F(keyvals ...interface{}) map[string]interface{} {
	if len(keyvals)%2 != 0 {
		return nil
	}
	m := make(map[string]interface{})
	for i := 0; i < len(keyvals); i += 2 {
		if key, ok := keyvals[i].(string); ok {
			m[key] = keyvals[i+1]
		}
	}
	return m
}
