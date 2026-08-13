package logging

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/trace"
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
	component  string
	throttler  *Throttler
	quiet      bool
	jsonOutput bool
	debug      bool
	// processCfg is non-nil only on the process-wide default logger. When
	// set, quiet/jsonOutput/debug are read from the immutable snapshot held
	// by the atomic pointer instead of the (always zero) local copies, so a
	// live Configure/SetQuiet/SetJSONOutput never races with emission.
	processCfg *atomic.Pointer[processConfig]
}

// processConfig is the immutable process-wide logging configuration published
// atomically by Configure/SetQuiet/SetJSONOutput. Readers load it without
// locking, so a configuration change never races with logger construction or
// log emission.
type processConfig struct {
	quiet      bool
	jsonOutput bool
	debug      bool
}

var processConfigValue atomic.Pointer[processConfig]

func init() {
	processConfigValue.Store(&processConfig{})
}

// defaultThrottler is the shared, concurrency-safe throttle used by every
// component logger. It is created once and never mutated.
var defaultThrottler = NewThrottler(5 * time.Minute)

// defaultLogger logs at process scope using the current immutable config
// snapshot rather than holding stale mutable copies.
var defaultLogger = &Logger{
	throttler:  defaultThrottler,
	processCfg: &processConfigValue,
}

// NewLogger creates a new logger for a component
// Configure applies the centrally parsed process logging settings.
func Configure(quiet, jsonOutput, debug bool) {
	processConfigValue.Store(&processConfig{quiet: quiet, jsonOutput: jsonOutput, debug: debug})
}

func NewLogger(component string) *Logger {
	cfg := processConfigValue.Load()
	return &Logger{
		component:  component,
		throttler:  defaultThrottler,
		quiet:      cfg.quiet,
		jsonOutput: cfg.jsonOutput,
		debug:      cfg.debug,
	}
}

// SetQuiet enables/disables quiet mode (errors only)
func SetQuiet(quiet bool) {
	cfg := *processConfigValue.Load()
	cfg.quiet = quiet
	processConfigValue.Store(&cfg)
}

// SetJSONOutput enables/disables JSON output
func SetJSONOutput(json bool) {
	cfg := *processConfigValue.Load()
	cfg.jsonOutput = json
	processConfigValue.Store(&cfg)
}

// Info logs an info-level event
func (l *Logger) Info(code string, fields map[string]interface{}) {
	l.log(nil, LevelInfo, code, "", fields)
}

// InfoContext logs an info-level event, correlating it with the active span
// in ctx (trace_id/span_id injected into Fields when present).
func (l *Logger) InfoContext(ctx context.Context, code string, fields map[string]interface{}) {
	l.log(ctx, LevelInfo, code, "", fields)
}

// InfoWithMsg logs an info-level event with a custom message
func (l *Logger) InfoWithMsg(code, message string, fields map[string]interface{}) {
	l.log(nil, LevelInfo, code, message, fields)
}

// InfoWithMsgContext logs an info-level event with a custom message and trace correlation.
func (l *Logger) InfoWithMsgContext(ctx context.Context, code, message string, fields map[string]interface{}) {
	l.log(ctx, LevelInfo, code, message, fields)
}

// Warn logs a warning-level event
func (l *Logger) Warn(code string, fields map[string]interface{}) {
	l.log(nil, LevelWarn, code, "", fields)
}

// WarnContext logs a warning-level event, correlating it with the active span in ctx.
func (l *Logger) WarnContext(ctx context.Context, code string, fields map[string]interface{}) {
	l.log(ctx, LevelWarn, code, "", fields)
}

// WarnWithMsg logs a warning-level event with a custom message
func (l *Logger) WarnWithMsg(code, message string, fields map[string]interface{}) {
	l.log(nil, LevelWarn, code, message, fields)
}

// WarnWithMsgContext logs a warning-level event with a custom message and trace correlation.
func (l *Logger) WarnWithMsgContext(ctx context.Context, code, message string, fields map[string]interface{}) {
	l.log(ctx, LevelWarn, code, message, fields)
}

// Error logs an error-level event
func (l *Logger) Error(code string, fields map[string]interface{}) {
	l.log(nil, LevelError, code, "", fields)
}

// ErrorContext logs an error-level event, correlating it with the active span in ctx.
func (l *Logger) ErrorContext(ctx context.Context, code string, fields map[string]interface{}) {
	l.log(ctx, LevelError, code, "", fields)
}

// ErrorWithMsg logs an error-level event with a custom message
func (l *Logger) ErrorWithMsg(code, message string, fields map[string]interface{}) {
	l.log(nil, LevelError, code, message, fields)
}

// ErrorWithMsgContext logs an error-level event with a custom message and trace correlation.
func (l *Logger) ErrorWithMsgContext(ctx context.Context, code, message string, fields map[string]interface{}) {
	l.log(ctx, LevelError, code, message, fields)
}

// Debug logs a debug-level event when debug mode has been enabled by the
// composition root. The package no longer reads process environment.
func (l *Logger) Debug(code string, fields map[string]interface{}) {
	debug := l.debug
	if l.processCfg != nil {
		debug = l.processCfg.Load().debug
	}
	if !debug {
		return
	}
	l.log(nil, LevelDebug, code, "", fields)
}

// DebugContext logs a debug-level event with trace correlation when debug mode is enabled.
func (l *Logger) DebugContext(ctx context.Context, code string, fields map[string]interface{}) {
	debug := l.debug
	if l.processCfg != nil {
		debug = l.processCfg.Load().debug
	}
	if !debug {
		return
	}
	l.log(ctx, LevelDebug, code, "", fields)
}

// WarnThrottled logs a warning with throttling (dedup by code+key fields)
// Returns true if logged, false if throttled
func (l *Logger) WarnThrottled(code string, key string, fields map[string]interface{}) bool {
	throttleKey := code + ":" + key
	if !l.throttler.Allow(throttleKey) {
		return false
	}
	l.log(nil, LevelWarn, code, "", fields)
	return true
}

// InfoThrottled logs info with throttling
func (l *Logger) InfoThrottled(code string, key string, fields map[string]interface{}) bool {
	throttleKey := code + ":" + key
	if !l.throttler.Allow(throttleKey) {
		return false
	}
	l.log(nil, LevelInfo, code, "", fields)
	return true
}

// log is the internal logging function
func (l *Logger) log(ctx context.Context, level, code, message string, fields map[string]interface{}) {
	quiet, jsonOutput := l.quiet, l.jsonOutput
	if l.processCfg != nil {
		cfg := l.processCfg.Load()
		quiet, jsonOutput = cfg.quiet, cfg.jsonOutput
	}
	// In quiet mode, only log errors
	if quiet && level != LevelError {
		return
	}

	// Correlate the event with the active span when one is present (GAP 4).
	// The caller's fields map is never mutated: injection copies into a
	// fresh map so a shared/immutable map stays intact.
	if ctx != nil {
		if sc := trace.SpanFromContext(ctx).SpanContext(); sc.IsValid() {
			merged := make(map[string]interface{}, len(fields)+2)
			for k, v := range fields {
				merged[k] = v
			}
			merged["trace_id"] = sc.TraceID().String()
			merged["span_id"] = sc.SpanID().String()
			fields = merged
		}
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

	if jsonOutput {
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

// InfoContext logs info using the default logger with trace correlation.
func InfoContext(ctx context.Context, code string, fields map[string]interface{}) {
	defaultLogger.InfoContext(ctx, code, fields)
}

// WarnContext logs warning using the default logger with trace correlation.
func WarnContext(ctx context.Context, code string, fields map[string]interface{}) {
	defaultLogger.WarnContext(ctx, code, fields)
}

// ErrorContext logs error using the default logger with trace correlation.
func ErrorContext(ctx context.Context, code string, fields map[string]interface{}) {
	defaultLogger.ErrorContext(ctx, code, fields)
}

// DebugContext logs debug using the default logger with trace correlation.
func DebugContext(ctx context.Context, code string, fields map[string]interface{}) {
	defaultLogger.DebugContext(ctx, code, fields)
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
