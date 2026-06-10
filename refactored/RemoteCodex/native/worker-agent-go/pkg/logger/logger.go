// Package logger provides structured logging for the Velox Worker Agent.
package logger

import (
	"fmt"
	"io"
	"log"
	"os"
	"sync"
)

// Level represents a log level.
type Level int

const (
	// DebugLevel logs are typically voluminous, and are usually disabled in production.
	DebugLevel Level = iota
	// InfoLevel is the default logging priority.
	InfoLevel
	// WarnLevel logs are more important than Info, but don't need individual human review.
	WarnLevel
	// ErrorLevel logs are high-priority. If an application is running smoothly, it shouldn't generate any error-level logs.
	ErrorLevel
)

// String returns the string representation of the log level.
func (l Level) String() string {
	switch l {
	case DebugLevel:
		return "DEBUG"
	case InfoLevel:
		return "INFO"
	case WarnLevel:
		return "WARN"
	case ErrorLevel:
		return "ERROR"
	default:
		return fmt.Sprintf("LEVEL(%d)", l)
	}
}

// ParseLevel parses a level string into a Level.
// Returns InfoLevel if the string is not recognized.
func ParseLevel(s string) Level {
	switch s {
	case "debug", "DEBUG":
		return DebugLevel
	case "info", "INFO":
		return InfoLevel
	case "warn", "WARN", "warning", "WARNING":
		return WarnLevel
	case "error", "ERROR":
		return ErrorLevel
	default:
		return InfoLevel
	}
}

// Logger provides structured logging with level filtering.
type Logger struct {
	mu       sync.Mutex
	level    Level
	out      io.Writer
	debugLog *log.Logger
	infoLog  *log.Logger
	warnLog  *log.Logger
	errorLog *log.Logger
	prefix   string
}

// New creates a new Logger with the specified level and output writer.
// If out is nil, os.Stdout is used.
func New(level Level, out io.Writer) *Logger {
	if out == nil {
		out = os.Stdout
	}

	l := &Logger{
		level: level,
		out:   out,
	}

	l.debugLog = log.New(out, "[DEBUG] ", log.LstdFlags|log.Lmsgprefix)
	l.infoLog = log.New(out, "[INFO] ", log.LstdFlags|log.Lmsgprefix)
	l.warnLog = log.New(out, "[WARN] ", log.LstdFlags|log.Lmsgprefix)
	l.errorLog = log.New(out, "[ERROR] ", log.LstdFlags|log.Lmsgprefix)

	return l
}

// Default returns the default logger (InfoLevel, stdout).
var defaultLogger = New(InfoLevel, os.Stdout)

// SetLevel sets the minimum log level.
func (l *Logger) SetLevel(level Level) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.level = level
}

// SetPrefix sets a prefix for all log messages.
func (l *Logger) SetPrefix(prefix string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.prefix = prefix
}

// SetOutput sets the output writer.
func (l *Logger) SetOutput(w io.Writer) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.out = w
	l.debugLog.SetOutput(w)
	l.infoLog.SetOutput(w)
	l.warnLog.SetOutput(w)
	l.errorLog.SetOutput(w)
}

// log logs a message at the specified level.
func (l *Logger) log(level Level, logger *log.Logger, format string, args ...interface{}) {
	if level < l.level {
		return
	}

	msg := fmt.Sprintf(format, args...)
	if l.prefix != "" {
		msg = l.prefix + " " + msg
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	logger.Println(msg)
}

// Debug logs a message at DebugLevel.
func (l *Logger) Debug(format string, args ...interface{}) {
	l.log(DebugLevel, l.debugLog, format, args...)
}

// Info logs a message at InfoLevel.
func (l *Logger) Info(format string, args ...interface{}) {
	l.log(InfoLevel, l.infoLog, format, args...)
}

// Warn logs a message at WarnLevel.
func (l *Logger) Warn(format string, args ...interface{}) {
	l.log(WarnLevel, l.warnLog, format, args...)
}

// Error logs a message at ErrorLevel.
func (l *Logger) Error(format string, args ...interface{}) {
	l.log(ErrorLevel, l.errorLog, format, args...)
}

// Fatal logs a message at ErrorLevel and exits with code 1.
func (l *Logger) Fatal(format string, args ...interface{}) {
	l.log(ErrorLevel, l.errorLog, format, args...)
	os.Exit(1)
}

// WithPrefix returns a new logger with the specified prefix.
func (l *Logger) WithPrefix(prefix string) *Logger {
	return New(l.level, l.out)
}

// Global functions that use the default logger

// SetDefaultLevel sets the level of the default logger.
func SetDefaultLevel(level Level) {
	defaultLogger.SetLevel(level)
}

// Debug logs a message at DebugLevel using the default logger.
func Debug(format string, args ...interface{}) {
	defaultLogger.Debug(format, args...)
}

// Info logs a message at InfoLevel using the default logger.
func Info(format string, args ...interface{}) {
	defaultLogger.Info(format, args...)
}

// Warn logs a message at WarnLevel using the default logger.
func Warn(format string, args ...interface{}) {
	defaultLogger.Warn(format, args...)
}

// Error logs a message at ErrorLevel using the default logger.
func Error(format string, args ...interface{}) {
	defaultLogger.Error(format, args...)
}

// Fatal logs a message at ErrorLevel using the default logger and exits with code 1.
func Fatal(format string, args ...interface{}) {
	defaultLogger.Fatal(format, args...)
}