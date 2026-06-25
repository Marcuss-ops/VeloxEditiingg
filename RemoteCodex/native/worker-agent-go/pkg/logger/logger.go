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

// LogCertRejected emits a standardized Warn-level event when a peer certificate
// (worker/mTLS client cert, or server-side leaf) is rejected for an
// application-layer policy reason — distinct from a TLS-layer handshake failure.
//
// Reason is one of the canonical codes documented in docs/rw-prod/ACTIONS.md §A3:
//
//   "missing_cert"          : tls_cert_file path empty
//   "missing_key"           : tls_key_file  path empty
//   "missing_ca"            : tls_ca_file   path empty
//   "partial_tls"           : one or two of cert/key/ca present, never all three
//   "cert_unreadable"       : os.Stat failed for non-NotExist reason
//   "key_pair_rejected"     : crypto/tls.LoadX509KeyPair rejected cert/key
//   "key_permission_world_readable" : key file mode & 0o077 != 0 (RW-PROD-001 A2)
//   "cert_expired"          : leaf.NotAfter < time.Now().UTC()
//   "cert_too_soon_to_expire" : leaf.NotAfter - now < 14d (RW-PROD-001 A1)
//   "cert_self_signed"      : leaf.Subject == leaf.Issuer (raise awareness)
//   "worker_id_invalid_shape" : shared/identity.IsValidWorkerID == false (RW-PROD-001 A4)
//
// NEVER pass the TLS key material, certificate PEM blob, or credential hash
// into this function — fingerprint is computed via openssl/sha256 OVER the
// raw cert bytes, NOT the cert bytes themselves. The structured fields below
// are the public-key surface operators need to triage a rejection without
// ever seeing private material.
func LogCertRejected(workerID, fingerprint, serial, reason string) {
	defaultLogger.Warn(
		"[AUTHZ] certificate rejected worker_id=%s fingerprint_sha256=%s serial=%s reason=%s",
		workerID, fingerprint, serial, reason,
	)
}
