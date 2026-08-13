// Package main / logging.go
//
// Structured logging for the server bootstrap + lifecycle. The package-level
// logger is immutable (write-once at init) and safe for concurrent use.
package main

import (
	"context"
	"fmt"

	"velox-server/internal/logging"
)

var serverLog = logging.NewLogger("server")

// logServerf formats the message and emits a structured event with the given
// level and code. The message is preserved verbatim so no operator-facing
// detail is lost during the log.Printf → structured-logger migration.
func logServerf(ctx context.Context, level, code, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	switch level {
	case logging.LevelWarn:
		serverLog.WarnWithMsgContext(ctx, code, msg, nil)
	case logging.LevelError:
		serverLog.ErrorWithMsgContext(ctx, code, msg, nil)
	default:
		serverLog.InfoWithMsgContext(ctx, code, msg, nil)
	}
}
