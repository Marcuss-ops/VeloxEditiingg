// Package grpcserver / logging.go
//
// Structured logging for the gRPC worker-control server. The package-level
// logger is immutable (write-once at init) and safe for concurrent use,
// matching the existing workers.registry / workers.update_handler pattern.
package grpcserver

import (
	"context"
	"fmt"

	"velox-server/internal/logging"
)

var grpcLog = logging.NewLogger("grpcserver")

// logGRPCf formats the message and emits a structured event with the given
// level and code, correlating trace_id/span_id from ctx. The message is
// preserved verbatim so no operator-facing detail is lost during the
// log.Printf → structured-logger migration.
func logGRPCf(ctx context.Context, level, code, format string, args ...interface{}) {
	msg := fmt.Sprintf(format, args...)
	switch level {
	case logging.LevelWarn:
		grpcLog.WarnWithMsgContext(ctx, code, msg, nil)
	case logging.LevelError:
		grpcLog.ErrorWithMsgContext(ctx, code, msg, nil)
	default:
		grpcLog.InfoWithMsgContext(ctx, code, msg, nil)
	}
}
