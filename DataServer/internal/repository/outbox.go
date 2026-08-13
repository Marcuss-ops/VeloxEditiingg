package repository

import (
	"context"

	"velox-server/internal/outbox"
)

// OutboxEmitter is the minimal interface the store uses to write outbox
// events. Bootstrap wires an *outbox.Store; nil is a safe no-op for callers
// that have not completed the cutover.
type OutboxEmitter interface {
	Insert(ctx context.Context, txn outbox.Executor, params outbox.InsertParams) (string, error)
}
