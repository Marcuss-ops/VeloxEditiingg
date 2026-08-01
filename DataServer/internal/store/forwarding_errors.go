package store

import "errors"

// ErrLeaseLost indicates that a lease-fenced forwarding operation no longer
// owns its row. Callers must stop mutating the forwarding and let the current
// lease holder continue.
var ErrLeaseLost = errors.New("store: forwarding lease lost")
