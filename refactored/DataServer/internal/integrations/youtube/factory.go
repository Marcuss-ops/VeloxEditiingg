// Package youtube factory: ServiceWithStore is a convenience constructor for
// callers (typically tests) that already hold a *store.SQLiteStore and want
// a *Service bound to it without going through YouTube's full ServiceConfig
// loader.
package youtube

import (
	"velox-server/internal/store"
)

// ServiceWithStore returns a YouTube *Service backed by the given SQLite
// store (or nil for in-memory mode). Member lookup helpers (Membership,
// BulkMembership) need this wiring so the package can be used directly
// from internal/store tests without instantiating the full YouTube module.
func ServiceWithStore(s *store.SQLiteStore) *Service {
	cfg := &ServiceConfig{}
	svc, _ := NewService(cfg, s)
	if svc == nil {
		return nil
	}
	return svc
}
