// Package workerauthz contains the canonical worker allowlist decision.
//
// Both the HTTP registration gate and the gRPC stream gate call this package
// so the security decision cannot drift between protocol surfaces.
package workerauthz

import "strings"

// Authorizer is an immutable worker allowlist decision engine.
type Authorizer struct {
	workers  map[string]struct{}
	empty    bool
	insecure bool
}

// New builds an authorizer from the already-parsed worker IDs. Empty entries
// and the legacy "*" value mean "no explicit allowlist"; that mode is only
// permissive when insecureDev is explicitly enabled.
func New(allowedWorkerIDs []string, insecureDev bool) *Authorizer {
	a := &Authorizer{
		workers:  make(map[string]struct{}),
		insecure: insecureDev,
	}
	for _, id := range allowedWorkerIDs {
		id = strings.TrimSpace(id)
		if id == "" || id == "*" {
			continue
		}
		a.workers[id] = struct{}{}
	}
	a.empty = len(a.workers) == 0
	return a
}

// IsAllowed returns the canonical allowlist decision for workerID.
func (a *Authorizer) IsAllowed(workerID string) bool {
	if a == nil {
		return false
	}
	workerID = strings.TrimSpace(workerID)
	if workerID == "" {
		return false
	}
	if !a.empty {
		_, ok := a.workers[workerID]
		return ok
	}
	return a.insecure
}

// IsInsecureDevBypass reports whether this authorizer is operating in the
// explicit empty-allowlist development mode. It is exposed only so protocol
// handlers can retain their protocol-specific warning logs; it is not part of
// the decision itself.
func (a *Authorizer) IsInsecureDevBypass() bool {
	return a != nil && a.empty && a.insecure
}
