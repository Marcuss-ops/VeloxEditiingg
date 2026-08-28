package main

// bootstrap_publishing.go wires the creatorflow.Resolver (publishing path)
// and the InstaEdit JWT verifier.  Split out of bootstrap_composition.go
// so the orchestrator stays focused on dependency order.

import (
	"context"
	"fmt"
	"strings"

	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/instaeditauth"
	"velox-server/internal/logging"
)

// wireResolver builds the canonical creatorflow.Resolver and attaches it
// to the ForwardingRunner so the sync and async paths converge.
func wireResolver(cfg *config.Config, p *persistenceDeps, m *moduleDeps) *creatorflow.Resolver {
	if p == nil || p.SQLite == nil || m == nil || m.Enqueuer == nil {
		return nil
	}
	resolver := creatorflow.NewResolver(cfg, m.Enqueuer, p.SQLite.Forwarding(), p.SQLite)
	if m.ForwardingRunner != nil {
		m.ForwardingRunner.SetResolver(resolver)
		logServerf(context.Background(), logging.LevelInfo, logging.CodeServerBootstrap, "[BOOTSTRAP] CreatorForwardingRunner wired to canonical Resolver (Blocco 5)")
	}
	return resolver
}

// wireInstaeditVerifier builds the InstaEdit control-plane JWT verifier
// when the shared secret is configured. A nil verifier means the
// /api/v1/instaedit routes are not mounted.
func wireInstaeditVerifier(cfg *config.Config, p *persistenceDeps) (*instaeditauth.Verifier, error) {
	secret := strings.TrimSpace(cfg.Auth.InstaeditControlJWTSecret)
	if secret == "" {
		return nil, nil
	}
	v, err := instaeditauth.New(secret)
	if err != nil {
		return nil, fmt.Errorf("bootstrap: instaedit auth verifier: %w", err)
	}
	logServerf(context.Background(), logging.LevelInfo, logging.CodeServerBootstrap, "[BOOTSTRAP] InstaEdit control JWT verifier configured")
	return v, nil
}
