package main

import (
	"testing"

	"github.com/gin-gonic/gin"
	"velox-server/internal/instaeditauth"
	instaedithandler "velox-server/internal/handlers/server/instaedit"
)

func TestRegisterInstaEditRoutes_DisabledWhenVerifierNil(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	deps := InstaEditRouteDeps{}

	if err := registerInstaEditRoutes(r, deps); err != nil {
		t.Fatalf("expected no error when feature is disabled, got %v", err)
	}

	instaeditPrefix := "/api/v1/instaedit"
	for _, route := range r.Routes() {
		if len(route.Path) >= len(instaeditPrefix) && route.Path[:len(instaeditPrefix)] == instaeditPrefix {
			t.Fatalf("InstaEdit routes should not be mounted when verifier is nil, got %s", route.Path)
		}
	}
}

func TestRegisterInstaEditRoutes_FailsWhenEnabledButServiceNil(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	deps := InstaEditRouteDeps{
		Verifier: &instaeditauth.Verifier{},
		// Service intentionally nil.
	}

	if err := registerInstaEditRoutes(r, deps); err == nil {
		t.Fatal("expected error when InstaEdit is enabled but service is missing")
	}
}

func TestRegisterInstaEditRoutes_SucceedsWithService(t *testing.T) {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	deps := InstaEditRouteDeps{
		Verifier: &instaeditauth.Verifier{},
		Service:  instaedithandler.NewService(nil, nil, nil, nil),
	}

	if err := registerInstaEditRoutes(r, deps); err != nil {
		t.Fatalf("expected no error with service, got %v", err)
	}

	found := false
	for _, route := range r.Routes() {
		if route.Path == "/api/v1/instaedit/jobs" {
			found = true
			break
		}
	}
	if !found {
		t.Fatal("expected /api/v1/instaedit/jobs route to be mounted")
	}
}
