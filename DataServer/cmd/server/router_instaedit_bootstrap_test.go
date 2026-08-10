package main

import (
	"path/filepath"
	"strings"
	"testing"

	"velox-server/internal/config"
	instaedithandler "velox-server/internal/handlers/server/instaedit"
	"velox-server/internal/instaeditauth"
	"velox-server/internal/metrics"

	"github.com/gin-gonic/gin"
)

// newRouterWithInstaEditBundle builds a minimal RouterBundle where only
// the InstaEdit deps vary. It wires enough of the bundle for newRouter
// to succeed when InstaEdit is disabled, and to reach the InstaEdit
// fail-fast check when it is enabled but incomplete.
func newRouterWithInstaEditBundle(t *testing.T, ie InstaEditRouteDeps) (*gin.Engine, error) {
	t.Helper()
	root := t.TempDir()
	cfg := &config.Config{
		Server: config.ServerConfig{
			Port:         18000,
			GRPCPort:     19000,
			GRPCPushMode: true,
			GinMode:      "test",
		},
		Runtime: config.RuntimeConfig{
			DataDir:        root,
			RuntimeDir:     root,
			VideosDir:      filepath.Join(root, "videos"),
			SecretsDir:     filepath.Join(root, "secrets"),
			StagingDir:     filepath.Join(root, "staging"),
			StorageDir:     filepath.Join(root, "storage"),
			Environment:    "development",
			ReleaseChannel: "dev",
		},
		Database: config.DatabaseConfig{
			DBPath: filepath.Join(root, "velox.db"),
		},
		Workers: config.WorkersConfig{
			AllowedWorkerIDs: []string{"worker-a"},
			MaxJobAttempts:   3,
			HeartbeatTimeout: 120,
		},
		Auth: config.AuthConfig{
			AdminToken: "test-admin-token",
		},
	}

	bundle := RouterBundle{
		Metrics:   MetricsRouteDeps{Registry: metrics.NewRegistry()},
		InstaEdit: ie,
	}

	return newRouter(cfg, bundle, &noopRegistry{})
}

// noopRegistry is a minimal registry implementation for tests.
type noopRegistry struct{}

func (noopRegistry) RegisterRoutes(r *gin.Engine) {}

// TestNewRouter_InstaEditDisabled_DoesNotMountRoutes ensures that when
// the InstaEdit verifier is not configured the route group is silently
// skipped without failing startup.
func TestNewRouter_InstaEditDisabled_DoesNotMountRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router, err := newRouterWithInstaEditBundle(t, InstaEditRouteDeps{})
	if err != nil {
		t.Fatalf("newRouter expected no error when InstaEdit is disabled, got %v", err)
	}

	for _, route := range router.Routes() {
		if strings.HasPrefix(route.Path, "/api/v1/instaedit") {
			t.Fatalf("expected no InstaEdit routes when disabled, found %s %s", route.Method, route.Path)
		}
	}
}

// TestNewRouter_InstaEditEnabledButServiceNil_FailsFast ensures that
// when the feature is configured (verifier present) but the service is
// missing, startup fails loudly rather than mounting a broken route
// group.
func TestNewRouter_InstaEditEnabledButServiceNil_FailsFast(t *testing.T) {
	gin.SetMode(gin.TestMode)

	_, err := newRouterWithInstaEditBundle(t, InstaEditRouteDeps{
		Verifier: &instaeditauth.Verifier{},
		// Service intentionally nil.
	})
	if err == nil {
		t.Fatal("expected newRouter to fail when InstaEdit is enabled but service is nil")
	}
}

// TestNewRouter_InstaEditEnabledAndWired_MountsRoutes ensures the happy
// path still mounts the InstaEdit BFF routes.
func TestNewRouter_InstaEditEnabledAndWired_MountsRoutes(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router, err := newRouterWithInstaEditBundle(t, InstaEditRouteDeps{
		Verifier: &instaeditauth.Verifier{},
		Service:  instaedithandler.NewService(nil, nil, nil, nil),
	})
	if err != nil {
		t.Fatalf("newRouter expected no error with full InstaEdit deps, got %v", err)
	}

	found := false
	for _, route := range router.Routes() {
		if route.Method == "POST" && route.Path == "/api/v1/velox/jobs" {
			t.Fatal("legacy POST /api/v1/velox/jobs route must not be mounted")
		}
		if route.Path == "/api/v1/instaedit/jobs" {
			found = true
		}
	}
	if !found {
		t.Fatal("expected /api/v1/instaedit/jobs to be mounted when InstaEdit is fully wired")
	}
}

// TestNewRouter_RetiredEditorRoutesNeverMounted is the global negative pin
// for the retired editor runtime: those surfaces must never be present on
// the route table, regardless of InstaEdit wiring state.
func TestNewRouter_RetiredEditorRoutesNeverMounted(t *testing.T) {
	gin.SetMode(gin.TestMode)

	router, err := newRouterWithInstaEditBundle(t, InstaEditRouteDeps{
		Verifier: &instaeditauth.Verifier{},
		Service:  instaedithandler.NewService(nil, nil, nil, nil),
	})
	if err != nil {
		t.Fatalf("newRouter expected no error with full InstaEdit deps, got %v", err)
	}

	forbiddenPrefixes := []string{
		"/api/darkeditor",
		"/dark_editor_v2",
	}
	for _, route := range router.Routes() {
		for _, prefix := range forbiddenPrefixes {
			if strings.HasPrefix(route.Path, prefix) {
				t.Errorf("retired editor route %s %s must not be mounted (forbidden prefix %s)", route.Method, route.Path, prefix)
			}
		}
	}

	// Positive anchors: the InstaEdit BFF contract surfaces are still there.
	for _, want := range []string{
		"/api/v1/instaedit/jobs",
		"/api/v1/instaedit/workers",
		"/api/v1/instaedit/assets",
		"/api/v1/instaedit/editor/projects/:project_id",
	} {
		found := false
		for _, route := range router.Routes() {
			if strings.HasPrefix(route.Path, want) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected InstaEdit BFF route under %s to be mounted", want)
		}
	}
}
