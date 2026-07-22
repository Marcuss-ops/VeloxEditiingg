package main

import (
	"path/filepath"
	"testing"

	"velox-server/internal/config"
	"velox-server/internal/metrics"
)

func TestProductionCompositionRegistersRequiredRoutes(t *testing.T) {
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
			AllowedWorkers:   "worker-a",
			AllowedWorkerIDs: []string{"worker-a"},
			MaxJobAttempts:   3,
			HeartbeatTimeout: 120,
		},
		Auth: config.AuthConfig{
			AdminToken: "test-admin-token",
		},
	}

	p, err := buildPersistence(cfg)
	if err != nil {
		t.Fatalf("buildPersistence: %v", err)
	}
	j, err := buildJobs(p)
	if err != nil {
		t.Fatalf("buildJobs: %v", err)
	}
	taskDeps, err := buildTasks(p)
	if err != nil {
		t.Fatalf("buildTasks: %v", err)
	}
	if err := wirePostBuild(j, taskDeps); err != nil {
		t.Fatalf("wirePostBuild: %v", err)
	}
	w, err := buildWorkers(cfg, p)
	if err != nil {
		t.Fatalf("buildWorkers: %v", err)
	}
	a, err := buildAssets(cfg, p, j)
	if err != nil {
		t.Fatalf("buildAssets: %v", err)
	}
	m, err := buildModules(cfg, p, j, w, a, taskDeps)
	if err != nil {
		t.Fatalf("buildModules: %v", err)
	}
	if m.Enqueuer == nil {
		t.Fatal("module enqueuer is nil")
	}

	bundle := RouterBundle{
		Script: ScriptRouteDeps{
			Cfg:         cfg,
			SQLiteStore: p.SQLite,
			Enqueuer:    m.Enqueuer,
		},
		Metrics: MetricsRouteDeps{Registry: metrics.NewRegistry()},
	}

	router, err := newRouter(cfg, bundle, m.Registry)
	if err != nil {
		t.Fatalf("newRouter failed: %v", err)
	}
	routes := router.Routes()

	want := map[string]bool{
		"POST /api/v1/script/generate-with-images": false,
		"POST /api/v1/script/generate":             false,
		"POST /api/v1/script/jobs/:kind":           false,
		"GET /api/v1/workers":                      false,
		"GET /health":                              false,
	}
	for _, route := range routes {
		key := route.Method + " " + route.Path
		if key == "POST /api/v1/script/generate-from-clips" {
			t.Fatalf("legacy route must not be mounted: %s", key)
		}
		if _, ok := want[key]; ok {
			want[key] = true
		}
	}
	for key, found := range want {
		if !found {
			t.Fatalf("missing route %s", key)
		}
	}
}
