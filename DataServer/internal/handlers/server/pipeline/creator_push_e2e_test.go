// Package pipeline: creator_push_e2e_test.go exercises the full HTTP →
// creatorflow.Resolver → atomic Job+Task write path for the
// POST /api/v1/creator/jobs endpoint.
//
// creator_push_test.go (sibling) covers the pure normalization layer
// (creatorPushRequest → normalizedCreatorPush). This file is the
// integration counterpart: it wires a real SQLite store, a real Enqueuer
// + creatorflow.Resolver, and runs the handler through a real
// httptest.Recorder + gin.New engine mounted via h.RegisterRoutes.
//
// The auth middleware is bypassed via adminAuthFake because the auth
// chain has its own unit coverage in handlers/server/api; this file
// exercises the creator_push contract exclusively.
package pipeline

import (
	"bytes"
	"encoding/json"
	"github.com/gin-gonic/gin"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"velox-server/internal/config"
	"velox-server/internal/creatorflow"
	"velox-server/internal/jobs/enqueue"
	"velox-server/internal/store"
)

// adminAuthFake short-circuits the bearer-token check the production
// router applies to /api/v1/creator/jobs. The auth chain is unit-tested
// separately; this file exercises the creator_push contract exclusively.
func adminAuthFake(c *gin.Context) { c.Next() }

func newCreatorPushE2EStack(t *testing.T) (*Handlers, *store.SQLiteStore, *store.SQLiteJobRepository) {
	t.Helper()
	tempDir := t.TempDir()
	db, err := store.NewSQLiteStore(filepath.Join(tempDir, "velox.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	if _, err := db.DB().Exec(
		`INSERT INTO delivery_destinations (destination_id, provider, name, enabled, configuration_json, created_at, updated_at) VALUES ('drive', 'google_drive', 'Drive', 1, '{}', datetime('now'), datetime('now'))`,
	); err != nil {
		t.Fatalf("seed delivery_destinations: %v", err)
	}
	jobRepo := store.NewSQLiteJobRepository(db)
	atomic := store.NewAtomicJobTaskCreator(db)
	testEnqueuer := enqueue.NewEnqueuer(atomic, jobRepo, nil, noopPlanResolver{})
	resolver := creatorflow.NewResolverFromDeps(testEnqueuer, db.Forwarding(), db, tempDir, filepath.Join(tempDir, "videos"), "")
	h := NewHandlersWithResolver(&config.Config{}, testEnqueuer, nil, resolver, jobRepo, nil, nil).WithStore(db)
	return h, db, jobRepo
}

func creatorPushE2EBody(sourceProvider, sourceJobID, targetExecutor string) map[string]interface{} {
	return map[string]interface{}{
		"source_provider":    sourceProvider,
		"source_job_id":      sourceJobID,
		"target_executor_id": targetExecutor,
		"payload": map[string]interface{}{
			"status":      "completed",
			"job_id":      sourceJobID,
			"video_name":  "E2E voiceover+stock+clip+scene",
			"script_text": "Creator-supplied script body.",
			"voiceover_paths": []interface{}{
				"velox-asset://voiceovers/audio.mp3",
			},
			"scenes": []interface{}{
				map[string]interface{}{
					"text":             "Prima scena",
					"clip_link":        "velox-asset://clips/clip-01.mp4",
					"duration_seconds": 7,
				},
			},
			"delivery_plan": []interface{}{
				map[string]interface{}{
					"destination_id": "drive",
					"priority":       1,
					"retry_budget":   3,
				},
			},
		},
	}
}

func postCreatorPush(t *testing.T, r *gin.Engine, body any) *httptest.ResponseRecorder {
	t.Helper()
	raw, err := json.Marshal(body)
	if err != nil {
		t.Fatalf("marshal body: %v", err)
	}
	req := httptest.NewRequest(http.MethodPost, "/api/v1/creator/jobs", bytes.NewReader(raw))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer test-admin-token")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}
