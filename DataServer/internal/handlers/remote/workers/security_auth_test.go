package workers

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"

	"velox-server/internal/config"
	"velox-server/internal/store"
	workersreg "velox-server/internal/workers"
)

func TestSecurity_UpdateStateRejectsForgedWorkerID(t *testing.T) {
	gin.SetMode(gin.TestMode)

	db, err := store.NewSQLiteStore(t.TempDir() + "/worker-update-security.db")
	if err != nil {
		t.Fatalf("NewSQLiteStore: %v", err)
	}
	defer db.Close()

	reg := workersreg.New(db)
	tokenMgr := workersreg.NewTokenManager(db)
	tokenA := tokenMgr.GenerateToken("worker-a")
	cmdMgr := workersreg.NewCommandManager(db)
	h := NewWorkerUpdateHandler(
		&config.Config{},
		reg,
		cmdMgr,
		tokenMgr,
		t.TempDir(),
		nil,
	)

	r := gin.New()
	r.POST("/worker/update_state", h.UpdateStateHandler())
	req := httptest.NewRequest(http.MethodPost, "/worker/update_state", strings.NewReader(`{"worker_id":"worker-b","state":"UPDATE_APPLIED"}`))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+tokenA)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Fatalf("worker-a token claiming worker-b status=%d, want %d body=%s", w.Code, http.StatusUnauthorized, w.Body.String())
	}
	if pending := cmdMgr.GetPendingCommands("worker-b"); len(pending) != 0 {
		t.Fatalf("forged WorkerID created %d pending commands, want 0", len(pending))
	}
}
