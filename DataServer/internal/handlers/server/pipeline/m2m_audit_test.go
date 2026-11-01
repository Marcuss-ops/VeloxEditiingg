package pipeline

import (
	"context"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"velox-server/internal/store"

	"github.com/gin-gonic/gin"
)

func TestAuditM2MRejectDoesNotWaitForSQLiteWriter(t *testing.T) {
	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "velox.db"))
	if err != nil {
		t.Fatalf("sqlite store: %v", err)
	}
	ctx := context.Background()
	lock, err := db.DB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatalf("begin writer transaction: %v", err)
	}
	defer lock.Rollback()
	if _, err := lock.ExecContext(ctx,
		`INSERT INTO m2m_api_keys (client_id, secret_hash) VALUES ('writer-lock', 'hash')`); err != nil {
		t.Fatalf("acquire sqlite writer lock: %v", err)
	}

	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("GET", "/api/v1/jobs/x", nil)
	started := time.Now()
	auditM2MReject(c, db, "writer-lock", "scope_missing:jobs.submit")
	if elapsed := time.Since(started); elapsed >= 500*time.Millisecond {
		t.Fatalf("rejection audit blocked response for %s while SQLite writer was held", elapsed)
	}
}
