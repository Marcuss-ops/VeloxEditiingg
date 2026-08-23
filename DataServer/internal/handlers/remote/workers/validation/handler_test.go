package validation

import (
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"velox-server/internal/store"
	"velox-server/internal/store/migrations"
)

type fakeValidationRepository struct {
	mu             sync.Mutex
	saveErr        error
	getErr         error
	listErr        error
	status         *ValidationStatus
	all            []map[string]any
	saved          *ValidationReport
	savedWorkerIDs map[string]struct{}
	saveCalls      int
	getCalls       []string
}

func (f *fakeValidationRepository) SaveValidation(report *ValidationReport) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.saveCalls++
	if f.saveErr != nil {
		return f.saveErr
	}
	copy := *report
	f.saved = &copy
	if f.savedWorkerIDs == nil {
		f.savedWorkerIDs = make(map[string]struct{})
	}
	f.savedWorkerIDs[report.WorkerID] = struct{}{}
	return nil
}

func (f *fakeValidationRepository) GetValidation(workerID string) (*ValidationStatus, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.getCalls = append(f.getCalls, workerID)
	if f.getErr != nil {
		return nil, f.getErr
	}
	if f.status == nil {
		return nil, nil
	}
	status := *f.status
	return &status, nil
}

func (f *fakeValidationRepository) GetAllValidations() ([]map[string]any, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.listErr != nil {
		return nil, f.listErr
	}
	return f.all, nil
}

func (f *fakeValidationRepository) callCounts() (saveCalls, getCalls int) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.saveCalls, len(f.getCalls)
}

func (f *fakeValidationRepository) savedIDs() map[string]struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()

	ids := make(map[string]struct{}, len(f.savedWorkerIDs))
	for workerID := range f.savedWorkerIDs {
		ids[workerID] = struct{}{}
	}
	return ids
}

func decodeResponse(t *testing.T, response *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var payload map[string]any
	require.NoError(t, json.Unmarshal(response.Body.Bytes(), &payload))
	return payload
}

func newValidationRequest(t *testing.T, method, routePath, requestPath string, body string, handler gin.HandlerFunc) *httptest.ResponseRecorder {
	t.Helper()
	router := gin.New()
	if method == http.MethodGet {
		router.GET(routePath, handler)
	} else {
		router.POST(routePath, handler)
	}

	req := httptest.NewRequest(method, requestPath, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	return recorder
}

func newValidationTestStore(t *testing.T) *store.SQLiteStore {
	t.Helper()

	db, err := store.NewSQLiteStoreFromPath(filepath.Join(t.TempDir(), "validation.db"), false)
	require.NoError(t, err)
	require.NoError(t, db.CreateValidationTableIfNotExists())
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func newMigratedValidationStore(t *testing.T) *store.SQLiteStore {
	t.Helper()

	db, err := store.NewSQLiteStore(filepath.Join(t.TempDir(), "validation.db"))
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, db.Close()) })
	return db
}

func validationColumns(t *testing.T, db *store.SQLiteStore) map[string]bool {
	t.Helper()

	rows, err := db.DB().Query(`PRAGMA table_info(worker_validations)`)
	require.NoError(t, err)
	t.Cleanup(func() { require.NoError(t, rows.Close()) })

	columns := make(map[string]bool)
	for rows.Next() {
		var cid, notNull, primaryKey int
		var name, columnType string
		var defaultValue sql.NullString
		require.NoError(t, rows.Scan(&cid, &name, &columnType, &notNull, &defaultValue, &primaryKey))
		columns[name] = true
	}
	require.NoError(t, rows.Err())
	return columns
}

func seedMigrationHistory(t *testing.T, db *store.SQLiteStore, through int) {
	t.Helper()

	_, err := db.DB().Exec(`CREATE TABLE schema_migrations (
		version INTEGER PRIMARY KEY,
		name TEXT NOT NULL,
		checksum TEXT NOT NULL,
		applied_at TEXT NOT NULL
	)`)
	require.NoError(t, err)

	entries, err := fs.ReadDir(migrations.SQLiteMigrationsFS(), "sqlite")
	require.NoError(t, err)
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sql") || strings.HasSuffix(entry.Name(), ".down.sql") {
			continue
		}
		var version int
		_, err := fmt.Sscanf(entry.Name(), "%d_", &version)
		require.NoError(t, err)
		if version > through {
			continue
		}
		contents, err := fs.ReadFile(migrations.SQLiteMigrationsFS(), "sqlite/"+entry.Name())
		require.NoError(t, err)
		checksum := fmt.Sprintf("%x", sha256.Sum256(contents))
		_, err = db.DB().Exec(`INSERT INTO schema_migrations (version, name, checksum, applied_at) VALUES (?, ?, ?, ?)`, version, entry.Name(), checksum, "2026-08-10T00:00:00Z")
		require.NoError(t, err)
	}
}
