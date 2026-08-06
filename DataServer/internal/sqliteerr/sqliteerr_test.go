package sqliteerr

import (
	"database/sql"
	"errors"
	"fmt"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func openTestDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite3", ":memory:")
	if err != nil {
		t.Fatalf("open in-memory db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

// execExpectError runs stmt and returns its error, failing the test if the
// statement unexpectedly succeeds.
func execExpectError(t *testing.T, db *sql.DB, stmt string, args ...any) error {
	t.Helper()
	_, err := db.Exec(stmt, args...)
	if err == nil {
		t.Fatalf("expected error from %q, got success", stmt)
	}
	return err
}

func TestIsUniqueConstraint_TypedExtendedCode(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE t (id TEXT PRIMARY KEY, k TEXT UNIQUE)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO t (id, k) VALUES ('a', 'k1')`); err != nil {
		t.Fatal(err)
	}

	// Raw driver error (no wrapping).
	raw := execExpectError(t, db, `INSERT INTO t (id, k) VALUES ('b', 'k1')`)
	if !IsUniqueConstraint(raw) {
		t.Fatalf("IsUniqueConstraint(raw) = false, want true (err=%v)", raw)
	}

	// Wrapped through %w — errors.As must still see the driver error.
	wrapped := fmt.Errorf("store.insert: %w", raw)
	if !IsUniqueConstraint(wrapped) {
		t.Fatalf("IsUniqueConstraint(wrapped) = false, want true (err=%v)", wrapped)
	}

	// Primary-key violation is also a uniqueness conflict.
	pk := execExpectError(t, db, `INSERT INTO t (id, k) VALUES ('a', 'k2')`)
	if !IsUniqueConstraint(pk) {
		t.Fatalf("IsUniqueConstraint(pk) = false, want true (err=%v)", pk)
	}
}

func TestIsUniqueConstraint_NonConstraintErrors(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER)`); err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		name  string
		stmt  string
		check func(error) bool
	}{
		{"no such column", `SELECT missing_col FROM t`, IsNoSuchColumn},
		{"no such table", `SELECT * FROM missing_table`, IsNoSuchTable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := execExpectError(t, db, tc.stmt)
			if IsUniqueConstraint(err) {
				t.Fatalf("IsUniqueConstraint(%q) = true, want false (err=%v)", tc.stmt, err)
			}
			if !tc.check(err) {
				t.Fatalf("%s(%q) = false, want true (err=%v)", tc.name, tc.stmt, err)
			}
		})
	}

	if IsUniqueConstraint(nil) {
		t.Fatal("IsUniqueConstraint(nil) = true, want false")
	}
	if IsUniqueConstraint(errors.New("some unrelated error")) {
		t.Fatal("IsUniqueConstraint(unrelated) = true, want false")
	}
}

func TestIsDuplicateColumn(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE t (a INTEGER)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`ALTER TABLE t ADD COLUMN a INTEGER`); err == nil {
		t.Fatal("expected duplicate-column error from ALTER TABLE")
	} else if !IsDuplicateColumn(err) {
		t.Fatalf("IsDuplicateColumn = false, want true (err=%v)", err)
	}

	wrapped := fmt.Errorf("migrations: %w", errors.New("no such column: b"))
	if IsDuplicateColumn(wrapped) {
		t.Fatal("IsDuplicateColumn(other schema error) = true, want false")
	}
}

func TestIsNoSuchColumn_WrappedAndNil(t *testing.T) {
	db := openTestDB(t)
	if _, err := db.Exec(`CREATE TABLE t (a INTEGER)`); err != nil {
		t.Fatal(err)
	}
	raw := execExpectError(t, db, `SELECT b FROM t`)
	wrapped := fmt.Errorf("supervisor labels query: %w", raw)
	if !IsNoSuchColumn(wrapped) {
		t.Fatalf("IsNoSuchColumn(wrapped) = false, want true (err=%v)", wrapped)
	}
	if IsNoSuchColumn(nil) {
		t.Fatal("IsNoSuchColumn(nil) = true, want false")
	}
	if IsNoSuchTable(nil) {
		t.Fatal("IsNoSuchTable(nil) = true, want false")
	}
	if IsNoSuchTable(errors.New("no such column: x")) {
		t.Fatal("IsNoSuchTable(no-such-column) = true, want false")
	}
}
