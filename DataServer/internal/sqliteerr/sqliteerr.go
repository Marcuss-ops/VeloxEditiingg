// Package sqliteerr is the single canonical classification point for
// SQLite driver errors on the master side. Store adapters, the migration
// runner and the metrics supervisor must NOT inspect driver error text
// inline: every driver-level decision routes through this package so the
// mapping (typed extended code where the driver exposes one, message match
// only where SQLite leaves no typed code) lives in exactly one place with
// tests.
//
// Classification contract:
//
//	IsUniqueConstraint — typed via sqlite3.Error.ExtendedCode, no message
//	                    parsing. Preferred pattern (see enqueue).
//	IsNoSuchColumn / IsNoSuchTable / IsDuplicateColumn — SQLite reports these
//	                    schema-level conditions as plain SQLITE_ERROR (no
//	                    dedicated extended code), so message matching is the
//	                    only driver-observable signal. The match is confined
//	                    to this adapter boundary and never leaks into domain
//	                    logic.
package sqliteerr

import (
	"errors"
	"strings"

	"github.com/mattn/go-sqlite3"
)

// IsUniqueConstraint reports whether err is a SQLite UNIQUE or PRIMARY KEY
// constraint violation. It uses the driver's typed extended error code, so
// no error-message parsing is involved.
func IsUniqueConstraint(err error) bool {
	if err == nil {
		return false
	}
	var e sqlite3.Error
	if errors.As(err, &e) {
		return isConstraintCode(e.ExtendedCode)
	}
	var ep *sqlite3.Error
	if errors.As(err, &ep) && ep != nil {
		return isConstraintCode(ep.ExtendedCode)
	}
	return false
}

// IsBusy reports transient SQLite writer contention. Callers may retry the
// complete transaction when this is true; the driver exposes both BUSY and
// LOCKED as typed primary result codes.
func IsBusy(err error) bool {
	if err == nil {
		return false
	}
	var e sqlite3.Error
	if errors.As(err, &e) {
		return e.Code == sqlite3.ErrBusy || e.Code == sqlite3.ErrLocked
	}
	var ep *sqlite3.Error
	if errors.As(err, &ep) && ep != nil {
		return ep.Code == sqlite3.ErrBusy || ep.Code == sqlite3.ErrLocked
	}
	return false
}

func isConstraintCode(code sqlite3.ErrNoExtended) bool {
	return code == sqlite3.ErrConstraintUnique || code == sqlite3.ErrConstraintPrimaryKey
}

// IsNoSuchColumn reports whether err is a SQLite "no such column" schema
// error (plain SQLITE_ERROR; message-matched at this adapter boundary).
func IsNoSuchColumn(err error) bool {
	return messageMatches(err, "no such column")
}

// IsNoSuchTable reports whether err is a SQLite "no such table" schema error
// (plain SQLITE_ERROR; message-matched at this adapter boundary).
func IsNoSuchTable(err error) bool {
	return messageMatches(err, "no such table")
}

// IsDuplicateColumn reports whether err is a SQLite "duplicate column name"
// ALTER TABLE error (plain SQLITE_ERROR; message-matched at this adapter
// boundary).
func IsDuplicateColumn(err error) bool {
	return messageMatches(err, "duplicate column")
}

// messageMatches lower-cases the full unwrap chain and checks for the
// substring. This is intentionally private: driver text inspection must stay
// inside this package.
func messageMatches(err error, needle string) bool {
	if err == nil {
		return false
	}
	return strings.Contains(strings.ToLower(err.Error()), needle)
}
