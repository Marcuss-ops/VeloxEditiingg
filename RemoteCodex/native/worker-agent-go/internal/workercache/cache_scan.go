package workercache

import (
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"velox-shared/assetref"
)

func columnExistsTx(tx *sql.Tx, table, column string) (bool, error) {
	rows, err := tx.Query(`PRAGMA table_info(` + quoteSQLiteIdentifier(table) + `)`)
	if err != nil {
		return false, err
	}
	defer rows.Close()
	for rows.Next() {
		var cid int
		var name, typ string
		var notNull, primaryKey int
		var defaultValue sql.NullString
		if err := rows.Scan(&cid, &name, &typ, &notNull, &defaultValue, &primaryKey); err != nil {
			return false, err
		}
		if name == column {
			return true, nil
		}
	}
	return false, rows.Err()
}

func tableExistsTx(tx *sql.Tx, name string) (bool, error) {
	var exists int
	err := tx.QueryRow(`SELECT COUNT(1) FROM sqlite_master WHERE type = 'table' AND name = ?`, name).Scan(&exists)
	return exists != 0, err
}

// scanDBI lets scanEntry work for both *sql.Row and *sql.Rows.
type scanDBI interface {
	Scan(...interface{}) error
}

func scanEntry(r scanDBI) (*Entry, error) {
	var (
		e                Entry
		assetKey         string
		storedHash       string
		dlInt            int
		createdS         string
		usedS            string
		leaseCount       int
		leaseJob         string
		reservationCount int
	)
	err := r.Scan(
		&assetKey, &storedHash, &e.LocalPath, &e.SizeBytes,
		&dlInt, &createdS, &usedS, &leaseCount, &leaseJob, &reservationCount,
	)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, ErrNotFound
		}
		return nil, fmt.Errorf("workercache.scanEntry: %w", err)
	}
	e.AssetKey = assetref.AssetKey(assetKey)
	e.ContentHash = assetref.ContentHash(displayContentHash(storedHash))
	e.storedContentHash = storedHash
	e.ActiveLeaseCount = leaseCount
	e.ActiveJobID = leaseJob
	e.ActiveReservationCount = reservationCount
	e.DownloadComplete = dlInt != 0
	if e.CreatedAt, err = parseRFC3339Nano(createdS); err != nil {
		return nil, fmt.Errorf("workercache.scanEntry: created_at: %w", err)
	}
	if e.LastUsedAt, err = parseRFC3339Nano(usedS); err != nil {
		return nil, fmt.Errorf("workercache.scanEntry: last_used_at: %w", err)
	}
	return &e, nil
}

// mustHaveAffected returns ErrNotFound if the result affected zero rows.
func mustHaveAffected(res sql.Result, assetKey, op string) error {
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("workercache.%s(%q): rows affected: %w", op, assetKey, err)
	}
	if n == 0 {
		return fmt.Errorf("%w: asset_key=%s", ErrNotFound, assetKey)
	}
	return nil
}

func parseRFC3339Nano(s string) (time.Time, error) {
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Parse(time.RFC3339, s)
}

func isUniqueConflict(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	for _, frag := range []string{"UNIQUE constraint failed", "constraint failed"} {
		if containsCI(msg, frag) {
			return true
		}
	}
	return false
}

func containsCI(haystack, needle string) bool {
	if needle == "" {
		return true
	}
	if len(haystack) < len(needle) {
		return false
	}
	h := []byte(haystack)
	n := []byte(needle)
	for i := 0; i+len(n) <= len(h); i++ {
		match := true
		for j := 0; j < len(n); j++ {
			hh := h[i+j]
			if hh >= 'A' && hh <= 'Z' {
				hh += 32
			}
			nn := n[j]
			if nn >= 'A' && nn <= 'Z' {
				nn += 32
			}
			if hh != nn {
				match = false
				break
			}
		}
		if match {
			return true
		}
	}
	return false
}
