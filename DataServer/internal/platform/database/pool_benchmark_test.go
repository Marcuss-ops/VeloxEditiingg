package database

import (
	"context"
	"database/sql"
	"fmt"
	"path/filepath"
	"sync"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

// pool_benchmark_test.go — A3-1 measurement harness.
//
// AGENTS.md and the 2026-09 audit both flag the 1-connection SQLite pool
// as the master's architectural throughput ceiling, and both demand
// MEASUREMENT before widening the default. This benchmark provides that
// measurement: it runs the same concurrent writer load (the shape of
// heartbeats + lease claims + attempt writes) against the same schema
// with pool sizes 1, 2, 4, 8 and reports ns/op per writer.
//
// Run with:
//
//	go test -bench=BenchmarkSQLiteConcurrentWrites -benchmem \
//	    -run=^$ ./internal/platform/database/
//
// INTERPRETATION GUIDE (read before touching sqliteDefaultMaxOpenConns):
//   - WAL mode allows ONE writer + N readers. Pool > 1 helps only when
//     the workload mixes reads into the writer storm (heartbeats do) or
//     when read queries would otherwise queue behind a long write.
//   - If ns/op is FLAT across pool sizes, the workload is pure-write and
//     the single-writer pool is already optimal — do not widen.
//   - If ns/op improves with pool size, check SQLITE_BUSY counters
//     (velox_db_busy_count) under load before shipping the change: more
//     connections means more lock contention, not less.

// poolBenchSetup opens a throwaway SQLite DB with the production DSN
// params (WAL, busy_timeout, NORMAL sync, FK on) and creates the minimal
// single-table schema the writers hammer.
func poolBenchSetup(b *testing.B) string {
	b.Helper()
	path := filepath.Join(b.TempDir(), "poolbench.db")
	db, err := sql.Open("sqlite3", ensureSQLiteDSN(path))
	if err != nil {
		b.Fatalf("open: %v", err)
	}
	defer db.Close()
	// DDL via one connection; pragmas come from the DSN.
	if _, err := db.Exec(`CREATE TABLE pool_bench (
		id INTEGER PRIMARY KEY,
		worker_id TEXT NOT NULL UNIQUE,
		sequence INTEGER NOT NULL,
		payload TEXT NOT NULL,
		updated_at TEXT NOT NULL
	)`); err != nil {
		b.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(`CREATE INDEX idx_pool_bench_worker ON pool_bench(worker_id)`); err != nil {
		b.Fatalf("create index: %v", err)
	}
	if _, err := db.Exec(`CREATE TABLE pool_bench_read (
		id INTEGER PRIMARY KEY,
		worker_id TEXT NOT NULL,
		status TEXT NOT NULL
	)`); err != nil {
		b.Fatalf("create read table: %v", err)
	}
	return path
}

// benchmarkConcurrentWriters drives `writers` goroutines, each performing
// b.N/writers iterations of: one UPSERT (heartbeat-shape write) + one
// bounded read (capacity-shape read) — the mixed profile the master's
// per-tick path actually produces.
func benchmarkConcurrentWriters(b *testing.B, poolSize, writers int) {
	b.Helper()
	path := poolBenchSetup(b)

	cfg := Config{
		Driver:       DriverSQLite,
		SQLitePath:   path,
		MaxOpenConns: poolSize,
		MaxIdleConns: poolSize,
	}
	h, err := Open(context.Background(), cfg)
	if err != nil {
		b.Fatalf("open pool: %v", err)
	}
	defer h.DB.Close()

	perWriter := b.N / writers
	if perWriter < 1 {
		perWriter = 1
	}

	b.ResetTimer()
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			workerID := fmt.Sprintf("bench-worker-%d", workerIdx)
			for i := 0; i < perWriter; i++ {
				// Write: heartbeat-shaped upsert.
				if _, err := h.DB.Exec(
					`INSERT INTO pool_bench (worker_id, sequence, payload, updated_at)
					 VALUES (?, ?, ?, strftime('%Y-%m-%dT%H:%M:%fZ','now'))
					 ON CONFLICT(worker_id) DO UPDATE SET sequence = excluded.sequence, updated_at = excluded.updated_at`,
					workerID, i, fmt.Sprintf("payload-%d-%d", workerIdx, i),
				); err != nil {
					b.Errorf("write: %v", err)
					return
				}
				// Read: capacity-shaped lookup (WAL readers shouldn't block).
				var n int
				if err := h.DB.QueryRow(
					`SELECT COUNT(*) FROM pool_bench_read WHERE worker_id = ?`, workerID,
				).Scan(&n); err != nil {
					b.Errorf("read: %v", err)
					return
				}
			}
		}(w)
	}
	wg.Wait()
}

func BenchmarkSQLiteConcurrentWriters_Pool1_W4(b *testing.B) { benchmarkConcurrentWriters(b, 1, 4) }
func BenchmarkSQLiteConcurrentWriters_Pool2_W4(b *testing.B) { benchmarkConcurrentWriters(b, 2, 4) }
func BenchmarkSQLiteConcurrentWriters_Pool4_W4(b *testing.B) { benchmarkConcurrentWriters(b, 4, 4) }
func BenchmarkSQLiteConcurrentWriters_Pool8_W4(b *testing.B) { benchmarkConcurrentWriters(b, 8, 4) }
func BenchmarkSQLiteConcurrentWriters_Pool1_W8(b *testing.B) { benchmarkConcurrentWriters(b, 1, 8) }
func BenchmarkSQLiteConcurrentWriters_Pool4_W8(b *testing.B) { benchmarkConcurrentWriters(b, 4, 8) }
