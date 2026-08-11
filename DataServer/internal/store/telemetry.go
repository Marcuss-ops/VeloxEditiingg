package store

import "database/sql"

// DBTelemetry is the narrow persistence-observability seam. The store owns
// when measurements are taken; the metrics package owns how they are exposed.
type DBTelemetry interface {
	ObserveDBTransaction(waitMS, transactionMS float64, busy, busyTimeout, retried bool, writeOps, readOps uint64)
	ObserveDBStats(stats sql.DBStats)
	RecordDBOperation(write bool)
	RecordDBRetry()
}

func (s *SQLiteStore) SetDBTelemetry(t DBTelemetry) {
	if s != nil {
		s.dbTelemetry = t
	}
}

func (s *SQLiteStore) observeDBOperation(write bool) {
	if s != nil && s.dbTelemetry != nil {
		s.dbTelemetry.RecordDBOperation(write)
		s.observeDBStats()
	}
}

func (s *SQLiteStore) observeDBTransaction(waitMS, transactionMS float64, busy, busyTimeout, retried bool, writeOps, readOps uint64) {
	if s != nil && s.dbTelemetry != nil {
		s.dbTelemetry.ObserveDBTransaction(waitMS, transactionMS, busy, busyTimeout, retried, writeOps, readOps)
		s.observeDBStats()
	}
}

func (s *SQLiteStore) observeDBStats() {
	if s == nil || s.db == nil || s.dbTelemetry == nil {
		return
	}
	s.dbTelemetry.ObserveDBStats(s.db.Stats())
}
