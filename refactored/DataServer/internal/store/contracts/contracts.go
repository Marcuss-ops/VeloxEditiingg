// Package contracts holds cross-backend test suites for the narrow repository
// interfaces defined in internal/store (spec §5). The pattern is:
//
//	func TestX_Contract(t *testing.T) {
//	    store.ArtifactRepositoryContract(t, store.NewSQLiteArtifactRepositoryContractFactory())
//	}
//
// The same suite can later be driven against a Postgres factory once §5b lands.
//
// Tests are intentionally placed in a sub-package so the production code in
// package store is exercised through the public interface — no internal access.
package contracts
