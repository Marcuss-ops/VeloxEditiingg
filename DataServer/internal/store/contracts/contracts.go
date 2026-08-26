// Package contracts holds cross-backend test suites for the narrow repository
// interfaces defined in internal/repository (spec §5). The pattern is:
//
//	func TestX_Contract(t *testing.T) {
//	    ArtifactRepositoryContract(t, NewSQLiteArtifactRepositoryFactory)
//	}
//
// Tests are intentionally placed in a sub-package so the production code in
// package store is exercised through the public interface — no internal access.
package contracts
