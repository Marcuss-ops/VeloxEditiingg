package store

// store_infra_errors.go keeps the historical helper name used by existing
// store methods. New code should call wrapDBInfrastructure from db_errors.go
// directly; both paths share the same DomainError-preserving behavior.

// wrapInfrastructureError is retained for existing callers while the store
// package migrates to the common database-boundary helper.
func wrapInfrastructureError(operation string, err error) error {
	return wrapDBInfrastructure(operation, err)
}
