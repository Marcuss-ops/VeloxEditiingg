package store

// leaseConflictError is a typed persistence boundary marker. It deliberately
// has no dependency on supervisor or transport packages; leaf policy packages
// can recognize LeaseLost without importing store.
type leaseConflictError string

func (e leaseConflictError) Error() string   { return string(e) }
func (e leaseConflictError) LeaseLost() bool { return true }
