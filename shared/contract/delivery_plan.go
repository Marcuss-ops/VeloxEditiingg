package contract

// DefaultDeliveryRetryBudget is the canonical fallback budget stamped on
// a delivery_plan entry when the caller does not supply one. It is owned
// by the shared contract so it can be referenced by the enqueue validator,
// the persistence layer, and the InstaEdit BFF without importing each
// other.
const DefaultDeliveryRetryBudget = 5
