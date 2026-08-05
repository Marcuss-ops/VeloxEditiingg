package artifacts

// Finalization delivery SQL is implemented by store.SQLiteArtifactFinalizer.
// Delivery plan resolution remains supplied through the existing resolver
// dependency and is consumed inside the store transaction.
