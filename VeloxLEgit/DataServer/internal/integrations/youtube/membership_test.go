package youtube

// membership_test.go used to host the 7 Membership / BulkMembership tests
// against a hand-rolled membershipStoreMock with 22 zero-return stubs.
// As of the S11 SQLite-fixture cutover, those tests have migrated to
// internal/store/sqlite_youtube_entities_test.go, where they exercise
// Service.Membership / Service.BulkMembership against a real
// *SQLiteStore fixture (built via youtube.ServiceWithStore). The legacy
// mock is gone with the file's body; this stub remains so any future
// per-handler test imports in package youtube keep resolving.
