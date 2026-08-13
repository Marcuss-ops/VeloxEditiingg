package store

import (
	"velox-server/internal/renderfingerprintstore"
)

// SaveRenderFingerprint is re-exported from the renderfingerprintstore leaf
// package, which owns the SQLite persistence for render fingerprints. The
// store package keeps this binding so existing in-package callers and tests
// keep compiling unchanged while the leaf depends on storecore, never on store.
var SaveRenderFingerprint = renderfingerprintstore.SaveRenderFingerprint
