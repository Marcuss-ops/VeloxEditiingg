package worker

// asset_integrity.go — remembered self-verified asset integrity.
//
// The worker computes the SHA-256 of every asset while writing it to the
// persistent cache (see writeVeloxAssetToCache). The digest+size of the most
// recent successful download is remembered per asset so that later payloads
// which reference the asset with partial (or no) integrity metadata can still
// be served as verified cache hits — the canonical download manager upgrades
// its cache probe with the remembered values (masterAssetTransferer.Check).
//
// The record is in-memory only: after a restart the next partial-metadata
// access simply re-downloads once and re-verifies.

// assetIntegrityRecord is the self-verified digest+size of the most recent
// successful download of an asset on this worker.
type assetIntegrityRecord struct {
	SHA256    string
	SizeBytes int64
}

// rememberAssetIntegrity records the digest+size computed while writing an
// asset to the persistent cache, so later payloads that reference the asset
// without integrity metadata can still be served as verified hits.
func (w *Worker) rememberAssetIntegrity(assetID, sha256Value string, sizeBytes int64) {
	if w == nil || assetID == "" || sha256Value == "" || sizeBytes <= 0 {
		return
	}
	w.assetIntegrityMu.Lock()
	defer w.assetIntegrityMu.Unlock()
	if w.assetIntegrity == nil {
		w.assetIntegrity = make(map[string]assetIntegrityRecord)
	}
	w.assetIntegrity[assetID] = assetIntegrityRecord{SHA256: sha256Value, SizeBytes: sizeBytes}
}

// rememberedAssetIntegrity returns the last self-verified digest+size seen
// for an asset on this worker.
func (w *Worker) rememberedAssetIntegrity(assetID string) (assetIntegrityRecord, bool) {
	if w == nil {
		return assetIntegrityRecord{}, false
	}
	w.assetIntegrityMu.Lock()
	defer w.assetIntegrityMu.Unlock()
	record, ok := w.assetIntegrity[assetID]
	return record, ok
}
