package worker

import "strings"

// rememberSourceLocator records the direct provider locator delivered by a
// trusted FutureAssetPlan. It contains no credentials and never performs I/O.
func (w *Worker) rememberSourceLocator(assetID, sourceURI string) {
	if w == nil || strings.TrimSpace(assetID) == "" || strings.TrimSpace(sourceURI) == "" {
		return
	}
	w.sourceLocatorMu.Lock()
	defer w.sourceLocatorMu.Unlock()
	if w.sourceLocators == nil {
		w.sourceLocators = make(map[string]string)
	}
	w.sourceLocators[strings.TrimSpace(assetID)] = strings.TrimSpace(sourceURI)
}

func (w *Worker) sourceLocator(assetID string) string {
	if w == nil {
		return ""
	}
	w.sourceLocatorMu.RLock()
	defer w.sourceLocatorMu.RUnlock()
	return w.sourceLocators[strings.TrimSpace(assetID)]
}
