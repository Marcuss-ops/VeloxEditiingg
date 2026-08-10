package deliveries

// Telemetry is the delivery runner's narrow operational-observability seam.
// Implementations must use bounded labels; delivery IDs and raw messages are
// intentionally absent from this interface.
type Telemetry interface {
	ObserveDelivery(provider string, queueMS, uploadMS, totalMS float64, status string)
	RecordDeliveryRetry(provider string)
	RecordDeliveryTimeout(provider string)
	RecordDeliveryUpload(provider string, bytes int64, mbps float64)
	RecordDeliveryProviderError(provider, code string)
}
