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
	// ObserveDeliveryUploadBreakdown separates the network round-trip time
	// from the local disk read/buffer time inside one successful upload.
	// Providers that cannot measure the split leave both values at 0 and
	// the observation is a no-op.
	ObserveDeliveryUploadBreakdown(provider string, networkMS, localBufferMS float64)
}
