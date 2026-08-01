// Package metrics / collector_video.go
//
// Video encode-amplification families, sliced out of collector.go so
// the Collector struct definition stays focused on registration.
// Recorded by RecordAttempt in collector_attempts.go.
package metrics

// initVideoFamilies creates the video encode-amplification counter
// families. Called once from NewCollector at boot.
func (c *Collector) initVideoFamilies() {
	c.videoEncodePasses = NewCounterFamily("velox_video_encode_passes_total",
		"Encode passes performed", []string{"executor_id"})
	c.videoFramesEnc = NewCounterFamily("velox_video_frames_encoded_total",
		"Frames encoded (sum across passes)", []string{"executor_id"})
	c.videoOutputFrames = NewCounterFamily("velox_video_output_frames_total",
		"Output frames published (lower-bound dedup)", []string{"executor_id"})
	c.videoStreamCopy = NewCounterFamily("velox_video_stream_copy_operations_total",
		"Stream-copy concat operations (cheap path)", []string{})
	c.videoReencode = NewCounterFamily("velox_video_reencode_operations_total",
		"Reencode concat operations (expensive path)", []string{"reason"})
}

// videoFamilies returns the video subset registered by NewCollector
// via allFamilies.
func (c *Collector) videoFamilies() []*Family {
	return []*Family{
		c.videoEncodePasses, c.videoFramesEnc, c.videoOutputFrames,
		c.videoStreamCopy, c.videoReencode,
	}
}
