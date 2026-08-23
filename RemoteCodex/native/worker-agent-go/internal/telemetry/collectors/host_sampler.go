// Package collectors provides host-level resource collectors.
package collectors

// SampledHost is the boot-time / one-shot host layer used by the worker
// registration and capability surfaces. It is refreshed independently from
// per-beat resource samples.
type SampledHost struct {
	RAMBytes      int64
	DiskFreeBytes int64
	HasGPU        bool
}

// SampleHost reads the host capability layer. RAM and disk values use the
// same memory/disk collectors as the runtime sampler; GPU visibility is
// delegated to the GPU collector.
func (s *Sampler) SampleHost() (*SampledHost, error) {
	out := &SampledHost{}

	mem, err := s.readProcMeminfo()
	if err == nil {
		out.RAMBytes = mem.total
	}

	free, err := s.statvfsFreeBytes()
	if err == nil {
		out.DiskFreeBytes = free
	}

	out.HasGPU = detectGPU()
	return out, nil
}
