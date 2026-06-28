// resource_host.go — wire-shape adapter for SampledResources.
//
// PR-3.6 hello/health wire envelope: convert a SampledResources
// snapshot into the keys the master expects on `Heartbeat.resources`.
// The mapping is keyed by `proto.WorkerResourceCounters` field name
// (snake_case) so the existing F2 decodeWorkerResources helper on the
// master side reads the keys directly via proto reflection.

package telemetry

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ToWireMap flattens a SampledResources snapshot into a
// map suitable for Heartbeat.Extra (structpb.Struct) via sendHeartbeat.
//
// Field names are snake_case to match the proto so the master's
// decodeWorkerResources helper can map straight onto WorkerResourceCounters.
//
// All zero-value fields are omitted to keep the heartbeat payload
// tight on idle/idle-busy workers (heartbeats are sent at 15s busy,
// 60s idle); metrics layer derives "not present" as "stale baseline".
func (s *SampledResources) ToWireMap() map[string]interface{} {
	if s == nil {
		return nil
	}
	out := make(map[string]interface{}, 22)
	addFloat := func(k string, v float64) {
		if v == 0 {
			return
		}
		out[k] = v
	}
	addI64 := func(k string, v int64) {
		if v == 0 {
			return
		}
		out[k] = v
	}
	addI32 := func(k string, v int32) {
		if v == 0 {
			return
		}
		out[k] = v
	}
	addFloat("cpu_utilization_ratio", s.CPUUtilRatio)
	addFloat("cpu_iowait_ratio", s.CPUIOWaitRatio)
	addFloat("cpu_steal_ratio", s.CPUStealRatio)
	addI64("memory_used_bytes", s.MemoryUsedBytes)
	addI64("memory_available_bytes", s.MemoryAvailableBytes)
	addI64("process_rss_bytes", s.ProcessRSSBytes)
	addI64("process_rss_peak_bytes", s.ProcessRSSPeakBytes)
	addI64("swap_used_bytes", s.SwapUsedBytes)
	addI64("major_page_faults_total", s.MajorPageFaultsTotal)
	addI64("disk_read_bytes_total", s.DiskReadBytesTotal)
	addI64("disk_write_bytes_total", s.DiskWriteBytesTotal)
	addI64("disk_free_bytes", s.DiskFreeBytes)
	addI64("temp_bytes_written", s.TempBytesWritten)
	addI32("temp_files_open", s.TempFilesOpen)
	addI64("network_receive_bytes_total", s.NetworkReceiveBytesTotal)
	addI64("network_transmit_bytes_total", s.NetworkTransmitBytesTotal)
	addI64("network_retransmits_total", s.NetworkRetransmitsTotal)
	addI32("active_tasks", s.ActiveTasks)
	addI32("task_slots", s.TaskSlots)
	addFloat("load1", s.Load1)
	addI32("run_queue", s.RunQueue)
	if !s.SampledAt.IsZero() {
		out["sampled_at"] = s.SampledAt.UTC().Format(time.RFC3339Nano)
	}
	return out
}

// TimestampProto is a helper for callers that want to inject the
// sampled_at as a protobuf Timestamp instead of an RFC3339 string.
// Not used by the map emission today; exposed for future direct-typed
// emit paths (PR-4 if we drop JSON map encoding on the worker).
func (s *SampledResources) TimestampProto() *timestamppb.Timestamp {
	if s == nil || s.SampledAt.IsZero() {
		return nil
	}
	return timestamppb.New(s.SampledAt)
}
