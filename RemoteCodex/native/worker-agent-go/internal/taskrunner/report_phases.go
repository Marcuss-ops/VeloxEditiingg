// report_phases.go — transport mapping for detailed phase events.
//
// TaskExecutionReport.DetailedPhases is the worker-side, telemetry-owned
// shape; this file owns the ONLY conversion onto the wire:
//
//	DetailedPhaseTiming → pb.PhaseTimingDetailed (field 20 of TaskResult)
//
// submitTaskResult calls ToProto on every entry; the master projects the
// repeated field into task_execution_events and derives the
// task_phase_timings summary rows.
package taskrunner

import (
	pb "velox-shared/controltransport/pb"

	"google.golang.org/protobuf/types/known/timestamppb"
)

// ToProto converts the detailed phase onto the wire message. Wall stamps
// are passed through as-is (UTC); duration is authoritative for graphing.
func (d DetailedPhaseTiming) ToProto() *pb.PhaseTimingDetailed {
	var startedAt, completedAt *timestamppb.Timestamp
	if !d.StartedAt.IsZero() {
		startedAt = timestamppb.New(d.StartedAt)
	}
	if !d.CompletedAt.IsZero() {
		completedAt = timestamppb.New(d.CompletedAt)
	}
	return &pb.PhaseTimingDetailed{
		PhaseOrder:      int32(d.PhaseOrder),
		Component:       d.Component,
		Action:          d.Action,
		StartedAt:       startedAt,
		CompletedAt:     completedAt,
		DurationMs:      d.DurationMS,
		Status:          d.Status,
		ErrorCode:       d.ErrorCode,
		ErrorMessage:    d.ErrorMessage,
		BytesIn:         d.BytesIn,
		BytesOut:        d.BytesOut,
		Frames:          d.Frames,
		MetadataJson:    d.MetadataJSON,
		Origin:          d.Origin,
		Scope:           d.Scope,
		EventType:       d.EventType,
		EventName:       d.EventName,
		EventIndex:      d.EventIndex,
		Phase:           d.Phase,
		ExecutorId:      d.ExecutorID,
		ExecutorVersion: d.ExecutorVersion,
		LeaseId:         d.LeaseID,

		SegmentIndex:     d.SegmentIndex,
		TrackKind:        d.TrackKind,
		TrackIndex:       d.TrackIndex,
		StartedOffsetMs:  d.StartedOffsetMS,
		FinishedOffsetMs: d.FinishedOffsetMS,
		CpuMs:            d.CPUMS,
		QueueWaitMs:      d.QueueWaitMS, FramesIn: d.FramesIn,
		FramesOut:              d.FramesOut,
		TelemetrySchemaVersion: d.TelemetrySchemaVersion,
	}
}
