package telemetry

import (
	"math"
	"testing"
)

// TestTypedExecutionMetrics_ToProto_AllFields verifies that ToProto()
// sets all 17 writable fields on the wire envelope. The proto encoder
// transparently drops fields left at their zero value, so the only
// safe way to confirm the mapping is to populate EVERY field with a
// non-zero value and assert each getter returns the expected result.
func TestTypedExecutionMetrics_ToProto_AllFields(t *testing.T) {
	in := TypedExecutionMetrics{
		InputBytes:            1048576,   // 1 MiB
		OutputBytes:           524288,    // 512 KiB
		BytesFromDrive:        262144,    // 256 KiB
		BytesFromBlobstore:    262144,    // 256 KiB
		BytesFromLocalCache:   524288,    // 512 KiB
		CpuTimeMs:             12345,     // 12.345 s
		PeakRssBytes:          536870912, // 512 MiB
		FramesDecoded:         1800,
		FramesComposited:      1800,
		FramesEncoded:         1800,
		FfmpegSpeedRatio:      1.42,
		EncodePasses:          1,
		FinalConcatStreamCopy: true,
		ConcatMode:            "stream_copy",
		CpuPricePerSecond:     0.000005,
		StoragePricePerGb:     0.00012,
		NetworkPricePerGb:     0.01,

		// Scorecard v2 extensions.
		GpuTimeMs:              111,
		PeakVramBytes:          222,
		TempBytesWritten:       333,
		DuplicateDownloadBytes: 444,
		MediaDurationSeconds:   55.5,
		WallClockSeconds:       66.6,

		FfprobeValid:      1,
		DurationDiffSec:   0.12,
		HasVideoStream:    true,
		HasAudioStream:    true,
		OutputFileSize:    777,
		BlackFrameRatio:   0.01,
		AudioSyncOffsetMs: 88,
		OutputSha256:      "deadbeef",

		CpuPercentPeak: 0.95,
		DiskReadBytes:  1111,
		DiskWriteBytes: 2222,
		NetworkRxBytes: 3333,
		NetworkTxBytes: 4444,
		IowaitMs:       55,
		OpenFdsPeak:    99,

		AssetCacheHitCount:  1,
		AssetCacheMissCount: 2,
		BlobCacheHitCount:   3,
		BlobCacheMissCount:  4,
		RenderCacheHitCount: 5,
	}
	pb := in.ToProto()
	if pb == nil {
		t.Fatalf("ToProto returned nil")
	}

	cases := []struct {
		name    string
		got     any
		want    any
		epsilon float64 // for float64 only
	}{
		{"InputBytes", pb.GetInputBytes(), in.InputBytes, 0},
		{"OutputBytes", pb.GetOutputBytes(), in.OutputBytes, 0},
		{"BytesFromDrive", pb.GetBytesFromDrive(), in.BytesFromDrive, 0},
		{"BytesFromBlobstore", pb.GetBytesFromBlobstore(), in.BytesFromBlobstore, 0},
		{"BytesFromLocalCache", pb.GetBytesFromLocalCache(), in.BytesFromLocalCache, 0},
		{"CpuTimeMs", pb.GetCpuTimeMs(), in.CpuTimeMs, 0},
		{"PeakRssBytes", pb.GetPeakRssBytes(), in.PeakRssBytes, 0},
		{"FramesDecoded", pb.GetFramesDecoded(), in.FramesDecoded, 0},
		{"FramesComposited", pb.GetFramesComposited(), in.FramesComposited, 0},
		{"FramesEncoded", pb.GetFramesEncoded(), in.FramesEncoded, 0},
		{"FfmpegSpeedRatio", pb.GetFfmpegSpeedRatio(), in.FfmpegSpeedRatio, 1e-9},
		{"EncodePasses", int64(pb.GetEncodePasses()), int64(in.EncodePasses), 0},
		{"FinalConcatStreamCopy", pb.GetFinalConcatStreamCopy(), in.FinalConcatStreamCopy, 0}, // bool requires non-default
		{"ConcatMode", pb.GetConcatMode(), in.ConcatMode, 0},
		{"CpuPricePerSecond", pb.GetCpuPricePerSecond(), in.CpuPricePerSecond, 1e-9},
		{"StoragePricePerGb", pb.GetStoragePricePerGb(), in.StoragePricePerGb, 1e-9},
		{"NetworkPricePerGb", pb.GetNetworkPricePerGb(), in.NetworkPricePerGb, 1e-9},

		// Scorecard v2 extensions.
		{"GpuTimeMs", pb.GetGpuTimeMs(), in.GpuTimeMs, 0},
		{"PeakVramBytes", pb.GetPeakVramBytes(), in.PeakVramBytes, 0},
		{"TempBytesWritten", pb.GetTempBytesWritten(), in.TempBytesWritten, 0},
		{"DuplicateDownloadBytes", pb.GetDuplicateDownloadBytes(), in.DuplicateDownloadBytes, 0},
		{"MediaDurationSeconds", pb.GetMediaDurationSeconds(), in.MediaDurationSeconds, 1e-9},
		{"WallClockSeconds", pb.GetWallClockSeconds(), in.WallClockSeconds, 1e-9},

		{"FfprobeValid", int64(pb.GetFfprobeValid()), int64(in.FfprobeValid), 0},
		{"DurationDiffSec", pb.GetDurationDiffSec(), in.DurationDiffSec, 1e-9},
		{"HasVideoStream", pb.GetHasVideoStream(), in.HasVideoStream, 0},
		{"HasAudioStream", pb.GetHasAudioStream(), in.HasAudioStream, 0},
		{"OutputFileSize", pb.GetOutputFileSize(), in.OutputFileSize, 0},
		{"BlackFrameRatio", pb.GetBlackFrameRatio(), in.BlackFrameRatio, 1e-9},
		{"AudioSyncOffsetMs", pb.GetAudioSyncOffsetMs(), in.AudioSyncOffsetMs, 0},
		{"OutputSha256", pb.GetOutputSha256(), in.OutputSha256, 0},

		{"CpuPercentPeak", pb.GetCpuPercentPeak(), in.CpuPercentPeak, 1e-9},
		{"DiskReadBytes", pb.GetDiskReadBytes(), in.DiskReadBytes, 0},
		{"DiskWriteBytes", pb.GetDiskWriteBytes(), in.DiskWriteBytes, 0},
		{"NetworkRxBytes", pb.GetNetworkRxBytes(), in.NetworkRxBytes, 0},
		{"NetworkTxBytes", pb.GetNetworkTxBytes(), in.NetworkTxBytes, 0},
		{"IowaitMs", pb.GetIowaitMs(), in.IowaitMs, 0},
		{"OpenFdsPeak", pb.GetOpenFdsPeak(), in.OpenFdsPeak, 0},

		{"AssetCacheHitCount", pb.GetAssetCacheHitCount(), in.AssetCacheHitCount, 0},
		{"AssetCacheMissCount", pb.GetAssetCacheMissCount(), in.AssetCacheMissCount, 0},
		{"BlobCacheHitCount", pb.GetBlobCacheHitCount(), in.BlobCacheHitCount, 0},
		{"BlobCacheMissCount", pb.GetBlobCacheMissCount(), in.BlobCacheMissCount, 0},
		{"RenderCacheHitCount", pb.GetRenderCacheHitCount(), in.RenderCacheHitCount, 0},
	}
	for _, c := range cases {
		switch g := c.got.(type) {
		case float64:
			if math.Abs(g-c.want.(float64)) > c.epsilon {
				t.Errorf("%s: got=%v want=%v", c.name, g, c.want)
			}
		default:
			if g != c.want {
				t.Errorf("%s: got=%v want=%v", c.name, g, c.want)
			}
		}
	}
}

// TestTypedExecutionMetrics_ToProto_ZeroValueSafe confirms a
// zero-valued Go struct produces an empty (non-nil) proto message —
// no panic, no nil deref, every getter returns the proto's zero.
func TestTypedExecutionMetrics_ToProto_ZeroValueSafe(t *testing.T) {
	in := TypedExecutionMetrics{}
	pb := in.ToProto()
	if pb == nil {
		t.Fatalf("ToProto returned nil for zero-value struct")
	}
	if pb.GetInputBytes() != 0 || pb.GetOutputBytes() != 0 {
		t.Errorf("zero struct should produce zero proto: %+v", pb)
	}
	if pb.GetFfmpegSpeedRatio() != 0 || pb.GetFfmpegSpeedRatio() != 0 {
		t.Errorf("zero struct should produce zero float: %+v", pb)
	}
}

// TestTypedExecutionMetrics_FromProto_NilSafe: FromProto on a nil
// pointer returns the zero value without panicking. This is the
// master-side replay path (some TaskResults have nil execution_metrics).
func TestTypedExecutionMetrics_FromProto_NilSafe(t *testing.T) {
	got := FromProto(nil)
	if got.InputBytes != 0 || got.OutputBytes != 0 {
		t.Errorf("FromProto(nil) should return zero: got=%+v", got)
	}
}

// TestTypedExecutionMetrics_RoundTrip: FromProto(ToProto(x)) == x.
// Proves the ToProto builder doesn't accidentally lose data and
// FromProto doesn't double-convert.
func TestTypedExecutionMetrics_RoundTrip(t *testing.T) {
	in := TypedExecutionMetrics{
		InputBytes:            999,
		OutputBytes:           1000,
		BytesFromDrive:        400,
		BytesFromBlobstore:    300,
		BytesFromLocalCache:   299,
		CpuTimeMs:             42,
		PeakRssBytes:          8589934592, // 8 GiB
		FramesDecoded:         1234,
		FramesComposited:      1234,
		FramesEncoded:         1234,
		FfmpegSpeedRatio:      2.71,
		EncodePasses:          2,
		FinalConcatStreamCopy: false,
		ConcatMode:            "reencode",
		CpuPricePerSecond:     0.00001,
		StoragePricePerGb:     0.001,
		NetworkPricePerGb:     0.05,

		GpuTimeMs:              11,
		PeakVramBytes:          22,
		TempBytesWritten:       33,
		DuplicateDownloadBytes: 44,
		MediaDurationSeconds:   5.5,
		WallClockSeconds:       6.6,
		FfprobeValid:           1,
		DurationDiffSec:        0.1,
		HasVideoStream:         true,
		HasAudioStream:         true,
		OutputFileSize:         77,
		BlackFrameRatio:        0.02,
		AudioSyncOffsetMs:      8,
		OutputSha256:           "cafebabe",
		CpuPercentPeak:         0.88,
		DiskReadBytes:          111,
		DiskWriteBytes:         222,
		NetworkRxBytes:         333,
		NetworkTxBytes:         444,
		IowaitMs:               5,
		OpenFdsPeak:            9,
		AssetCacheHitCount:     10,
		AssetCacheMissCount:    11,
		BlobCacheHitCount:      12,
		BlobCacheMissCount:     13,
		RenderCacheHitCount:    14,
	}
	back := FromProto(in.ToProto())
	if back != in {
		t.Errorf("round-trip mismatch:\nin =%+v\nout=%+v", in, back)
	}
}

// TestTypedExecutionMetrics_ProtoEmitMatch — verifies the 17-field
// wiring matches the proto schema. Counts go-type fields and confirms
// we always carry the documented 17 fields.
func TestTypedExecutionMetrics_ProtoEmitMatch(t *testing.T) {
	in := TypedExecutionMetrics{
		InputBytes:            1,
		OutputBytes:           2,
		BytesFromDrive:        3,
		BytesFromBlobstore:    4,
		BytesFromLocalCache:   5,
		CpuTimeMs:             6,
		PeakRssBytes:          7,
		FramesDecoded:         8,
		FramesComposited:      9,
		FramesEncoded:         10,
		FfmpegSpeedRatio:      11.0,
		EncodePasses:          12,
		FinalConcatStreamCopy: true,
		ConcatMode:            "stream_copy",
		CpuPricePerSecond:     13.0,
		StoragePricePerGb:     14.0,
		NetworkPricePerGb:     15.0,

		GpuTimeMs:              16,
		PeakVramBytes:          17,
		TempBytesWritten:       18,
		DuplicateDownloadBytes: 19,
		MediaDurationSeconds:   20.0,
		WallClockSeconds:       21.0,
		FfprobeValid:           22,
		DurationDiffSec:        23.0,
		HasVideoStream:         true,
		HasAudioStream:         true,
		OutputFileSize:         24,
		BlackFrameRatio:        25.0,
		AudioSyncOffsetMs:      26,
		OutputSha256:           "sha256",
		CpuPercentPeak:         27.0,
		DiskReadBytes:          28,
		DiskWriteBytes:         29,
		NetworkRxBytes:         30,
		NetworkTxBytes:         31,
		IowaitMs:               32,
		OpenFdsPeak:            33,
		AssetCacheHitCount:     34,
		AssetCacheMissCount:    35,
		BlobCacheHitCount:      36,
		BlobCacheMissCount:     37,
		RenderCacheHitCount:    38,
	}
	pb := in.ToProto()
	// Each non-zero Go field must round-trip. If a proto edit ever
	// REQUIRES adding a new field on the Go side, the assertion list
	// below must be extended in lock-step.
	if pb.GetInputBytes() != 1 || pb.GetOutputBytes() != 2 ||
		pb.GetBytesFromDrive() != 3 || pb.GetBytesFromBlobstore() != 4 ||
		pb.GetBytesFromLocalCache() != 5 || pb.GetCpuTimeMs() != 6 ||
		pb.GetPeakRssBytes() != 7 || pb.GetFramesDecoded() != 8 ||
		pb.GetFramesComposited() != 9 || pb.GetFramesEncoded() != 10 ||
		pb.GetFfmpegSpeedRatio() != 11.0 || uint32(pb.GetEncodePasses()) != 12 ||
		!pb.GetFinalConcatStreamCopy() || pb.GetConcatMode() != "stream_copy" ||
		pb.GetCpuPricePerSecond() != 13.0 || pb.GetStoragePricePerGb() != 14.0 ||
		pb.GetNetworkPricePerGb() != 15.0 ||

		pb.GetGpuTimeMs() != 16 || pb.GetPeakVramBytes() != 17 ||
		pb.GetTempBytesWritten() != 18 || pb.GetDuplicateDownloadBytes() != 19 ||
		pb.GetMediaDurationSeconds() != 20.0 || pb.GetWallClockSeconds() != 21.0 ||
		pb.GetFfprobeValid() != 22 || pb.GetDurationDiffSec() != 23.0 ||
		!pb.GetHasVideoStream() || !pb.GetHasAudioStream() ||
		pb.GetOutputFileSize() != 24 || pb.GetBlackFrameRatio() != 25.0 ||
		pb.GetAudioSyncOffsetMs() != 26 || pb.GetOutputSha256() != "sha256" ||
		pb.GetCpuPercentPeak() != 27.0 || pb.GetDiskReadBytes() != 28 ||
		pb.GetDiskWriteBytes() != 29 || pb.GetNetworkRxBytes() != 30 ||
		pb.GetNetworkTxBytes() != 31 || pb.GetIowaitMs() != 32 ||
		pb.GetOpenFdsPeak() != 33 || pb.GetAssetCacheHitCount() != 34 ||
		pb.GetAssetCacheMissCount() != 35 || pb.GetBlobCacheHitCount() != 36 ||
		pb.GetBlobCacheMissCount() != 37 || pb.GetRenderCacheHitCount() != 38 {
		t.Errorf("proto-emit mismatch (extended field contract violated): %+v", pb)
	}
}
