package contract

import (
	"fmt"
	"math"
	"math/big"
	"sort"
	"strconv"
	"strings"
)

// VideoSourceCertificateV1 records reusable media facts from a fully verified
// source. A sparse file cannot produce this proof: SHA256 identifies every byte.
// Keyframes are microseconds from the source presentation origin, not timeline
// placement. The native mux still checks the requested packet boundary.
type VideoSourceCertificateV1 struct {
	Chunks                        *VideoSourceChunksV1        `json:"chunks,omitempty"`
	Version                       int                         `json:"version"`
	SHA256                        string                      `json:"sha256"`
	SizeBytes                     int64                       `json:"size_bytes"`
	ProfileID                     string                      `json:"profile_id"`
	PreparationRevision           string                      `json:"preparation_revision"`
	Evidence                      VideoWindowOutputEvidenceV1 `json:"evidence"`
	SourceTimestampOriginTicks    int64                       `json:"source_timestamp_origin_ticks,omitempty"`
	SourceTimestampOriginVerified bool                        `json:"source_timestamp_origin_verified,omitempty"`
	AverageFPSNum                 int                         `json:"average_fps_num,omitempty"`
	AverageFPSDen                 int                         `json:"average_fps_den,omitempty"`
	DurationUS                    int64                       `json:"duration_us"`
	KeyframesUS                   []int64                     `json:"keyframes_us"`
}

func (c VideoSourceCertificateV1) Validate() error {
	p, err := KnownCanonicalVideoProfileV1(c.ProfileID)
	if err != nil {
		return err
	}
	revision, err := VideoWindowPreparationRevision(p, CanonicalPreparationQualityPolicyDefault)
	if err != nil {
		return err
	}
	if c.Chunks != nil {
		if err := c.Chunks.Validate(c.SizeBytes); err != nil {
			return err
		}
	}
	e := c.Evidence
	level, err := strconv.Atoi(strings.ReplaceAll(p.CodecLevel, ".", ""))
	if err != nil || c.Version != 1 || !isLowerSHA256(c.SHA256) || c.SizeBytes <= 0 || c.PreparationRevision != revision ||
		e.Codec != p.Codec || e.Profile != strings.ToLower(p.CodecProfile) || e.Level != level || e.Width != p.Width || e.Height != p.Height ||
		e.PixelFormat != p.PixelFormat || e.FPSNum != p.FPSNum || e.FPSDen != p.FPSDen || e.TimeBaseNum <= 0 || e.TimeBaseDen <= 0 ||
		e.BFrames != 0 || e.FrameCount <= 0 || !e.FirstFrameKeyframe || e.ExtradataSHA256 != CanonicalVideoExtradataSHA256 || !p.ClosedGOP {
		return fmt.Errorf("video source certificate: incomplete or incompatible canonical evidence")
	}
	if e.FrameCount > math.MaxInt64/int64(p.FPSDen)/1_000_000 {
		return fmt.Errorf("video source certificate: duration overflow")
	}
	want := e.FrameCount * int64(p.FPSDen) * 1_000_000 / int64(p.FPSNum)
	if c.DurationUS != want || len(c.KeyframesUS) == 0 || len(c.KeyframesUS) > 8192 || c.KeyframesUS[0] != 0 {
		return fmt.Errorf("video source certificate: invalid duration or keyframe index")
	}
	if c.SourceTimestampOriginTicks != 0 || c.SourceTimestampOriginVerified || c.AverageFPSNum != 0 || c.AverageFPSDen != 0 {
		averageLeft := new(big.Int).Mul(big.NewInt(int64(c.AverageFPSNum)), big.NewInt(int64(e.FPSDen)))
		averageRight := new(big.Int).Mul(big.NewInt(int64(e.FPSNum)), big.NewInt(int64(c.AverageFPSDen)))
		if !c.SourceTimestampOriginVerified || c.SourceTimestampOriginTicks != 0 || c.AverageFPSNum <= 0 || c.AverageFPSDen <= 0 || averageLeft.Cmp(averageRight) != 0 {
			return fmt.Errorf("video source certificate: invalid rational timestamp-origin or frame-rate evidence")
		}
	}
	for i, v := range c.KeyframesUS {
		if v < 0 || v >= c.DurationUS || (i > 0 && c.KeyframesUS[i-1] >= v) {
			return fmt.Errorf("video source certificate: invalid keyframe order or bounds")
		}
		frame, _, ok := c.keyframeFrameForUS(v)
		if !ok || frame >= e.FrameCount {
			return fmt.Errorf("video source certificate: keyframe is not on a canonical frame")
		}

	}
	return nil
}

// HasKeyframe permits the one-microsecond rounding difference between frame
// coordinates and timestamp coordinates. The mux still validates the packets.
func (c VideoSourceCertificateV1) HasKeyframe(atUS int64) bool {
	if atUS < 0 {
		return false
	}
	i := sort.Search(len(c.KeyframesUS), func(i int) bool { return c.KeyframesUS[i] >= atUS-1 })
	return i < len(c.KeyframesUS) && c.KeyframesUS[i]-atUS <= 1
}

// NextKeyframeFrame chooses the first certified boundary at or after a consumed
// source frame. Rational arithmetic preserves fractional-rate frame positions.
func (c VideoSourceCertificateV1) NextKeyframeFrame(startFrame int64) (int64, int64, bool) {
	if c.Evidence.FPSNum <= 0 || c.Evidence.FPSDen <= 0 {
		return 0, 0, false
	}
	for _, us := range c.KeyframesUS {
		frame, _, ok := c.keyframeFrameForUS(us)
		if !ok {
			return 0, 0, false
		}
		if frame >= startFrame && frame < c.Evidence.FrameCount {
			return frame, us, true
		}
	}
	return 0, 0, false
}

func (c VideoSourceCertificateV1) keyframeFrameForUS(us int64) (int64, int64, bool) {
	if us < 0 || c.Evidence.FPSNum <= 0 || c.Evidence.FPSDen <= 0 {
		return 0, 0, false
	}
	scale := new(big.Int).Mul(big.NewInt(int64(c.Evidence.FPSDen)), big.NewInt(1000000))
	timestamp := new(big.Int).Mul(big.NewInt(us), big.NewInt(int64(c.Evidence.FPSNum)))
	frame := new(big.Int).Add(timestamp, new(big.Int).Div(new(big.Int).Set(scale), big.NewInt(2)))
	frame.Div(frame, scale)
	delta := new(big.Int).Sub(timestamp, new(big.Int).Mul(frame, scale))
	delta.Abs(delta)
	if !frame.IsInt64() || delta.Cmp(big.NewInt(int64(c.Evidence.FPSNum))) > 0 {
		return 0, 0, false
	}
	return frame.Int64(), us, true
}
