package artifactsstore

import "testing"

func TestExpectedAudioStreamsDoesNotScaleWithDeliveryFanout(t *testing.T) {
	for _, tc := range []struct {
		destinations int
		want         int
	}{
		{destinations: 0, want: 0},
		{destinations: 1, want: 1},
		{destinations: 5, want: 1},
	} {
		if got := expectedAudioStreamsForDeliveries(tc.destinations); got != tc.want {
			t.Fatalf("destinations=%d: expected audio streams=%d, want %d", tc.destinations, got, tc.want)
		}
	}
}
