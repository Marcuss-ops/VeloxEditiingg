package deliverycontract

import "testing"

func TestStatusFromExternalMapsToTypedDeliveryStatus(t *testing.T) {
	cases := []struct {
		external string
		want     DeliveryStatus
	}{
		{"published", DeliverySucceeded},
		{"publication_completed", DeliverySucceeded},
		{"completed", DeliverySucceeded},
		{"COMPLETED", DeliverySucceeded},
		{"  Published  ", DeliverySucceeded},
		{"failed", DeliveryFailed},
		{"dead_letter", DeliveryFailed},
		{"blocked_auth", DeliveryBlockedAuth},
		{"retry_wait", DeliveryRetryWait},
		{"rate_limited", DeliveryRetryWait},
		{"cancel_requested", DeliveryCancelled},
		{"cancelled", DeliveryCancelled},
		{"  processing  ", DeliveryRunning},
		{"", DeliveryRunning},
		{"unexpected_observation", DeliveryRunning},
	}
	for _, tc := range cases {
		if got := StatusFromExternal(tc.external); got != tc.want {
			t.Errorf("StatusFromExternal(%q) = %q, want %q", tc.external, got, tc.want)
		}
	}
}
